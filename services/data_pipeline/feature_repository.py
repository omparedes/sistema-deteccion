"""HU03 PostgreSQL: mismo motor puro, rol explicito, transaccion e idempotencia."""
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .adapters import file_sha256
from .database_check import _error_code
from .feature_run import FEATURE_CONTRACT, validate_matrix
from .features import (PARTITIONS, SCHEMA_VERSION, VERSION, FeatureError, WindowPolicy, digest,
                       extract_features, iso, read_schedules, timestamp, validate_window)
from .normalized_contract import CONTRACT_PATH, validate_normalized
from .operational import validate_identity
from .repository import RepositoryError

READ_NORMALIZED = """
SELECT n.*,a.actor_code,s.source_code,r.external_event_id,r.record_number,
       r.processing_details,b.file_sha256,b.directory_sha256
FROM public.normalized_events n JOIN public.actors a ON a.id=n.actor_id
JOIN public.raw_events r ON r.id=n.raw_event_id
JOIN public.data_sources s ON s.id=r.source_id
JOIN public.ingest_batches b ON b.id=r.batch_id
ORDER BY n.id
"""


def activate_featurizer(connection):
    connection.execute("RESET ROLE")
    validate_identity(connection)
    connection.execute("SET LOCAL ROLE pipeline_featurizer")
    row = connection.execute("SELECT current_user,session_user,rolsuper,rolbypassrls,rolcreatedb,rolcreaterole "
                             "FROM pg_roles WHERE rolname=current_user").fetchone()
    if row[:2] != ("pipeline_featurizer", "pipeline_runner") or any(row[2:]):
        raise RepositoryError("FEATURE_ROLE_NOT_MINIMAL")


def project_event(row, directory_sha):
    traces = [e for e in row["processing_details"]
              if isinstance(e, dict) and e.get("normalization_version", "").startswith("hu02-v")]
    reference = f"csv-sha256:{row['file_sha256']}:{row['record_number']}"
    if (not traces or traces[-1]["directory_sha256"] != directory_sha
            or row["directory_sha256"] != directory_sha or traces[-1]["raw_reference"] != reference):
        raise RepositoryError("FEATURE_NORMALIZATION_CONTEXT_MISMATCH")
    event = {
        "event_id": str(row["id"]), "source": row["source_code"],
        "source_event_id": row["external_event_id"], "timestamp": iso(row["occurred_at"]),
        "user_id": row["actor_code"], "operation": row["operation"],
        "file_size_bytes": row["file_size_bytes"], "is_synthetic": row["is_synthetic"],
        "device_id": row["device_code"], "file_extension": row["file_extension"],
        "source_type": row["source_type"], "source_path": row["source_path"],
        "destination_path": row["destination_path"], "destination_type": row["destination_type"],
        "destination_authorized": row["destination_authorized"],
        "data_classification": row["data_classification"], "operation_result": row["operation_result"],
        "raw_event_reference": reference,
    }
    # El DTO HU02 no inventa columnas opcionales ausentes; preservar la misma proyeccion.
    if row["session_code"] is not None:
        event["session_id"] = row["session_code"]
    if row["file_type"] is not None:
        event["file_type"] = row["file_type"]
    return validate_normalized(event)


def domain_payload(window):
    validate_window(window)
    return {"vector": window["features"], "metadata": window["metadata"]}


class FeatureRepository:
    def __init__(self, settings, directory):
        if settings.sslmode != "verify-full":
            raise RepositoryError("FEATURE_VERIFY_FULL_REQUIRED")
        self.settings, self.directory_path = settings, Path(directory)
        self.schedules, self.directory_sha = read_schedules(self.directory_path)
        self.context = {"normalization_version": "hu02-v1", "directory_sha256": self.directory_sha,
                        "normalized_contract_sha256": file_sha256(CONTRACT_PATH),
                        "feature_schema_version": SCHEMA_VERSION,
                        "feature_contract_sha256": file_sha256(FEATURE_CONTRACT)}

    def connect(self):
        return psycopg.connect(**self.settings.connection_options(), autocommit=True,
                               prepare_threshold=None)

    def calculate(self, connection, batch_size=500, policy=None, actor_codes=None):
        activate_featurizer(connection)
        scoped = policy is not None or actor_codes is not None
        policy = policy or WindowPolicy()
        actors = {code: id for id, code in connection.execute(
            "SELECT id,actor_code FROM public.actors").fetchall()}
        if set(actors) != set(self.schedules):
            raise RepositoryError("FEATURE_ACTOR_DIRECTORY_MISMATCH")
        if actor_codes is not None:
            if (not isinstance(actor_codes, (tuple, list)) or not actor_codes
                    or any(not isinstance(code, str) for code in actor_codes)
                    or len(set(actor_codes)) != len(actor_codes)
                    or not set(actor_codes) <= set(actors)):
                raise RepositoryError("INVALID_FEATURE_ACTOR_SCOPE")
            actors = {code: actors[code] for code in sorted(actor_codes)}
        source_snapshot = hashlib.sha256()
        def events():
            with connection.cursor(name="hu03_normalized_stream", row_factory=dict_row) as reader:
                if scoped:
                    query = READ_NORMALIZED.replace("ORDER BY n.id", "WHERE n.actor_id=ANY(%s::uuid[]) "
                        "AND n.occurred_at >= %s AND n.occurred_at < %s ORDER BY n.id")
                    reader.execute(query, (list(actors.values()), policy.start, policy.end))
                else:
                    reader.execute(READ_NORMALIZED)
                while rows := reader.fetchmany(batch_size):
                    for row in rows:
                        event = project_event(row, self.directory_sha)
                        if scoped and (event["user_id"] not in actors
                                or not policy.start <= timestamp(event["timestamp"]) < policy.end):
                            raise RepositoryError("FEATURE_READER_SCOPE_MISMATCH")
                        source_snapshot.update((digest(event) + "\n").encode())
                        yield event
        windows, counts = extract_features(events(), self.schedules, self.context, policy)
        validate_matrix(windows)
        if file_sha256(self.directory_path) != self.directory_sha:
            raise RepositoryError("FEATURE_DIRECTORY_CHANGED")
        return windows, counts, actors, source_snapshot.hexdigest()

    def persist(self, connection, windows, actors, snapshot, batch_size=500,
                allow_recalculate=False, fail_after=None, progress=None, coverage=None):
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size debe ser positivo")
        activate_featurizer(connection)
        if len({w["pipeline_version"] for w in windows}) > 1:
            raise RepositoryError("MIXED_FEATURE_CONTEXTS")
        existing = {}
        version = windows[0]["pipeline_version"] if windows else (VERSION + ":" + digest(self.context) if coverage else None)
        if version:
            rows = connection.execute("SELECT id,actor_id,window_start,window_end,features FROM public.feature_windows "
                                      "WHERE pipeline_version=%s", (version,)).fetchall()
            existing = {str(row[0]): row for row in rows}
        incoming_ids = {w["window_id"] for w in windows}
        # Una lista seleccionada no representa una cobertura exhaustiva. Solo
        # el recalculo de una cobertura explicita puede identificar obsoletas.
        if coverage is not None:
            actor_ids = set(actors.values())
            if any(not coverage.start <= timestamp(w["window_start"]) < timestamp(w["window_end"]) <= coverage.end
                   for w in windows):
                raise RepositoryError("FEATURE_WINDOW_OUTSIDE_RECALCULATION_COVERAGE")
            # Un solapamiento parcial no recalcula toda la ventana anterior.
            if any(id not in incoming_ids and row[1] in actor_ids
                   and coverage.start <= row[2] and row[3] <= coverage.end
                   for id, row in existing.items()):
                raise RepositoryError("OBSOLETE_FEATURE_WINDOWS_REQUIRE_REVIEW")
        inserted = updated = unchanged = 0
        for offset in range(0, len(windows), batch_size):
            inserts, updates = [], []
            for window in windows[offset:offset + batch_size]:
                payload = domain_payload(window)
                id, actor = window["window_id"], actors[window["user_id"]]
                previous = existing.get(id)
                if previous:
                    if previous[1:4] != (actor, timestamp(window["window_start"]), timestamp(window["window_end"])):
                        raise RepositoryError("FEATURE_IDENTITY_CHANGED")
                    old = previous[4]
                    if {"vector": old["vector"], "metadata": old["metadata"]} == payload:
                        unchanged += 1
                        continue
                    if not allow_recalculate:
                        raise RepositoryError("FEATURE_RECALCULATION_REQUIRES_EXPLICIT_OPTION")
                    if old["metadata"]["input_fingerprint_sha256"] == payload["metadata"]["input_fingerprint_sha256"]:
                        raise RepositoryError("FEATURE_VALUES_CHANGED_WITHOUT_INPUT_CHANGE")
                    audit = dict(old["audit"])
                    audit["history"] = list(audit.get("history", [])) + [{
                        "revision": audit["revision"], "input_fingerprint_sha256": old["metadata"]["input_fingerprint_sha256"],
                        "payload_sha256": digest(old), "reason": "RECALCULATED_SAME_CONTEXT",
                    }]
                    audit["revision"] += 1
                    audit["last_changed_at"] = datetime.now(timezone.utc).isoformat()
                    audit["source_snapshot_sha256"] = snapshot
                    updates.append((Jsonb(dict(payload, audit=audit)), id))
                else:
                    moment = datetime.now(timezone.utc).isoformat()
                    audit = {"created_at": moment, "last_changed_at": moment,
                             "revision": 1, "source_snapshot_sha256": snapshot}
                    inserts.append((id, actor, timestamp(window["window_start"]), timestamp(window["window_end"]),
                                    Jsonb(dict(payload, audit=audit)), window["pipeline_version"]))
            with connection.cursor() as writer:
                if inserts:
                    writer.executemany("INSERT INTO public.feature_windows(id,actor_id,window_start,window_end,features,pipeline_version,is_synthetic) "
                                       "VALUES (%s,%s,%s,%s,%s,%s,true)", inserts)
                if updates:
                    writer.executemany("UPDATE public.feature_windows SET features=%s WHERE id=%s", updates)
            inserted += len(inserts); updated += len(updates)
            if fail_after is not None and inserted + updated >= fail_after:
                raise RepositoryError("INJECTED_FEATURE_FAILURE")
            if progress:
                progress(min(offset + batch_size, len(windows)), inserted, updated)
        return {"inserted": inserted, "updated": updated, "already_existing": unchanged,
                "errors": 0, "generated_nonempty": len(windows)}

    def process(self, connection, batch_size=500, policy=None, reference=None,
                allow_recalculate=False, fail_after=None, progress=None, actor_codes=None):
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size debe ser positivo")
        activate_featurizer(connection)
        revision_function = connection.execute(
            "SELECT to_regprocedure('public.has_normalization_revisions()')::text").fetchone()[0]
        if isinstance(revision_function, str) and connection.execute(
                "SELECT public.has_normalization_revisions()").fetchone()[0] is True:
            raise RepositoryError("EXPLICIT_REVISION_RECALCULATION_REQUIRED")
        connection.execute("SET LOCAL statement_timeout='60s'")
        connection.execute("SET LOCAL lock_timeout='10s'")
        # Comparte puerta de normalizacion para no leer un contexto a medio actualizar.
        connection.execute("SELECT pg_advisory_xact_lock(hashtext('hu02-normalization-v1'))")
        connection.execute("SELECT pg_advisory_xact_lock(hashtext('hu03-features-v1'))")
        windows, counts, actors, snapshot = self.calculate(connection, batch_size, policy, actor_codes)
        if reference is not None:
            with Path(reference).open(encoding="utf-8") as stream:
                expected = {}
                for line in stream:
                    item = json.loads(line)
                    item["metadata"].pop("matrix_row", None)
                    expected[item["window_id"]] = item
            if expected != {w["window_id"]: w for w in windows}:
                raise RepositoryError("POSTGRES_FEATURES_DIFFER_FROM_OFFLINE_REFERENCE")
            if policy is None and counts.get("received_valid", 0) != sum(
                    w["features"]["operation_count"] for w in expected.values()):
                raise RepositoryError("FEATURE_SOURCE_COUNT_MISMATCH")
        result = self.persist(connection, windows, actors, snapshot, batch_size,
                              allow_recalculate, fail_after, progress, coverage=policy or WindowPolicy())
        result.update(events_read=counts.get("received_valid", 0), events_used=counts.get("events_used", 0),
                      complete_eligible=sum(w["metadata"]["model_eligible"] for w in windows),
                      provisional=sum(not w["metadata"]["model_eligible"] for w in windows),
                      snapshot_sha256=snapshot, offline_reference_matches=reference is not None,
                      pipeline_version=windows[0]["pipeline_version"] if windows else None)
        policy = policy or WindowPolicy()
        slots = policy.slots_by_period()
        result["partitions"] = {
            period: {"events": sum(w["features"]["operation_count"] for w in windows if w["metadata"]["partition"] == period),
                     "windows": sum(w["metadata"]["partition"] == period for w in windows),
                     "eligible": sum(w["metadata"]["partition"] == period and w["metadata"]["model_eligible"] for w in windows)}
            for period, _, _ in PARTITIONS}
        result["empty_windows_omitted"] = sum(slots.values()) * len(actors) - len(windows)
        return result

    def run(self, batch_size=500, policy=None, reference=None, allow_recalculate=False, progress=None, actor_codes=None):
        try:
            with self.connect() as connection:
                connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
                with connection.transaction():
                    result = self.process(connection, batch_size, policy, reference, allow_recalculate,
                                          progress=progress, actor_codes=actor_codes)
                return dict(result, commit=True)
        except psycopg.Error as error:
            raise RepositoryError(_error_code(error)) from None
