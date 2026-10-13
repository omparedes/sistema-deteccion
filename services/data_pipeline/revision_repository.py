"""P18/P15 transaction boundaries; callable only after migration 0009 is authorized."""
import copy
import uuid
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .adapters import file_sha256
from .database_check import _error_code
from .feature_repository import FeatureRepository, activate_featurizer, project_event
from .features import WindowPolicy, digest, extract_features, timestamp
from .normalization_repository import activate_normalizer
from .repository import RepositoryError
from .revisions import ACTIVE_INPUT_POLICY, prepare_revision


READ_CURRENT = """
SELECT r.*,s.source_code,b.directory_sha256,b.file_sha256,
       to_jsonb(n) AS sql_snapshot,a.actor_code
FROM public.raw_events r JOIN public.data_sources s ON s.id=r.source_id
JOIN public.ingest_batches b ON b.id=r.batch_id
LEFT JOIN public.normalized_events n ON n.raw_event_id=r.id
LEFT JOIN public.actors a ON a.id=n.actor_id WHERE r.id=%s
"""


class RevisionRepository:
    def __init__(self, settings, directory):
        self.features = FeatureRepository(settings, directory)

    def connect(self):
        return self.features.connect()

    @staticmethod
    def lock(connection):
        connection.execute("SET LOCAL statement_timeout='60s'")
        connection.execute("SET LOCAL lock_timeout='10s'")
        connection.execute("SELECT pg_advisory_xact_lock(hashtext('hu02-normalization-v1'))")
        connection.execute("SELECT pg_advisory_xact_lock(hashtext('hu03-features-v1'))")

    def current(self, connection, raw_id):
        activate_normalizer(connection)
        with connection.cursor(row_factory=dict_row) as reader:
            reader.execute(READ_CURRENT, (raw_id,)); row = reader.fetchone()
        if row is None:
            raise RepositoryError('REVISION_RAW_NOT_VISIBLE')
        if row['directory_sha256'] != self.features.directory_sha:
            raise RepositoryError('REVISION_DIRECTORY_CONTEXT_MISMATCH')
        materialization = row['sql_snapshot']
        event = None
        if materialization:
            projected = dict(materialization, actor_code=row['actor_code'],
                             source_code=row['source_code'], external_event_id=row['external_event_id'],
                             record_number=row['record_number'], file_sha256=row['file_sha256'],
                             directory_sha256=row['directory_sha256'], processing_details=row['processing_details'])
            projected['occurred_at'] = timestamp(projected['occurred_at'])
            event = project_event(projected, self.features.directory_sha)
        return dict(raw_id=str(row['id']), status=row['processing_status'],
                    known_users=sorted(self.features.schedules),
                    revision=row['normalization_revision'], rules_version=row['normalization_rules_version'],
                    history=row['normalization_versions'], pending_scopes=row['pending_feature_scopes'],
                    source=row['source_code'], source_event_id=row['external_event_id'],
                    raw_reference=f"csv-sha256:{row['file_sha256']}:{row['record_number']}",
                    duplicate_of=str(row['duplicate_of']) if row['duplicate_of'] else None,
                    directory_sha256=row['directory_sha256'], normalized_event=event,
                    sql_snapshot=materialization, processing_details=row['processing_details'],
                    processing_error=row['processing_error'], payload_sha256=row['payload_sha256'])

    def revise(self, connection, raw_id, request, fail_after_prepare=False, fail_after_write=False):
        """Caller transaction required. Function revalidates current version under lock."""
        self.lock(connection)
        current = self.current(connection, raw_id)
        prepared = prepare_revision(current, request, datetime.now(timezone.utc).isoformat())
        if fail_after_prepare:
            raise RepositoryError('INJECTED_REVISION_PREPARATION_FAILURE')
        # Send the ORIGINAL request fingerprint, not a changed retry DTO.
        result = connection.execute('SELECT public.revise_normalization(%s,%s::jsonb,%s::jsonb)',
            (raw_id, Jsonb(request), Jsonb(prepared))).fetchone()[0]
        if fail_after_write:
            raise RepositoryError('INJECTED_REVISION_WRITE_FAILURE')
        return result

    def calculate_scope(self, connection, user, policy, batch_size=500):
        if type(batch_size) is not int or batch_size < 1 or user not in self.features.schedules:
            raise RepositoryError('INVALID_RECALCULATION_SCOPE')
        activate_featurizer(connection)
        actors = {code:id for id,code in connection.execute('SELECT id,actor_code FROM public.actors').fetchall()}
        if set(actors) != set(self.features.schedules):
            raise RepositoryError('FEATURE_ACTOR_DIRECTORY_MISMATCH')
        inputs = {}
        query = """SELECT n.*,a.actor_code,s.source_code,r.external_event_id,r.record_number,
            r.processing_details,r.normalization_revision,r.normalization_rules_version,
            b.file_sha256,b.directory_sha256,r.id AS input_raw_id
            FROM public.normalized_events n JOIN public.actors a ON a.id=n.actor_id
            JOIN public.raw_events r ON r.id=n.raw_event_id
            JOIN public.data_sources s ON s.id=r.source_id
            JOIN public.ingest_batches b ON b.id=r.batch_id
            WHERE n.actor_id=%s AND n.occurred_at >= %s AND n.occurred_at < %s
            AND r.processing_status='processed' AND r.duplicate_of IS NULL ORDER BY n.id"""
        def events():
            with connection.cursor(name='revised_feature_scope', row_factory=dict_row) as reader:
                reader.execute(query, (actors[user], policy.start, policy.end))
                while rows := reader.fetchmany(batch_size):
                    for row in rows:
                        event = project_event(row, self.features.directory_sha)
                        if event['user_id'] != user or not policy.start <= timestamp(event['timestamp']) < policy.end:
                            raise RepositoryError('FEATURE_READER_SCOPE_MISMATCH')
                        inputs[event['event_id']] = dict(raw_id=str(row['input_raw_id']),
                            event_id=event['event_id'], revision=row['normalization_revision'],
                            rules_version=row['normalization_rules_version'], event_sha256=digest(event),
                            timestamp=event['timestamp'])
                        yield event
        context = dict(self.features.context, normalization_version=ACTIVE_INPUT_POLICY)
        windows, counts = extract_features(events(), self.features.schedules, context, policy)
        for window in windows:
            start, end = timestamp(window['window_start']), timestamp(window['window_end'])
            window['metadata']['normalization_inputs'] = sorted([
                {k:v for k,v in item.items() if k != 'timestamp'} for item in inputs.values()
                if start <= timestamp(item['timestamp']) < end], key=lambda item:item['raw_id'])
        if file_sha256(self.features.directory_path) != self.features.directory_sha:
            raise RepositoryError('FEATURE_DIRECTORY_CHANGED')
        return windows, counts

    def recalculate(self, connection, user, policy, request_id, batch_size=500, fail_after_write=False):
        uuid.UUID(request_id)
        self.lock(connection)
        windows, counts = self.calculate_scope(connection, user, policy, batch_size)
        result = connection.execute('SELECT public.reconcile_feature_scope(%s,%s,%s,%s,%s::jsonb)',
            (user, policy.start, policy.end, request_id, Jsonb(windows))).fetchone()[0]
        if fail_after_write:
            raise RepositoryError('INJECTED_RECALCULATION_FAILURE')
        return dict(result, events_used=counts.get('events_used', 0))

    def pending(self, connection, raw_id):
        return copy.deepcopy(self.current(connection, raw_id)['pending_scopes'])

    def run_revision(self, raw_id, request):
        return self._run(lambda c: self.revise(c, raw_id, request))

    def run_recalculation(self, user, policy, request_id, batch_size=500):
        return self._run(lambda c: self.recalculate(c, user, policy, request_id, batch_size))

    def _run(self, action):
        try:
            with self.connect() as connection:
                connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
                with connection.transaction():
                    result = action(connection)
                return dict(result, commit=True)
        except psycopg.Error as error:
            raise RepositoryError(_error_code(error)) from None
