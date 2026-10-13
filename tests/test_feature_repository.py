"""Persistencia HU03: pruebas offline y reales opt-in, siempre fixtures rollback."""
import copy
import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import psycopg
from psycopg.types.json import Jsonb
from pglast import parse_sql, parse_plpgsql

from services.data_pipeline.config import load_settings
from services.data_pipeline.features import extract_features, digest, timestamp, WindowPolicy
from services.data_pipeline.feature_repository import FeatureRepository, activate_featurizer, domain_payload, READ_NORMALIZED
from services.data_pipeline.repository import RepositoryError
from test_features import oracle_cases, SCHEDULES, event

ROOT = Path(__file__).resolve().parents[1]


class FeatureCalculationScopeTest(unittest.TestCase):
    def setup_reader(self, events):
        repo = FeatureRepository.__new__(FeatureRepository)
        repo.context = {"directory_sha256": "fixture"}
        repo.directory_path = Path("fixture-directory.csv")
        repo.directory_sha = "fixture"
        repo.schedules = SCHEDULES
        actors = {code: uuid.uuid5(uuid.NAMESPACE_URL, code) for code in SCHEDULES}
        c = Mock()
        c.execute.return_value.fetchall.return_value = [(id, code) for code, id in actors.items()]
        reader = Mock()
        reader.fetchmany.side_effect = [events, []]
        c.cursor.return_value.__enter__ = Mock(return_value=reader)
        c.cursor.return_value.__exit__ = Mock(return_value=False)
        return repo, c, reader, actors

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    @patch("services.data_pipeline.feature_repository.file_sha256", return_value="fixture")
    @patch("services.data_pipeline.feature_repository.project_event", side_effect=lambda row, sha: row)
    def test_actor_and_half_open_coverage_are_parameterized(self, project, sha, activate):
        repo, c, reader, actors = self.setup_reader(oracle_cases()[0][1])
        policy = WindowPolicy(timestamp("2026-09-02T14:00:00Z"), timestamp("2026-09-02T15:00:00Z"))
        windows, counts, selected, snapshot = repo.calculate(c, 2, policy, ["EMP001"])
        self.assertEqual(selected, {"EMP001": actors["EMP001"]})
        self.assertEqual(counts["received_valid"], 3)
        self.assertEqual(windows[0]["features"]["total_bytes"], 3584)
        sql, parameters = reader.execute.call_args.args
        self.assertIn("n.actor_id=ANY(%s::uuid[])", sql)
        self.assertIn("n.occurred_at >= %s AND n.occurred_at < %s", sql)
        self.assertEqual(parameters, ([actors["EMP001"]], policy.start, policy.end))
        self.assertEqual(len(snapshot), 64)

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    @patch("services.data_pipeline.feature_repository.file_sha256", return_value="fixture")
    @patch("services.data_pipeline.feature_repository.project_event", side_effect=lambda row, sha: row)
    def test_global_default_query_and_values_are_unchanged(self, project, sha, activate):
        repo, c, reader, actors = self.setup_reader(oracle_cases()[0][1])
        windows, counts, selected, snapshot = repo.calculate(c)
        reader.execute.assert_called_once_with(READ_NORMALIZED)
        self.assertEqual(selected, actors)
        expected = extract_features(oracle_cases()[0][1], SCHEDULES, repo.context)[0]
        self.assertEqual(windows, expected)

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_invalid_actor_scope_fails_before_reading(self, activate):
        for codes in ([], "EMP001", ["EMP999"], ["EMP001", "EMP001"], [None], [[]]):
            with self.subTest(codes=codes):
                repo, c, reader, actors = self.setup_reader([])
                with self.assertRaisesRegex(RepositoryError, "INVALID_FEATURE_ACTOR_SCOPE"):
                    repo.calculate(c, actor_codes=codes)
                reader.execute.assert_not_called()

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    @patch("services.data_pipeline.feature_repository.file_sha256", return_value="fixture")
    @patch("services.data_pipeline.feature_repository.project_event", side_effect=lambda row, sha: row)
    def test_reader_cannot_return_another_actor_or_exclusive_end(self, project, sha, activate):
        policy = WindowPolicy(timestamp("2026-09-02T14:00:00Z"), timestamp("2026-09-02T15:00:00Z"))
        for record in (event("other-actor", "2026-09-02T14:10:00Z", 100, user="EMP002"),
                       event("exclusive-end", "2026-09-02T15:00:00Z", 100)):
            repo, c, reader, actors = self.setup_reader([record])
            with self.assertRaisesRegex(RepositoryError, "FEATURE_READER_SCOPE_MISMATCH"):
                repo.calculate(c, policy=policy, actor_codes=["EMP001"])

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    @patch("services.data_pipeline.feature_repository.file_sha256", return_value="fixture")
    @patch("services.data_pipeline.feature_repository.project_event", side_effect=lambda row, sha: row)
    def test_actor_input_order_does_not_change_scope_ids_or_fingerprint(self, project, sha, activate):
        results = []
        for codes in (["EMP001", "EMP002"], ["EMP002", "EMP001"]):
            repo, c, reader, actors = self.setup_reader(oracle_cases()[0][1])
            results.append(repo.calculate(c, actor_codes=codes))
        self.assertEqual(results[0], results[1])

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    @patch("services.data_pipeline.feature_repository.file_sha256", return_value="fixture")
    @patch("services.data_pipeline.feature_repository.project_event", side_effect=lambda row, sha: row)
    def test_empty_scoped_read_produces_no_invented_window(self, project, sha, activate):
        repo, c, reader, actors = self.setup_reader([])
        windows, counts, selected, snapshot = repo.calculate(c, actor_codes=["EMP001"])
        self.assertEqual(windows, [])
        self.assertEqual(counts.get("events_used", 0), 0)
        self.assertEqual(list(selected), ["EMP001"])


class FeatureRepositoryOfflineTest(unittest.TestCase):
    def fixture(self):
        repo = FeatureRepository.__new__(FeatureRepository)
        repo.context = {"directory_sha256": "fixture"}
        w = extract_features(oracle_cases()[0][1], SCHEDULES, repo.context)[0][0]
        actors = {w["user_id"]: uuid.uuid4()}
        stored = (w["window_id"], actors[w["user_id"]], timestamp(w["window_start"]),
                  timestamp(w["window_end"]), dict(domain_payload(w),
                  audit={"revision": 1, "created_at": "2026-10-12T00:00:00Z",
                         "last_changed_at": "2026-10-12T00:00:00Z",
                         "source_snapshot_sha256": "a"*64}))
        c = Mock()
        c.execute.return_value.fetchall.return_value = [stored]
        c.execute.return_value.fetchone.return_value = [None]  # Schema before SQL0009.
        c.cursor.return_value.__enter__ = Mock(return_value=Mock())
        c.cursor.return_value.__exit__ = Mock(return_value=False)
        return repo, w, actors, stored, c

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_scoped_process_is_idempotent_and_reports_only_selected_population(self, activate):
        repo, w, actors, stored, c = self.fixture()
        policy = WindowPolicy(start=stored[2], end=stored[3])
        repo.calculate = Mock(return_value=([w], {"received_valid": 3, "events_used": 3}, actors, "a"*64))
        first = repo.process(c, policy=policy, actor_codes=["EMP001"])
        second = repo.process(c, policy=policy, actor_codes=["EMP001"])
        self.assertEqual(first, second)
        self.assertEqual((first["inserted"], first["updated"], first["already_existing"]), (0, 0, 1))
        self.assertEqual(first["empty_windows_omitted"], 0)
        self.assertEqual(first["partitions"]["train"]["windows"], 1)
        repo.calculate.assert_called_with(c, 500, policy, ["EMP001"])
        c.cursor.return_value.__enter__.return_value.executemany.assert_not_called()

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_partial_coverage_keeps_full_hour_outside_replacement_scope(self, activate):
        repo, w, actors, stored, c = self.fixture()
        before = copy.deepcopy(stored)
        policy = oracle_cases()[1][2]
        partial = extract_features(oracle_cases()[0][1], SCHEDULES, repo.context, policy)[0][0]
        result = repo.persist(c, [partial], actors, "a"*64, coverage=policy)
        self.assertEqual((result["inserted"], result["updated"]), (1, 0))
        self.assertEqual(stored, before)
        sql, args = c.cursor.return_value.__enter__.return_value.executemany.call_args.args
        self.assertTrue(sql.startswith("INSERT INTO public.feature_windows"))
        self.assertEqual(len(args), 1)
        self.assertEqual(args[0][0], partial["window_id"])
        self.assertNotEqual(partial["window_id"], w["window_id"])
        self.assertFalse(partial["metadata"]["model_eligible"])

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_empty_partial_coverage_does_not_retire_overlapping_full_hour(self, activate):
        repo, w, actors, stored, c = self.fixture()
        result = repo.persist(c, [], actors, "a"*64, coverage=oracle_cases()[1][2])
        self.assertEqual((result["inserted"], result["updated"]), (0, 0))
        c.cursor.assert_not_called()

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_selected_window_ignores_other_actors_and_other_intervals(self, activate):
        repo, w, actors, stored, c = self.fixture()
        unrelated = (str(uuid.uuid4()), uuid.uuid4(), *stored[2:])
        other_same_actor = (str(uuid.uuid4()), stored[1], timestamp("2026-09-02T16:00:00Z"),
                            timestamp("2026-09-02T17:00:00Z"), stored[4])
        c.execute.return_value.fetchall.return_value = [stored, unrelated, other_same_actor]
        result = repo.persist(c, [w], actors, "a"*64)
        self.assertEqual((result["inserted"], result["updated"], result["already_existing"]), (0, 0, 1))
        c.cursor.return_value.__enter__.return_value.executemany.assert_not_called()
        self.assertEqual(stored[4]["audit"]["revision"], 1)

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_explicit_coverage_blocks_true_obsolete_window(self, activate):
        repo, w, actors, stored, c = self.fixture()
        obsolete = (str(uuid.uuid4()), *stored[1:])
        c.execute.return_value.fetchall.return_value = [stored, obsolete]
        with self.assertRaisesRegex(RepositoryError, "OBSOLETE_FEATURE_WINDOWS_REQUIRE_REVIEW"):
            repo.persist(c, [w], actors, "a"*64, coverage=WindowPolicy())
        c.cursor.assert_not_called()

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_empty_explicit_coverage_detects_existing_obsolete(self, activate):
        repo, w, actors, stored, c = self.fixture()
        with self.assertRaisesRegex(RepositoryError, "OBSOLETE_FEATURE_WINDOWS_REQUIRE_REVIEW"):
            repo.persist(c, [], actors, "a"*64, coverage=WindowPolicy())
        c.cursor.assert_not_called()

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_coverage_is_bounded_by_actor_and_half_open_interval(self, activate):
        repo, w, actors, stored, c = self.fixture()
        other_actor = (str(uuid.uuid4()), uuid.uuid4(), *stored[2:])
        at_end = (str(uuid.uuid4()), stored[1], stored[3], timestamp("2026-09-02T16:00:00Z"), stored[4])
        c.execute.return_value.fetchall.return_value = [stored, other_actor, at_end]
        result = repo.persist(c, [w], actors, "a"*64,
                              coverage=WindowPolicy(start=stored[2], end=stored[3]))
        self.assertEqual(result["already_existing"], 1)

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_selected_recalculation_requires_permission_and_preserves_audit(self, activate):
        repo, w, actors, stored, c = self.fixture()
        changed = extract_features(oracle_cases()[0][1] + [
            event("late-test", "2026-09-02T14:20:00Z", 4096, destination="projects.internal")
        ], SCHEDULES, repo.context)[0][0]
        unrelated = (str(uuid.uuid4()), uuid.uuid4(), *stored[2:])
        c.execute.return_value.fetchall.return_value = [stored, unrelated]
        with self.assertRaisesRegex(RepositoryError, "FEATURE_RECALCULATION_REQUIRES_EXPLICIT_OPTION"):
            repo.persist(c, [changed], actors, "b"*64)
        result = repo.persist(c, [changed], actors, "b"*64, allow_recalculate=True)
        self.assertEqual(result["updated"], 1)
        sql, args = c.cursor.return_value.__enter__.return_value.executemany.call_args.args
        self.assertTrue(sql.startswith("UPDATE public.feature_windows SET features="))
        self.assertEqual(len(args), 1)
        self.assertEqual(args[0][1], w["window_id"])
        audit = args[0][0].obj["audit"]
        self.assertEqual(audit["revision"], 2)
        self.assertEqual(audit["created_at"], stored[4]["audit"]["created_at"])
        self.assertEqual(audit["history"][0]["revision"], 1)
        self.assertEqual(stored[4]["audit"]["revision"], 1)

    @patch("services.data_pipeline.feature_repository.activate_featurizer")
    def test_coverage_rejects_window_outside_interval(self, activate):
        repo, w, actors, stored, c = self.fixture()
        with self.assertRaisesRegex(RepositoryError, "FEATURE_WINDOW_OUTSIDE_RECALCULATION_COVERAGE"):
            repo.persist(c, [w], actors, "a"*64,
                         coverage=WindowPolicy(start=stored[3], end=timestamp("2026-09-02T16:00:00Z")))
        c.cursor.assert_not_called()

    def test_tls_must_verify_identity(self):
        settings = Mock(); settings.sslmode = "require"
        with self.assertRaises(RepositoryError):
            FeatureRepository(settings, ROOT/"data/generated/v2/employees_directory.csv")

    def test_domain_payload_has_only_vector_and_metadata(self):
        case = oracle_cases()[0]
        window = extract_features(case[1], SCHEDULES, {"directory_sha256":"fixture"})[0][0]
        payload = domain_payload(window)
        self.assertEqual(set(payload), {"vector", "metadata"})
        self.assertNotIn("user_id", payload["vector"])

    def test_additive_migration_parses_pg17(self):
        sql = (ROOT/"scripts/db/migrations/0007_feature_persistence.sql").read_text()
        self.assertTrue(parse_sql(sql)); self.assertTrue(parse_plpgsql(sql))
        self.assertNotIn("PASSWORD", sql)
        self.assertNotIn("DROP ", sql)
        self.assertIn("GRANT UPDATE(features)", sql)
        self.assertIn("INHERIT FALSE, SET TRUE, ADMIN FALSE", sql)


@unittest.skipUnless(os.environ.get("RUN_FEATURE_INTEGRATION") == "1", "requiere Supabase y autorizacion")
class FeatureRepositoryIntegrationTest(unittest.TestCase):
    def test_partial_scope_keeps_neighbors_and_rolls_back(self):
        repo = FeatureRepository(load_settings(pipeline=True), ROOT/"data/generated/v2/employees_directory.csv")
        context = dict(repo.context, fixture_nonce=uuid.uuid4().hex)
        repo.context = context
        base = oracle_cases()[0][1]
        windows = extract_features(base + [
            event("scope-neighbor-"+uuid.uuid4().hex, "2026-09-02T15:10:00Z", 100),
            event("scope-other-"+uuid.uuid4().hex, "2026-09-02T14:10:00Z", 200, user="EMP002")
        ], SCHEDULES, context)[0]
        policy = oracle_cases()[1][2]
        partial = extract_features(base, SCHEDULES, context, policy)[0][0]
        with repo.connect() as c:
            with c.transaction(force_rollback=True):
                activate_featurizer(c)
                before = c.execute("SELECT id,features,created_at FROM public.feature_windows ORDER BY id").fetchall()
                actors = {code:id for id,code in c.execute("SELECT id,actor_code FROM public.actors").fetchall()}
                self.assertEqual(repo.persist(c, windows, actors, "a"*64)["inserted"], 3)
                original = c.execute("SELECT id,features,created_at FROM public.feature_windows ORDER BY id").fetchall()
                selected = {"EMP001": actors["EMP001"]}
                empty = repo.persist(c, [], selected, "a"*64, coverage=policy)
                self.assertEqual((empty["inserted"], empty["updated"]), (0, 0))
                first = repo.persist(c, [partial], selected, "a"*64, coverage=policy)
                self.assertEqual((first["inserted"], first["updated"]), (1, 0))
                after = c.execute("SELECT id,features,created_at FROM public.feature_windows ORDER BY id").fetchall()
                without_partial = [row for row in after if str(row[0]) != partial["window_id"]]
                self.assertEqual(without_partial, original)
                second = repo.persist(c, [partial], selected, "a"*64, coverage=policy)
                self.assertEqual((second["inserted"], second["updated"], second["already_existing"]), (0, 0, 1))
                self.assertEqual(c.execute("SELECT id,features,created_at FROM public.feature_windows ORDER BY id").fetchall(), after)
                with self.assertRaisesRegex(RepositoryError, "OBSOLETE_FEATURE_WINDOWS_REQUIRE_REVIEW"):
                    repo.persist(c, [], selected, "a"*64, coverage=policy)
            with c.transaction():
                activate_featurizer(c)
                self.assertEqual(c.execute("SELECT id,features,created_at FROM public.feature_windows ORDER BY id").fetchall(), before)

    def test_operational_permissions_constraints_manual_oracles_rollback_recovery(self):
        repo = FeatureRepository(load_settings(pipeline=True), ROOT/"data/generated/v2/employees_directory.csv")
        with repo.connect() as c:
            with c.transaction(force_rollback=True):
                def denied(sql, args=None, expected="42501"):
                    state = None
                    try:
                        with c.transaction(): c.execute(sql, args)
                    except psycopg.Error as e: state = e.sqlstate
                    self.assertEqual(state, expected)
                denied("SELECT * FROM public.feature_windows LIMIT 1") # grupo NO INHERIT
                activate_featurizer(c)
                before = c.execute("SELECT count(*) FROM public.feature_windows").fetchone()[0]
                actors = {code:id for id,code in c.execute("SELECT id,actor_code FROM public.actors").fetchall()}
                denied("SELECT payload FROM public.raw_events LIMIT 1")
                denied("UPDATE public.raw_events SET payload='{}'")
                denied("DELETE FROM public.raw_events")
                denied("UPDATE public.normalized_events SET file_size_bytes=0")
                denied("DELETE FROM public.normalized_events")
                denied("DELETE FROM public.feature_windows")
                denied("CREATE TABLE public.feature_forbidden_fixture(id integer)")
                denied("UPDATE public.feature_windows SET window_start=now()")
                context = dict(repo.context, fixture_nonce=uuid.uuid4().hex)
                windows = [extract_features(case[1], SCHEDULES, context, case[2])[0][0]
                           for case in oracle_cases()]
                # A y B son diferentes intervalos; los cinco oraculos tienen claves distintas.
                with self.assertRaises(RepositoryError):
                    with c.transaction():
                        repo.persist(c, windows, actors, "a"*64, 2, fail_after=2)
                self.assertEqual(c.execute("SELECT count(*) FROM public.feature_windows").fetchone()[0], before)
                first = repo.persist(c, windows, actors, "a"*64, 2)
                self.assertEqual(first["inserted"], 5)
                second = repo.persist(c, windows, actors, "a"*64, 2)
                self.assertEqual((second["inserted"],second["updated"],second["already_existing"]), (0,0,5))
                def insert(w, actor=None, id=None, synthetic=True):
                    payload = dict(domain_payload(w), audit={"created_at":"2026-10-12T00:00:00Z","last_changed_at":"2026-10-12T00:00:00Z",
                                                             "revision":1,"source_snapshot_sha256":"a"*64})
                    return (id or w["window_id"], actor or actors[w["user_id"]], timestamp(w["window_start"]),
                            timestamp(w["window_end"]), Jsonb(payload), w["pipeline_version"], synthetic)
                sql = "INSERT INTO public.feature_windows(id,actor_id,window_start,window_end,features,pipeline_version,is_synthetic) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                denied(sql, insert(windows[0]), "23505")
                denied(sql, insert(windows[0], id=str(uuid.uuid4())), "23505") # clave natural
                denied(sql, insert(windows[0], actor=uuid.uuid4(), id=str(uuid.uuid4())))
                denied(sql, insert(windows[0], id=str(uuid.uuid4()), synthetic=False))
                bad = copy.deepcopy(windows[0]); bad["features"]["off_hours_ratio"] = 2
                # CHECK23514: enviar JSONB directamente para probar SQL sin validador Python.
                args = list(insert(windows[0], id=str(uuid.uuid4())))
                args[4] = Jsonb(dict(domain_payload(windows[0]), audit={"revision":1,"created_at":"2026-10-12T00:00:00Z",
                                   "last_changed_at":"2026-10-12T00:00:00Z","source_snapshot_sha256":"a"*64}))
                value = args[4].obj; value["vector"] = dict(value["vector"], off_hours_ratio=2)
                denied(sql, tuple(args), "23514")
                stored = c.execute("SELECT features FROM public.feature_windows WHERE id=%s", (windows[1]["window_id"],)).fetchone()[0]
                self.assertFalse(stored["metadata"]["model_eligible"])
                for w in windows:
                    stored = c.execute("SELECT features FROM public.feature_windows WHERE id=%s", (w["window_id"],)).fetchone()[0]
                    self.assertEqual({"vector":stored["vector"],"metadata":stored["metadata"]}, domain_payload(w))
                # Recalculo autorizado: mismos limites/contexto, nueva huella y revision2.
                changed = extract_features(oracle_cases()[0][1]+[
                    event("late-db-fixture","2026-09-02T14:20:00Z",4096,destination="projects.internal")],
                    SCHEDULES,context)[0][0]
                with self.assertRaises(RepositoryError):
                    with c.transaction():repo.persist(c,[changed]+windows[1:],actors,"b"*64)
                update = repo.persist(c,[changed]+windows[1:],actors,"b"*64,allow_recalculate=True)
                self.assertEqual(update["updated"],1)
                audit=c.execute("SELECT features->'audit' FROM public.feature_windows WHERE id=%s",
                                (changed["window_id"],)).fetchone()[0]
                self.assertEqual(audit["revision"],2)
            with c.transaction():
                activate_featurizer(c)
                self.assertEqual(c.execute("SELECT count(*) FROM public.feature_windows").fetchone()[0], before)
                self.assertFalse(c.execute("SELECT EXISTS(SELECT 1 FROM public.feature_windows WHERE pipeline_version=%s)",
                                          (windows[0]["pipeline_version"],)).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
