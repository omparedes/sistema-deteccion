"""Validación OFFLINE de sintaxis/estructura; no demuestra aplicación en DB."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from pglast import parse_plpgsql, parse_sql
from pglast.parser import get_postgresql_version

REPO = Path(__file__).resolve().parents[1]
DB = REPO / "scripts/db"
MIGRATIONS = DB / "migrations"
NAMES = {"data_sources", "actors", "raw_events", "normalized_events", "feature_windows",
         "model_versions", "detection_results", "alerts", "alert_reviews", "ingest_batches"}


def statements(path):
    return [item.stmt(skip_none=True) for item in parse_sql(path.read_text(encoding="utf-8"))]


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from walk(child)


def strings(values):
    return [v["sval"] for v in values]


class MigrationSqlTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.initial = statements(MIGRATIONS / "0001_initial_schema.sql")
        cls.trace = statements(MIGRATIONS / "0002_ingestion_traceability.sql")
        cls.security = statements(MIGRATIONS / "0003_ingestion_security.sql")

    def test_all_sql_and_plpgsql_parse_with_pg17_without_network(self):
        self.assertEqual(get_postgresql_version()[0], 17)
        with patch("socket.create_connection", side_effect=AssertionError("network forbidden")), \
             patch("psycopg.connect", side_effect=AssertionError("database forbidden")):
            for path in DB.rglob("*.sql"):
                if path.name.endswith(".pending.sql"):
                    continue
                text = path.read_text(encoding="utf-8")
                tree = parse_sql(text)
                self.assertGreater(len(tree), 0, path.name)
                if any(type(s.stmt).__name__ == "DoStmt" for s in tree):
                    self.assertTrue(parse_plpgsql(text), path.name)

    def test_reuses_nine_tables_and_adds_only_one(self):
        initial = {s["relation"]["relname"] for s in self.initial if s["@"] == "CreateStmt"}
        added = {s["relation"]["relname"] for s in self.trace if s["@"] == "CreateStmt"}
        self.assertEqual(initial, NAMES - {"ingest_batches"})
        self.assertEqual(added, {"ingest_batches"})

    def test_batch_and_duplicate_foreign_keys_match_sources(self):
        constraints = {n["conname"]: n for n in walk(self.trace)
                       if n.get("@") == "Constraint" and "conname" in n}
        batch = constraints["raw_batch_source_fk"]
        self.assertEqual(strings(batch["fk_attrs"]), ["batch_id", "source_id"])
        self.assertEqual(strings(batch["pk_attrs"]), ["id", "source_id"])
        self.assertEqual(batch["pktable"]["relname"], "ingest_batches")
        duplicate = constraints["raw_duplicate_source_fk"]
        self.assertEqual(strings(duplicate["fk_attrs"]), ["duplicate_of", "source_id"])
        self.assertEqual(duplicate["pktable"]["relname"], "raw_events")

    def test_uniqueness_preserves_duplicate_occurrences(self):
        unique = [n for n in walk(self.trace) if n.get("@") == "Constraint"
                  and n.get("contype", {}).get("name") == "CONSTR_UNIQUE"]
        keys = {tuple(strings(n["keys"])) for n in unique}
        self.assertIn(("batch_id", "record_number"), keys)
        self.assertIn(("source_id", "file_sha256", "source_schema_version"), keys)
        self.assertTrue(all("external_event_id" not in key and "payload_sha256" not in key for key in keys))

    def test_hash_payload_and_status_guards_exist(self):
        names = {n.get("conname") for n in walk(self.trace) if n.get("@") == "Constraint"}
        self.assertTrue({"raw_new_row_hash_required", "raw_payload_object", "raw_batch_row_valid",
                         "ingest_hash_required", "ingest_completed_valid", "ingest_failed_valid"} <= names)
        # El parser acepta las expresiones; verdad de restricciones requiere DB futura.

    def test_role_cannot_login_or_bypass_and_has_no_password(self):
        role = next(s for s in self.security if s["@"] == "CreateRoleStmt")
        self.assertEqual(role["role"], "pipeline_ingestor")
        options = {o["defname"]: o["arg"] for o in role["options"]}
        for option in ("canlogin", "superuser", "createdb", "createrole", "isreplication", "bypassrls"):
            self.assertIs(options[option]["boolval"], False)
        self.assertNotIn("password", options)
        self.assertFalse(any(s["@"] == "GrantRoleStmt" for s in self.security))

    def test_runtime_grants_exclude_raw_updates_and_future_tables(self):
        for statement in self.security:
            if statement["@"] != "GrantStmt" or not statement["is_grant"]:
                continue
            self.assertEqual([r.get("rolename") for r in statement["grantees"]], ["pipeline_ingestor"])
            if statement["objtype"]["name"] != "OBJECT_TABLE":
                continue
            objects = {o["relname"] for o in statement["objects"]}
            self.assertTrue(objects <= {"data_sources", "ingest_batches", "raw_events"})
            for privilege in statement["privileges"]:
                self.assertIn(privilege["priv_name"], {"select", "insert", "update"})
                if privilege["priv_name"] == "update":
                    self.assertEqual(objects, {"ingest_batches"})
                    columns = set(strings(privilege["cols"]))
                    self.assertTrue(columns <= {"status", "row_count", "ingestion_error", "completed_at",
                                                "archive_reference", "archive_sha256", "metadata"})
                    self.assertNotIn("file_sha256", columns)

    def test_all_internal_tables_revoked_from_api_roles(self):
        revoke = next(s for s in self.security if s["@"] == "GrantStmt" and not s["is_grant"])
        self.assertEqual({o["relname"] for o in revoke["objects"]}, NAMES)
        roles = {r.get("rolename", r["roletype"]["name"]) for r in revoke["grantees"]}
        self.assertEqual(roles, {"anon", "authenticated", "service_role", "ROLESPEC_PUBLIC"})

    def test_policies_only_for_ingestion_role_and_supported_operations(self):
        policies = [s for s in self.security if s["@"] == "CreatePolicyStmt"]
        self.assertEqual(len(policies), 7)
        for policy in policies:
            self.assertEqual([r.get("rolename") for r in policy["roles"]], ["pipeline_ingestor"])
            self.assertIn(policy["cmd_name"], {"select", "insert", "update"})
            self.assertIn(policy["table"]["relname"], {"data_sources", "raw_events", "ingest_batches"})

    def test_migrations_do_not_write_rows_commit_or_manage_supabase_schemas(self):
        for stmt in self.trace + self.security:
            self.assertNotIn(stmt["@"], {"InsertStmt", "UpdateStmt", "DeleteStmt", "TransactionStmt", "AlterDefaultPrivilegesStmt"})
            for node in walk(stmt):
                if node.get("@") == "RangeVar":
                    self.assertEqual(node["schemaname"], "public")
                    self.assertIn(node["relname"], NAMES)

    def test_pre_and_post_checks_guard_baseline_and_permissions(self):
        pre = (DB / "checks/sprint01_preflight.sql").read_text(encoding="utf-8")
        post = (DB / "checks/sprint01_postflight.sql").read_text(encoding="utf-8")
        self.assertIn("PROJECT_NAME_CONFLICT_REQUIRES_REVIEW", pre)
        self.assertIn("INGEST_ROLE_ALREADY_EXISTS_REQUIRES_REVIEW", pre)
        self.assertIn("API_PRIVILEGES_REMAIN", post)
        self.assertIn("INTO has_rows", post)
        self.assertIn("SCHEMA_PHASE_MUST_NOT_LOAD_ROWS", post)

    def test_empty_rollback_locks_refuses_data_and_never_cascades(self):
        path = DB / "rollback/sprint01_empty_schema.sql"
        text = path.read_text(encoding="utf-8")
        nodes = statements(path)
        self.assertEqual(nodes[0]["@"], "LockStmt")
        self.assertEqual(nodes[0]["mode"], 8)  # AccessExclusiveLock: sin carrera con cargas.
        self.assertIn("ROLLBACK_REFUSED_DATA_PRESENT", text)
        self.assertIn("ROLLBACK_ROLE_BASELINE_OR_MEMBERSHIP_MISMATCH", text)
        drops = [s for s in nodes if s["@"] == "DropStmt"]
        self.assertEqual(len(drops), 10)
        self.assertTrue(all(s["behavior"]["name"] == "DROP_RESTRICT" for s in drops))
        self.assertTrue(all(s["objects"][0][0]["sval"] == "public" for s in drops))
        revoke = next(s for s in nodes if s["@"] == "GrantStmt")
        self.assertFalse(revoke["is_grant"])
        self.assertEqual([r["rolename"] for r in revoke["grantees"]], ["pipeline_ingestor"])
        self.assertEqual([p["priv_name"] for p in revoke["privileges"]], ["usage"])


if __name__ == "__main__":
    unittest.main()
