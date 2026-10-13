"""Revision estructural offline; no sustituye la ejecucion PostgreSQL del CHECK."""
import unittest
from pathlib import Path
from pglast import parse_sql, parse_plpgsql


class FeatureAuditSqlTest(unittest.TestCase):
    def test_pending_migration_parses_and_is_additive(self):
        path = Path(__file__).resolve().parents[1] / "scripts/db/migrations/0008_feature_audit_validation.sql"
        sql = path.read_text(encoding="utf-8")
        self.assertTrue(parse_sql(sql))
        self.assertTrue(parse_plpgsql(sql))
        self.assertNotIn("CREATE OR REPLACE", sql)
        for forbidden in ("DROP ", "DELETE ", "UPDATE ", "PASSWORD", "BYPASSRLS", "DISABLE ROW LEVEL"):
            self.assertNotIn(forbidden, sql)
        self.assertIn("revision_value = trunc(revision_value)", sql)
        self.assertIn("created_value <= changed_value", sql)
        self.assertIn("CHECK (public.feature_audit_valid(features) IS TRUE)", sql)
        self.assertIn("FROM PUBLIC, anon, authenticated, service_role", sql)


if __name__ == "__main__":
    unittest.main()
