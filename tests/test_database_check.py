"""Pruebas offline de seguridad del chequeo DB, con conexión simulada."""
import builtins
import contextlib
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
import psycopg
from services.data_pipeline.config import Settings
from services.data_pipeline.database_check import DatabaseCheckError, _error_code, check_database
from services.data_pipeline.cli import main

FAKE_URL = "postgresql://fictional:synthetic-password@db.test-ref.supabase.co:5432/postgres?sslmode=require"


class DatabaseCheckTest(unittest.TestCase):
    def setUp(self):
        self.settings = Settings(FAKE_URL, "test", "INFO", "require")
        self.connection = MagicMock()
        self.connection.pgconn.ssl_in_use = True
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.base = {"select_one": 1, "database": "postgres", "server_version": "17.11",
                     "read_only": "on", "default_read_only": "off"}
        self.cursor.fetchone.side_effect = [self.base, {"version": "server-info"},
                                           {"ssl": False, "version": None, "cipher": None, "bits": None}]

        def ensure_read_only(statement, parameters=None):
            self.assertIs(self.connection.read_only, True)
            self.assertTrue(statement.lstrip().startswith("SELECT "))
            self.assertNotIn(";", statement)

        self.cursor.execute.side_effect = ensure_read_only
        self.connector = patch("psycopg.connect", return_value=self.connection)
        self.connect = self.connector.start()
        self.addCleanup(self.connector.stop)

    def test_tls_read_only_and_cleanup_even_pooler_default_off(self):
        report = check_database(self.settings)
        self.assertTrue(report["transaction_read_only"])
        self.assertFalse(report["session_default_read_only"])
        self.assertTrue(report["tls"]["client_to_endpoint"])
        self.assertFalse(report["tls"]["server_pg_stat_ssl"]["ssl"])
        self.assertFalse(report["tls"]["certificate_identity_verified"])
        self.assertFalse(report["writes_executed"])
        self.connection.rollback.assert_called_once()
        self.connection.close.assert_called_once()
        self.connection.commit.assert_not_called()
        self.assertEqual(self.connect.call_args.kwargs["sslmode"], "require")
        self.assertIsNone(self.connect.call_args.kwargs["prepare_threshold"])

    def test_audit_only_catalogs_missing_objects_detected(self):
        self.cursor.fetchall.side_effect = [
            [{"schema": "public"}, {"schema": "auth"}], [], [], [], [], [],
            [{"superuser": False, "bypass_rls": True, "database_owner": True}],
            [{"table_count": 0}], [], [], [],
        ]
        report = check_database(self.settings, audit=True)
        self.assertEqual(report["audit"]["public_table_count"], 0)
        self.assertEqual(len(report["audit"]["missing_initial_public_tables"]), 9)
        self.assertEqual(report["audit"]["project_objects"], [])
        self.assertNotIn("synthetic-password", str(report))

    def test_tls_inactive_rejected_before_select(self):
        self.connection.pgconn.ssl_in_use = False
        with self.assertRaises(DatabaseCheckError) as caught:
            check_database(self.settings)
        self.assertEqual(caught.exception.code, "DB_TLS_NOT_ACTIVE")
        self.cursor.execute.assert_not_called()
        self.connection.close.assert_called_once()

    def test_read_write_transaction_rejected(self):
        self.base["read_only"] = "off"
        with self.assertRaises(DatabaseCheckError) as caught:
            check_database(self.settings)
        self.assertEqual(caught.exception.code, "DB_READ_ONLY_NOT_CONFIRMED")
        self.connection.rollback.assert_called_once()
        self.connection.close.assert_called_once()

    def test_wrong_select_result_rejected(self):
        self.base["select_one"] = 0
        with self.assertRaises(DatabaseCheckError):
            check_database(self.settings)
        self.connection.close.assert_called_once()

    def test_authentication_error_redacted(self):
        self.connect.side_effect = psycopg.errors.InvalidPassword("synthetic-password server detail")
        with self.assertRaises(DatabaseCheckError) as caught:
            check_database(self.settings)
        self.assertEqual(caught.exception.code, "DB_AUTHENTICATION_FAILED")
        self.assertNotIn("synthetic-password", str(caught.exception))

    def test_tls_error_no_retry_or_downgrade(self):
        self.settings = Settings(FAKE_URL.replace("require", "verify-full"), "test", "INFO", "verify-full")
        self.connect.side_effect = psycopg.OperationalError("certificate verify failed synthetic-password")
        with self.assertRaises(DatabaseCheckError) as caught:
            check_database(self.settings)
        self.assertEqual(caught.exception.code, "DB_TLS_FAILED_NO_DOWNGRADE")
        self.connect.assert_called_once()
        self.assertEqual(self.connect.call_args.kwargs["sslmode"], "verify-full")

    def test_error_classifications_are_safe(self):
        for message, code in (("could not translate host name synthetic-secret", "DB_DNS_FAILED"),
                              ("connection timeout expired synthetic-secret", "DB_CONNECTION_TIMEOUT"),
                              ("WinError 10013 permission denied", "DB_NETWORK_RESTRICTED"),
                              ("unrecognized synthetic-secret", "DB_CONNECTION_OR_QUERY_FAILED")):
            error = psycopg.OperationalError(message)
            self.assertEqual(_error_code(error), code)
        self.assertEqual(_error_code(psycopg.errors.InsufficientPrivilege("private")), "DB_PERMISSION_DENIED")
        self.assertEqual(_error_code(psycopg.errors.QueryCanceled("private")), "DB_QUERY_TIMEOUT")

    def test_query_error_closes_and_redacts(self):
        self.cursor.execute.side_effect = psycopg.errors.InsufficientPrivilege("synthetic-password")
        with self.assertRaises(DatabaseCheckError) as caught:
            check_database(self.settings)
        self.assertEqual(caught.exception.code, "DB_PERMISSION_DENIED")
        self.connection.close.assert_called_once()

    def test_cleanup_error_does_not_leak(self):
        self.connection.rollback.side_effect = psycopg.OperationalError("synthetic-password")
        report = check_database(self.settings)
        self.assertEqual(report["connection"], "ok")
        self.connection.close.assert_called_once()

    def test_missing_driver_controlled(self):
        original_import = builtins.__import__

        def no_driver(name, *args, **kwargs):
            if name == "psycopg":
                raise ImportError("not installed")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=no_driver), self.assertRaises(DatabaseCheckError) as caught:
            check_database(self.settings)
        self.assertEqual(caught.exception.code, "DB_DRIVER_NOT_INSTALLED_IN_THIS_ENVIRONMENT")

    def test_cli_error_does_not_print_credentials_or_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(f"DATABASE_URL={FAKE_URL}\nAPP_ENV=test\nLOG_LEVEL=INFO\n")
            self.connect.side_effect = psycopg.errors.InvalidPassword("synthetic-password")
            output = io.StringIO()
            with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stderr(output):
                code = main(["db-check", "--env-file", str(path)])
            self.assertEqual(code, 2)
            self.assertIn("DB_AUTHENTICATION_FAILED", output.getvalue())
            self.assertNotIn("synthetic-password", output.getvalue())
            self.assertNotIn("Traceback", output.getvalue())


if __name__ == "__main__":
    unittest.main()
