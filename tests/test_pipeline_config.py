"""Validación offline de configuración con credenciales exclusivamente ficticias."""
import contextlib
import fnmatch
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from services.data_pipeline.config import ConfigurationError, load_settings
from services.data_pipeline.cli import main

# Nunca se conecta; solo fixture para validación sintáctica.
FAKE_URL = "postgresql://fictional:synthetic-password@db.test-ref.supabase.co:5432/postgres?sslmode=require"


class PipelineConfigurationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = self.root / ".env"

    def write_env(self, url=FAKE_URL, extra=""):
        self.env.write_text(f'DATABASE_URL="{url}"\nAPP_ENV=test\nLOG_LEVEL=INFO\n' + extra, encoding="utf-8")
        return self.env

    def test_env_loading_priority_repr_and_no_network(self):
        self.write_env()
        with patch("socket.create_connection", side_effect=AssertionError("network forbidden")):
            settings = load_settings(self.env, {"LOG_LEVEL": "WARNING"})
        self.assertEqual(settings.database_url, FAKE_URL)
        self.assertEqual(settings.log_level, "WARNING")
        self.assertNotIn("synthetic-password", repr(settings))
        self.assertNotIn(FAKE_URL, repr(settings))
        self.assertEqual(settings.connection_options()["sslmode"], "require")
        self.assertEqual(settings.connection_options()["gssencmode"], "disable")

    def test_environment_only_and_missing_url(self):
        settings = load_settings(self.root / "absent.env", {"DATABASE_URL": FAKE_URL})
        self.assertEqual(settings.app_env, "development")
        with self.assertRaises(ConfigurationError):
            load_settings(self.root / "absent.env", {})

    def test_tls_enforced_and_query_overrides_rejected(self):
        for mode in ("disable", "allow", "prefer", "", "invalid"):
            with self.assertRaises(ConfigurationError):
                load_settings(self.write_env(FAKE_URL.replace("require", mode)), {})
        for url in (FAKE_URL + "&sslmode=disable", FAKE_URL + "&host=localhost"):
            with self.assertRaises(ConfigurationError):
                load_settings(self.write_env(url), {})

    def test_omitted_tls_mode_still_requires_encryption(self):
        settings = load_settings(self.write_env(FAKE_URL.split("?")[0]), {})
        self.assertEqual(settings.sslmode, "require")
        self.assertEqual(settings.connection_options()["sslmode"], "require")
        cert = self.root / "ca.crt"
        cert.write_text("fixture-only")
        settings = load_settings(self.write_env(FAKE_URL.split("?")[0], "DATABASE_SSL_ROOT_CERT=ca.crt\n"), {})
        self.assertEqual(settings.sslmode, "verify-full")

    def test_panel_metadata_does_not_override_database_url(self):
        path = self.write_env(extra="host=ignored\nport=6543\ndatabase=ignored\nuser=ignored\npassword=ignored\n")
        settings = load_settings(path, {})
        self.assertEqual(settings.database_url, FAKE_URL)

    def test_explicit_verify_full_never_downgraded(self):
        with self.assertRaises(ConfigurationError):
            load_settings(self.write_env(FAKE_URL.replace("require", "verify-full")), {})

    def test_only_approved_cloud_direct_or_session_endpoint(self):
        for url in (FAKE_URL.replace(":5432", ":6543"), FAKE_URL.replace("db.test-ref.supabase.co", "localhost"),
                    FAKE_URL.replace("postgresql://", "https://"), FAKE_URL + "#fragment"):
            with self.assertRaises(ConfigurationError):
                load_settings(self.write_env(url), {})
        session = FAKE_URL.replace("db.test-ref.supabase.co", "aws-0-test.pooler.supabase.com")
        self.assertEqual(load_settings(self.write_env(session), {}).sslmode, "require")

    def test_production_certificate_required_and_resolved(self):
        with self.assertRaises(ConfigurationError):
            load_settings(self.write_env(), {"APP_ENV": "production"})
        verified = FAKE_URL.replace("require", "verify-full")
        with self.assertRaises(ConfigurationError):
            load_settings(self.write_env(verified), {})
        with self.assertRaises(ConfigurationError):
            load_settings(self.write_env(verified, "DATABASE_SSL_ROOT_CERT=missing.crt\n"), {})
        cert = self.root / "test.crt"
        cert.write_text("fixture-only-not-a-real-certificate")
        settings = load_settings(self.write_env(verified, "DATABASE_SSL_ROOT_CERT=test.crt\n"), {"APP_ENV": "production"})
        self.assertEqual(settings.sslrootcert, cert.resolve())
        self.assertEqual(settings.connection_options()["sslrootcert"], str(cert.resolve()))
        # Valida existencia, no autenticidad de certificado ni handshake remoto.

    def test_invalid_dotenv_and_variables_do_not_echo_values(self):
        for content in ("DATABASE_URL='synthetic-secret", "TOKEN=synthetic-secret", "synthetic-secret", "LOG_LEVEL=INFO\nLOG_LEVEL=DEBUG"):
            self.env.write_text(content)
            with self.assertRaises(ConfigurationError) as caught:
                load_settings(self.env, {})
            self.assertNotIn("synthetic-secret", str(caught.exception))
        self.write_env()
        for overrides in ({"APP_ENV": "unknown"}, {"LOG_LEVEL": "TRACE"}, {"DATABASE_URL": "PENDING"}):
            with self.assertRaises(ConfigurationError):
                load_settings(self.env, overrides)

    def test_unreadable_dotenv_is_controlled(self):
        with self.assertRaises(ConfigurationError):
            load_settings(self.root, {})

    def test_cli_success_and_errors_never_print_connection(self):
        self.write_env()
        for url, expected_exit in ((FAKE_URL, 0), ("postgresql://user:synthetic-secret@[invalid", 2)):
            self.write_env(url)
            output = io.StringIO()
            with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                code = main(["config-check", "--env-file", str(self.env)])
            self.assertEqual(code, expected_exit)
            self.assertNotIn(url, output.getvalue())
            self.assertNotIn("synthetic-secret", output.getvalue())
            self.assertNotIn("synthetic-password", output.getvalue())

    def test_root_env_protected_example_public(self):
        rules = [line.strip() for line in (REPO / ".gitignore").read_text().splitlines()
                 if line.strip() and not line.startswith("#")]

        def ignored(filename):
            result = False
            for rule in rules:
                negative = rule.startswith("!")
                pattern = rule[1:] if negative else rule
                if fnmatch.fnmatchcase(filename, pattern):
                    result = not negative
            return result

        self.assertTrue(ignored(".env"))
        self.assertTrue(ignored(".env.test"))
        self.assertFalse(ignored(".env.example"))
        self.assertTrue((REPO / ".env").is_file())
        self.assertTrue((REPO / ".env.example").is_file())
        # Git no está inicializado: comprobación de reglas para estos archivos raíz.


if __name__ == "__main__":
    unittest.main()
