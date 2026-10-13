"""Casos offline esenciales; secretos siempre ficticios, sin conexion."""
import csv
import io
import contextlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch,Mock,MagicMock
from pglast import parse_sql, parse_plpgsql
from services.data_pipeline.config import load_settings,ConfigurationError
from services.data_pipeline.repository import archive_file,payload_sha256,RepositoryError,PostgresRepository
from services.data_pipeline.cli import main
from services.data_pipeline.operational import validate_identity
from services.data_pipeline.database_check import DatabaseCheckError


class RepositoryOfflineTest(unittest.TestCase):
    def test_summary_bigint_sum_is_json_serializable(self):
        import json
        from decimal import Decimal
        settings=Mock(); settings.sslmode='verify-full'
        repository=PostgresRepository(settings)
        connection=MagicMock()
        connection.execute.return_value.fetchall.side_effect=[
            [('endpoint',10000,9820)],[('completed',3,Decimal('20000'))],[]]
        context=MagicMock(); context.__enter__.return_value=connection
        with patch.object(repository,'connect',return_value=context),patch('services.data_pipeline.repository.validate_identity'):
            result=repository.summary()
        self.assertEqual(json.loads(json.dumps(result))['batches'],[['completed',3,20000]])

    def test_identity_rejects_admin_session_bypass_and_public_create(self):
        for identity,public_admin,create in [
            (('pipeline_runner','postgres',False,False,False,False,False),False,False),
            (('pipeline_runner','pipeline_runner',False,True,False,False,False),False,False),
            (('pipeline_runner','pipeline_runner',False,False,False,False,False),True,False),
            (('pipeline_runner','pipeline_runner',False,False,False,False,False),False,True)]:
            connection=Mock(); connection.pgconn.ssl_in_use=True
            cursors=[Mock(),Mock(),Mock(),Mock()]
            cursors[0].fetchone.return_value=identity
            cursors[1].fetchall.return_value=[('pipeline_ingestor',True,False,False)]
            cursors[2].fetchone.return_value=(public_admin,)
            cursors[3].fetchone.return_value=(create,)
            connection.execute.side_effect=cursors
            with self.assertRaises(DatabaseCheckError): validate_identity(connection)

    def test_hash_preserves_raw_values_and_ignores_key_order(self):
        self.assertEqual(payload_sha256({'a':'N/A','b':''}),payload_sha256({'b':'','a':'N/A'}))
        self.assertNotEqual(payload_sha256({'a':'N/A'}),payload_sha256({'a':'0'}))
        self.assertNotEqual(payload_sha256({'a':'USD'}),payload_sha256({'a':'usd'}))

    def test_archive_exact_immutable_copy_and_corruption(self):
        import hashlib
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp)/'source.csv'; p.write_bytes(b'a,b\r\nN/A,\r\n')
            digest=hashlib.sha256(p.read_bytes()).hexdigest()
            archive=Path(temp)/'archive'
            target=archive_file(p,archive,digest)
            self.assertEqual(p.read_bytes(),target.read_bytes())
            self.assertEqual(target,archive_file(p,archive,digest))
            target.write_bytes(b'corrupted')
            with self.assertRaises(RepositoryError): archive_file(p,archive,digest)
            self.assertEqual(target.read_bytes(),b'corrupted')
            self.assertEqual(p.read_bytes(),b'a,b\r\nN/A,\r\n')

    def test_archive_rejects_changed_source(self):
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp)/'source.csv'; p.write_text('changed')
            with self.assertRaises(RepositoryError): archive_file(p,Path(temp)/'archive','0'*64)
            self.assertEqual(list((Path(temp)/'archive').iterdir()),[])

    def test_pipeline_config_has_no_admin_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            env=Path(temp)/'.env'
            env.write_text('DATABASE_URL=postgresql://postgres:fake-password@db.example.supabase.co:5432/postgres?sslmode=require\n')
            with self.assertRaises(ConfigurationError): load_settings(env,{},pipeline=True)

    def test_pipeline_requires_exact_role_and_verify_full(self):
        with tempfile.TemporaryDirectory() as temp:
            cert=Path(temp)/'ca.crt'; cert.write_text('offline fixture only')
            env=Path(temp)/'.env'
            def write(role,mode):
                env.write_text(f'PIPELINE_DATABASE_URL=postgresql://{role}:fake-password@db.example.supabase.co:5432/postgres?sslmode={mode}\nDATABASE_SSL_ROOT_CERT={cert}\n')
            for role,mode in [('postgres','verify-full'),('pipeline_runner','require')]:
                write(role,mode)
                with self.assertRaises(ConfigurationError): load_settings(env,{},pipeline=True)
            write('pipeline_runner','verify-full')
            s=load_settings(env,{},pipeline=True)
            self.assertEqual(s.sslmode,'verify-full')
            self.assertNotIn('fake-password',repr(s))

    def test_cli_missing_operational_url_stops_before_network(self):
        with tempfile.TemporaryDirectory() as temp:
            env=Path(temp)/'.env'; env.write_text('APP_ENV=test\n')
            with patch('psycopg.connect',side_effect=AssertionError('network forbidden')),contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(['ingest','--env-file',str(env)]),2)

    def test_0004_does_not_modify_applied_migrations_or_embed_secret(self):
        p=Path(__file__).resolve().parents[1]/'scripts/db/migrations/0004_pipeline_runner.sql'
        text=p.read_text(encoding='utf-8')
        self.assertTrue(parse_sql(text)); self.assertTrue(parse_plpgsql(text))
        self.assertIn('PASSWORD NULL',text)
        self.assertIn('WITH INHERIT TRUE, SET FALSE, ADMIN FALSE',text)
        self.assertNotIn('service_role',text)


if __name__=='__main__': unittest.main()
