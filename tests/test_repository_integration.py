"""Integracion opt-in: conexion operativa real y fixtures con rollback."""
import csv
import os
import tempfile
import unittest
import uuid
from pathlib import Path

from services.data_pipeline.adapters import SOURCES,SourceFile,file_sha256
from services.data_pipeline.config import load_settings
from services.data_pipeline.operational import check_operational,validate_identity
from services.data_pipeline.repository import PostgresRepository,RepositoryError,archive_file


@unittest.skipUnless(os.environ.get('RUN_PIPELINE_INTEGRATION')=='1','requiere conexion operativa configurada y autorizada')
class RepositoryIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.settings=load_settings(pipeline=True)
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.spec=SOURCES[0]
        self.path=Path(self.temp.name)/self.spec.filename
        self.payload={k:'' for k in self.spec.columns}
        self.payload.update({'id_evento':'fixture-'+uuid.uuid4().hex,'FECHA':'INVALID','TAMANO':'N/A'})
        with self.path.open('w',encoding='utf-8',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=self.spec.columns)
            writer.writeheader(); writer.writerow(self.payload); writer.writerow(self.payload)
        self.repo=PostgresRepository(self.settings,Path(self.temp.name)/'archive')
        digest=file_sha256(self.path)
        self.info=SourceFile(self.path,self.spec.code,digest,self.path.stat().st_size)
        self.archived=archive_file(self.path,self.repo.archive,digest)

    def test_operational_security(self):
        result=check_operational(self.settings)
        self.assertTrue(result['fixtures_rolled_back'])
        self.assertFalse(result['bypass_rls'])

    def test_small_roundtrip_idempotency_failure_recovery_and_rollback(self):
        with self.repo.connect() as connection:
            validate_identity(connection)
            before=connection.execute('SELECT count(*) FROM public.raw_events').fetchone()[0]
            batch_id=None
            with connection.transaction(force_rollback=True):
                # Fallo inyectado tras primer grupo; savepoint elimina incluso lote/fuente.
                with self.assertRaises(RepositoryError):
                    with connection.transaction():
                        self.repo._persist(connection,self.spec,self.path,self.info,self.archived,1,fail_after=1)
                self.assertEqual(connection.execute('SELECT count(*) FROM public.raw_events').fetchone()[0],before)
                # Registrar failed con cero RAW y recuperar misma identidad.
                self.repo.record_error(connection,self.spec,self.path,self.info,'INJECTED_FAILURE')
                first=self.repo._persist(connection,self.spec,self.path,self.info,self.archived,1)
                batch_id=first['batch_id']
                self.assertEqual(first['new_rows'],2)
                self.assertEqual(first['duplicate_occurrences'],1)
                rows=connection.execute('SELECT payload,record_number FROM public.raw_events WHERE batch_id=%s ORDER BY record_number',(batch_id,)).fetchall()
                self.assertEqual(rows,[(self.payload,1),(self.payload,2)])
                second=self.repo._persist(connection,self.spec,self.path,self.info,self.archived,1)
                self.assertEqual(second['status'],'already_imported')
                self.assertEqual(second['new_rows'],0)
                self.assertEqual(second['batch_id'],batch_id)
            self.assertEqual(connection.execute('SELECT count(*) FROM public.raw_events').fetchone()[0],before)
            self.assertFalse(connection.execute('SELECT EXISTS(SELECT 1 FROM public.ingest_batches WHERE id=%s)',(batch_id,)).fetchone()[0])


if __name__=='__main__': unittest.main()
