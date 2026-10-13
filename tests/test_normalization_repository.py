"""Seguridad y persistencia HU02: offline + integracion opt-in reversible."""
import json
import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import Mock,patch

import psycopg
from psycopg.types.json import Jsonb
from pglast import parse_sql,parse_plpgsql

from services.data_pipeline.adapters import SOURCES
from services.data_pipeline.config import load_settings
from services.data_pipeline.normalization import payload_digest
from services.data_pipeline.normalization_repository import NormalizationRepository,SQL_FIELDS,sql_values,activate_normalizer
from services.data_pipeline.repository import RepositoryError

ROOT=Path(__file__).resolve().parents[1]
DIRECTORY=ROOT/'data/generated/v2/employees_directory.csv'


class NormalizationRepositoryOfflineTest(unittest.TestCase):
    def test_security_migration_is_aditive_and_has_no_password(self):
        text=(ROOT/'scripts/db/migrations/0005_normalization_security.sql').read_text(encoding='utf-8')
        self.assertTrue(parse_sql(text)); self.assertTrue(parse_plpgsql(text))
        self.assertIn('WITH INHERIT FALSE, SET TRUE, ADMIN FALSE',text)
        self.assertNotIn('PASSWORD',text)
        self.assertNotIn('DROP ',text)
        self.assertIn('canonical.id=raw_events.duplicate_of',text)

    def test_no_tls_downgrade(self):
        settings=Mock();settings.sslmode='require'
        with self.assertRaises(RepositoryError): NormalizationRepository(settings,DIRECTORY)

    def test_sql_mapping_preserves_optional_nulls_and_timestamp(self):
        event={'event_id':str(uuid.uuid4()),'timestamp':'2026-09-01T12:00:00Z','operation':'file_access','file_size_bytes':1024,'is_synthetic':True,'device_id':'PC1','destination_authorized':None}
        values=dict(zip(SQL_FIELDS,sql_values(event,uuid.uuid4(),uuid.uuid4())))
        self.assertEqual(values['occurred_at'].hour,12)
        self.assertEqual(values['device_code'],'PC1')
        self.assertIsNone(values['destination_authorized'])


@unittest.skipUnless(os.environ.get('RUN_NORMALIZATION_INTEGRATION')=='1','requiere autorizacion y Supabase configurado')
class NormalizationRepositoryIntegrationTest(unittest.TestCase):
    def test_roles_constraints_idempotency_injected_rollback_recovery(self):
        repo=NormalizationRepository(load_settings(pipeline=True),DIRECTORY)
        with repo.connect() as connection:
            before=connection.execute('SELECT count(*) FROM public.raw_events').fetchone()[0]
            with connection.transaction(force_rollback=True):
                def denied(statement,parameters=None,state='42501'):
                    actual=None
                    try:
                        with connection.transaction():connection.execute(statement,parameters)
                    except psycopg.Error as error:actual=error.sqlstate
                    self.assertEqual(actual,state)
                denied('SELECT * FROM public.actors LIMIT 1') # NO INHERIT efectivo
                source=connection.execute("SELECT id FROM public.data_sources WHERE source_code='endpoint'").fetchone()[0]
                digest=uuid.uuid4().hex*2
                batch=connection.execute("INSERT INTO public.ingest_batches(source_id,source_filename,file_sha256,byte_size,directory_sha256) VALUES (%s,'endpoint_file_logs.csv',%s,100,%s) RETURNING id",(source,digest,repo.directory_sha)).fetchone()[0]
                connection.execute("UPDATE public.ingest_batches SET status='importing' WHERE id=%s",(batch,))
                payload={k:'' for k in SOURCES[0].columns}
                payload.update({'id_evento':'fixture-'+uuid.uuid4().hex,'USUARIO':'EMP001','FECHA':'01/09/2026 09:00','TAMANO':'1','UNIDAD':'KB','ACCION':'OPEN','ARCHIVO':'fixture.txt'})
                rows=[]
                for ordinal in [1,2,3]:
                    value=dict(payload)
                    if ordinal==2:value.update(id_evento='fixture-bad-'+uuid.uuid4().hex,FECHA='invalid')
                    rows.append(connection.execute("INSERT INTO public.raw_events(source_id,external_event_id,payload,batch_id,record_number,payload_sha256) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id",(source,value['id_evento'],Jsonb(value),batch,ordinal,payload_digest(value))).fetchone()[0])
                connection.execute("UPDATE public.ingest_batches SET status='completed',row_count=3,archive_reference='fixture',archive_sha256=%s,completed_at=now() WHERE id=%s",(digest,batch))
                with connection.transaction(force_rollback=True):
                    activate_normalizer(connection)
                    denied("UPDATE public.raw_events SET payload='{}' WHERE id=%s",(rows[0],))
                    denied('DELETE FROM public.raw_events WHERE id=%s',(rows[0],))
                    denied('DELETE FROM public.normalized_events WHERE raw_event_id=%s',(rows[0],))
                    denied("UPDATE public.raw_events SET payload_sha256=%s WHERE id=%s",('0'*64,rows[0]))
                    denied("UPDATE public.raw_events SET processing_status='duplicate',duplicate_of=%s,processing_details='[{}]' WHERE id=%s",(uuid.uuid4(),rows[0]),'23503')

                    denied('CREATE TABLE public.normalizer_forbidden_fixture(id integer)')
                    denied("INSERT INTO public.actors(actor_code) VALUES ('EMP999')")
                    actor=repo.seed_actors(connection)['EMP001']
                    event={'event_id':str(uuid.uuid4()),'timestamp':'2026-09-01T14:00:00Z','operation':'file_access','file_size_bytes':1024,'is_synthetic':True}
                    statement='INSERT INTO public.normalized_events('+','.join(SQL_FIELDS)+') VALUES ('+','.join(['%s']*len(SQL_FIELDS))+')'
                    values=sql_values(event,rows[0],actor)
                    connection.execute(statement,values)
                    denied(statement,values,'23505') # PK/RAW uniqueness actual
                    denied(statement,sql_values(dict(event,event_id=str(uuid.uuid4())),uuid.uuid4(),actor)) # RLS bloquea RAW inexistente
                    state=None
                    try:
                        with connection.transaction():
                            connection.execute("UPDATE public.raw_events SET processing_status='processed',processing_details='[{}]' WHERE id=%s",(rows[1],))
                            connection.execute('SET CONSTRAINTS raw_normalization_result_integrity IMMEDIATE')
                    except psycopg.Error as error:state=error.sqlstate
                    self.assertEqual(state,'23514')
                with self.assertRaises(RepositoryError):
                    with connection.transaction():repo.process(connection,2,[batch],fail_after=2)
                self.assertEqual(connection.execute('SELECT processing_status FROM public.raw_events WHERE id=%s',(rows[0],)).fetchone()[0],'pending')
                first=repo.process(connection,2,[batch])
                self.assertEqual((first['processed'],first['rejected'],first['duplicate']),(1,1,1))
                second=repo.process(connection,2,[batch])
                self.assertEqual((second['changed_raw'],second['new_normalized']),(0,0))
                self.assertEqual(connection.execute('SELECT duplicate_of FROM public.raw_events WHERE id=%s',(rows[2],)).fetchone()[0],rows[0])
                connection.execute('SET CONSTRAINTS raw_normalization_result_integrity IMMEDIATE')
            self.assertEqual(connection.execute('SELECT count(*) FROM public.raw_events').fetchone()[0],before)
            self.assertFalse(connection.execute('SELECT EXISTS(SELECT 1 FROM public.ingest_batches WHERE id=%s)',(batch,)).fetchone()[0])


if __name__=='__main__':unittest.main()
