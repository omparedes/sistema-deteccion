"""NOT run by default. Requires authorization, SQL0011, and rollback-only fixtures."""
import copy
import json
import os
import unittest
import uuid
from datetime import timedelta

import psycopg
from psycopg.types.json import Jsonb

from services.data_pipeline.adapters import SOURCES
from services.data_pipeline.config import load_settings
from services.data_pipeline.features import WindowPolicy, iso
from services.data_pipeline.normalization import payload_digest
from services.data_pipeline.normalization_repository import NormalizationRepository, activate_normalizer
from services.data_pipeline.feature_repository import activate_featurizer
from services.data_pipeline.revision_repository import RevisionRepository
from services.data_pipeline.repository import RepositoryError
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
DIRECTORY=ROOT/'data/generated/v2/employees_directory.csv'


@unittest.skipUnless(os.environ.get('RUN_REVISION_INTEGRATION')=='1',
                     'requires explicit authorization and applied SQL0011')
class RevisionIntegrationTest(unittest.TestCase):
    def test_revisions_scopes_security_idempotency_failures_and_exact_rollback(self):
        settings=load_settings(pipeline=True); repo=RevisionRepository(settings,DIRECTORY)
        normal=NormalizationRepository(settings,DIRECTORY)
        with repo.connect() as c:
            with c.transaction():
                activate_normalizer(c)
                self.assertIsNotNone(c.execute("SELECT to_regprocedure('public.revise_normalization(uuid,jsonb,jsonb)')").fetchone()[0])
                self.assertIn("((q -> 'normalized_event') - 'event_id')",c.execute("SELECT pg_get_functiondef('public.revise_normalization(uuid,jsonb,jsonb)'::regprocedure)").fetchone()[0])
                baseline=c.execute('SELECT id,payload,payload_sha256,processing_status,processing_details,normalization_versions FROM public.raw_events ORDER BY id').fetchall()
                normals=c.execute('SELECT to_jsonb(n) FROM public.normalized_events n ORDER BY id').fetchall()
                activate_featurizer(c)
                features=c.execute('SELECT to_jsonb(f) FROM public.feature_windows f ORDER BY id').fetchall()
            with c.transaction(force_rollback=True):
                # Select an EMPTY two-hour area for both users; definitive windows are never fixtures.
                activate_featurizer(c)
                start=c.execute("""SELECT t FROM generate_series(timestamptz '2026-09-01 05:00Z',timestamptz '2026-09-30 02:00Z',interval '1 hour') t
                   WHERE NOT EXISTS(SELECT 1 FROM public.feature_windows f JOIN public.actors a ON a.id=f.actor_id
                     WHERE a.actor_code IN ('EMP001','EMP002') AND f.window_start<t+interval '2 hour' AND t<f.window_end)
                   AND NOT EXISTS(SELECT 1 FROM public.normalized_events n JOIN public.actors a ON a.id=n.actor_id
                     WHERE a.actor_code IN ('EMP001','EMP002') AND n.occurred_at>=t AND n.occurred_at<t+interval '2 hour') ORDER BY t LIMIT 1""").fetchone()[0]
                first=WindowPolicy(start,start+timedelta(hours=1));second=WindowPolicy(first.end,first.end+timedelta(hours=1))
                c.execute('RESET ROLE')
                source=c.execute("SELECT id FROM public.data_sources WHERE source_code='endpoint'").fetchone()[0]
                nonce=uuid.uuid4().hex;file_hash=nonce*2
                batch=c.execute("INSERT INTO public.ingest_batches(source_id,source_filename,file_sha256,byte_size,directory_sha256) VALUES (%s,'endpoint_file_logs.csv',%s,100,%s) RETURNING id",(source,file_hash,normal.directory_sha)).fetchone()[0]
                c.execute("UPDATE public.ingest_batches SET status='importing' WHERE id=%s",(batch,))
                payloads=[]
                for index,(user,at,size) in enumerate([('EMP001',start+timedelta(minutes=10),1),('EMP001',start+timedelta(minutes=10),1),('EMP001',first.end+timedelta(minutes=10),2),('EMP002',start+timedelta(minutes=10),3)]):
                    payload={k:'' for k in SOURCES[0].columns}
                    payload.update(id_evento='revision-'+nonce+str(index),USUARIO=user,FECHA=iso(at),TAMANO=str(size),UNIDAD='KB',ACCION='OPEN',ARCHIVO='fixture.txt')
                    if index==1:payload=copy.deepcopy(payloads[0])
                    payloads.append(payload)
                ids=[]
                for ordinal,payload in enumerate(payloads,1):
                    ids.append(c.execute('INSERT INTO public.raw_events(source_id,external_event_id,payload,batch_id,record_number,payload_sha256) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id',
                        (source,payload['id_evento'],Jsonb(payload),batch,ordinal,payload_digest(payload))).fetchone()[0])
                c.execute("UPDATE public.ingest_batches SET status='completed',row_count=4,archive_reference='fixture',archive_sha256=%s,completed_at=now() WHERE id=%s",(file_hash,batch))
                initial=normal.process(c,2,[batch]);self.assertEqual((initial['processed'],initial['duplicate']),(3,1))
                for user,policy in [('EMP001',first),('EMP001',second),('EMP002',first)]:
                    self.assertEqual(repo.recalculate(c,user,policy,str(uuid.uuid4()),2)['inserted'],1)
                partial=WindowPolicy(first.start,first.start+timedelta(minutes=30))
                activate_featurizer(c)
                full_before=c.execute('SELECT id,features,lifecycle FROM public.feature_windows WHERE window_start=%s AND window_end=%s AND actor_id=(SELECT id FROM public.actors WHERE actor_code=\'EMP001\')',(first.start,first.end)).fetchone()
                self.assertEqual(repo.recalculate(c,'EMP001',partial,str(uuid.uuid4()),2)['inserted'],1)
                self.assertEqual(c.execute('SELECT id,features,lifecycle FROM public.feature_windows WHERE id=%s',(full_before[0],)).fetchone(),full_before)
                current=repo.current(c,ids[0]);self.assertEqual(current['revision'],1)
                fixture_raw=c.execute('SELECT payload,payload_sha256,source_id,batch_id,record_number,external_event_id FROM public.raw_events WHERE id=%s',(ids[0],)).fetchone()
                def sql_rejected(sql,args=None,expected='23514'):
                    code=None
                    try:
                        with c.transaction():c.execute(sql,args)
                    except psycopg.Error as error:code=error.sqlstate
                    self.assertEqual(code,expected)
                request=dict(request_id=str(uuid.uuid4()),expected_revision=1,reason='Approved fixture correction',rules_version='hu02-v2',
                    directory_sha256=normal.directory_sha,status='processed',normalized_event=current['normalized_event'],audit=[{'code':'FIXTURE_REVISION','severity':'info'}])
                request['normalized_event']=dict(request['normalized_event'],user_id='EMP002',timestamp=iso(first.end+timedelta(minutes=10)))
                # Both JSON null and SQL NULL are invalid request roots; no privileged bypass.
                for invalid in [None,Jsonb(None),Jsonb(dict(request,reason='')),Jsonb(dict(request,unknown_key=True))]:
                    sql_rejected('SELECT public.revise_normalization(%s,%s::jsonb,%s::jsonb)',(ids[0],invalid,Jsonb({})))
                c.execute('RESET ROLE')
                sql_rejected('SELECT public.revise_normalization(%s,%s::jsonb,%s::jsonb)',(ids[0],Jsonb(request),Jsonb({})),'42501')
                activate_featurizer(c)
                sql_rejected('SELECT public.revise_normalization(%s,%s::jsonb,%s::jsonb)',(ids[0],Jsonb(request),Jsonb({})),'42501')
                activate_normalizer(c)
                sql_rejected('UPDATE public.raw_events SET normalization_versions=\'[]\'::jsonb WHERE id=%s',(ids[0],),'42501')
                sql_rejected('SELECT public.reconcile_feature_scope(%s,%s,%s,%s,\'[]\'::jsonb)',('EMP001',first.start,first.end,uuid.uuid4()),'42501')
                with self.assertRaisesRegex(RepositoryError,'PREPARATION_FAILURE'):
                    with c.transaction():repo.revise(c,ids[0],request,fail_after_prepare=True)
                self.assertEqual(repo.current(c,ids[0])['revision'],1)
                with self.assertRaisesRegex(RepositoryError,'REVISION_WRITE_FAILURE'):
                    with c.transaction():repo.revise(c,ids[0],request,fail_after_write=True)
                self.assertEqual(repo.current(c,ids[0])['revision'],1)
                result=repo.revise(c,ids[0],request);self.assertTrue(result['changed']);self.assertEqual(result['revision'],2)
                active=repo.current(c,ids[0]);self.assertEqual(len(active['history']),2)
                self.assertEqual(active['history'][0]['normalized_event']['event_id'],current['normalized_event']['event_id'])
                self.assertEqual(active['history'][0]['normalized_event'],current['normalized_event'])
                self.assertEqual(active['history'][0]['sql_snapshot'],current['sql_snapshot'])
                self.assertEqual(active['history'][0]['processing_details'],current['processing_details'])
                self.assertEqual(active['history'][-1]['reason'],request['reason'])
                self.assertEqual((active['revision'],active['rules_version']),(2,'hu02-v2'))
                history_before=copy.deepcopy(active['history'])
                self.assertFalse(repo.revise(c,ids[0],request)['changed'])
                self.assertEqual(repo.current(c,ids[0])['history'],history_before)
                stale=dict(request,request_id=str(uuid.uuid4()))
                sql_rejected('SELECT public.revise_normalization(%s,%s::jsonb,%s::jsonb)',(ids[0],Jsonb(stale),Jsonb({})),'40001')
                sql_rejected('SELECT public.revise_normalization(%s,%s::jsonb,%s::jsonb)',(ids[0],Jsonb(dict(request,reason='Different content')),Jsonb({})))
                sql_rejected('SELECT public.revise_normalization(%s,%s::jsonb,%s::jsonb)',(ids[1],Jsonb(request),Jsonb({})))
                with self.assertRaisesRegex(RepositoryError,'CONFLICT'):
                    with c.transaction():repo.revise(c,ids[0],stale)
                with self.assertRaisesRegex(RepositoryError,'CANONICAL'):
                    with c.transaction():repo.revise(c,ids[1],dict(request,request_id=str(uuid.uuid4())))
                activate_featurizer(c)
                neighbors=c.execute('SELECT id,features,lifecycle,retirement_history FROM public.feature_windows WHERE actor_id IN (SELECT id FROM public.actors WHERE actor_code IN (\'EMP001\',\'EMP002\')) AND window_start>=%s AND window_end<=%s ORDER BY id',(start,second.end)).fetchall()
                with self.assertRaisesRegex(RepositoryError,'RECALCULATION_FAILURE'):
                    with c.transaction():repo.recalculate(c,'EMP002',second,str(uuid.uuid4()),2,fail_after_write=True)
                activate_featurizer(c)
                self.assertEqual(c.execute('SELECT id,features,lifecycle,retirement_history FROM public.feature_windows WHERE actor_id IN (SELECT id FROM public.actors WHERE actor_code IN (\'EMP001\',\'EMP002\')) AND window_start>=%s AND window_end<=%s ORDER BY id',(start,second.end)).fetchall(),neighbors)
                self.assertEqual(repo.recalculate(c,'EMP001',first,str(uuid.uuid4()),2)['inserted'],0)
                self.assertEqual(repo.recalculate(c,'EMP002',second,str(uuid.uuid4()),2)['inserted'],1)
                repeated=repo.recalculate(c,'EMP002',second,str(uuid.uuid4()),2)
                self.assertEqual((repeated['inserted'],repeated['updated'],repeated['already_existing']),(0,0,1))
                self.assertFalse(repo.pending(c,ids[0]))
                # Valid -> quarantine -> valid retains all previous SQL snapshots.
                state=repo.current(c,ids[0]);q=dict(request,request_id=str(uuid.uuid4()),expected_revision=2,status='rejected',normalized_event=None,audit=[{'code':'FIXTURE_REJECT','severity':'error'}])
                repo.revise(c,ids[0],q);self.assertIsNone(repo.current(c,ids[0])['normalized_event'])
                quarantined=repo.current(c,ids[0]);self.assertEqual(quarantined['revision'],3)
                self.assertIsNone(quarantined['history'][-1]['normalized_event'])
                self.assertIsNone(quarantined['history'][-1]['sql_snapshot'])
                self.assertEqual(quarantined['history'][1]['sql_snapshot'],state['sql_snapshot'])
                q=dict(request,request_id=str(uuid.uuid4()),expected_revision=3,normalized_event=state['normalized_event'])
                repo.revise(c,ids[0],q);self.assertEqual(repo.current(c,ids[0])['revision'],4)
                self.assertEqual(len(repo.current(c,ids[0])['history']),4)
                self.assertEqual(c.execute('SELECT payload,payload_sha256,source_id,batch_id,record_number,external_event_id FROM public.raw_events WHERE id=%s',(ids[0],)).fetchone(),fixture_raw)
                def denied(sql,args=None):
                    code=None
                    try:
                        with c.transaction():c.execute(sql,args)
                    except psycopg.Error as error:code=error.sqlstate
                    self.assertEqual(code,'42501')
                activate_normalizer(c)
                denied("UPDATE public.raw_events SET payload='{}' WHERE id=%s",(ids[0],))
                denied('DELETE FROM public.normalized_events WHERE raw_event_id=%s',(ids[0],))
                denied('CREATE TABLE public.revision_forbidden_fixture(id integer)')
                denied('SELECT public.reconcile_feature_scope(%s,%s,%s,%s,\'[]\')',('EMP001',first.start,first.end,uuid.uuid4()))
                activate_featurizer(c)
                denied('SELECT public.revise_normalization(%s,\'{}\',\'{}\')',(ids[0],))
                # A newly normalized late event must also invalidate an existing neighbor.
                # This is a new fixture batch, never appended to a completed immutable batch.
                late=c.execute('SELECT id,features FROM public.feature_windows WHERE lifecycle=\'active\' AND actor_id=(SELECT id FROM public.actors WHERE actor_code=\'EMP001\') AND window_start=%s AND window_end=%s',(second.start,second.end)).fetchone()
                c.execute('RESET ROLE');late_hash=uuid.uuid4().hex*2
                late_batch=c.execute("INSERT INTO public.ingest_batches(source_id,source_filename,file_sha256,byte_size,directory_sha256) VALUES (%s,'endpoint_file_logs.csv',%s,100,%s) RETURNING id",(source,late_hash,normal.directory_sha)).fetchone()[0]
                c.execute("UPDATE public.ingest_batches SET status='importing' WHERE id=%s",(late_batch,))
                late_payload=dict(payloads[2],id_evento='late-'+nonce,FECHA=iso(second.start+timedelta(minutes=30)),TAMANO='4')
                late_raw=c.execute('INSERT INTO public.raw_events(source_id,external_event_id,payload,batch_id,record_number,payload_sha256) VALUES (%s,%s,%s,%s,1,%s) RETURNING id',(source,late_payload['id_evento'],Jsonb(late_payload),late_batch,payload_digest(late_payload))).fetchone()[0]
                c.execute("UPDATE public.ingest_batches SET status='completed',row_count=1,archive_reference='fixture',archive_sha256=%s,completed_at=now() WHERE id=%s",(late_hash,late_batch))
                self.assertEqual(normal.process(c,2,[late_batch])['processed'],1)
                self.assertEqual(len(repo.pending(c,late_raw)),1)
                activate_featurizer(c)
                archived=c.execute('SELECT features,lifecycle,retirement_history FROM public.feature_windows WHERE id=%s',(late[0],)).fetchone()
                self.assertEqual(archived[0],late[1]);self.assertEqual(archived[1],'archived')
                self.assertEqual(archived[2][-1]['reason'],'CANONICAL_EVENT_ACTIVATED')
                rebuilt=repo.recalculate(c,'EMP001',second,str(uuid.uuid4()),2)
                self.assertEqual((rebuilt['inserted'],rebuilt['updated']),(0,1))
                self.assertEqual(repo.recalculate(c,'EMP001',second,str(uuid.uuid4()),2)['already_existing'],1)
                # Timestamp-only and actor-only changes, plus a revision with unchanged numbers.
                repo.recalculate(c,'EMP002',second,str(uuid.uuid4()),2)
                active=repo.current(c,ids[0])
                move=dict(request,request_id=str(uuid.uuid4()),expected_revision=4,
                          normalized_event=dict(active['normalized_event'],timestamp=iso(first.start+timedelta(minutes=10))))
                repo.revise(c,ids[0],move)
                self.assertEqual(len(repo.pending(c,ids[0])),2)
                activate_featurizer(c)
                self.assertEqual(c.execute("SELECT count(*) FROM public.feature_windows WHERE lifecycle='active' AND actor_id=(SELECT id FROM public.actors WHERE actor_code='EMP002') AND window_start>=%s AND window_end<=%s",(first.start,second.end)).fetchone()[0],0)
                self.assertEqual(repo.recalculate(c,'EMP002',second,str(uuid.uuid4()),2)['inserted'],0)
                repo.recalculate(c,'EMP002',first,str(uuid.uuid4()),2)
                generated,_=repo.calculate_scope(c,'EMP002',first,2)
                vector=c.execute("SELECT features->'vector' FROM public.active_feature_windows WHERE actor_id=(SELECT id FROM public.actors WHERE actor_code='EMP002') AND window_start=%s AND window_end=%s",(first.start,first.end)).fetchone()[0]
                self.assertEqual(vector,generated[0]['features']);self.assertEqual(len(vector),11)
                neighbor=c.execute("SELECT to_jsonb(f) FROM public.feature_windows f WHERE lifecycle='active' AND actor_id=(SELECT id FROM public.actors WHERE actor_code='EMP001') AND window_start=%s AND window_end=%s",(second.start,second.end)).fetchone()[0]
                active=repo.current(c,ids[0])
                move=dict(request,request_id=str(uuid.uuid4()),expected_revision=5,
                          normalized_event=dict(active['normalized_event'],user_id='EMP001'))
                repo.revise(c,ids[0],move)
                self.assertEqual(len(repo.pending(c,ids[0])),2)
                repo.recalculate(c,'EMP002',first,str(uuid.uuid4()),2)
                repo.recalculate(c,'EMP001',first,str(uuid.uuid4()),2)
                self.assertEqual(c.execute("SELECT to_jsonb(f) FROM public.feature_windows f WHERE lifecycle='active' AND actor_id=(SELECT id FROM public.actors WHERE actor_code='EMP001') AND window_start=%s AND window_end=%s",(second.start,second.end)).fetchone()[0],neighbor)
                old_vector=c.execute("SELECT features->'vector' FROM public.active_feature_windows WHERE actor_id=(SELECT id FROM public.actors WHERE actor_code='EMP001') AND window_start=%s AND window_end=%s",(first.start,first.end)).fetchone()[0]
                active=repo.current(c,ids[0])
                unchanged=dict(request,request_id=str(uuid.uuid4()),expected_revision=6,normalized_event=active['normalized_event'])
                repo.revise(c,ids[0],unchanged)
                self.assertEqual(len(repo.pending(c,ids[0])),1)
                rid=str(uuid.uuid4());repo.recalculate(c,'EMP001',first,rid,2)
                self.assertEqual(repo.recalculate(c,'EMP001',first,rid,2)['already_existing'],1)
                self.assertEqual(c.execute("SELECT features->'vector' FROM public.active_feature_windows WHERE actor_id=(SELECT id FROM public.actors WHERE actor_code='EMP001') AND window_start=%s AND window_end=%s",(first.start,first.end)).fetchone()[0],old_vector)
                recovered=repo.current(c,ids[0])
                self.assertEqual(recovered['revision'],7);self.assertEqual(len(recovered['history']),7)
                self.assertEqual(recovered['history'][0]['sql_snapshot'],current['sql_snapshot'])
                self.assertEqual(c.execute('SELECT payload,payload_sha256,source_id,batch_id,record_number,external_event_id FROM public.raw_events WHERE id=%s',(ids[0],)).fetchone(),fixture_raw)
                # Constraint trigger validates final fixture state before the outer rollback.
                activate_normalizer(c);c.execute('SET CONSTRAINTS ALL IMMEDIATE')
            with c.transaction():
                activate_normalizer(c)
                self.assertEqual(c.execute('SELECT id,payload,payload_sha256,processing_status,processing_details,normalization_versions FROM public.raw_events ORDER BY id').fetchall(),baseline)
                self.assertEqual(c.execute('SELECT to_jsonb(n) FROM public.normalized_events n ORDER BY id').fetchall(),normals)
                activate_featurizer(c)
                self.assertEqual(c.execute('SELECT to_jsonb(f) FROM public.feature_windows f ORDER BY id').fetchall(),features)
