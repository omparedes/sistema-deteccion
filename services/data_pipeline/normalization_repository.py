"""HU02 PostgreSQL: lectura por cursor, rol explicito, transaccion atomica."""
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .adapters import RawRecord,SourceFile,SOURCES,file_sha256
from .database_check import _error_code
from .normalization import EmployeeDirectory,VERSION,normalize_record,payload_digest
from .normalized_contract import load_contract
from .operational import validate_identity
from .repository import RepositoryError

SQL_FIELDS=['id','raw_event_id','actor_id','occurred_at','operation','device_code','session_code',
    'file_extension','file_type','file_size_bytes','source_type','source_path','destination_type',
    'destination_path','destination_authorized','data_classification','operation_result','is_synthetic']

READ_SQL="""
WITH base AS (
 SELECT r.*,s.source_code,b.source_filename,b.file_sha256,b.byte_size,b.source_schema_version,
        b.directory_sha256,b.created_at AS batch_created
 FROM public.raw_events r JOIN public.ingest_batches b ON b.id=r.batch_id
 JOIN public.data_sources s ON s.id=r.source_id
 WHERE (%s::uuid[] IS NULL OR r.batch_id=ANY(%s::uuid[]))
), annotated AS (
 SELECT b.*,CASE WHEN min(b.payload::text) OVER identity_window=max(b.payload::text) OVER identity_window THEN 1 ELSE 2 END AS variants,
 first_value(b.id) OVER w AS canonical_id,
 first_value(b.file_sha256) OVER w AS canonical_file_sha,
 first_value(b.record_number) OVER w AS canonical_record_number
 FROM base b
 WINDOW identity_window AS (PARTITION BY b.source_id,b.external_event_id),
 w AS (PARTITION BY b.source_id,b.external_event_id,b.payload
              ORDER BY b.received_at,b.batch_created,b.batch_id,b.record_number,b.id)
)
SELECT a.*,to_jsonb(n) AS persisted_normalized
FROM annotated a LEFT JOIN public.normalized_events n ON n.raw_event_id=a.id
ORDER BY a.received_at,a.batch_created,a.batch_id,a.record_number,a.id
"""


def activate_normalizer(connection):
    connection.execute('RESET ROLE')
    validate_identity(connection)
    connection.execute('SET LOCAL ROLE pipeline_normalizer')
    row=connection.execute("SELECT current_user,session_user,rolsuper,rolbypassrls,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user").fetchone()
    if row[:2]!=('pipeline_normalizer','pipeline_runner') or any(row[2:]):
        raise RepositoryError('NORMALIZATION_ROLE_NOT_MINIMAL')


def sql_values(event,raw_id,actor_id):
    value={key:event.get(key) for key in SQL_FIELDS}
    value.update(id=event['event_id'],raw_event_id=raw_id,actor_id=actor_id,
                 occurred_at=datetime.fromisoformat(event['timestamp'].replace('Z','+00:00')))
    value['device_code']=event.get('device_id')
    value['session_code']=event.get('session_id')
    return tuple(value[key] for key in SQL_FIELDS)


class NormalizationRepository:
    def __init__(self,settings,directory):
        if settings.sslmode!='verify-full': raise RepositoryError('NORMALIZATION_VERIFY_FULL_REQUIRED')
        self.settings=settings
        self.directory_path=Path(directory)
        self.directory_sha=file_sha256(self.directory_path)
        self.directory=EmployeeDirectory.read(self.directory_path)
        with self.directory_path.open(encoding='utf-8-sig',newline='') as stream:
            self.employees=list(csv.DictReader(stream))
        if len(self.employees)!=25 or any(row.get('DataOrigin')!='SYNTHETIC' for row in self.employees):
            raise RepositoryError('NORMALIZATION_DIRECTORY_SCOPE_INVALID')
        self.schema=load_contract()

    def connect(self):
        return psycopg.connect(**self.settings.connection_options(),autocommit=True,prepare_threshold=None)

    def seed_actors(self,connection):
        for row in self.employees:
            connection.execute("INSERT INTO public.actors(actor_code,role_name,department,is_synthetic) VALUES (%s,%s,%s,true) ON CONFLICT(actor_code) DO NOTHING",(row['EmployeeCode'],row['Position'],row['Team']))
        actors={code:(id,role,team) for id,code,role,team in connection.execute('SELECT id,actor_code,role_name,department FROM public.actors').fetchall()}
        if any(row['EmployeeCode'] not in actors or actors[row['EmployeeCode']][1:]!=(row['Position'],row['Team']) for row in self.employees):
            raise RepositoryError('ACTOR_PROFILE_CHANGED_REQUIRES_REVIEW')
        return {code:value[0] for code,value in actors.items()}

    def result_for_row(self,row):
        if row['directory_sha256']!=self.directory_sha or row['source_schema_version']!='raw-v1':
            raise RepositoryError('NORMALIZATION_CONTEXT_CHANGED_REQUIRES_REVIEW')
        source=next(s for s in SOURCES if s.code==row['source_code'])
        if row['external_event_id']!=row['payload'].get(source.event_id_column) or payload_digest(row['payload'])!=row['payload_sha256']:
            raise RepositoryError('RAW_ID_OR_HASH_INTEGRITY_FAILED')
        record=RawRecord(SourceFile(Path(row['source_filename']),row['source_code'],row['file_sha256'],row['byte_size']),row['record_number'],row['payload'],source.event_id_column)
        result=normalize_record(record,self.directory,self.directory_sha,self.schema)
        original_id=row['external_event_id']
        parent=None
        if isinstance(original_id,str) and original_id.strip():
            if row['variants']>1:
                result['status']='rejected';result['normalized_event']=None
                result['audit'].append({'version':VERSION,'stage':'deduplication','code':'CONFLICTING_EVENT_ID','field':'source_event_id','before':original_id,'after':None,'severity':'error'})
            if row['id']!=row['canonical_id']:
                parent=row['canonical_id'];result['status']='duplicate';result['normalized_event']=None
                result['duplicate_of']=f"csv-sha256:{row['canonical_file_sha']}:{row['canonical_record_number']}"
                result['audit'].append({'version':VERSION,'stage':'deduplication','code':'EXACT_DUPLICATE','field':'raw_reference','before':result['raw_reference'],'after':result['duplicate_of'],'severity':'info'})
        for entry in result['audit']:
            entry.setdefault('moment',result['temporal']['timestamp'] if result['temporal'] else None)
            entry.setdefault('moment_kind','event_timestamp')
        logical={'version':VERSION,'directory_sha256':self.directory_sha,'status':result['status'],
                 'event':result['normalized_event'],'duplicate_of':str(parent) if parent else None,'audit':result['audit']}
        fingerprint=hashlib.sha256(json.dumps(logical,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        return result,parent,fingerprint

    def process(self,connection,batch_size=500,batch_ids=None,fail_after=None,progress=None):
        """Usar dentro de una transaccion; permite savepoint/rollback de fixtures."""
        if type(batch_size) is not int or batch_size<1: raise ValueError('batch_size debe ser positivo')
        activate_normalizer(connection)
        connection.execute("SET LOCAL statement_timeout='60s'")
        connection.execute("SET LOCAL lock_timeout='10s'")
        connection.execute("SELECT pg_advisory_xact_lock(hashtext('hu02-normalization-v1'))")
        actors=self.seed_actors(connection)
        counts=Counter();changed=skipped=new_normalized=0
        source_counts={}
        insert='INSERT INTO public.normalized_events('+','.join(SQL_FIELDS)+') VALUES ('+','.join(['%s']*len(SQL_FIELDS))+')'
        with connection.cursor(name='hu02_raw_stream',row_factory=dict_row) as reader:
            reader.execute(READ_SQL,(batch_ids,batch_ids))
            while True:
                rows=reader.fetchmany(batch_size)
                if not rows: break
                normal=[];updates=[]
                for row in rows:
                    if row.get('normalization_revision', 1) > 1:
                        raise RepositoryError('EXPLICIT_NORMALIZATION_REVISION_REQUIRED')
                    result,parent,fingerprint=self.result_for_row(row)
                    status=result['status'];counts[status]+=1
                    source_counts.setdefault(row['source_code'],Counter())[status]+=1
                    details=row['processing_details']
                    if row['processing_status']!='pending':
                        entries=[item for item in details if isinstance(item,dict) and item.get('normalization_version')==VERSION]
                        if not entries or entries[-1].get('fingerprint')!=fingerprint or row['processing_status']!=status or row['duplicate_of']!=parent:
                            raise RepositoryError('NORMALIZATION_EXISTING_RESULT_CHANGED_REQUIRES_REVIEW')
                        persisted=row['persisted_normalized']
                        if status=='processed':
                            event=result['normalized_event']
                            expected=sql_values(event,row['id'],actors[event['user_id']])
                            if not persisted: raise RepositoryError('NORMALIZED_EVENT_MISSING')
                            for key,value in zip(SQL_FIELDS,expected):
                                actual=persisted[key]
                                if key=='occurred_at':
                                    if datetime.fromisoformat(actual)!=value: raise RepositoryError('NORMALIZED_EVENT_CHANGED_REQUIRES_REVIEW')
                                    continue
                                if str(actual)!=str(value): raise RepositoryError('NORMALIZED_EVENT_CHANGED_REQUIRES_REVIEW')
                        elif persisted: raise RepositoryError('NONCANONICAL_NORMALIZED_EVENT_EXISTS')
                        skipped+=1;continue
                    if row['persisted_normalized']: raise RepositoryError('PENDING_RAW_ALREADY_HAS_RESULT')
                    if status=='processed':
                        event=result['normalized_event']
                        normal.append(sql_values(event,row['id'],actors[event['user_id']]))
                    errors=[a for a in result['audit'] if a['severity']=='error']
                    error=json.dumps([{'code':a['code'],'field':a['field']} for a in errors],sort_keys=True) if errors else None
                    entry={'normalization_version':VERSION,'directory_sha256':self.directory_sha,
                           'fingerprint':fingerprint,'status':status,'raw_reference':result['raw_reference'],
                           'executed_at':datetime.now(timezone.utc).isoformat(),'audit':result['audit']}
                    updates.append((status,error,Jsonb(details+[entry]),parent,row['id']))
                with connection.cursor() as writer:
                    if normal: writer.executemany(insert,normal)
                    if updates: writer.executemany('UPDATE public.raw_events SET processing_status=%s,processing_error=%s,processing_details=%s,duplicate_of=%s WHERE id=%s',updates)
                changed+=len(updates)
                new_normalized+=len(normal)
                if fail_after is not None and changed>=fail_after: raise RepositoryError('INJECTED_NORMALIZATION_FAILURE')
                if progress: progress(sum(counts.values()),changed)
        if file_sha256(self.directory_path)!=self.directory_sha: raise RepositoryError('DIRECTORY_CHANGED')
        return {'version':VERSION,'raw_received':sum(counts.values()),'processed':counts['processed'],
                'rejected':counts['rejected'],'duplicate':counts['duplicate'],'changed_raw':changed,'unchanged_raw':skipped,
                'new_normalized':new_normalized,
                'per_source':{s:dict(c) for s,c in source_counts.items()}}

    def run(self,batch_size=500,progress=None):
        try:
            with self.connect() as connection:
                with connection.transaction():
                    result=self.process(connection,batch_size,progress=progress)
                result['commit']=True
                return result
        except psycopg.Error as error:
            raise RepositoryError(_error_code(error)) from None
