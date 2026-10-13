"""Persistencia RAW operativa: una transaccion por archivo, sin normalizacion."""
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from .adapters import IngestionError, SOURCES, SourceFile, file_sha256, read_batches
from .config import REPO_ROOT
from .database_check import _error_code
from .operational import validate_identity


class RepositoryError(Exception):
    """Solo codigos seguros; nunca mensajes originales del controlador."""


def payload_sha256(payload):
    encoded = json.dumps(dict(payload), sort_keys=True, ensure_ascii=False,
                         separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def archive_file(path, archive, expected_hash):
    """Publicar copia por SHA sin sobrescribir; original permanece intacto."""
    archive = Path(archive)
    archive.mkdir(parents=True, exist_ok=True)
    target = archive / (expected_hash + '.csv')
    if target.exists():
        if file_sha256(target) != expected_hash:
            raise RepositoryError('ARCHIVE_HASH_MISMATCH')
        return target
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=archive, delete=False) as stream:
            temporary = Path(stream.name)
            with Path(path).open('rb') as original:
                for chunk in iter(lambda: original.read(1024 * 1024), b''):
                    stream.write(chunk)
        if file_sha256(temporary) != expected_hash:
            raise RepositoryError('FILE_CHANGED_BEFORE_ARCHIVE')
        try:
            os.link(temporary, target)
        except FileExistsError:
            if file_sha256(target) != expected_hash:
                raise RepositoryError('ARCHIVE_HASH_MISMATCH')
        return target
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)  # solo staging propio, nunca V2/copia publicada


class PostgresRepository:
    def __init__(self, settings, archive=None):
        if settings.sslmode != 'verify-full':
            raise RepositoryError('PIPELINE_VERIFY_FULL_REQUIRED')
        self.settings = settings
        self.archive = Path(archive) if archive else REPO_ROOT / 'data/raw/hu01'

    def connect(self):
        return psycopg.connect(**self.settings.connection_options(), autocommit=True,
                               prepare_threshold=None)

    @staticmethod
    def source(connection, code):
        connection.execute("INSERT INTO public.data_sources(source_code,source_format,is_synthetic) VALUES (%s,'csv',true) ON CONFLICT(source_code) DO NOTHING", (code,))
        row = connection.execute('SELECT id FROM public.data_sources WHERE source_code=%s', (code,)).fetchone()
        if not row:
            raise RepositoryError('SOURCE_NOT_VISIBLE')
        return row[0]

    @staticmethod
    def lock(connection, source, digest):
        connection.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', (str(source) + ':' + str(digest) + ':raw-v1',))

    def _persist(self, connection, spec, path, info, archived, batch_size, progress=None,
                 fail_after=None, directory_hash=None):
        """Tambien usable dentro de una transaccion reversible de integracion."""
        source = self.source(connection, spec.code)
        self.lock(connection, source, info.sha256)
        existing = connection.execute("SELECT id,status,row_count,file_sha256,archive_reference FROM public.ingest_batches WHERE source_id=%s AND file_sha256=%s AND source_schema_version='raw-v1'", (source,info.sha256)).fetchone()
        if existing and existing[1] == 'completed':
            actual = connection.execute('SELECT count(*) FROM public.raw_events WHERE batch_id=%s', (existing[0],)).fetchone()[0]
            if actual != existing[2]:
                raise RepositoryError('COMPLETED_COUNT_MISMATCH')
            return {'source':spec.code,'file':path.name,'status':'already_imported','new_rows':0,
                    'existing_rows':actual,'sha256':info.sha256,'batch_id':str(existing[0])}
        if existing:
            batch_id = existing[0]
            connection.execute('SELECT id FROM public.ingest_batches WHERE id=%s FOR UPDATE', (batch_id,))
            if connection.execute('SELECT count(*) FROM public.raw_events WHERE batch_id=%s', (batch_id,)).fetchone()[0]:
                raise RepositoryError('INTERRUPTED_BATCH_HAS_ROWS_REQUIRES_REVIEW')
        else:
            batch_id = connection.execute("INSERT INTO public.ingest_batches(source_id,source_filename,file_sha256,byte_size,archive_reference,archive_sha256,directory_sha256) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id", (source,path.name,info.sha256,info.byte_size,str(archived),info.sha256,directory_hash)).fetchone()[0]
        attempt = Jsonb([{'event':'started','at_utc':datetime.now(timezone.utc).isoformat()}])
        connection.execute("UPDATE public.ingest_batches SET status='importing',row_count=0,ingestion_error=NULL,completed_at=NULL,metadata=jsonb_set(metadata,'{attempts}',COALESCE(metadata->'attempts','[]'::jsonb) || %s) WHERE id=%s", (attempt,batch_id))
        count = 0
        for batch in read_batches(path, batch_size, spec.code):
            if batch.file.sha256 != info.sha256:
                raise RepositoryError('FILE_CHANGED_BEFORE_IMPORT')
            parameters = [(source,r.external_event_id,Jsonb(dict(r.payload)),batch_id,r.row_number,payload_sha256(r.payload)) for r in batch.records]
            with connection.cursor() as cursor:
                cursor.executemany("INSERT INTO public.raw_events(source_id,external_event_id,payload,is_synthetic,batch_id,record_number,payload_sha256) VALUES (%s,%s,%s,true,%s,%s,%s)", parameters)
            count += len(parameters)
            if progress:
                progress(spec.code, count)
            if fail_after is not None and count >= fail_after:
                raise RepositoryError('INJECTED_FAILURE')
        if file_sha256(path) != info.sha256 or file_sha256(archived) != info.sha256:
            raise RepositoryError('FILE_CHANGED')
        actual, unique, minimum, maximum = connection.execute('SELECT count(*),count(DISTINCT payload),min(record_number),max(record_number) FROM public.raw_events WHERE batch_id=%s', (batch_id,)).fetchone()
        if actual != count or (count and (minimum != 1 or maximum != count)):
            raise RepositoryError('RAW_COUNT_MISMATCH')
        done = Jsonb([{'event':'completed','at_utc':datetime.now(timezone.utc).isoformat(),'rows':count}])
        connection.execute("UPDATE public.ingest_batches SET status='completed',row_count=%s,completed_at=now(),archive_reference=%s,archive_sha256=%s,metadata=jsonb_set(metadata,'{attempts}',COALESCE(metadata->'attempts','[]'::jsonb) || %s) || %s WHERE id=%s", (count,str(archived),info.sha256,done,Jsonb({'repository_version':'hu01-v1'}),batch_id))
        return {'source':spec.code,'file':path.name,'status':'completed','new_rows':count,
                'unique_rows':unique,'duplicate_occurrences':count-unique,'sha256':info.sha256,'batch_id':str(batch_id)}

    def record_error(self, connection, spec, path, info, code):
        with connection.transaction():
            source = self.source(connection, spec.code)
            digest = info.sha256 if info else None
            self.lock(connection, source, digest)
            batch = connection.execute("SELECT id,status FROM public.ingest_batches WHERE source_id=%s AND file_sha256=%s AND source_schema_version='raw-v1'", (source,digest)).fetchone() if digest else None
            error = Jsonb({'code':code})
            attempt = Jsonb([{'event':'failed','code':code,'at_utc':datetime.now(timezone.utc).isoformat()}])
            if batch and batch[1] == 'completed':
                return  # nunca convertir exito previo en fallo ni editarlo
            if batch:
                if connection.execute('SELECT count(*) FROM public.raw_events WHERE batch_id=%s', (batch[0],)).fetchone()[0]:
                    return  # datos existentes requieren revision, no borrarlos
                connection.execute("UPDATE public.ingest_batches SET status='failed',row_count=0,ingestion_error=%s,completed_at=NULL,metadata=jsonb_set(metadata,'{attempts}',COALESCE(metadata->'attempts','[]'::jsonb) || %s) WHERE id=%s", (error,attempt,batch[0]))
            else:
                connection.execute("INSERT INTO public.ingest_batches(source_id,source_filename,file_sha256,byte_size,status,ingestion_error,metadata) VALUES (%s,%s,%s,%s,'failed',%s,%s)", (source,path.name,digest,info.byte_size if info else None,error,Jsonb({'attempts':[{'event':'failed','code':code,'at_utc':datetime.now(timezone.utc).isoformat()}]})))

    def import_file(self, spec, path, batch_size=500, progress=None, directory_hash=None):
        if isinstance(batch_size,bool) or batch_size < 1:
            raise ValueError('batch_size debe ser positivo')
        path = Path(path)
        info = None
        try:
            with self.connect() as connection:
                validate_identity(connection)
                try:
                    digest = file_sha256(path)
                    info = SourceFile(path,spec.code,digest,path.stat().st_size)
                    archived = archive_file(path,self.archive,digest)
                    with connection.transaction():
                        connection.execute("SET LOCAL lock_timeout='10s'")
                        connection.execute("SET LOCAL statement_timeout='60s'")
                        return self._persist(connection,spec,path,info,archived,batch_size,progress,directory_hash=directory_hash)
                except (IngestionError,RepositoryError,psycopg.Error,OSError) as error:
                    code = error.code if isinstance(error,IngestionError) else str(error) if isinstance(error,RepositoryError) else _error_code(error)
                    try:
                        self.record_error(connection,spec,path,info,code)
                    except psycopg.Error:
                        raise RepositoryError('IMPORT_FAILED_ERROR_RECORD_UNAVAILABLE') from None
                    raise RepositoryError(code) from None
        except psycopg.Error as error:
            raise RepositoryError(_error_code(error)) from None

    def summary(self):
        with self.connect() as connection:
            validate_identity(connection)
            rows = connection.execute("SELECT ds.source_code,count(*),count(DISTINCT r.payload) FROM public.raw_events r JOIN public.data_sources ds ON ds.id=r.source_id GROUP BY ds.source_code ORDER BY ds.source_code").fetchall()
            batches = connection.execute('SELECT status,count(*),sum(row_count) FROM public.ingest_batches GROUP BY status ORDER BY status').fetchall()
            batches = [(status,int(count),int(total)) for status,count,total in batches]
            errors = connection.execute("SELECT ingestion_error->>'code',count(*) FROM public.ingest_batches WHERE status='failed' GROUP BY ingestion_error->>'code'").fetchall()
            return {'sources':{code:{'rows':count,'unique_rows':unique,'duplicates':count-unique} for code,count,unique in rows},'batches':batches,'errors':errors}


def import_dataset(settings, directory, batch_size=500, progress=None):
    from .operational import check_operational
    if isinstance(batch_size,bool) or not isinstance(batch_size,int) or batch_size < 1:
        raise ValueError('batch_size debe ser positivo')
    check_operational(settings)  # conexion real propia y pruebas RLS reversibles antes de carga
    repository = PostgresRepository(settings)
    directory = Path(directory)
    employees = directory / 'employees_directory.csv'
    directory_hash = file_sha256(employees) if employees.is_file() else None
    if directory_hash:
        archive_file(employees,repository.archive,directory_hash)
    results = []
    for spec in SOURCES:
        results.append(repository.import_file(spec,directory/spec.filename,batch_size,progress,directory_hash))
    return {'files':results,'summary':repository.summary(),'new_rows':sum(r['new_rows'] for r in results)}
