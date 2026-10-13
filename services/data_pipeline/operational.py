"""Identidad y permisos de la conexion operativa; fixtures siempre revertidas."""
import uuid
import psycopg
from .database_check import DatabaseCheckError, _error_code


def validate_identity(connection):
    if not connection.pgconn.ssl_in_use:
        raise DatabaseCheckError('PIPELINE_TLS_REQUIRED')
    row = connection.execute("SELECT current_user,session_user,rolsuper,rolbypassrls,rolcreatedb,rolcreaterole,rolreplication FROM pg_roles WHERE rolname=current_user").fetchone()
    if not row or row[:2] != ('pipeline_runner', 'pipeline_runner') or any(row[2:]):
        raise DatabaseCheckError('PIPELINE_ROLE_NOT_MINIMAL')
    memberships = connection.execute("SELECT r.rolname,m.inherit_option,m.set_option,m.admin_option FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.roleid WHERE m.member=(SELECT oid FROM pg_roles WHERE rolname=current_user)").fetchall()
    allowed = {('pipeline_ingestor',True,False,False)}
    observed = set(memberships)
    normalized = allowed | {('pipeline_normalizer',False,True,False)}
    if observed not in (allowed, normalized,
                       normalized | {('pipeline_featurizer',False,True,False)}):
        raise DatabaseCheckError('PIPELINE_MEMBERSHIP_INVALID')
    if connection.execute("SELECT has_database_privilege(current_user,current_database(),'CREATE') OR EXISTS(SELECT 1 FROM pg_roles r WHERE (r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolname IN ('postgres','service_role','supabase_admin')) AND pg_has_role(current_user,r.oid,'MEMBER'))").fetchone()[0]:
        raise DatabaseCheckError('PIPELINE_ADMIN_INHERITANCE_FORBIDDEN')
    if connection.execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname NOT LIKE 'pg_%' AND has_schema_privilege(current_user,oid,'CREATE')) OR EXISTS(SELECT 1 FROM pg_class WHERE relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))").fetchone()[0]:
        raise DatabaseCheckError('PIPELINE_SCHEMA_PRIVILEGES_EXCESSIVE')


def check_operational(settings):
    if settings.sslmode != 'verify-full':
        raise DatabaseCheckError('PIPELINE_VERIFY_FULL_REQUIRED')
    connection = None
    try:
        connection = psycopg.connect(**settings.connection_options(), prepare_threshold=None)
        validate_identity(connection)
        checks = []

        def deny(name, statement, parameters=None):
            state = None
            try:
                with connection.transaction():
                    connection.execute(statement, parameters)
            except psycopg.Error as error:
                state = error.sqlstate
            if state != '42501':
                raise DatabaseCheckError('PIPELINE_EXPECTED_DENIAL_MISSING')
            checks.append(name)

        connection.execute("SET LOCAL statement_timeout='20s'")
        connection.execute("INSERT INTO public.data_sources(source_code,source_format) VALUES ('endpoint','csv') ON CONFLICT(source_code) DO NOTHING")
        source = connection.execute("SELECT id FROM public.data_sources WHERE source_code='endpoint'").fetchone()[0]
        digest = uuid.uuid4().hex * 2
        batch = connection.execute("INSERT INTO public.ingest_batches(source_id,source_filename,file_sha256,byte_size) VALUES (%s,'security-fixture.csv',%s,0) RETURNING id", (source,digest)).fetchone()[0]
        connection.execute("UPDATE public.ingest_batches SET status='importing' WHERE id=%s", (batch,))
        event = connection.execute("INSERT INTO public.raw_events(source_id,batch_id,record_number,payload,payload_sha256) VALUES (%s,%s,1,'{}',%s) RETURNING id", (source,batch,digest)).fetchone()[0]
        checks.append('allowed_source_batch_raw_insert')
        deny('raw_update_denied', "UPDATE public.raw_events SET payload='{}' WHERE id=%s", (event,))
        deny('raw_delete_denied', 'DELETE FROM public.raw_events WHERE id=%s', (event,))
        deny('future_tables_denied', 'SELECT * FROM public.actors LIMIT 1')
        deny('persistent_schema_creation_denied', 'CREATE TABLE public.pipeline_security_fixture(id integer)')
        deny('unapproved_source_rls_denied', "INSERT INTO public.data_sources(source_code,source_format) VALUES ('unapproved','csv')")
        connection.execute("UPDATE public.ingest_batches SET status='completed',row_count=1,archive_reference='fixture',archive_sha256=%s,completed_at=now() WHERE id=%s", (digest,batch))
        if connection.execute('UPDATE public.ingest_batches SET row_count=2 WHERE id=%s', (batch,)).rowcount != 0:
            raise DatabaseCheckError('PIPELINE_COMPLETED_NOT_IMMUTABLE')
        checks.append('completed_batch_rls_immutable')
        for role in ('anon','authenticated'):
            if connection.execute("SELECT has_table_privilege(%s,'public.raw_events','SELECT,INSERT,UPDATE,DELETE') OR has_any_column_privilege(%s,'public.raw_events','SELECT,INSERT,UPDATE,REFERENCES')", (role,role)).fetchone()[0]:
                raise DatabaseCheckError('PIPELINE_PUBLIC_ACCESS_REMAINING')
        connection.rollback()
        if connection.execute('SELECT EXISTS(SELECT 1 FROM public.ingest_batches WHERE id=%s)', (batch,)).fetchone()[0]:
            raise DatabaseCheckError('PIPELINE_FIXTURE_ROLLBACK_FAILED')
        return {'operational_connection':'ok','current_user':'pipeline_runner','session_user':'pipeline_runner',
                'sslmode':'verify-full','bypass_rls':False,'checks':checks,'fixtures_rolled_back':True}
    except psycopg.Error as error:
        raise DatabaseCheckError(_error_code(error)) from None
    finally:
        if connection is not None:
            connection.rollback()
            connection.close()
