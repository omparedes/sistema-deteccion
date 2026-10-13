-- Cuenta operativa sin secreto. Password manual fuera de este archivo.
-- Ejecutar una vez, en transaccion externa, despues de 0001-0003.
DO $guard$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pipeline_runner') THEN
        RAISE EXCEPTION 'RUNNER_ALREADY_EXISTS_REQUIRES_REVIEW';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pipeline_ingestor'
                   AND NOT rolcanlogin AND NOT rolbypassrls AND NOT rolsuper) THEN
        RAISE EXCEPTION 'INGESTOR_GROUP_NOT_MINIMAL';
    END IF;
END
$guard$;
CREATE ROLE pipeline_runner LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS PASSWORD NULL;
GRANT pipeline_ingestor TO pipeline_runner WITH INHERIT TRUE, SET FALSE, ADMIN FALSE;
DO $verify$
BEGIN
    IF NOT pg_has_role('pipeline_runner','pipeline_ingestor','USAGE')
       OR pg_has_role('pipeline_runner','pipeline_ingestor','SET')
       OR has_schema_privilege('pipeline_runner','public','CREATE')
       OR EXISTS (SELECT 1 FROM pg_class c WHERE c.relowner=(SELECT oid FROM pg_roles WHERE rolname='pipeline_runner')) THEN
        RAISE EXCEPTION 'RUNNER_PRIVILEGES_INVALID';
    END IF;
END
$verify$;
