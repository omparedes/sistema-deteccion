-- PREPARADO, NO EJECUTADO. Antes del COMMIT del esquema, todavía sin datos.
DO $postflight$
DECLARE
    names text[] := ARRAY['data_sources','actors','raw_events','normalized_events',
        'feature_windows','model_versions','detection_results','alerts','alert_reviews','ingest_batches'];
    t text;
    has_rows boolean;
BEGIN
    IF (SELECT count(*) FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relname=ANY(names) AND c.relkind='r' AND c.relrowsecurity) <> 10 THEN
        RAISE EXCEPTION 'PROJECT_TABLES_OR_RLS_INCOMPLETE';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='pipeline_ingestor'
                   AND NOT rolcanlogin AND NOT rolsuper AND NOT rolbypassrls
                   AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication) THEN
        RAISE EXCEPTION 'INGEST_ROLE_NOT_MINIMAL';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_roles r ON r.oid=c.relowner
               JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname='public' AND c.relname=ANY(names) AND r.rolname='pipeline_ingestor') THEN
        RAISE EXCEPTION 'INGEST_ROLE_MUST_NOT_OWN_TABLES';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles r CROSS JOIN unnest(names) t(name)
               WHERE r.rolname IN ('anon','authenticated','service_role')
                 AND (has_table_privilege(r.oid, format('public.%I',t.name),'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                      OR has_any_column_privilege(r.oid,format('public.%I',t.name),'SELECT,INSERT,UPDATE,REFERENCES'))) THEN
        RAISE EXCEPTION 'API_PRIVILEGES_REMAIN';
    END IF;
    IF NOT has_column_privilege('pipeline_ingestor','public.raw_events','payload','INSERT')
       OR has_column_privilege('pipeline_ingestor','public.raw_events','payload','UPDATE')
       OR has_table_privilege('pipeline_ingestor','public.raw_events','DELETE,TRUNCATE')
       OR has_table_privilege('pipeline_ingestor','public.actors','SELECT,INSERT,UPDATE,DELETE') THEN
        RAISE EXCEPTION 'INGEST_PRIVILEGE_BOUNDARY_INVALID';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_policies
               WHERE schemaname='public' AND tablename=ANY(names)
                 AND roles <> ARRAY['pipeline_ingestor']::name[]) THEN
        RAISE EXCEPTION 'UNEXPECTED_POLICY_ROLE';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_policies
        WHERE schemaname='public' AND tablename=ANY(names)) <> 7 THEN
        RAISE EXCEPTION 'INGEST_POLICIES_INCOMPLETE';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid='public.raw_events'::regclass AND conname='raw_batch_source_fk'
                     AND contype='f' AND convalidated AND array_length(conkey,1)=2)
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                      WHERE conrelid='public.raw_events'::regclass AND conname='raw_batch_row_unique' AND contype='u') THEN
        RAISE EXCEPTION 'TRACEABILITY_CONSTRAINTS_MISSING';
    END IF;
    FOREACH t IN ARRAY names LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I)',t) INTO has_rows;
        IF has_rows THEN
            RAISE EXCEPTION 'SCHEMA_PHASE_MUST_NOT_LOAD_ROWS';
        END IF;
    END LOOP;
END
$postflight$;
