-- PREPARADO, NO EJECUTADO. Baseline fresco F2-B1: public sin tablas proyecto.
-- Ejecutar antes de 0001 dentro de la transacción externa autorizada.
DO $preflight$
DECLARE
    names text[] := ARRAY['data_sources','actors','raw_events','normalized_events',
        'feature_windows','model_versions','detection_results','alerts','alert_reviews','ingest_batches'];
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = ANY(names)) THEN
        RAISE EXCEPTION 'PROJECT_NAME_CONFLICT_REQUIRES_REVIEW';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'pipeline_ingestor') THEN
        RAISE EXCEPTION 'INGEST_ROLE_ALREADY_EXISTS_REQUIRES_REVIEW';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=current_user AND (rolcreaterole OR rolsuper))
       OR NOT has_schema_privilege(current_user, 'public', 'CREATE') THEN
        RAISE EXCEPTION 'ADMIN_DDL_PERMISSION_REQUIRED';
    END IF;
    IF (SELECT count(*) FROM pg_catalog.pg_roles
        WHERE rolname IN ('anon','authenticated','service_role')) <> 3 THEN
        RAISE EXCEPTION 'SUPABASE_API_ROLE_BASELINE_MISSING';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname='pgcrypto') THEN
        RAISE EXCEPTION 'MANAGED_PGCRYPTO_BASELINE_REQUIRES_REVIEW';
    END IF;
END
$preflight$;
