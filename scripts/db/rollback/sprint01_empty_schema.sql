-- PREPARADO, NO EJECUTADO. Reversión poscommit SOLO para esquema fresco VACÍO
-- del bundle 0001/0002/0003, con aprobación y evidencia de aplicación propia.
-- Usar transacción externa; no CASCADE, no truncado ni borrado de datos.
LOCK TABLE public.data_sources, public.actors, public.raw_events,
    public.normalized_events, public.feature_windows, public.model_versions,
    public.detection_results, public.alerts, public.alert_reviews,
    public.ingest_batches IN ACCESS EXCLUSIVE MODE;

DO $rollback_guard$
DECLARE
    names text[] := ARRAY['data_sources','actors','raw_events','normalized_events',
        'feature_windows','model_versions','detection_results','alerts','alert_reviews','ingest_batches'];
    t text;
    has_rows boolean;
BEGIN
    IF (SELECT count(*) FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relname=ANY(names) AND c.relkind='r'
          AND pg_catalog.pg_get_userbyid(c.relowner)=current_user) <> 10 THEN
        RAISE EXCEPTION 'ROLLBACK_BASELINE_OR_OWNER_MISMATCH';
    END IF;
    FOREACH t IN ARRAY names LOOP
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I)',t) INTO has_rows;
        IF has_rows THEN
            RAISE EXCEPTION 'ROLLBACK_REFUSED_DATA_PRESENT';
        END IF;
    END LOOP;
    -- PostgreSQL puede dar membresía ADMIN al creador no-superuser del rol.
    -- Solo esa cuenta administrativa propietaria puede ser miembro al revertir;
    -- cualquier miembro operativo/externo obliga a revisar y abortar.
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='pipeline_ingestor' AND NOT rolcanlogin)
       OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r ON r.oid=m.roleid
                   WHERE r.rolname='pipeline_ingestor'
                     AND m.member <> (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user)) THEN
        RAISE EXCEPTION 'ROLLBACK_ROLE_BASELINE_OR_MEMBERSHIP_MISMATCH';
    END IF;
END
$rollback_guard$;

DROP TABLE public.alert_reviews;
DROP TABLE public.alerts;
DROP TABLE public.detection_results;
DROP TABLE public.model_versions;
DROP TABLE public.feature_windows;
DROP TABLE public.normalized_events;
DROP TABLE public.raw_events;
DROP TABLE public.ingest_batches;
DROP TABLE public.actors;
DROP TABLE public.data_sources;
-- Retirar solamente el grant de esquema creado por 0003; no alterar PUBLIC.
REVOKE USAGE ON SCHEMA public FROM pipeline_ingestor;
DROP ROLE pipeline_ingestor;
-- No borrar extensiones ni roles/esquemas managed. Dependencias externas
-- provocarán error por RESTRICT implícito y rollback del bundle externo.
