-- Aditiva: permisos de HU02 activados explicitamente; sin passwords ni tablas nuevas.
-- Aplicar una vez en transaccion externa. No modificar 0001-0004.
DO $guard$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pipeline_normalizer') THEN
        RAISE EXCEPTION 'NORMALIZER_ALREADY_EXISTS_REQUIRES_REVIEW';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='pipeline_runner' AND rolcanlogin
                   AND NOT rolsuper AND NOT rolbypassrls AND NOT rolcreaterole) THEN
        RAISE EXCEPTION 'RUNNER_NOT_MINIMAL';
    END IF;
END
$guard$;
CREATE ROLE pipeline_normalizer NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
    NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT pipeline_normalizer TO pipeline_runner WITH INHERIT FALSE, SET TRUE, ADMIN FALSE;
GRANT USAGE ON SCHEMA public TO pipeline_normalizer;
GRANT SELECT ON public.data_sources,public.ingest_batches,public.raw_events,
    public.actors,public.normalized_events TO pipeline_normalizer;
GRANT INSERT(actor_code,role_name,department,is_synthetic) ON public.actors TO pipeline_normalizer;
GRANT INSERT(id,raw_event_id,actor_id,occurred_at,operation,device_code,session_code,
    file_extension,file_type,file_size_bytes,source_type,source_path,destination_type,
    destination_path,destination_authorized,data_classification,operation_result,is_synthetic)
    ON public.normalized_events TO pipeline_normalizer;
GRANT UPDATE(processing_status,processing_error,processing_details,duplicate_of)
    ON public.raw_events TO pipeline_normalizer;

CREATE POLICY normalizer_sources_read ON public.data_sources FOR SELECT TO pipeline_normalizer
    USING(is_synthetic AND source_code IN ('endpoint','cloud','transfer'));
CREATE POLICY normalizer_batches_read ON public.ingest_batches FOR SELECT TO pipeline_normalizer
    USING(status='completed' AND EXISTS(SELECT 1 FROM public.data_sources s WHERE s.id=ingest_batches.source_id));
CREATE POLICY normalizer_raw_read ON public.raw_events FOR SELECT TO pipeline_normalizer
    USING(is_synthetic AND EXISTS(SELECT 1 FROM public.ingest_batches b WHERE b.id=raw_events.batch_id));
CREATE POLICY normalizer_actors_read ON public.actors FOR SELECT TO pipeline_normalizer
    USING(is_synthetic AND actor_code ~ '^EMP[0-9]{3}$' AND actor_code BETWEEN 'EMP001' AND 'EMP025');
CREATE POLICY normalizer_actors_insert ON public.actors FOR INSERT TO pipeline_normalizer
    WITH CHECK(is_synthetic AND actor_code ~ '^EMP[0-9]{3}$' AND actor_code BETWEEN 'EMP001' AND 'EMP025');
CREATE POLICY normalizer_events_read ON public.normalized_events FOR SELECT TO pipeline_normalizer
    USING(is_synthetic AND EXISTS(SELECT 1 FROM public.raw_events r WHERE r.id=normalized_events.raw_event_id));
CREATE POLICY normalizer_events_insert ON public.normalized_events FOR INSERT TO pipeline_normalizer
    WITH CHECK(is_synthetic
        AND EXISTS(SELECT 1 FROM public.raw_events r WHERE r.id=normalized_events.raw_event_id
                   AND r.processing_status='pending' AND r.duplicate_of IS NULL)
        AND EXISTS(SELECT 1 FROM public.actors a WHERE a.id=normalized_events.actor_id));
CREATE POLICY normalizer_raw_update ON public.raw_events FOR UPDATE TO pipeline_normalizer
    USING(is_synthetic AND EXISTS(SELECT 1 FROM public.ingest_batches b WHERE b.id=raw_events.batch_id))
    WITH CHECK(is_synthetic AND EXISTS(SELECT 1 FROM public.ingest_batches b WHERE b.id=raw_events.batch_id)
        AND jsonb_array_length(processing_details)>0
        AND ((processing_status='processed' AND processing_error IS NULL AND duplicate_of IS NULL
              AND EXISTS(SELECT 1 FROM public.normalized_events n WHERE n.raw_event_id=raw_events.id))
          OR (processing_status='rejected' AND processing_error IS NOT NULL AND duplicate_of IS NULL
              AND NOT EXISTS(SELECT 1 FROM public.normalized_events n WHERE n.raw_event_id=raw_events.id))
          OR (processing_status='duplicate' AND duplicate_of IS NOT NULL
              AND EXISTS(SELECT 1 FROM public.raw_events canonical WHERE canonical.id=raw_events.duplicate_of
                          AND canonical.processing_status IN ('processed','rejected')))));

DO $verify$
BEGIN
    IF NOT pg_has_role('pipeline_runner','pipeline_normalizer','SET')
       OR pg_has_role('pipeline_runner','pipeline_normalizer','USAGE')
       OR has_column_privilege('pipeline_runner','public.raw_events','processing_status','UPDATE')
       OR has_column_privilege('pipeline_normalizer','public.raw_events','payload','UPDATE')
       OR has_table_privilege('pipeline_normalizer','public.raw_events','DELETE,TRUNCATE')
       OR has_schema_privilege('pipeline_normalizer','public','CREATE') THEN
        RAISE EXCEPTION 'NORMALIZER_PRIVILEGE_BOUNDARY_INVALID';
    END IF;
END
$verify$;
