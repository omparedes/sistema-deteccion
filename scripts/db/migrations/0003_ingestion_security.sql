-- PREPARADA, NO APLICADA. Rol de permisos, NO cuenta de conexión ni contraseña.
-- Requiere 0001/0002 en la misma transacción externa autorizada.
-- No alterar roles/esquemas/objetos administrados ni defaults globales Supabase.

CREATE ROLE pipeline_ingestor NOLOGIN INHERIT NOSUPERUSER NOCREATEDB
    NOCREATEROLE NOREPLICATION NOBYPASSRLS;

REVOKE ALL PRIVILEGES ON TABLE
    public.data_sources, public.actors, public.raw_events,
    public.normalized_events, public.feature_windows, public.model_versions,
    public.detection_results, public.alerts, public.alert_reviews,
    public.ingest_batches
    FROM PUBLIC, anon, authenticated, service_role;

GRANT USAGE ON SCHEMA public TO pipeline_ingestor;
GRANT SELECT ON TABLE public.data_sources, public.ingest_batches, public.raw_events
    TO pipeline_ingestor;
GRANT INSERT (source_code, description, source_format, is_synthetic)
    ON public.data_sources TO pipeline_ingestor;
GRANT INSERT (source_id, source_filename, file_sha256, source_schema_version,
    byte_size, archive_reference, archive_sha256, directory_sha256, metadata,
    status, row_count, ingestion_error, completed_at)
    ON public.ingest_batches TO pipeline_ingestor;
GRANT UPDATE (status, row_count, ingestion_error, completed_at,
    archive_reference, archive_sha256, metadata)
    ON public.ingest_batches TO pipeline_ingestor;
GRANT INSERT (source_id, external_event_id, payload, is_synthetic,
    batch_id, record_number, payload_sha256)
    ON public.raw_events TO pipeline_ingestor;

CREATE POLICY ingest_sources_read ON public.data_sources
    FOR SELECT TO pipeline_ingestor
    USING (is_synthetic AND source_code IN ('endpoint', 'cloud', 'transfer'));
CREATE POLICY ingest_sources_insert ON public.data_sources
    FOR INSERT TO pipeline_ingestor
    WITH CHECK (is_synthetic AND source_format = 'csv'
                AND source_code IN ('endpoint', 'cloud', 'transfer'));

CREATE POLICY ingest_batches_read ON public.ingest_batches
    FOR SELECT TO pipeline_ingestor
    USING (EXISTS (SELECT 1 FROM public.data_sources ds
                   WHERE ds.id = ingest_batches.source_id AND ds.is_synthetic
                     AND ds.source_code IN ('endpoint', 'cloud', 'transfer')));
CREATE POLICY ingest_batches_insert ON public.ingest_batches
    FOR INSERT TO pipeline_ingestor
    WITH CHECK (status IN ('pending', 'failed') AND row_count = 0
                AND EXISTS (SELECT 1 FROM public.data_sources ds
                            WHERE ds.id = ingest_batches.source_id AND ds.is_synthetic
                              AND ds.source_code IN ('endpoint', 'cloud', 'transfer')));
CREATE POLICY ingest_batches_update ON public.ingest_batches
    FOR UPDATE TO pipeline_ingestor
    USING (status IN ('pending', 'importing', 'failed')
           AND EXISTS (SELECT 1 FROM public.data_sources ds
                       WHERE ds.id = ingest_batches.source_id AND ds.is_synthetic
                         AND ds.source_code IN ('endpoint', 'cloud', 'transfer')))
    WITH CHECK (EXISTS (SELECT 1 FROM public.data_sources ds
                       WHERE ds.id = ingest_batches.source_id AND ds.is_synthetic
                         AND ds.source_code IN ('endpoint', 'cloud', 'transfer')));

CREATE POLICY ingest_raw_read ON public.raw_events
    FOR SELECT TO pipeline_ingestor
    USING (is_synthetic AND batch_id IS NOT NULL
           AND EXISTS (SELECT 1 FROM public.data_sources ds
                       WHERE ds.id = raw_events.source_id AND ds.is_synthetic
                         AND ds.source_code IN ('endpoint', 'cloud', 'transfer')));
CREATE POLICY ingest_raw_insert ON public.raw_events
    FOR INSERT TO pipeline_ingestor
    WITH CHECK (is_synthetic AND batch_id IS NOT NULL AND record_number IS NOT NULL
                AND payload_sha256 IS NOT NULL AND processing_status = 'pending'
                AND processing_error IS NULL AND duplicate_of IS NULL
                AND jsonb_array_length(processing_details) = 0
                AND EXISTS (SELECT 1 FROM public.ingest_batches b
                            WHERE b.id = raw_events.batch_id
                              AND b.source_id = raw_events.source_id
                              AND b.status = 'importing'));

-- Sin UPDATE/DELETE/TRUNCATE en RAW ni acceso a tablas de fases posteriores.
-- El resto de tablas mantiene RLS sin policies runtime (deny por defecto).
-- No se concede membresía a authenticator/anon/authenticated/service_role.
-- Login/membresía operativa se provisionará aparte SOLO con autorización.
-- No REVOKE global CONNECT, USAGE ni EXECUTE sobre objetos administrados.
