-- PREPARADA, NO APLICADA. Requiere 0001; aplicar junto con 0003 y checks
-- en UNA transacción externa autorizada. No ejecutar por glob de *.sql.
-- La versión .pending.sql se conserva solo como propuesta histórica.

CREATE TABLE public.ingest_batches (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL REFERENCES public.data_sources(id),
    source_filename text NOT NULL CHECK (length(source_filename) > 0),
    file_sha256 text CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
    source_schema_version text NOT NULL DEFAULT 'raw-v1'
        CHECK (length(source_schema_version) > 0),
    byte_size bigint CHECK (byte_size >= 0),
    archive_reference text CHECK (length(archive_reference) > 0),
    archive_sha256 text CHECK (archive_sha256 ~ '^[0-9a-f]{64}$'),
    directory_sha256 text CHECK (directory_sha256 ~ '^[0-9a-f]{64}$'),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(metadata) = 'object'),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'importing', 'completed', 'failed')),
    row_count bigint NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    ingestion_error jsonb CHECK (jsonb_typeof(ingestion_error) = 'object'),
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    CONSTRAINT ingest_file_identity UNIQUE (source_id, file_sha256, source_schema_version),
    CONSTRAINT ingest_batch_source_identity UNIQUE (id, source_id),
    -- Sin huella solo se admite un intento fallido antes de poder leer/hash.
    -- Reintento con archivo disponible crea/recupera lote con identidad por SHA.
    CONSTRAINT ingest_hash_required CHECK (
        status = 'failed' OR (file_sha256 IS NOT NULL AND byte_size IS NOT NULL)),
    CONSTRAINT ingest_completed_valid CHECK (
        status <> 'completed' OR
        (archive_reference IS NOT NULL AND archive_sha256 IS NOT NULL
         AND archive_sha256 = file_sha256 AND completed_at IS NOT NULL
         AND ingestion_error IS NULL)),
    CONSTRAINT ingest_failed_valid CHECK (
        status <> 'failed' OR
        (ingestion_error IS NOT NULL AND row_count = 0 AND completed_at IS NULL)),
    CONSTRAINT ingest_completed_time_valid CHECK (
        completed_at IS NULL OR (status = 'completed' AND completed_at >= created_at))
);

ALTER TABLE public.raw_events
    ADD COLUMN batch_id uuid,
    ADD COLUMN record_number bigint,
    ADD COLUMN payload_sha256 text,
    ADD COLUMN duplicate_of uuid,
    ADD COLUMN processing_details jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD CONSTRAINT raw_batch_row_valid CHECK (
        (batch_id IS NULL AND record_number IS NULL) OR
        (batch_id IS NOT NULL AND record_number IS NOT NULL AND record_number > 0)),
    ADD CONSTRAINT raw_payload_sha_valid CHECK (
        payload_sha256 IS NULL OR payload_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT raw_new_row_hash_required CHECK (
        batch_id IS NULL OR payload_sha256 IS NOT NULL),
    ADD CONSTRAINT raw_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    ADD CONSTRAINT raw_processing_details_array CHECK (jsonb_typeof(processing_details) = 'array'),
    ADD CONSTRAINT raw_duplicate_not_self CHECK (duplicate_of IS NULL OR duplicate_of <> id),
    ADD CONSTRAINT raw_batch_row_unique UNIQUE (batch_id, record_number),
    ADD CONSTRAINT raw_event_source_identity UNIQUE (id, source_id),
    ADD CONSTRAINT raw_batch_source_fk FOREIGN KEY (batch_id, source_id)
        REFERENCES public.ingest_batches(id, source_id),
    ADD CONSTRAINT raw_duplicate_source_fk FOREIGN KEY (duplicate_of, source_id)
        REFERENCES public.raw_events(id, source_id);

CREATE INDEX idx_raw_source_external_event
    ON public.raw_events(source_id, external_event_id);

ALTER TABLE public.ingest_batches ENABLE ROW LEVEL SECURITY;
-- NO UNIQUE por external_event_id/payload_sha256: las copias RAW se conservan.
-- Las FK no sustituyen reconciliación de conteos/archivo/transacciones.
