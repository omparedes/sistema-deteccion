-- Correccion de recursion RLS de 0005. Sin permisos adicionales ni perdida de datos.
-- Relaciones de resultado se validan al COMMIT, fuera de expansion recursiva de policies.
ALTER POLICY normalizer_raw_update ON public.raw_events
    WITH CHECK(is_synthetic AND EXISTS(SELECT 1 FROM public.ingest_batches b WHERE b.id=raw_events.batch_id)
        AND jsonb_array_length(processing_details)>0
        AND ((processing_status='processed' AND processing_error IS NULL AND duplicate_of IS NULL)
          OR (processing_status='rejected' AND processing_error IS NOT NULL AND duplicate_of IS NULL)
          OR (processing_status='duplicate' AND duplicate_of IS NOT NULL)));

CREATE FUNCTION public.enforce_raw_normalization_result() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $function$
DECLARE
    actual public.raw_events%ROWTYPE;
    canonical public.raw_events%ROWTYPE;
    normal_count bigint;
BEGIN
    -- HU01 inserta pending sin necesidad de acceso a normalized_events.
    IF NEW.processing_status='pending' THEN RETURN NEW; END IF;
    SELECT * INTO actual FROM public.raw_events WHERE id=NEW.id;
    IF NOT FOUND THEN RAISE EXCEPTION 'RAW_RESULT_NOT_VISIBLE' USING ERRCODE='23514'; END IF;
    SELECT count(*) INTO normal_count FROM public.normalized_events WHERE raw_event_id=actual.id;
    IF (actual.processing_status='processed' AND normal_count<>1)
       OR (actual.processing_status IN ('rejected','duplicate') AND normal_count<>0) THEN
        RAISE EXCEPTION 'RAW_NORMALIZED_RESULT_MISMATCH' USING ERRCODE='23514';
    END IF;
    IF actual.processing_status='duplicate' THEN
        SELECT * INTO canonical FROM public.raw_events WHERE id=actual.duplicate_of;
        IF NOT FOUND OR canonical.processing_status NOT IN ('processed','rejected')
           OR canonical.source_id<>actual.source_id OR canonical.payload<>actual.payload
           OR canonical.external_event_id IS DISTINCT FROM actual.external_event_id THEN
            RAISE EXCEPTION 'RAW_DUPLICATE_RESULT_MISMATCH' USING ERRCODE='23514';
        END IF;
    END IF;
    RETURN NEW;
END
$function$;
REVOKE ALL ON FUNCTION public.enforce_raw_normalization_result()
    FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.enforce_raw_normalization_result()
    TO pipeline_ingestor,pipeline_normalizer;
CREATE CONSTRAINT TRIGGER raw_normalization_result_integrity
    AFTER INSERT OR UPDATE ON public.raw_events
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
    EXECUTE FUNCTION public.enforce_raw_normalization_result();
