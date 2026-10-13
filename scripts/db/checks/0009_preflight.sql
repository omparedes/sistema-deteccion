-- READ ONLY; run before 0009, inside the same externally managed transaction.
DO $$
BEGIN
 IF current_setting('server_version_num')::integer/10000<>17
    OR EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.raw_events'::regclass AND attname='normalization_versions' AND NOT attisdropped)
    OR EXISTS(SELECT 1 FROM public.feature_windows GROUP BY actor_id,window_start,window_end HAVING count(*)>1)
    OR EXISTS(SELECT 1 FROM public.raw_events r LEFT JOIN public.normalized_events n ON n.raw_event_id=r.id
       WHERE (r.processing_status='processed' AND n.id IS NULL) OR (r.processing_status IN ('rejected','duplicate') AND n.id IS NOT NULL))
    OR EXISTS(SELECT 1 FROM public.feature_windows WHERE public.feature_payload_valid(features,window_start,window_end,pipeline_version) IS NOT TRUE OR public.feature_audit_valid(features) IS NOT TRUE)
    OR EXISTS(SELECT 1 FROM pg_roles WHERE rolname IN ('pipeline_runner','pipeline_normalizer','pipeline_featurizer') AND (rolsuper OR rolbypassrls OR rolcreatedb OR rolcreaterole)) THEN
    RAISE EXCEPTION 'REVISION_PREFLIGHT_REQUIRES_REVIEW';
 END IF;
END;
$$;
