-- HU03 aditiva. Transaccion externa obligatoria; no modificar 0001-0006.
DO $guard$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='pipeline_featurizer')
       OR EXISTS(SELECT 1 FROM public.feature_windows)
       OR current_setting('server_version_num')::integer/10000<>17 THEN
        RAISE EXCEPTION 'FEATURE_PREFLIGHT_REQUIRES_REVIEW';
    END IF;
END
$guard$;

CREATE FUNCTION public.feature_payload_valid(p jsonb, start_at timestamptz,
    end_at timestamptz, version_at text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $function$
DECLARE
    v jsonb:=p->'vector';
    m jsonb:=p->'metadata';
    k text;
    x numeric;
    n numeric;
    exposure numeric;
    local_start timestamp:=start_at AT TIME ZONE 'America/Lima';
    local_end timestamp:=end_at AT TIME ZONE 'America/Lima';
    period text;
    keys text[]:=ARRAY['operation_count','total_bytes','operations_per_hour','off_hours_count',
      'off_hours_ratio','distinct_destination_count','missing_destination_count',
      'missing_destination_ratio','download_count','upload_count','usb_copy_count'];
BEGIN
    IF p IS NULL OR jsonb_typeof(p)<>'object' OR jsonb_typeof(v)<>'object'
       OR jsonb_typeof(m)<>'object' OR jsonb_typeof(p->'audit')<>'object'
       OR (SELECT count(*) FROM jsonb_object_keys(p))<>3
       OR (SELECT count(*) FROM jsonb_object_keys(v))<>11
       OR NOT v ?& keys
       OR NOT m ?& ARRAY['partition','coverage','complete','model_eligible','exposure_minutes',
          'schema_version','input_fingerprint_sha256','context','diagnostics']
       OR NOT (p->'audit') ?& ARRAY['created_at','last_changed_at','revision','source_snapshot_sha256']
       OR end_at<=start_at
       OR version_at !~ '^hu03-v1:[a-f0-9]{64}$' THEN RETURN false; END IF;
    FOREACH k IN ARRAY keys LOOP
        IF jsonb_typeof(v->k)<>'number' THEN RETURN false; END IF;
        x:=(v->>k)::numeric;
        IF x<0 OR x::text IN ('NaN','Infinity','-Infinity') THEN RETURN false; END IF;
        IF k NOT IN ('operations_per_hour','off_hours_ratio','missing_destination_ratio')
           AND x<>trunc(x) THEN RETURN false; END IF;
    END LOOP;
    n:=(v->>'operation_count')::numeric;
    exposure:=extract(epoch FROM end_at-start_at)/60;
    IF n<1 OR exposure<=0 OR exposure>60
       OR jsonb_typeof(m->'exposure_minutes')<>'number'
       OR abs((m->>'exposure_minutes')::numeric-exposure)>0.000000001
       OR abs((v->>'operations_per_hour')::numeric-n*60/exposure)>0.000000001
       OR jsonb_typeof(m->'complete')<>'boolean'
       OR jsonb_typeof(m->'model_eligible')<>'boolean'
       OR (m->>'coverage') NOT IN ('closed','open')
       OR (m->>'schema_version') IS DISTINCT FROM 'features-v1'
       OR COALESCE(m->>'input_fingerprint_sha256','') !~ '^[a-f0-9]{64}$'
       OR jsonb_typeof(m->'context')<>'object'
       OR COALESCE(m->'context'->>'directory_sha256','') !~ '^[a-f0-9]{64}$'
       OR (m->>'complete')::boolean IS DISTINCT FROM (exposure=60)
       OR (m->>'model_eligible')::boolean IS DISTINCT FROM (exposure=60 AND m->>'coverage'='closed')
       THEN RETURN false; END IF;
    FOREACH k IN ARRAY ARRAY['off_hours_count','missing_destination_count','download_count','upload_count','usb_copy_count','distinct_destination_count'] LOOP
        IF (v->>k)::numeric>n THEN RETURN false; END IF;
    END LOOP;
    IF (v->>'distinct_destination_count')::numeric>n-(v->>'missing_destination_count')::numeric
       OR abs((v->>'off_hours_ratio')::numeric-(v->>'off_hours_count')::numeric/n)>0.000000001
       OR abs((v->>'missing_destination_ratio')::numeric-(v->>'missing_destination_count')::numeric/n)>0.000000001
       OR (v->>'off_hours_ratio')::numeric>1 OR (v->>'missing_destination_ratio')::numeric>1
       THEN RETURN false; END IF;
    IF local_start >= timestamp '2026-09-01' AND local_start < timestamp '2026-09-21'
       AND local_end<=timestamp '2026-09-21' THEN period:='train';
    ELSIF local_start>=timestamp '2026-09-21' AND local_start<timestamp '2026-09-26'
       AND local_end<=timestamp '2026-09-26' THEN period:='validation';
    ELSIF local_start>=timestamp '2026-09-26' AND local_start<timestamp '2026-10-01'
       AND local_end<=timestamp '2026-10-01' THEN period:='test';
    ELSE RETURN false; END IF;
    IF m->>'partition' IS DISTINCT FROM period
       OR date_trunc('hour',start_at AT TIME ZONE 'UTC')+interval '1 hour'
          < end_at AT TIME ZONE 'UTC' THEN RETURN false; END IF;
    IF jsonb_typeof(m->'diagnostics')<>'object'
       OR NOT (m->'diagnostics') ?& ARRAY['avg_file_size','max_file_size','transfer_frequency']
       OR (m->'context'->>'normalization_version') IS DISTINCT FROM 'hu02-v1'
       OR (m->'context'->>'feature_schema_version') IS DISTINCT FROM 'features-v1'
       OR jsonb_typeof(p->'audit'->'revision')<>'number'
       OR (p->'audit'->>'revision')::numeric<1
       OR COALESCE(p->'audit'->>'source_snapshot_sha256','') !~ '^[a-f0-9]{64}$'
       THEN RETURN false; END IF;
    FOREACH k IN ARRAY ARRAY['avg_file_size','max_file_size','transfer_frequency'] LOOP
        IF jsonb_typeof(m->'diagnostics'->k)<>'number'
           OR (m->'diagnostics'->>k)::numeric<0 THEN RETURN false; END IF;
    END LOOP;
    IF abs((m->'diagnostics'->>'avg_file_size')::numeric-(v->>'total_bytes')::numeric/n)>0.000001
       OR (m->'diagnostics'->>'max_file_size')::numeric < (m->'diagnostics'->>'avg_file_size')::numeric
       OR (m->'diagnostics'->>'max_file_size')::numeric > (v->>'total_bytes')::numeric
       OR (m->'diagnostics'->>'transfer_frequency')::numeric > (v->>'operations_per_hour')::numeric
       THEN RETURN false; END IF;
    RETURN true;
EXCEPTION WHEN OTHERS THEN RETURN false;
END
$function$;
REVOKE ALL ON FUNCTION public.feature_payload_valid(jsonb,timestamptz,timestamptz,text)
    FROM PUBLIC,anon,authenticated,service_role;

ALTER TABLE public.feature_windows
    ADD CONSTRAINT feature_window_logical_unique UNIQUE(actor_id,window_start,window_end,pipeline_version),
    ADD CONSTRAINT feature_window_payload_check CHECK
      (public.feature_payload_valid(features,window_start,window_end,pipeline_version) IS TRUE);

CREATE ROLE pipeline_featurizer NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
    NOCREATEROLE NOREPLICATION NOBYPASSRLS;
GRANT pipeline_featurizer TO pipeline_runner WITH INHERIT FALSE, SET TRUE, ADMIN FALSE;
GRANT USAGE ON SCHEMA public TO pipeline_featurizer;
GRANT SELECT ON public.normalized_events,public.actors,public.data_sources,public.ingest_batches
    TO pipeline_featurizer;
GRANT SELECT(id,source_id,batch_id,external_event_id,processing_status,duplicate_of,
    is_synthetic,processing_details,record_number) ON public.raw_events TO pipeline_featurizer;
GRANT SELECT ON public.feature_windows TO pipeline_featurizer;
GRANT INSERT(id,actor_id,window_start,window_end,features,pipeline_version,is_synthetic)
    ON public.feature_windows TO pipeline_featurizer;
GRANT UPDATE(features) ON public.feature_windows TO pipeline_featurizer;
GRANT EXECUTE ON FUNCTION public.feature_payload_valid(jsonb,timestamptz,timestamptz,text)
    TO pipeline_featurizer;

CREATE POLICY feature_sources_read ON public.data_sources FOR SELECT TO pipeline_featurizer
    USING(is_synthetic AND source_code IN ('endpoint','cloud','transfer'));
CREATE POLICY feature_batches_read ON public.ingest_batches FOR SELECT TO pipeline_featurizer
    USING(status='completed' AND EXISTS(SELECT 1 FROM public.data_sources s WHERE s.id=ingest_batches.source_id));
CREATE POLICY feature_raw_metadata_read ON public.raw_events FOR SELECT TO pipeline_featurizer
    USING(is_synthetic AND processing_status='processed' AND duplicate_of IS NULL
          AND EXISTS(SELECT 1 FROM public.ingest_batches b WHERE b.id=raw_events.batch_id));
CREATE POLICY feature_actors_read ON public.actors FOR SELECT TO pipeline_featurizer
    USING(is_synthetic AND actor_code ~ '^EMP[0-9]{3}$' AND actor_code BETWEEN 'EMP001' AND 'EMP025');
CREATE POLICY feature_normalized_read ON public.normalized_events FOR SELECT TO pipeline_featurizer
    USING(is_synthetic AND EXISTS(SELECT 1 FROM public.raw_events r WHERE r.id=normalized_events.raw_event_id)
          AND EXISTS(SELECT 1 FROM public.actors a WHERE a.id=normalized_events.actor_id));
CREATE POLICY feature_windows_read ON public.feature_windows FOR SELECT TO pipeline_featurizer
    USING(is_synthetic AND EXISTS(SELECT 1 FROM public.actors a WHERE a.id=feature_windows.actor_id));
CREATE POLICY feature_windows_insert ON public.feature_windows FOR INSERT TO pipeline_featurizer
    WITH CHECK(is_synthetic AND EXISTS(SELECT 1 FROM public.actors a WHERE a.id=feature_windows.actor_id));
CREATE POLICY feature_windows_update ON public.feature_windows FOR UPDATE TO pipeline_featurizer
    USING(is_synthetic AND EXISTS(SELECT 1 FROM public.actors a WHERE a.id=feature_windows.actor_id))
    WITH CHECK(is_synthetic AND EXISTS(SELECT 1 FROM public.actors a WHERE a.id=feature_windows.actor_id));

DO $verify$
BEGIN
    IF pg_has_role('pipeline_runner','pipeline_featurizer','USAGE')
       OR NOT pg_has_role('pipeline_runner','pipeline_featurizer','SET')
       OR has_column_privilege('pipeline_featurizer','public.raw_events','payload','SELECT')
       OR has_table_privilege('pipeline_featurizer','public.raw_events','INSERT,UPDATE,DELETE')
       OR has_table_privilege('pipeline_featurizer','public.normalized_events','INSERT,UPDATE,DELETE')
       OR has_schema_privilege('pipeline_featurizer','public','CREATE')
       OR has_table_privilege('pipeline_runner','public.feature_windows','INSERT,UPDATE,DELETE') THEN
        RAISE EXCEPTION 'FEATURE_PERMISSION_BOUNDARY_INVALID';
    END IF;
END
$verify$;
