-- PREPARED ONLY. External transaction, preflight and postflight required.
-- No operational backfill, no RAW payload/hash updates, no new tables.
ALTER TABLE public.raw_events
 ADD COLUMN normalization_revision integer NOT NULL DEFAULT 1 CHECK(normalization_revision>=1),
 ADD COLUMN normalization_rules_version text NOT NULL DEFAULT 'hu02-v1' CHECK(normalization_rules_version ~ '^hu02-v[1-9][0-9]*$'),
 ADD COLUMN normalization_versions jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(normalization_versions)='array'),
 ADD COLUMN pending_feature_scopes jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(pending_feature_scopes)='array');
ALTER TABLE public.feature_windows
 ADD COLUMN lifecycle text NOT NULL DEFAULT 'active' CHECK(lifecycle IN ('active','archived')),
 ADD COLUMN retirement_history jsonb NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(retirement_history)='array'),
 ADD COLUMN invalidated_by uuid;
CREATE UNIQUE INDEX feature_active_logical_unique ON public.feature_windows(actor_id,window_start,window_end) WHERE lifecycle='active';
CREATE VIEW public.active_feature_windows WITH (security_invoker=true) AS SELECT * FROM public.feature_windows WHERE lifecycle='active';
REVOKE ALL ON public.active_feature_windows FROM PUBLIC,anon,authenticated,service_role;
GRANT SELECT ON public.active_feature_windows TO pipeline_featurizer;
GRANT SELECT(normalization_revision,normalization_rules_version) ON public.raw_events TO pipeline_featurizer;

CREATE OR REPLACE FUNCTION public.feature_payload_valid(p jsonb, start_at timestamptz,
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
       OR COALESCE(m->'context'->>'normalization_version','') NOT IN ('hu02-v1','active-results-v1')
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

-- Pure representation helper: preserves optional NULLs and absent optional fields.
CREATE FUNCTION public.normalization_dto(n jsonb,actor text,source text,external_id text,raw_ref text)
RETURNS jsonb LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
 SELECT CASE WHEN n IS NULL THEN NULL ELSE jsonb_build_object(
 'event_id',n->>'id','source',source,'source_event_id',external_id,'timestamp',n->>'occurred_at',
 'user_id',actor,'operation',n->>'operation','file_size_bytes',n->'file_size_bytes','is_synthetic',n->'is_synthetic',
 'device_id',n->'device_code','file_extension',n->'file_extension','source_type',n->'source_type',
 'source_path',n->'source_path','destination_type',n->'destination_type','destination_path',n->'destination_path',
 'destination_authorized',n->'destination_authorized','data_classification',n->'data_classification',
 'operation_result',n->'operation_result','raw_event_reference',raw_ref)
 || CASE WHEN n->>'session_code' IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('session_id',n->'session_code') END
 || CASE WHEN n->>'file_type' IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('file_type',n->'file_type') END END;
$$;

CREATE FUNCTION public.normalization_revision_request_valid(q jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE e jsonb:=q->'normalized_event'; k text; t timestamptz; x numeric;
BEGIN
 IF jsonb_typeof(q) IS DISTINCT FROM 'object'
    OR NOT q ?& ARRAY['request_id','expected_revision','reason','rules_version','directory_sha256','status','normalized_event','audit']
    OR EXISTS(SELECT 1 FROM jsonb_object_keys(q) k WHERE k NOT IN ('request_id','expected_revision','reason','rules_version','directory_sha256','status','normalized_event','audit','field_states','temporal'))
    OR jsonb_typeof(q->'expected_revision') IS DISTINCT FROM 'number'
    OR (q->>'expected_revision')::numeric NOT BETWEEN 1 AND 2147483646
    OR (q->>'expected_revision')::numeric<>trunc((q->>'expected_revision')::numeric)
    OR COALESCE(q->>'expected_revision','') !~ '^[1-9][0-9]*$'
    OR jsonb_typeof(q->'reason') IS DISTINCT FROM 'string' OR length(btrim(q->>'reason')) NOT BETWEEN 1 AND 2000
    OR COALESCE(q->>'rules_version','') !~ '^hu02-v[1-9][0-9]*$'
    OR COALESCE(q->>'directory_sha256','') !~ '^[a-f0-9]{64}$'
    OR jsonb_typeof(q->'audit') IS DISTINCT FROM 'array' OR jsonb_array_length(q->'audit')=0
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(q->'audit') a WHERE jsonb_typeof(a) IS DISTINCT FROM 'object'
         OR jsonb_typeof(a->'code') IS DISTINCT FROM 'string' OR length(btrim(a->>'code'))=0 OR COALESCE(a->>'severity','') NOT IN ('info','warning','error')) THEN RETURN false; END IF;
 PERFORM (q->>'request_id')::uuid;
 IF q->>'status'='rejected' THEN
    RETURN e='null'::jsonb AND EXISTS(SELECT 1 FROM jsonb_array_elements(q->'audit') a WHERE a->>'severity'='error');
 END IF;
 IF q->>'status' IS DISTINCT FROM 'processed' OR jsonb_typeof(e) IS DISTINCT FROM 'object'
    OR NOT e ?& ARRAY['event_id','source','source_event_id','timestamp','user_id','operation','file_size_bytes','is_synthetic','raw_event_reference']
    OR EXISTS(SELECT 1 FROM jsonb_array_elements(q->'audit') a WHERE a->>'severity'='error')
    OR EXISTS(SELECT 1 FROM jsonb_object_keys(e) k WHERE k NOT IN ('event_id','source','source_event_id','timestamp','user_id','operation','file_size_bytes','is_synthetic','raw_event_reference','device_id','session_id','file_extension','file_type','source_type','source_path','destination_type','destination_path','destination_authorized','data_classification','operation_result'))
    OR e->'is_synthetic' IS DISTINCT FROM 'true'::jsonb
    OR jsonb_typeof(e->'file_size_bytes') IS DISTINCT FROM 'number'
    OR COALESCE(e->>'operation','') NOT IN ('file_access','file_download','file_modify','file_copy','web_upload','cloud_sync','email_attachment','usb_copy','other') THEN RETURN false; END IF;
 FOREACH k IN ARRAY ARRAY['event_id','source','source_event_id','timestamp','user_id','raw_event_reference'] LOOP
    IF jsonb_typeof(e->k) IS DISTINCT FROM 'string' OR length(e->>k)=0 THEN RETURN false; END IF;
 END LOOP;
 FOREACH k IN ARRAY ARRAY['device_id','session_id','file_extension','file_type','source_type','source_path','destination_type','destination_path','data_classification','operation_result'] LOOP
    IF e ? k AND jsonb_typeof(e->k) NOT IN ('string','null') THEN RETURN false; END IF;
 END LOOP;
 IF e ? 'destination_authorized' AND jsonb_typeof(e->'destination_authorized') NOT IN ('boolean','null') THEN RETURN false; END IF;
 PERFORM (e->>'event_id')::uuid;
 IF (e->>'timestamp') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})$' THEN RETURN false; END IF;
 IF (e->>'file_size_bytes') !~ '^(0|[1-9][0-9]*)$' THEN RETURN false; END IF;
 t:=(e->>'timestamp')::timestamptz; x:=(e->>'file_size_bytes')::numeric;
 RETURN isfinite(t) AND t >= timestamptz '2026-09-01 05:00Z' AND t < timestamptz '2026-10-01 05:00Z'
    AND x>=0 AND x<=9223372036854775807 AND x=trunc(x);
EXCEPTION WHEN OTHERS THEN RETURN false;
END;
$$;

-- Narrow privileged boundary; no arbitrary SQL, no operational table UPDATE/DELETE grants.
CREATE FUNCTION public.revise_normalization(raw_id uuid,q jsonb,prepared jsonb) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r public.raw_events%ROWTYPE; n public.normalized_events%ROWTYPE; s text; ds text; ref text;
 old_dto jsonb; e jsonb; new_sql jsonb; actor uuid; prior jsonb; entry jsonb; versions jsonb; scopes jsonb:='[]';
 item jsonb; w public.feature_windows%ROWTYPE; at_value timestamptz:=clock_timestamp(); rid uuid;
BEGIN
 IF session_user<>'pipeline_runner' OR current_setting('role')<>'pipeline_normalizer' THEN
    RAISE EXCEPTION 'REVISION_ROLE_REQUIRED' USING ERRCODE='42501'; END IF;
 IF public.normalization_revision_request_valid(q) IS NOT TRUE THEN RAISE EXCEPTION 'INVALID_REVISION_REQUEST' USING ERRCODE='23514'; END IF;
 rid:=(q->>'request_id')::uuid;
 PERFORM pg_advisory_xact_lock(hashtext('hu02-normalization-v1'));
 PERFORM pg_advisory_xact_lock(hashtext('hu03-features-v1'));
 SELECT * INTO STRICT r FROM public.raw_events WHERE id=raw_id FOR UPDATE;
 SELECT source_code INTO s FROM public.data_sources WHERE id=r.source_id AND is_synthetic AND source_code IN ('endpoint','cloud','transfer');
 SELECT directory_sha256 INTO ds FROM public.ingest_batches WHERE id=r.batch_id AND status='completed' AND source_id=r.source_id;
 IF NOT r.is_synthetic OR r.duplicate_of IS NOT NULL OR r.processing_status NOT IN ('processed','rejected') OR s IS NULL OR ds IS DISTINCT FROM q->>'directory_sha256' THEN
    RAISE EXCEPTION 'REVISION_SUBJECT_NOT_AUTHORIZED' USING ERRCODE='23514'; END IF;
 FOR prior IN SELECT value FROM jsonb_array_elements(r.normalization_versions) LOOP
    IF prior->>'request_id'=rid::text THEN
      IF prior->'request' IS DISTINCT FROM q THEN RAISE EXCEPTION 'REVISION_REQUEST_REUSED' USING ERRCODE='23514'; END IF;
      RETURN jsonb_build_object('changed',false,'revision',prior->'revision','active_revision',r.normalization_revision,'pending_scopes',r.pending_feature_scopes);
    END IF;
 END LOOP;
 IF r.normalization_revision<>(q->>'expected_revision')::integer THEN RAISE EXCEPTION 'NORMALIZATION_REVISION_CONFLICT' USING ERRCODE='40001'; END IF;
 SELECT * INTO n FROM public.normalized_events WHERE raw_event_id=r.id FOR UPDATE;
 IF (r.processing_status='processed')<>(n.id IS NOT NULL) THEN RAISE EXCEPTION 'ACTIVE_RESULT_INCONSISTENT'; END IF;
 SELECT actor_code INTO s FROM public.actors WHERE id=n.actor_id;
 SELECT 'csv-sha256:'||file_sha256||':'||r.record_number INTO ref FROM public.ingest_batches WHERE id=r.batch_id;
 old_dto:=public.normalization_dto(CASE WHEN n.id IS NULL THEN NULL ELSE to_jsonb(n) END,s,
    (SELECT source_code FROM public.data_sources WHERE id=r.source_id),r.external_event_id,ref);
 e:=prepared->'entry'->'result'->'normalized_event';
 IF q->>'status'='processed' THEN
    IF e IS NULL OR (e-'event_id') IS DISTINCT FROM (q->'normalized_event'-'event_id')
       OR e->>'source' IS DISTINCT FROM (SELECT source_code FROM public.data_sources WHERE id=r.source_id)
       OR e->>'source_event_id' IS DISTINCT FROM r.external_event_id OR e->>'raw_event_reference' IS DISTINCT FROM ref THEN
       RAISE EXCEPTION 'REVISION_ORIGIN_MISMATCH' USING ERRCODE='23514'; END IF;
    PERFORM (e->>'event_id')::uuid;
    SELECT id INTO actor FROM public.actors WHERE actor_code=e->>'user_id' AND is_synthetic AND actor_code ~ '^EMP[0-9]{3}$' AND actor_code BETWEEN 'EMP001' AND 'EMP025';
    IF actor IS NULL THEN RAISE EXCEPTION 'REVISION_ACTOR_NOT_AUTHORIZED' USING ERRCODE='23514'; END IF;
 ELSE e:=NULL; END IF;
 -- Compute dependencies from SQL truth, never a caller-supplied scope list.
 FOR item IN SELECT value FROM jsonb_array_elements(jsonb_build_array(old_dto,e)) LOOP
    IF item IS NOT NULL AND item<>'null'::jsonb THEN
       scopes:=scopes||jsonb_build_array(jsonb_build_object('user_id',item->>'user_id',
         'start',date_trunc('hour',(item->>'timestamp')::timestamptz AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
         'end',(date_trunc('hour',(item->>'timestamp')::timestamptz AT TIME ZONE 'UTC')+interval '1 hour') AT TIME ZONE 'UTC'));
    END IF;
 END LOOP;
 SELECT COALESCE(jsonb_agg(value ORDER BY value::text),'[]'::jsonb) INTO scopes FROM (SELECT DISTINCT value FROM jsonb_array_elements(scopes)) u;
 -- Archive all actually dependent windows, including provisional partial windows.
 FOR w IN SELECT f.* FROM public.feature_windows f JOIN public.actors a ON a.id=f.actor_id
   WHERE f.lifecycle='active' AND f.is_synthetic AND (
     (old_dto IS NOT NULL AND a.actor_code=old_dto->>'user_id' AND f.window_start<=(old_dto->>'timestamp')::timestamptz AND (old_dto->>'timestamp')::timestamptz<f.window_end)
     OR (e IS NOT NULL AND a.actor_code=e->>'user_id' AND f.window_start<=(e->>'timestamp')::timestamptz AND (e->>'timestamp')::timestamptz<f.window_end)) FOR UPDATE OF f LOOP
    IF EXISTS(SELECT 1 FROM public.detection_results WHERE feature_window_id=w.id) THEN RAISE EXCEPTION 'DOWNSTREAM_RESULTS_REQUIRE_REVIEW'; END IF;
    UPDATE public.feature_windows SET lifecycle='archived',invalidated_by=rid,
      retirement_history=retirement_history||jsonb_build_array(jsonb_build_object('request_id',rid,'at',at_value,'reason','NORMALIZATION_REVISED','snapshot',to_jsonb(w)-'retirement_history')) WHERE id=w.id;
 END LOOP;
 versions:=r.normalization_versions;
 IF jsonb_array_length(versions)=0 THEN versions:=jsonb_build_array(jsonb_build_object('revision',r.normalization_revision,'rules_version',r.normalization_rules_version,
   'request_id',NULL,'reason','INITIAL_MATERIALIZATION','status',r.processing_status,'normalized_event',old_dto,
   'sql_snapshot',CASE WHEN n.id IS NULL THEN NULL ELSE to_jsonb(n) END,'processing_details',r.processing_details,'processing_error',r.processing_error)); END IF;
 -- Snapshot is saved in the transaction before replacement; failed INSERT rolls it all back.
 IF n.id IS NOT NULL THEN DELETE FROM public.normalized_events WHERE id=n.id AND raw_event_id=r.id; END IF;
 IF e IS NOT NULL THEN
   INSERT INTO public.normalized_events(id,raw_event_id,actor_id,occurred_at,operation,device_code,session_code,file_extension,file_type,file_size_bytes,source_type,source_path,destination_type,destination_path,destination_authorized,data_classification,operation_result,is_synthetic)
   VALUES((e->>'event_id')::uuid,r.id,actor,(e->>'timestamp')::timestamptz,e->>'operation',e->>'device_id',e->>'session_id',e->>'file_extension',e->>'file_type',(e->>'file_size_bytes')::bigint,e->>'source_type',e->>'source_path',e->>'destination_type',e->>'destination_path',(e->>'destination_authorized')::boolean,e->>'data_classification',e->>'operation_result',true)
   RETURNING to_jsonb(normalized_events) INTO new_sql;
 END IF;
 entry:=jsonb_build_object('revision',r.normalization_revision+1,'rules_version',q->>'rules_version','request_id',rid,
   'request',q,'reason',q->>'reason','changed_at',at_value,'status',q->>'status','normalized_event',e,
   'sql_snapshot',new_sql,'audit',q->'audit','field_states',q->'field_states','temporal',q->'temporal','affected_scopes',scopes);
 UPDATE public.raw_events SET normalization_revision=normalization_revision+1,
   normalization_rules_version=q->>'rules_version',normalization_versions=versions||jsonb_build_array(entry),
   processing_status=q->>'status',processing_error=CASE WHEN q->>'status'='rejected' THEN (q->'audit')::text ELSE NULL END,
   processing_details=processing_details||jsonb_build_array(jsonb_build_object('normalization_version',q->>'rules_version',
     'directory_sha256',ds,'raw_reference',ref,'revision',r.normalization_revision+1,'request_id',rid,'audit',q->'audit','executed_at',at_value)),
   pending_feature_scopes=(SELECT COALESCE(jsonb_agg(value ORDER BY value::text),'[]'::jsonb) FROM
      (SELECT DISTINCT value FROM jsonb_array_elements(r.pending_feature_scopes||scopes)) u)
 WHERE id=r.id RETURNING pending_feature_scopes INTO scopes;
 SET CONSTRAINTS public.raw_normalization_result_integrity IMMEDIATE;
 SET CONSTRAINTS public.raw_normalization_result_integrity DEFERRED;
 RETURN jsonb_build_object('changed',true,'revision',r.normalization_revision+1,'active_revision',r.normalization_revision+1,'pending_scopes',scopes,'recalculation_required',jsonb_array_length(scopes)>0);
END;
$$;

CREATE FUNCTION public.reconcile_feature_scope(actor_code_value text,start_at timestamptz,end_at timestamptz,rid uuid,windows jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE actor uuid; w public.feature_windows%ROWTYPE; item jsonb; payload jsonb; audit jsonb;
 previous public.feature_windows%ROWTYPE; inserted integer:=0; updated integer:=0; retired integer:=0; unchanged integer:=0;
 at_value timestamptz:=clock_timestamp(); ids uuid[]:='{}'; wid uuid; logical_seen text[]:='{}'; logical_key text;
BEGIN
 IF session_user<>'pipeline_runner' OR current_setting('role')<>'pipeline_featurizer' THEN RAISE EXCEPTION 'RECALCULATION_ROLE_REQUIRED' USING ERRCODE='42501'; END IF;
 IF rid IS NULL OR NOT isfinite(start_at) OR NOT isfinite(end_at) OR NOT start_at<end_at
    OR start_at<timestamptz '2026-09-01 05:00Z' OR end_at>timestamptz '2026-10-01 05:00Z'
    OR jsonb_typeof(windows) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'INVALID_RECALCULATION_SCOPE' USING ERRCODE='23514'; END IF;
 SELECT id INTO actor FROM public.actors WHERE actor_code=actor_code_value AND is_synthetic AND actor_code ~ '^EMP[0-9]{3}$' AND actor_code BETWEEN 'EMP001' AND 'EMP025';
 IF actor IS NULL THEN RAISE EXCEPTION 'RECALCULATION_ACTOR_NOT_AUTHORIZED'; END IF;
 PERFORM pg_advisory_xact_lock(hashtext('hu02-normalization-v1'));
 PERFORM pg_advisory_xact_lock(hashtext('hu03-features-v1'));
 -- All items validated before any archive or insert.
 FOR item IN SELECT value FROM jsonb_array_elements(windows) LOOP
    wid:=(item->>'window_id')::uuid;
    logical_key:=(item->>'window_start')||'/'||(item->>'window_end');
    IF wid=ANY(ids) OR logical_key=ANY(logical_seen) OR item->>'user_id' IS DISTINCT FROM actor_code_value
       OR item->'is_synthetic' IS DISTINCT FROM 'true'::jsonb
       OR NOT start_at<=(item->>'window_start')::timestamptz OR NOT (item->>'window_end')::timestamptz<=end_at
       OR item->'metadata'->'context'->>'normalization_version' IS DISTINCT FROM 'active-results-v1'
       OR jsonb_typeof(item->'metadata'->'normalization_inputs') IS DISTINCT FROM 'array'
       OR jsonb_array_length(item->'metadata'->'normalization_inputs')<>(item->'features'->>'operation_count')::integer THEN
       RAISE EXCEPTION 'RECALCULATION_WINDOW_INVALID' USING ERRCODE='23514'; END IF;
    -- Trace must match currently active canonical SQL rows, not an old local CSV.
    IF EXISTS(SELECT 1 FROM jsonb_array_elements(item->'metadata'->'normalization_inputs') i
       LEFT JOIN public.raw_events r ON r.id=(i->>'raw_id')::uuid
       LEFT JOIN public.normalized_events n ON n.raw_event_id=r.id
       WHERE NOT i ?& ARRAY['raw_id','event_id','revision','rules_version','event_sha256']
          OR COALESCE(i->>'event_sha256','') !~ '^[a-f0-9]{64}$'
          OR n.id IS NULL OR r.processing_status<>'processed' OR r.duplicate_of IS NOT NULL
          OR n.actor_id<>actor OR n.occurred_at<(item->>'window_start')::timestamptz OR n.occurred_at>=(item->>'window_end')::timestamptz
          OR (i->>'event_id')::uuid<>n.id OR COALESCE((i->>'revision')::numeric,0)<>r.normalization_revision
          OR i->>'rules_version' IS DISTINCT FROM r.normalization_rules_version)
       OR (SELECT count(DISTINCT i->>'raw_id') FROM jsonb_array_elements(item->'metadata'->'normalization_inputs') i)<>jsonb_array_length(item->'metadata'->'normalization_inputs')
       OR (SELECT count(*) FROM public.normalized_events n JOIN public.raw_events r ON r.id=n.raw_event_id
            WHERE n.actor_id=actor AND n.occurred_at>=(item->>'window_start')::timestamptz AND n.occurred_at<(item->>'window_end')::timestamptz AND r.processing_status='processed' AND r.duplicate_of IS NULL)
          <>jsonb_array_length(item->'metadata'->'normalization_inputs') THEN
       RAISE EXCEPTION 'RECALCULATION_INPUT_MANIFEST_MISMATCH' USING ERRCODE='23514'; END IF;
    IF (SELECT COALESCE(sum(n.file_size_bytes),0) FROM public.normalized_events n WHERE n.actor_id=actor
        AND n.occurred_at>=(item->>'window_start')::timestamptz AND n.occurred_at<(item->>'window_end')::timestamptz)
       IS DISTINCT FROM (item->'features'->>'total_bytes')::numeric THEN
       RAISE EXCEPTION 'RECALCULATION_SOURCE_VOLUME_MISMATCH' USING ERRCODE='23514'; END IF;
    payload:=jsonb_build_object('vector',item->'features','metadata',item->'metadata','audit',jsonb_build_object('created_at',at_value,'last_changed_at',at_value,'revision',1,'source_snapshot_sha256',encode(sha256(convert_to((item->'metadata'->'normalization_inputs')::text,'UTF8')),'hex')));
    IF public.feature_payload_valid(payload,(item->>'window_start')::timestamptz,(item->>'window_end')::timestamptz,item->>'pipeline_version') IS NOT TRUE OR public.feature_audit_valid(payload) IS NOT TRUE THEN RAISE EXCEPTION 'RECALCULATION_PAYLOAD_INVALID' USING ERRCODE='23514'; END IF;
    ids:=array_append(ids,wid); logical_seen:=array_append(logical_seen,logical_key);
 END LOOP;
 -- No caller may omit a non-empty complete slot and label it obsolete.
 IF EXISTS(SELECT 1 FROM public.normalized_events n JOIN public.raw_events r ON r.id=n.raw_event_id
    WHERE n.actor_id=actor AND n.occurred_at>=start_at AND n.occurred_at<end_at AND r.processing_status='processed' AND r.duplicate_of IS NULL
      AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(windows) i WHERE (i->>'window_start')::timestamptz<=n.occurred_at AND n.occurred_at<(i->>'window_end')::timestamptz)) THEN
    RAISE EXCEPTION 'RECALCULATION_SCOPE_NOT_EXHAUSTIVE' USING ERRCODE='23514'; END IF;
 FOR w IN SELECT * FROM public.feature_windows WHERE actor_id=actor AND lifecycle='active'
    AND start_at<=window_start AND window_end<=end_at AND NOT id=ANY(ids) FOR UPDATE LOOP
    IF EXISTS(SELECT 1 FROM public.detection_results WHERE feature_window_id=w.id) THEN RAISE EXCEPTION 'DOWNSTREAM_RESULTS_REQUIRE_REVIEW'; END IF;
    UPDATE public.feature_windows SET lifecycle='archived',invalidated_by=rid,
      retirement_history=retirement_history||jsonb_build_array(jsonb_build_object('request_id',rid,'at',at_value,'reason','OBSOLETE_IN_AUTHORIZED_SCOPE','snapshot',to_jsonb(w)-'retirement_history')) WHERE id=w.id;
    retired:=retired+1;
 END LOOP;
 FOR item IN SELECT value FROM jsonb_array_elements(windows) LOOP
    wid:=(item->>'window_id')::uuid;
    SELECT * INTO previous FROM public.feature_windows WHERE id=wid FOR UPDATE;
    IF previous.id IS NOT NULL AND (previous.actor_id<>actor OR previous.window_start<>(item->>'window_start')::timestamptz OR previous.window_end<>(item->>'window_end')::timestamptz OR previous.pipeline_version<>item->>'pipeline_version') THEN RAISE EXCEPTION 'RECALCULATION_IDENTITY_CONFLICT'; END IF;
    IF previous.id IS NOT NULL AND previous.lifecycle='active' AND previous.features->'vector'=item->'features' AND previous.features->'metadata'=item->'metadata' THEN
       unchanged:=unchanged+1; CONTINUE;
    END IF;
    audit:=jsonb_build_object('created_at',COALESCE(previous.features->'audit'->>'created_at',to_jsonb(at_value)#>>'{}'),
      'last_changed_at',at_value,'revision',COALESCE((previous.features->'audit'->>'revision')::integer,0)+1,
      'source_snapshot_sha256',encode(sha256(convert_to((item->'metadata'->'normalization_inputs')::text,'UTF8')),'hex'),'request_id',rid);
    payload:=jsonb_build_object('vector',item->'features','metadata',item->'metadata','audit',audit);
    IF previous.id IS NULL THEN
       INSERT INTO public.feature_windows(id,actor_id,window_start,window_end,features,pipeline_version,is_synthetic,lifecycle)
         VALUES(wid,actor,(item->>'window_start')::timestamptz,(item->>'window_end')::timestamptz,payload,item->>'pipeline_version',true,'active');
       inserted:=inserted+1;
    ELSE
       IF EXISTS(SELECT 1 FROM public.detection_results WHERE feature_window_id=wid) THEN RAISE EXCEPTION 'DOWNSTREAM_RESULTS_REQUIRE_REVIEW'; END IF;
       UPDATE public.feature_windows SET features=payload,lifecycle='active',invalidated_by=NULL,
         retirement_history=retirement_history||jsonb_build_array(jsonb_build_object('request_id',rid,'at',at_value,'reason','RECALCULATED','snapshot',to_jsonb(previous)-'retirement_history')) WHERE id=wid;
       updated:=updated+1;
    END IF;
 END LOOP;
 -- Clear only pending scopes fully covered by this exhaustive recalculation.
 UPDATE public.raw_events r SET pending_feature_scopes=(SELECT COALESCE(jsonb_agg(value),'[]'::jsonb)
   FROM jsonb_array_elements(r.pending_feature_scopes) WHERE NOT (value->>'user_id'=actor_code_value AND start_at<=(value->>'start')::timestamptz AND (value->>'end')::timestamptz<=end_at))
 WHERE EXISTS(SELECT 1 FROM jsonb_array_elements(r.pending_feature_scopes) p WHERE p->>'user_id'=actor_code_value AND start_at<=(p->>'start')::timestamptz AND (p->>'end')::timestamptz<=end_at);
 -- Flush the existing deferred trigger inside the privileged, fixed-scope boundary.
 SET CONSTRAINTS public.raw_normalization_result_integrity IMMEDIATE;
 SET CONSTRAINTS public.raw_normalization_result_integrity DEFERRED;
 RETURN jsonb_build_object('inserted',inserted,'updated',updated,'archived',retired,'already_existing',unchanged,'errors',0);
END;
$$;

CREATE FUNCTION public.has_normalization_revisions() RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT EXISTS(SELECT 1 FROM public.raw_events WHERE normalization_revision>1 OR jsonb_array_length(pending_feature_scopes)>0);
$$;
REVOKE ALL ON FUNCTION public.normalization_dto(jsonb,text,text,text,text),public.normalization_revision_request_valid(jsonb),public.revise_normalization(uuid,jsonb,jsonb),public.reconcile_feature_scope(text,timestamptz,timestamptz,uuid,jsonb),public.has_normalization_revisions() FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.revise_normalization(uuid,jsonb,jsonb) TO pipeline_normalizer;
GRANT EXECUTE ON FUNCTION public.reconcile_feature_scope(text,timestamptz,timestamptz,uuid,jsonb),public.has_normalization_revisions() TO pipeline_featurizer;
-- No new members, LOGIN roles, passwords, generic DML or public policies.

CREATE FUNCTION public.guard_normalization_history() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE old_len integer:=jsonb_array_length(OLD.normalization_versions); new_len integer:=jsonb_array_length(NEW.normalization_versions);
 item jsonb; index_value integer:=0; ids uuid[]:='{}'; request_value uuid;
BEGIN
 IF (NEW.payload,NEW.payload_sha256,NEW.id,NEW.source_id,NEW.external_event_id,NEW.batch_id,NEW.record_number,NEW.received_at,NEW.is_synthetic)
      IS DISTINCT FROM (OLD.payload,OLD.payload_sha256,OLD.id,OLD.source_id,OLD.external_event_id,OLD.batch_id,OLD.record_number,OLD.received_at,OLD.is_synthetic) THEN
    RAISE EXCEPTION 'IMMUTABLE_RAW_CHANGED' USING ERRCODE='23514'; END IF;
 IF NEW.normalization_revision=OLD.normalization_revision THEN
    IF NEW.normalization_versions IS DISTINCT FROM OLD.normalization_versions OR NEW.normalization_rules_version<>OLD.normalization_rules_version
       OR (old_len>0 AND (NEW.processing_status,NEW.processing_error,NEW.processing_details,NEW.duplicate_of)
           IS DISTINCT FROM (OLD.processing_status,OLD.processing_error,OLD.processing_details,OLD.duplicate_of)) THEN
       RAISE EXCEPTION 'REVISION_HISTORY_OR_ACTIVE_RESULT_CHANGED_WITHOUT_REVISION' USING ERRCODE='23514'; END IF;
    RETURN NEW;
 END IF;
 IF NEW.duplicate_of IS NOT NULL OR NEW.normalization_revision<>OLD.normalization_revision+1
    OR new_len<>NEW.normalization_revision OR NEW.processing_status NOT IN ('processed','rejected')
    OR (NEW.normalization_versions->-1->>'revision')::numeric<>NEW.normalization_revision
    OR NEW.normalization_versions->-1->>'rules_version' IS DISTINCT FROM NEW.normalization_rules_version
    OR NEW.normalization_versions->-1->>'status' IS DISTINCT FROM NEW.processing_status THEN
    RAISE EXCEPTION 'INVALID_ACTIVE_REVISION' USING ERRCODE='23514'; END IF;
 IF old_len>0 AND EXISTS(SELECT 1 FROM generate_series(0,old_len-1) i WHERE OLD.normalization_versions->i IS DISTINCT FROM NEW.normalization_versions->i) THEN
    RAISE EXCEPTION 'NORMALIZATION_HISTORY_REWRITTEN' USING ERRCODE='23514'; END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(NEW.normalization_versions) LOOP
    index_value:=index_value+1;
    IF jsonb_typeof(item->'revision') IS DISTINCT FROM 'number' OR (item->>'revision')::numeric<>index_value
       OR COALESCE(item->>'rules_version','') !~ '^hu02-v[1-9][0-9]*$' OR NOT item ?& ARRAY['sql_snapshot','reason','status','normalized_event']
       OR length(btrim(item->>'reason'))=0 THEN RAISE EXCEPTION 'INVALID_NORMALIZATION_HISTORY_ENTRY' USING ERRCODE='23514'; END IF;
    IF item->>'request_id' IS NOT NULL THEN
       request_value:=(item->>'request_id')::uuid;
       IF request_value=ANY(ids) THEN RAISE EXCEPTION 'DUPLICATE_REVISION_REQUEST' USING ERRCODE='23514'; END IF;
       ids:=array_append(ids,request_value);
    END IF;
 END LOOP;
 RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION public.guard_normalization_history() FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.guard_normalization_history() TO pipeline_ingestor,pipeline_normalizer;
CREATE TRIGGER raw_revision_history_guard BEFORE UPDATE ON public.raw_events FOR EACH ROW EXECUTE FUNCTION public.guard_normalization_history();

-- Preserve exact vectors even for the existing, authorized legacy update path.
CREATE FUNCTION public.guard_feature_retirement_history() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE old_len integer:=jsonb_array_length(OLD.retirement_history);
BEGIN
 IF (NEW.id,NEW.actor_id,NEW.window_start,NEW.window_end,NEW.pipeline_version,NEW.is_synthetic,NEW.created_at)
    IS DISTINCT FROM (OLD.id,OLD.actor_id,OLD.window_start,OLD.window_end,OLD.pipeline_version,OLD.is_synthetic,OLD.created_at) THEN
    RAISE EXCEPTION 'FEATURE_LOGICAL_IDENTITY_IMMUTABLE' USING ERRCODE='23514'; END IF;
 IF OLD.lifecycle='archived' AND NEW.lifecycle='archived' AND NEW.features IS DISTINCT FROM OLD.features THEN
    RAISE EXCEPTION 'ARCHIVED_WINDOWS_REQUIRE_RECONCILIATION' USING ERRCODE='23514'; END IF;
 IF jsonb_array_length(NEW.retirement_history)<old_len OR EXISTS(SELECT 1 FROM generate_series(0,old_len-1) i WHERE OLD.retirement_history->i IS DISTINCT FROM NEW.retirement_history->i) THEN
    RAISE EXCEPTION 'FEATURE_HISTORY_REWRITTEN' USING ERRCODE='23514'; END IF;
 IF (NEW.features,NEW.lifecycle) IS DISTINCT FROM (OLD.features,OLD.lifecycle)
    AND jsonb_array_length(NEW.retirement_history)=old_len THEN
    NEW.retirement_history:=OLD.retirement_history||jsonb_build_array(jsonb_build_object('at',clock_timestamp(),
       'reason','CONTROLLED_FEATURE_UPDATE','snapshot',to_jsonb(OLD)-'retirement_history'));
 END IF;
 RETURN NEW;
END;
$$;
REVOKE ALL ON FUNCTION public.guard_feature_retirement_history() FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.guard_feature_retirement_history() TO pipeline_featurizer;
CREATE TRIGGER feature_retirement_history_guard BEFORE UPDATE ON public.feature_windows FOR EACH ROW EXECUTE FUNCTION public.guard_feature_retirement_history();

-- Batch activation of a newly normalized canonical event, including late arrivals.
-- Installed only: no scan/backfill of existing processed RAW or definitive windows.
CREATE FUNCTION public.invalidate_initial_normalization() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE n public.normalized_events%ROWTYPE; code text; scope jsonb; w public.feature_windows%ROWTYPE;
BEGIN
 IF OLD.processing_status<>'pending' OR NEW.processing_status<>'processed' THEN RETURN NEW; END IF;
 IF session_user<>'pipeline_runner' OR current_setting('role')<>'pipeline_normalizer' THEN RAISE EXCEPTION 'INITIAL_NORMALIZATION_ROLE_REQUIRED' USING ERRCODE='42501'; END IF;
 SELECT * INTO STRICT n FROM public.normalized_events WHERE raw_event_id=NEW.id;
 SELECT actor_code INTO code FROM public.actors WHERE id=n.actor_id AND is_synthetic AND actor_code ~ '^EMP[0-9]{3}$' AND actor_code BETWEEN 'EMP001' AND 'EMP025';
 IF NOT NEW.is_synthetic OR NEW.duplicate_of IS NOT NULL OR code IS NULL OR NOT isfinite(n.occurred_at) THEN
    RAISE EXCEPTION 'INITIAL_NORMALIZATION_SUBJECT_INVALID' USING ERRCODE='23514'; END IF;
 -- Other periods remain valid HU02 events; this migration authorizes HU03 scopes only for V2.
 IF n.occurred_at<timestamptz '2026-09-01 05:00Z' OR n.occurred_at>=timestamptz '2026-10-01 05:00Z' THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtext('hu02-normalization-v1'));
 PERFORM pg_advisory_xact_lock(hashtext('hu03-features-v1'));
 scope:=jsonb_build_object('user_id',code,'start',date_trunc('hour',n.occurred_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
    'end',(date_trunc('hour',n.occurred_at AT TIME ZONE 'UTC')+interval '1 hour') AT TIME ZONE 'UTC');
 NEW.pending_feature_scopes:=NEW.pending_feature_scopes||jsonb_build_array(scope);
 FOR w IN SELECT * FROM public.feature_windows WHERE actor_id=n.actor_id AND is_synthetic AND lifecycle='active'
    AND window_start<=n.occurred_at AND n.occurred_at<window_end FOR UPDATE LOOP
    IF EXISTS(SELECT 1 FROM public.detection_results WHERE feature_window_id=w.id) THEN RAISE EXCEPTION 'DOWNSTREAM_RESULTS_REQUIRE_REVIEW'; END IF;
    UPDATE public.feature_windows SET lifecycle='archived',invalidated_by=NEW.id,
       retirement_history=retirement_history||jsonb_build_array(jsonb_build_object('raw_event_id',NEW.id,'at',clock_timestamp(),
          'reason','CANONICAL_EVENT_ACTIVATED','snapshot',to_jsonb(w)-'retirement_history')) WHERE id=w.id;
 END LOOP;
 RETURN NEW;
EXCEPTION WHEN no_data_found THEN RAISE EXCEPTION 'INITIAL_NORMALIZATION_RESULT_MISSING' USING ERRCODE='23514';
END;
$$;
REVOKE ALL ON FUNCTION public.invalidate_initial_normalization() FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.invalidate_initial_normalization() TO pipeline_normalizer;
CREATE TRIGGER raw_initial_normalization_invalidation BEFORE UPDATE ON public.raw_events FOR EACH ROW EXECUTE FUNCTION public.invalidate_initial_normalization();
