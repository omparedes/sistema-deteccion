-- Fix JSONB extraction/subtraction precedence only. No data, ACL or schema changes.
-- External transaction required; functional branches remain pending prevalidation.
CREATE OR REPLACE FUNCTION public.revise_normalization(raw_id uuid, q jsonb, prepared jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
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
    IF e IS NULL OR (e-'event_id') IS DISTINCT FROM ((q -> 'normalized_event') - 'event_id')
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
$function$
