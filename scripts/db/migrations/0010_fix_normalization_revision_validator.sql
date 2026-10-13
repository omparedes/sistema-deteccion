-- Fix ambiguous JSON key aliases; preserve signature, owner, ACL and validation rules.
-- Malformed input returns false; unexpected internal errors propagate. External transaction required.
CREATE OR REPLACE FUNCTION public.normalization_revision_request_valid(q jsonb) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE e jsonb:=q->'normalized_event'; k text; t timestamptz; x numeric;
BEGIN
 IF jsonb_typeof(q) IS DISTINCT FROM 'object'
    OR NOT q ?& ARRAY['request_id','expected_revision','reason','rules_version','directory_sha256','status','normalized_event','audit']
    OR EXISTS(SELECT 1 FROM jsonb_object_keys(q) AS request_keys(key_name) WHERE request_keys.key_name NOT IN ('request_id','expected_revision','reason','rules_version','directory_sha256','status','normalized_event','audit','field_states','temporal'))
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
    OR EXISTS(SELECT 1 FROM jsonb_object_keys(e) AS event_keys(key_name) WHERE event_keys.key_name NOT IN ('event_id','source','source_event_id','timestamp','user_id','operation','file_size_bytes','is_synthetic','raw_event_reference','device_id','session_id','file_extension','file_type','source_type','source_path','destination_type','destination_path','destination_authorized','data_classification','operation_result'))
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
EXCEPTION WHEN invalid_text_representation OR invalid_parameter_value OR invalid_datetime_format OR datetime_field_overflow OR numeric_value_out_of_range THEN RETURN false;
END;
$$;

