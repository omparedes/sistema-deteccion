-- READ ONLY after 0009 within the same transaction. No claim of effective RLS testing.
DO $$
DECLARE role_value text; function_oid oid;
BEGIN
 IF EXISTS(SELECT 1 FROM public.raw_events WHERE normalization_revision<>1 OR normalization_versions<>'[]'::jsonb OR pending_feature_scopes<>'[]'::jsonb)
    OR EXISTS(SELECT 1 FROM public.feature_windows WHERE lifecycle<>'active' OR retirement_history<>'[]'::jsonb OR invalidated_by IS NOT NULL)
    OR has_column_privilege('pipeline_normalizer','public.raw_events','payload','UPDATE')
    OR has_table_privilege('pipeline_normalizer','public.normalized_events','UPDATE,DELETE')
    OR has_column_privilege('pipeline_featurizer','public.raw_events','normalization_versions','UPDATE')
    OR NOT has_function_privilege('pipeline_normalizer','public.revise_normalization(uuid,jsonb,jsonb)','EXECUTE')
    OR NOT has_function_privilege('pipeline_featurizer','public.reconcile_feature_scope(text,timestamptz,timestamptz,uuid,jsonb)','EXECUTE')
    OR has_function_privilege('anon','public.revise_normalization(uuid,jsonb,jsonb)','EXECUTE')
    OR has_function_privilege('authenticated','public.reconcile_feature_scope(text,timestamptz,timestamptz,uuid,jsonb)','EXECUTE')
    OR has_function_privilege('service_role','public.revise_normalization(uuid,jsonb,jsonb)','EXECUTE') THEN
   RAISE EXCEPTION 'REVISION_POSTFLIGHT_REQUIRES_REVIEW';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_class WHERE oid IN ('public.raw_events'::regclass,'public.normalized_events'::regclass,'public.feature_windows'::regclass) AND NOT relrowsecurity)
    OR EXISTS(SELECT 1 FROM pg_roles WHERE rolname IN ('pipeline_runner','pipeline_normalizer','pipeline_featurizer') AND (rolsuper OR rolbypassrls OR rolcreatedb OR rolcreaterole)) THEN
    RAISE EXCEPTION 'REVISION_RLS_OR_ROLE_BOUNDARY_INVALID';
 END IF;
 FOREACH role_value IN ARRAY ARRAY['anon','authenticated','service_role'] LOOP
    IF has_table_privilege(role_value,'public.active_feature_windows','SELECT') THEN RAISE EXCEPTION 'ACTIVE_VIEW_API_ACCESS'; END IF;
    FOR function_oid IN SELECT oid FROM pg_proc WHERE pronamespace='public'::regnamespace
      AND proname IN ('normalization_dto','normalization_revision_request_valid','revise_normalization','reconcile_feature_scope','has_normalization_revisions','guard_normalization_history','guard_feature_retirement_history','invalidate_initial_normalization') LOOP
      IF has_function_privilege(role_value,function_oid,'EXECUTE') THEN RAISE EXCEPTION 'REVISION_API_EXECUTE_ACCESS'; END IF;
    END LOOP;
 END LOOP;
END;
$$;
