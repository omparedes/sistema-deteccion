-- Emergency DISABLE only, retain ALL columns, history, active results and archives.
-- Requires separate authorization and an externally managed transaction.
-- Before commit compare snapshots and pending scopes. Never DROP history columns.
REVOKE EXECUTE ON FUNCTION public.revise_normalization(uuid,jsonb,jsonb) FROM pipeline_normalizer;
REVOKE EXECUTE ON FUNCTION public.reconcile_feature_scope(text,timestamptz,timestamptz,uuid,jsonb) FROM pipeline_featurizer;
-- Reactivation requires reviewed grants and integration tests, not a data rollback.
