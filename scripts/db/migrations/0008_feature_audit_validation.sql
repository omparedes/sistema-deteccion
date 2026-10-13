-- PENDING: requiere autorizacion de aplicacion. No modifica 0001--0007.
-- Ejecutar completo en una transaccion; cualquier error exige ROLLBACK.
-- El ejecutor controla BEGIN/COMMIT y verifica roles/permisos antes y despues.
CREATE FUNCTION public.feature_audit_valid(p jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER
SET search_path = pg_catalog, public AS $$
DECLARE
  a jsonb;
  created_value timestamptz;
  changed_value timestamptz;
  revision_value numeric;
  date_pattern text := '^[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$';
BEGIN
  a := p->'audit';
  IF jsonb_typeof(a) IS DISTINCT FROM 'object'
     OR jsonb_typeof(a->'created_at') IS DISTINCT FROM 'string'
     OR jsonb_typeof(a->'last_changed_at') IS DISTINCT FROM 'string'
     OR jsonb_typeof(a->'revision') IS DISTINCT FROM 'number'
     OR (a->>'created_at') !~ date_pattern
     OR (a->>'last_changed_at') !~ date_pattern THEN
    RETURN false;
  END IF;
  created_value := (a->>'created_at')::timestamptz;
  changed_value := (a->>'last_changed_at')::timestamptz;
  revision_value := (a->>'revision')::numeric;
  RETURN isfinite(created_value) AND isfinite(changed_value)
     AND created_value <= changed_value
     AND revision_value >= 1 AND revision_value = trunc(revision_value);
EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow OR SQLSTATE '22009'
            OR invalid_text_representation OR numeric_value_out_of_range THEN
  RETURN false;
END;
$$;

REVOKE ALL ON FUNCTION public.feature_audit_valid(jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.feature_audit_valid(jsonb) TO pipeline_featurizer;

-- Preflight sobre datos reales: aborta, nunca corrige/backfillea registros.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.feature_windows
             WHERE public.feature_audit_valid(features) IS NOT TRUE) THEN
    RAISE EXCEPTION 'FEATURE_AUDIT_EXISTING_ROWS_INVALID';
  END IF;
END;
$$;

ALTER TABLE public.feature_windows
  ADD CONSTRAINT feature_windows_audit_valid
  CHECK (public.feature_audit_valid(features) IS TRUE);

-- Postflight (lectura): todos los registros deben seguir pasando.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.feature_windows
             WHERE public.feature_audit_valid(features) IS NOT TRUE) THEN
    RAISE EXCEPTION 'FEATURE_AUDIT_POSTFLIGHT_FAILED';
  END IF;
END;
$$;
