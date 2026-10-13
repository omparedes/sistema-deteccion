"""Offline execution-migration regression. Parsing is not live functional proof."""
import unittest
from pathlib import Path
from pglast import parse_sql,parse_plpgsql

MIGRATION=Path(__file__).resolve().parents[1]/'scripts/db/migrations/0011_fix_normalization_revision_execution.sql'


class RevisionExecutionMigrationTest(unittest.TestCase):
    def test_single_replacement_parses_pg17(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        self.assertEqual(len(parse_sql(sql)),1)
        self.assertEqual(len(parse_plpgsql(sql)),1)
        self.assertIn('CREATE OR REPLACE FUNCTION public.revise_normalization',sql)

    def test_42725_actual_candidate_rhs_is_subtraction_of_extracted_json(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        rhs=sql.split("(e-'event_id') IS DISTINCT FROM ",1)[1].splitlines()[0]
        expression=parse_sql('SELECT '+rhs)[0].stmt.targetList[0].val
        self.assertEqual(expression.name[0].sval,'-')
        self.assertEqual(expression.lexpr.name[0].sval,'->')
        self.assertEqual(expression.rexpr.val.sval,'event_id')
        self.assertNotIn("(q->'normalized_event'-'event_id')",sql)

    def test_body_diff_is_only_confirmed_precedence_fix(self):
        old=(MIGRATION.parent/'0009_normalization_versioning_feature_invalidation.sql').read_text(encoding='utf-8')
        old=old[old.index('CREATE FUNCTION public.revise_normalization'):old.index('CREATE FUNCTION public.reconcile_feature_scope')]
        old_body=old.split('AS $$',1)[1].rsplit('$$;',1)[0]
        new_body=MIGRATION.read_text(encoding='utf-8').split('AS $function$',1)[1].rsplit('$function$',1)[0]
        self.assertEqual(new_body,old_body.replace("(q->'normalized_event'-'event_id')","((q -> 'normalized_event') - 'event_id')"))

    def test_quarantine_already_converts_json_null_to_sql_null(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        self.assertIn('ELSE e:=NULL; END IF;',sql)
        self.assertIn("item IS NOT NULL AND item<>'null'::jsonb",sql)
        self.assertIn('IF e IS NOT NULL THEN',sql)

    def test_history_and_idempotency_guards_preserved(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        for text in ["'INITIAL_MATERIALIZATION'","'sql_snapshot'","prior->'request' IS DISTINCT FROM q",'NORMALIZATION_REVISION_CONFLICT',"'active_revision',r.normalization_revision",'RETURNING to_jsonb(normalized_events)']:
            self.assertIn(text,sql)

    def test_stage_scope_and_atomic_boundaries_preserved(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        for text in ['SECURITY DEFINER',"SET search_path TO 'pg_catalog', 'public', 'pg_temp'","session_user<>'pipeline_runner'","current_setting('role')<>'pipeline_normalizer'",'FOR UPDATE',"old_dto->>'user_id'","e->>'user_id'",'SET CONSTRAINTS public.raw_normalization_result_integrity IMMEDIATE']:
            self.assertIn(text,sql)
        self.assertNotRegex(sql,r'(?im)^\s*(GRANT|REVOKE|ALTER|DROP|CREATE TABLE)\b')
        self.assertNotIn('UPDATE public.raw_events SET payload',sql)
