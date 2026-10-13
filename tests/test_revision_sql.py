"""SQL0009 syntax/boundaries offline; does not apply migrations or prove RLS."""
import unittest
from pathlib import Path
from pglast import parse_sql, parse_plpgsql

ROOT=Path(__file__).resolve().parents[1]/'scripts/db'


class RevisionSqlOfflineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql=(ROOT/'migrations/0009_normalization_versioning_feature_invalidation.sql').read_text(encoding='utf-8')

    def test_pg17_sql_and_plpgsql_parse(self):
        self.assertGreater(len(parse_sql(self.sql)),15)
        self.assertGreaterEqual(len(parse_plpgsql(self.sql)),7)

    def test_preflight_postflight_and_disable_parse_without_data_cleanup(self):
        for path in ['checks/0009_preflight.sql','checks/0009_postflight.sql','rollback/0009_disable_revisions.sql']:
            sql=(ROOT/path).read_text(encoding='utf-8')
            self.assertTrue(parse_sql(sql));self.assertNotIn('DELETE FROM',sql);self.assertNotIn('DROP COLUMN',sql)
            if sql.lstrip().startswith('-- READ ONLY'):
                self.assertTrue(parse_plpgsql(sql))

    def test_no_new_tables_admin_memberships_or_payload_changes(self):
        self.assertNotIn('CREATE TABLE',self.sql);self.assertNotIn('CREATE ROLE',self.sql)
        self.assertNotIn('UPDATE public.raw_events SET payload',self.sql)
        self.assertNotIn('DELETE FROM public.raw_events',self.sql)
        self.assertNotIn('DELETE FROM public.feature_windows',self.sql)
        self.assertNotIn('GRANT UPDATE',self.sql)
        self.assertNotIn('GRANT ALL',self.sql)

    def test_security_definer_boundaries_require_explicit_stage(self):
        for role in ['pipeline_normalizer','pipeline_featurizer']:
            self.assertIn("session_user<>'pipeline_runner' OR current_setting('role')<>'"+role+"'",self.sql)
        self.assertIn('SET search_path=pg_catalog,public',self.sql)
        self.assertIn('FROM PUBLIC,anon,authenticated,service_role',self.sql)
        self.assertIn('WITH (security_invoker=true)',self.sql)

    def test_history_is_append_only_and_versions_positive_integer(self):
        for text in ['normalization_revision integer','normalization_revision>=1','NORMALIZATION_HISTORY_REWRITTEN',
                     'DUPLICATE_REVISION_REQUEST','REVISION_REQUEST_REUSED','FOR UPDATE','NORMALIZATION_REVISION_CONFLICT',
                     "'sql_snapshot'","'INITIAL_MATERIALIZATION'"]:
            self.assertIn(text,self.sql)

    def test_dependency_invalidation_contains_old_and_new_event_not_arbitrary_scope(self):
        self.assertIn("a.actor_code=old_dto->>'user_id'",self.sql)
        self.assertIn("a.actor_code=e->>'user_id'",self.sql)
        self.assertIn("SET lifecycle='archived',invalidated_by=rid",self.sql)
        self.assertIn("'NORMALIZATION_REVISED'",self.sql)

    def test_reconciliation_protects_partial_overlap_and_proves_exhaustiveness(self):
        self.assertIn('start_at<=window_start AND window_end<=end_at',self.sql)
        self.assertIn('RECALCULATION_SCOPE_NOT_EXHAUSTIVE',self.sql)
        self.assertIn('RECALCULATION_INPUT_MANIFEST_MISMATCH',self.sql)
        self.assertIn("WHERE lifecycle='active'",self.sql)

    def test_original_vector_math_is_unchanged(self):
        old=(ROOT/'migrations/0007_feature_persistence.sql').read_text(encoding='utf-8')
        original=old[old.index('CREATE FUNCTION public.feature_payload_valid'):old.index('REVOKE ALL ON FUNCTION public.feature_payload_valid')]
        expected=original.replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION',1).replace(
            "(m->'context'->>'normalization_version') IS DISTINCT FROM 'hu02-v1'",
            "COALESCE(m->'context'->>'normalization_version','') NOT IN ('hu02-v1','active-results-v1')")
        self.assertIn(expected,self.sql)

    def test_active_window_unique_across_contexts_and_no_false_empty_vector(self):
        self.assertIn('feature_active_logical_unique',self.sql)
        self.assertIn('actor_id,window_start,window_end',self.sql)
        self.assertIn("'vector',item->'features'",self.sql)
        self.assertNotIn("'operation_count',0",self.sql)

    def test_new_canonical_or_late_activation_invalidates_without_backfill(self):
        self.assertIn("OLD.processing_status<>'pending' OR NEW.processing_status<>'processed'",self.sql)
        self.assertIn('raw_initial_normalization_invalidation BEFORE UPDATE',self.sql)
        self.assertIn("'CANONICAL_EVENT_ACTIVATED'",self.sql)
        self.assertIn('NEW.pending_feature_scopes:=',self.sql)
        self.assertNotIn('UPDATE public.raw_events SET processing_status',self.sql)
