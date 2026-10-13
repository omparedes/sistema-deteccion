"""Permanent valid-request regression and input cases for the pure SQL validator.

Offline tests parse and inspect the migration; PostgreSQL evaluation is explicit.
"""
import copy
import re
import unittest
from pathlib import Path
from pglast import parse_sql, parse_plpgsql
from services.data_pipeline.normalized_contract import validate_normalized

MIGRATION = Path(__file__).resolve().parents[1] / 'scripts/db/migrations/0010_fix_normalization_revision_validator.sql'


def validator_cases():
    event = dict(event_id='15d3f086-91f2-5796-8d23-29ef8e5c0820', source='endpoint',
                 source_event_id='synthetic-validator-regression', timestamp='2026-09-01T06:10:00Z',
                 user_id='EMP002', operation='file_access', file_size_bytes=1000,
                 is_synthetic=True, raw_event_reference='csv-sha256:' + 'a' * 64 + ':1')
    request = dict(request_id='a19f3085-2a0a-517c-86aa-f8bb358dc0ee', expected_revision=1,
                   reason='Approved synthetic fixture correction', rules_version='hu02-v2',
                   directory_sha256='a' * 64, status='processed', normalized_event=event,
                   audit=[{'code': 'FIXTURE_REVISION', 'severity': 'info'}])
    cases = [{'id': 'f5_e2_valid_request_regression', 'request': request, 'expected': True}]
    def add(name, key, value, valid=False, nested=False):
        q = copy.deepcopy(request)
        (q['normalized_event'] if nested else q)[key] = value
        cases.append(dict(id=name, request=q, expected=valid))
    add('empty_reason', 'reason', '')
    add('blank_reason', 'reason', '  ')
    add('null_reason', 'reason', None)
    add('reason_wrong_type', 'reason', 1)
    add('unknown_request_key', 'unknown', 1)
    add('unknown_event_key', 'unknown', 1, nested=True)
    add('bad_uuid', 'request_id', 'bad')
    for name, value in [('null', None), ('fraction', 1.5), ('bool', True), ('zero', 0), ('overflow', 2147483647), ('text', '1')]:
        add('revision_' + name, 'expected_revision', value)
    add('audit_null', 'audit', None)
    add('audit_scalar', 'audit', 'bad')
    add('audit_empty', 'audit', [])
    add('audit_wrong_severity', 'audit', [{'code':'FIXTURE', 'severity':'bad'}])
    add('bad_rules', 'rules_version', 'hu02-v0')
    add('bad_directory', 'directory_sha256', 'bad')
    add('bad_status', 'status', 'pending')
    add('event_null', 'normalized_event', None)
    add('event_scalar', 'normalized_event', 1)
    add('event_array', 'normalized_event', [])
    add('event_uuid_bad', 'event_id', 'bad', nested=True)
    add('invalid_day', 'timestamp', '2026-09-31T06:10:00Z', nested=True)
    add('timestamp_missing_zone', 'timestamp', '2026-09-01T06:10:00', nested=True)
    add('timestamp_outside_period', 'timestamp', '2026-10-01T06:10:00Z', nested=True)
    add('file_negative', 'file_size_bytes', -1, nested=True)
    add('file_fraction', 'file_size_bytes', 1.5, nested=True)
    add('file_null', 'file_size_bytes', None, nested=True)
    add('file_overflow', 'file_size_bytes', 9223372036854775808, nested=True)
    add('file_zero', 'file_size_bytes', 0, True, nested=True)
    add('file_maximum', 'file_size_bytes', 9223372036854775807, True, nested=True)
    add('optional_null', 'destination_path', None, True, nested=True)
    add('optional_wrong_type', 'destination_path', [], nested=True)
    add('not_synthetic', 'is_synthetic', False, nested=True)
    add('invalid_operation', 'operation', 'bad', nested=True)
    for name, root in [('root_null',None), ('root_array',[]), ('root_string','bad')]:
        cases.append(dict(id=name, request=root, expected=False))
    q = copy.deepcopy(request); del q['reason']
    cases.append(dict(id='missing_reason',request=q,expected=False))
    q = copy.deepcopy(request); q.update(status='rejected', normalized_event=None, audit=[{'code':'FIXTURE_REJECT','severity':'error'}])
    cases.append(dict(id='valid_quarantine',request=q,expected=True))
    q = copy.deepcopy(q); q['audit'][0]['severity']='info'
    cases.append(dict(id='invalid_quarantine',request=q,expected=False))
    return cases


class RevisionValidatorMigrationTest(unittest.TestCase):
    def test_single_function_and_pg17_parse(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        self.assertEqual(len(parse_sql(sql)),1)
        self.assertEqual(len(parse_plpgsql(sql)),1)
        self.assertIn('CREATE OR REPLACE FUNCTION public.normalization_revision_request_valid(q jsonb)',sql)

    def test_both_key_references_are_qualified(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        self.assertIn('request_keys.key_name NOT IN',sql)
        self.assertIn('event_keys.key_name NOT IN',sql)
        self.assertNotRegex(sql,r'jsonb_object_keys\([qe]\) k')

    def test_security_and_expected_input_exceptions_only(self):
        sql=MIGRATION.read_text(encoding='utf-8')
        self.assertIn('IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,public',sql)
        self.assertNotIn('WHEN OTHERS',sql)
        self.assertNotRegex(sql,r'(?im)^\s*(GRANT|REVOKE|ALTER|DROP|INSERT|UPDATE|DELETE|CREATE TABLE)\b')
        self.assertIn('invalid_text_representation OR invalid_parameter_value',sql)

    def test_f5_e2_request_is_contractual_and_cases_are_unique(self):
        cases=validator_cases()
        validate_normalized(cases[0]['request']['normalized_event'])
        self.assertTrue(cases[0]['expected'])
        self.assertEqual(len(cases),len({case['id'] for case in cases}))
        self.assertGreaterEqual(len(cases),40)

    def test_validation_body_changes_are_only_aliases_and_exception_scope(self):
        old=(MIGRATION.parent/'0009_normalization_versioning_feature_invalidation.sql').read_text(encoding='utf-8')
        old=old[old.index('CREATE FUNCTION public.normalization_revision_request_valid'):old.index('-- Narrow privileged boundary')]
        old=old.replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION',1).replace('FROM jsonb_object_keys(q) k WHERE k NOT IN','FROM jsonb_object_keys(q) AS request_keys(key_name) WHERE request_keys.key_name NOT IN').replace('FROM jsonb_object_keys(e) k WHERE k NOT IN','FROM jsonb_object_keys(e) AS event_keys(key_name) WHERE event_keys.key_name NOT IN').replace('EXCEPTION WHEN OTHERS THEN RETURN false;','EXCEPTION WHEN invalid_text_representation OR invalid_parameter_value OR invalid_datetime_format OR datetime_field_overflow OR numeric_value_out_of_range THEN RETURN false;')
        self.assertTrue(MIGRATION.read_text(encoding='utf-8').endswith(old))
