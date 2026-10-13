"""P18/P15 deterministic small fixtures; these are OFFLINE tests, not PostgreSQL."""
import copy
import unittest
import uuid
from unittest.mock import Mock, MagicMock, patch

from services.data_pipeline.features import WindowPolicy, extract_features, timestamp
from services.data_pipeline.repository import RepositoryError
from services.data_pipeline.revisions import (ACTIVE_INPUT_POLICY, prepare_revision,
    recover_revision, reconcile_windows, scopes_for, invalidate_dependents)
from services.data_pipeline.revision_repository import RevisionRepository
from test_features import event, SCHEDULES

MOMENT = '2026-10-12T14:00:00Z'
DIRECTORY = 'a'*64
CONTEXT = {'normalization_version': ACTIVE_INPUT_POLICY, 'directory_sha256': DIRECTORY}


def fixture():
    e = event('revision-fixture', '2026-09-02T14:10:00Z', 1024)
    e.update(raw_event_reference='csv-sha256:'+'b'*64+':1')
    current = dict(raw_id=str(uuid.uuid5(uuid.NAMESPACE_URL, 'raw-fixture')), status='processed',
                   revision=1, rules_version='hu02-v1', directory_sha256=DIRECTORY, history=[],
                   source=e['source'], source_event_id=e['source_event_id'],
                   raw_reference=e['raw_event_reference'], duplicate_of=None, normalized_event=e,
                   sql_snapshot={'id':e['event_id'], 'normalized_at':MOMENT},
                   processing_details=[{'code':'ORIGINAL'}], payload_sha256='c'*64)
    request = dict(request_id=str(uuid.uuid5(uuid.NAMESPACE_URL, 'revision-request')),
                   expected_revision=1, reason='Regla explicitamente revisada', rules_version='hu02-v2',
                   directory_sha256=DIRECTORY, status='processed', normalized_event=copy.deepcopy(e),
                   audit=[{'code':'APPROVED_REVISION','severity':'info'}])
    return current, request


def activate(current, plan):
    result = copy.deepcopy(current)
    if plan['changed']:
        entry = plan['entry']; result.update(revision=plan['revision'], history=plan['history'],
            rules_version=entry['rules_version'], status=entry['result']['status'],
            normalized_event=entry['result']['normalized_event'], pending_scopes=plan['pending_scopes'])
    return result


def calculate(events, context=CONTEXT, policy=None):
    return extract_features(events, SCHEDULES, context, policy)[0]


def stored(windows):
    return {w['window_id']:dict(window=copy.deepcopy(w), user_id=w['user_id'],
        window_start=w['window_start'], window_end=w['window_end'], lifecycle='active', retirement_history=[]) for w in windows}


class RevisionOfflineTest(unittest.TestCase):
    def test_initial_baseline_and_second_revision_preserve_exact_previous_result(self):
        current, request = fixture(); before = copy.deepcopy(current)
        plan = prepare_revision(current, request, MOMENT)
        self.assertEqual(plan['revision'], 2)
        first = recover_revision(plan['history'], 1)
        self.assertEqual(first['snapshot'], before)
        self.assertEqual(recover_revision(plan['history'], 2)['reason'], request['reason'])
        self.assertEqual(current, before)

    def test_active_revision_and_rules_version_are_separate(self):
        current, request = fixture(); request['rules_version']='hu02-v1'
        revised = activate(current, prepare_revision(current, request, MOMENT))
        self.assertEqual((revised['revision'], revised['rules_version']), (2, 'hu02-v1'))
        self.assertNotEqual(current['normalized_event']['event_id'], revised['normalized_event']['event_id'])
        self.assertEqual(revised['raw_id'], current['raw_id'])

    def test_retry_is_noop_including_after_later_revision(self):
        current, request = fixture(); revised=activate(current,prepare_revision(current,request,MOMENT))
        retry=prepare_revision(revised,request,MOMENT)
        self.assertFalse(retry['changed']); self.assertEqual(retry['revision'],2)
        other=copy.deepcopy(request);other.update(request_id=str(uuid.uuid5(uuid.NAMESPACE_URL,'later')),expected_revision=2)
        third=activate(revised,prepare_revision(revised,other,MOMENT))
        retry_after_later = prepare_revision(third,request,MOMENT)
        self.assertFalse(retry_after_later['changed'])
        self.assertEqual((retry_after_later['revision'],retry_after_later['active_revision']),(2,3))
        self.assertEqual(third['revision'],3)

    def test_persisted_sql_request_retry_is_supported(self):
        current, request=fixture();current['history']=[{'revision':2,'request_id':request['request_id'],'request':request}]
        current['revision']=2
        self.assertFalse(prepare_revision(current,request,MOMENT)['changed'])

    def test_reused_request_id_with_changed_content_is_rejected(self):
        current,request=fixture();revised=activate(current,prepare_revision(current,request,MOMENT));request['reason']='Otra'
        with self.assertRaisesRegex(RepositoryError,'REUSED'):prepare_revision(revised,request,MOMENT)

    def test_optimistic_conflict_does_not_mutate_current(self):
        current,request=fixture();current['revision']=2;before=copy.deepcopy(current)
        with self.assertRaisesRegex(RepositoryError,'CONFLICT'):prepare_revision(current,request,MOMENT)
        self.assertEqual(current,before)

    def test_invalid_revision_numbers_are_rejected(self):
        for value in [0,-1,1.5,True,'1',2147483647]:
            with self.subTest(value=value):
                current,request=fixture();request['expected_revision']=value
                with self.assertRaises(RepositoryError):prepare_revision(current,request,MOMENT)

    def test_database_precision_and_bigint_limits_rejected_before_write(self):
        current,request=fixture();request['normalized_event']['timestamp']='2026-09-02T14:10:00.1234567Z'
        with self.assertRaisesRegex(RepositoryError,'PRECISION'):prepare_revision(current,request,MOMENT)
        current,request=fixture();request['normalized_event']['file_size_bytes']=9223372036854775808
        with self.assertRaisesRegex(RepositoryError,'BIGINT'):prepare_revision(current,request,MOMENT)

    def test_duplicate_cannot_create_logical_revision(self):
        current,request=fixture();current.update(status='duplicate',duplicate_of='other')
        with self.assertRaisesRegex(RepositoryError,'CANONICAL'):prepare_revision(current,request,MOMENT)

    def test_origin_context_and_reason_are_not_invented(self):
        for change in ['source','raw_event_reference','directory_sha256','reason','rules_version']:
            current,request=fixture()
            if change in {'source','raw_event_reference'}:request['normalized_event'][change]='different'
            else:request[change]=''
            with self.subTest(change=change),self.assertRaises(RepositoryError):prepare_revision(current,request,MOMENT)

    def test_valid_to_quarantine_preserves_valid_snapshot_and_scope(self):
        current,request=fixture();request.update(status='rejected',normalized_event=None,audit=[{'code':'INVALID_DATE','severity':'error'}])
        plan=prepare_revision(current,request,MOMENT)
        self.assertEqual(len(plan['pending_scopes']),1)
        self.assertEqual(recover_revision(plan['history'],1)['snapshot']['normalized_event'],current['normalized_event'])
        self.assertIsNone(plan['entry']['result']['normalized_event'])

    def test_quarantine_to_valid_creates_one_destination_scope(self):
        current,request=fixture();current.update(status='rejected',normalized_event=None,sql_snapshot=None)
        plan=prepare_revision(current,request,MOMENT)
        self.assertEqual(plan['entry']['result']['status'],'processed');self.assertEqual(len(plan['pending_scopes']),1)

    def test_quarantine_requires_error_and_no_event(self):
        current,request=fixture();request['status']='rejected'
        with self.assertRaises(RepositoryError):prepare_revision(current,request,MOMENT)

    def test_unknown_identity_is_not_invented_during_revision(self):
        current,request=fixture();request['normalized_event']['user_id']='EMP009X'
        with self.assertRaisesRegex(RepositoryError,'DIRECTORY'):prepare_revision(current,request,MOMENT)

    def test_recovery_is_a_copy_and_missing_revision_is_rejected(self):
        current,request=fixture();plan=prepare_revision(current,request,MOMENT)
        old=recover_revision(plan['history'],1);old['snapshot']['status']='changed'
        self.assertEqual(plan['history'][0]['snapshot']['status'],'processed')
        with self.assertRaises(RepositoryError):recover_revision(plan['history'],4)

    def test_prepare_is_deterministic(self):
        current,request=fixture()
        self.assertEqual(prepare_revision(current,request,MOMENT),prepare_revision(current,request,MOMENT))


class RecalculationOfflineTest(unittest.TestCase):
    def setUp(self):
        self.current,self.request=fixture();self.e=self.current['normalized_event']
        self.policy=WindowPolicy(timestamp('2026-09-02T14:00:00Z'),timestamp('2026-09-02T15:00:00Z'))
        self.rid=self.request['request_id']

    def reconcile(self, existing, events, users=('EMP001',), policy=None):
        return reconcile_windows(existing,calculate(events,policy=policy or self.policy),users,policy or self.policy,self.rid,MOMENT)

    def test_single_window_and_neighbors_preserved(self):
        neighbor=event('neighbor','2026-09-02T15:10:00Z',200);other=event('other','2026-09-02T14:10:00Z',300,user='EMP002')
        before=stored(calculate([self.e,neighbor,other]));late=event('late','2026-09-02T14:50:00Z',512)
        after,changes=self.reconcile(before,[self.e,late]);self.assertEqual(len(changes),1)
        for id in set(before)-set(changes):self.assertEqual(after[id],before[id])

    def test_late_event_changes_counts_once_and_retry_is_noop(self):
        before=stored(calculate([self.e]));late=event('late','2026-09-02T14:20:00Z',512)
        after,changes=self.reconcile(before,[self.e,late]);again,repeated=self.reconcile(after,[self.e,late])
        self.assertEqual(repeated,[]);self.assertEqual(after,again)
        self.assertEqual(after[changes[0]]['window']['features']['total_bytes'],1536)

    def test_moving_time_affects_old_and_new_hour(self):
        moved=copy.deepcopy(self.e);moved['timestamp']='2026-09-02T15:00:00Z'
        scopes=scopes_for(self.e,moved);self.assertEqual(len(scopes),2)
        self.assertEqual(scopes[0]['end'],scopes[1]['start'])

    def test_moving_actor_affects_both_people(self):
        moved=copy.deepcopy(self.e);moved['user_id']='EMP002'
        self.assertEqual({x['user_id'] for x in scopes_for(self.e,moved)},{'EMP001','EMP002'})

    def test_empty_window_is_archived_not_zero_filled_and_history_is_exact(self):
        before=stored(calculate([self.e]));after,changes=self.reconcile(before,[])
        old=after[changes[0]];self.assertEqual(old['lifecycle'],'archived')
        self.assertEqual(old['window']['features']['operation_count'],1)
        self.assertEqual(old['retirement_history'][0]['window'],before[changes[0]])
        again,repeated=self.reconcile(after,[]);self.assertEqual(again,after);self.assertFalse(repeated)

    def test_previously_empty_window_receives_activity(self):
        after,changes=self.reconcile({},[self.e]);self.assertEqual(len(changes),1)
        self.assertEqual(after[changes[0]]['lifecycle'],'active')

    def test_partial_scope_does_not_archive_full_hour(self):
        before=stored(calculate([self.e]));policy=WindowPolicy(timestamp('2026-09-02T14:00:00Z'),timestamp('2026-09-02T14:30:00Z'))
        after,changes=self.reconcile(before,[],policy=policy)
        self.assertFalse(changes);self.assertEqual(before,after)

    def test_partial_new_window_keeps_full_and_provisional_state(self):
        before=stored(calculate([self.e]));policy=WindowPolicy(timestamp('2026-09-02T14:00:00Z'),timestamp('2026-09-02T14:30:00Z'))
        after,changes=self.reconcile(before,[self.e],policy=policy)
        self.assertEqual(len(changes),1);self.assertFalse(after[changes[0]]['window']['metadata']['model_eligible'])
        for id in before:self.assertEqual(before[id],after[id])

    def test_rule_only_revision_keeps_all_eleven_numeric_values(self):
        plan=prepare_revision(self.current,self.request,MOMENT);new=plan['entry']['result']['normalized_event']
        original=calculate([self.e])[0];revised=calculate([new])[0]
        self.assertEqual(original['features'],revised['features']);self.assertEqual(len(revised['features']),11)
        self.assertEqual(original['window_id'],revised['window_id'])
        self.assertNotEqual(original['metadata']['input_fingerprint_sha256'],revised['metadata']['input_fingerprint_sha256'])

    def test_quarantine_removes_activity_only_from_old_scope(self):
        before=stored(calculate([self.e,event('neighbor','2026-09-02T15:10:00Z',128)]))
        after,changes=self.reconcile(before,[]);self.assertEqual(len(changes),1)
        self.assertEqual(sum(r['lifecycle']=='active' for r in after.values()),1)

    def test_scopes_outside_dataset_rejected_not_silently_dropped(self):
        self.e['timestamp']='2026-10-02T00:00:00Z'
        with self.assertRaisesRegex(RepositoryError,'PERIOD'):scopes_for(self.e)

    def test_complete_operation_repeat_and_history_recovery(self):
        plan=prepare_revision(self.current,self.request,MOMENT);active=activate(self.current,plan)
        before=stored(calculate([self.e]));after,_=self.reconcile(before,[active['normalized_event']])
        again,changes=self.reconcile(after,[active['normalized_event']]);self.assertFalse(changes)
        self.assertEqual(after,again);self.assertFalse(prepare_revision(active,self.request,MOMENT)['changed'])
        self.assertEqual(recover_revision(active['history'],1)['snapshot']['normalized_event'],self.e)

    def test_recalculation_plan_does_not_mutate_inputs(self):
        before=stored(calculate([self.e]));snapshot=copy.deepcopy(before)
        self.reconcile(before,[]);self.assertEqual(before,snapshot)

    def test_revision_archives_both_source_and_destination_only(self):
        destination=event('destination','2026-09-02T15:10:00Z',200,user='EMP002')
        neighbor=event('neighbor','2026-09-02T16:10:00Z',300)
        before=stored(calculate([self.e,destination,neighbor]))
        moved=dict(self.e,user_id='EMP002',timestamp='2026-09-02T15:20:00Z')
        after,changes=invalidate_dependents(before,self.e,moved,self.rid,MOMENT)
        self.assertEqual(len(changes),2)
        for id in set(before)-set(changes):self.assertEqual(before[id],after[id])
        for id in changes:self.assertEqual(after[id]['lifecycle'],'archived')

    def test_same_hour_change_archives_only_one_window_and_retry_is_noop(self):
        before=stored(calculate([self.e]));after,changes=invalidate_dependents(before,self.e,self.e,self.rid,MOMENT)
        again,repeated=invalidate_dependents(after,self.e,self.e,self.rid,MOMENT)
        self.assertEqual(len(changes),1);self.assertEqual(again,after);self.assertFalse(repeated)

    def test_revision_invalidates_a_dependent_partial_but_recalculation_scope_protects_full_neighbor(self):
        partial=WindowPolicy(timestamp('2026-09-02T14:00:00Z'),timestamp('2026-09-02T14:30:00Z'))
        before=stored(calculate([self.e])+calculate([self.e],policy=partial))
        after,changes=invalidate_dependents(before,self.e,None,self.rid,MOMENT)
        self.assertEqual(len(changes),2)
        self.assertTrue(all(item['lifecycle']=='archived' for item in after.values()))

    def test_failed_prepared_coordination_keeps_original_state_unchanged(self):
        before=stored(calculate([self.e]));original=copy.deepcopy((self.current,before))
        def operation():
            plan=prepare_revision(self.current,self.request,MOMENT)
            invalidate_dependents(before,self.e,plan['entry']['result']['normalized_event'],self.rid,MOMENT)
            raise RepositoryError('FAIL_BEFORE_ACTIVATION')
        with self.assertRaises(RepositoryError):operation()
        self.assertEqual((self.current,before),original)

    def test_scopes_at_partition_boundary_remain_separate(self):
        old=dict(self.e,timestamp='2026-09-21T04:59:59Z')
        new=dict(self.e,timestamp='2026-09-21T05:00:00Z')
        scopes=scopes_for(old,new);self.assertEqual(len(scopes),2)
        self.assertEqual(scopes[0]['end'],scopes[1]['start'])

    def test_materially_equal_revision_requires_provenance_refresh_not_vector_change(self):
        before=stored(calculate([self.e]));plan=prepare_revision(self.current,self.request,MOMENT)
        archived,_=invalidate_dependents(before,self.e,plan['entry']['result']['normalized_event'],self.rid,MOMENT)
        final,changes=self.reconcile(archived,[plan['entry']['result']['normalized_event']])
        id=changes[0]
        self.assertEqual(final[id]['window']['features'],before[id]['window']['features'])
        self.assertEqual(final[id]['lifecycle'],'active')
        self.assertEqual(final[id]['retirement_history'][0]['snapshot']['window'],before[id]['window'])


class RevisionRepositoryOfflineTest(unittest.TestCase):
    @patch('services.data_pipeline.revision_repository.file_sha256', return_value=DIRECTORY)
    @patch('services.data_pipeline.revision_repository.activate_featurizer')
    @patch('services.data_pipeline.revision_repository.project_event')
    def test_scope_reader_traces_actual_revision_and_reuses_existing_math(self, project, role, sha):
        repo=object.__new__(RevisionRepository);repo.features=Mock()
        repo.features.schedules=SCHEDULES;repo.features.directory_sha=DIRECTORY;repo.features.context={'normalization_version':'hu02-v1','directory_sha256':DIRECTORY}
        current,_=fixture();project.return_value=current['normalized_event']
        c=MagicMock();actors={name:uuid.uuid5(uuid.NAMESPACE_URL,name) for name in SCHEDULES}
        c.execute.return_value.fetchall.return_value=[(id,code) for code,id in actors.items()]
        reader=c.cursor.return_value.__enter__.return_value
        reader.fetchmany.side_effect=[[dict(input_raw_id=current['raw_id'],normalization_revision=2,normalization_rules_version='hu02-v2')],[]]
        policy=WindowPolicy(timestamp('2026-09-02T14:00:00Z'),timestamp('2026-09-02T15:00:00Z'))
        windows,counts=repo.calculate_scope(c,'EMP001',policy,2)
        self.assertEqual(len(windows),1);self.assertEqual(counts['events_used'],1)
        self.assertEqual(windows[0]['features']['total_bytes'],1024)
        trace=windows[0]['metadata']['normalization_inputs'][0]
        self.assertEqual((trace['revision'],trace['rules_version']),(2,'hu02-v2'))
        self.assertEqual(windows[0]['metadata']['context']['normalization_version'],ACTIVE_INPUT_POLICY)
        query,params=reader.execute.call_args.args
        self.assertIn("r.processing_status='processed'",query);self.assertIn('r.duplicate_of IS NULL',query)
        self.assertEqual(params,(actors['EMP001'],policy.start,policy.end))

    @patch('services.data_pipeline.revision_repository.file_sha256', return_value=DIRECTORY)
    @patch('services.data_pipeline.revision_repository.activate_featurizer')
    def test_scope_reader_omits_empty_window_and_has_no_invented_zero_vector(self, role, sha):
        repo=object.__new__(RevisionRepository);repo.features=Mock()
        repo.features.schedules=SCHEDULES;repo.features.context=CONTEXT;repo.features.directory_sha=DIRECTORY
        c=MagicMock();c.execute.return_value.fetchall.return_value=[(uuid.uuid4(),code) for code in SCHEDULES]
        c.cursor.return_value.__enter__.return_value.fetchmany.return_value=[]
        windows,counts=repo.calculate_scope(c,'EMP001',WindowPolicy())
        self.assertEqual(windows,[]);self.assertEqual(counts,{})

    def test_failure_after_prepare_performs_no_sql_mutation(self):
        repo=object.__new__(RevisionRepository);repo.current=Mock(return_value=fixture()[0]);repo.lock=Mock();c=Mock()
        with self.assertRaisesRegex(RepositoryError,'PREPARATION_FAILURE'):
            repo.revise(c,fixture()[0]['raw_id'],fixture()[1],fail_after_prepare=True)
        c.execute.assert_not_called()

    def test_failure_after_revision_write_requires_caller_rollback(self):
        repo=object.__new__(RevisionRepository);repo.current=Mock(return_value=fixture()[0]);repo.lock=Mock();c=Mock()
        c.execute.return_value.fetchone.return_value=[{'changed':True}]
        with self.assertRaisesRegex(RepositoryError,'REVISION_WRITE_FAILURE'):
            repo.revise(c,fixture()[0]['raw_id'],fixture()[1],fail_after_write=True)
        self.assertEqual(c.execute.call_count,1)

    def test_revision_uses_parametrized_atomic_boundary(self):
        repo=object.__new__(RevisionRepository);repo.current=Mock(return_value=fixture()[0]);repo.lock=Mock();c=Mock()
        c.execute.return_value.fetchone.return_value=[{'changed':True}]
        self.assertTrue(repo.revise(c,fixture()[0]['raw_id'],fixture()[1])['changed'])
        query,args=c.execute.call_args.args;self.assertIn('revise_normalization(%s',query)
        self.assertEqual(args[1].obj,fixture()[1])

    def test_failure_after_recalculation_propagates_to_transaction(self):
        repo=object.__new__(RevisionRepository);repo.lock=Mock();repo.calculate_scope=Mock(return_value=([],{}));c=Mock()
        c.execute.return_value.fetchone.return_value=[{'inserted':0}]
        with self.assertRaisesRegex(RepositoryError,'RECALCULATION_FAILURE'):
            repo.recalculate(c,'EMP001',WindowPolicy(),str(uuid.uuid4()),fail_after_write=True)

    def test_transaction_is_repeatable_read_and_exception_prevents_commit_result(self):
        repo=object.__new__(RevisionRepository);c=Mock();repo.connect=Mock()
        repo.connect.return_value.__enter__=Mock(return_value=c);repo.connect.return_value.__exit__=Mock(return_value=False)
        c.transaction.return_value.__enter__=Mock();c.transaction.return_value.__exit__=Mock(return_value=False)
        def failure(connection):raise RepositoryError('INJECTED')
        with self.assertRaises(RepositoryError):repo._run(failure)
        self.assertEqual(c.transaction.return_value.__exit__.call_args.args[0],RepositoryError)
