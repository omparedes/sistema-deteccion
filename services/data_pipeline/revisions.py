"""Explicit HU02 revisions and scoped HU03 reconciliation, without database I/O."""
import copy
import re
import uuid

from .features import WindowPolicy, digest, iso, timestamp, validate_window
from .normalized_contract import validate_normalized
from .repository import RepositoryError

ACTIVE_INPUT_POLICY = 'active-results-v1'


def positive_revision(value):
    if type(value) is not int or not 1 <= value <= 2147483647:
        raise RepositoryError('INVALID_NORMALIZATION_REVISION')
    return value


def scopes_for(*events):
    """Old and new complete UTC hours, clipped to the approved dataset partitions."""
    scopes = {}
    for event in events:
        if event is None:
            continue
        slot = WindowPolicy().assign(event['timestamp'])
        if slot is None:
            raise RepositoryError('REVISION_OUTSIDE_APPROVED_PERIOD')
        _, start, end = slot
        key = (event['user_id'], iso(start), iso(end))
        scopes[key] = dict(user_id=key[0], start=key[1], end=key[2])
    return [scopes[key] for key in sorted(scopes)]


def prepare_revision(current, request, moment):
    """No mutation; a retry returns the original revision, even after later revisions."""
    timestamp(moment)
    required = {'request_id', 'expected_revision', 'reason', 'rules_version',
                'directory_sha256', 'status', 'normalized_event', 'audit'}
    if not required <= set(request) or set(request) - required - {'field_states', 'temporal'}:
        raise RepositoryError('INVALID_REVISION_REQUEST_SHAPE')
    positive_revision(request['expected_revision'])
    if request['expected_revision'] == 2147483647:
        raise RepositoryError('INVALID_NORMALIZATION_REVISION_OVERFLOW')
    try:
        if str(uuid.UUID(request['request_id'])) != request['request_id']:
            raise ValueError()
    except (ValueError, AttributeError, TypeError):
        raise RepositoryError('INVALID_REVISION_REQUEST_ID') from None
    if (not isinstance(request['reason'], str) or not request['reason'].strip()
            or len(request['reason']) > 2000
            or not isinstance(request['rules_version'], str)
            or not re.fullmatch(r'hu02-v[1-9][0-9]*', request['rules_version'])
            or request['directory_sha256'] != current['directory_sha256']
            or not isinstance(request['audit'], list) or not request['audit']
            or any(not isinstance(a, dict) or not isinstance(a.get('code'), str)
                   or not a['code'].strip()
                   or a.get('severity') not in {'info', 'warning', 'error'} for a in request['audit'])):
        raise RepositoryError('INVALID_REVISION_REASON_OR_CONTEXT')
    if current.get('duplicate_of') is not None or current['status'] not in {'processed', 'rejected'}:
        raise RepositoryError('ONLY_CANONICAL_TERMINAL_RAW_CAN_BE_REVISED')
    status, event = request['status'], request['normalized_event']
    if status == 'processed':
        validate_normalized(event)
        known_users = current.get('known_users', [f'EMP{i:03d}' for i in range(1, 26)])
        if event['user_id'] not in known_users:
            raise RepositoryError('REVISION_IDENTITY_NOT_IN_APPROVED_DIRECTORY')
        if event['file_size_bytes'] > 9223372036854775807:
            raise RepositoryError('REVISION_SIZE_EXCEEDS_POSTGRES_BIGINT')
        precision = re.search(r'\.([0-9]+)', event['timestamp'])
        if precision and len(precision.group(1)) > 6:
            raise RepositoryError('REVISION_TIMESTAMP_EXCEEDS_POSTGRES_PRECISION')
        if (event['source'] != current['source'] or event['source_event_id'] != current['source_event_id']
                or event.get('raw_event_reference') != current['raw_reference']
                or any(a['severity'] == 'error' for a in request['audit'])):
            raise RepositoryError('REVISION_ORIGIN_OR_STATUS_CHANGED')
        # Validate period before preparing any mutation.
        scopes_for(event)
    elif status != 'rejected' or event is not None or not any(a['severity'] == 'error' for a in request['audit']):
        raise RepositoryError('INVALID_REVISION_QUARANTINE_RESULT')
    fingerprint = digest(request)
    history = copy.deepcopy(current.get('history', []))
    for previous in history:
        if previous.get('request_id') == request['request_id']:
            stored_fingerprint = previous.get('request_fingerprint')
            if stored_fingerprint is None and 'request' in previous:
                stored_fingerprint = digest(previous['request'])
            if stored_fingerprint != fingerprint:
                raise RepositoryError('REVISION_REQUEST_REUSED_WITH_DIFFERENT_CONTENT')
            return dict(changed=False, revision=previous['revision'], history=history,
                        active_revision=current['revision'],
                        pending_scopes=copy.deepcopy(current.get('pending_scopes', [])))
    if request['expected_revision'] != positive_revision(current['revision']):
        raise RepositoryError('NORMALIZATION_REVISION_CONFLICT')
    if not history:
        history.append(dict(revision=current['revision'], rules_version=current['rules_version'],
                            request_id=None, reason='INITIAL_MATERIALIZATION',
                            snapshot=copy.deepcopy(current)))
    old = current.get('normalized_event')
    entry = dict(revision=current['revision'] + 1, rules_version=request['rules_version'],
                 request_id=request['request_id'], request_fingerprint=fingerprint,
                 reason=request['reason'], changed_at=moment, result=copy.deepcopy(request),
                 previous_snapshot=copy.deepcopy({k:v for k,v in current.items()
                                                 if k not in {'history', 'pending_scopes'}}))
    # Result UUID is version-specific; RAW remains the stable logical identity.
    if event is not None:
        event = copy.deepcopy(event)
        event['event_id'] = str(uuid.uuid5(uuid.NAMESPACE_URL,
            f"normalization:{current['raw_id']}:{entry['revision']}:{request['rules_version']}"))
        entry['result']['normalized_event'] = event
    entry['affected_scopes'] = scopes_for(old, event)
    history.append(entry)
    return dict(changed=True, revision=entry['revision'], history=history, entry=entry,
                active_revision=entry['revision'],
                pending_scopes=entry['affected_scopes'])


def recover_revision(history, revision):
    positive_revision(revision)
    for entry in history:
        if entry['revision'] == revision:
            return copy.deepcopy(entry)
    raise RepositoryError('NORMALIZATION_REVISION_NOT_FOUND')


def invalidate_dependents(windows, old_event, new_event, request_id, moment):
    """Pure preview/oracle for atomic SQL invalidation, including dependent partials."""
    uuid.UUID(request_id); timestamp(moment)
    result = copy.deepcopy(windows)
    changed = []
    for id, previous in windows.items():
        dependent = any(event is not None and previous['user_id'] == event['user_id']
            and timestamp(previous['window_start']) <= timestamp(event['timestamp']) < timestamp(previous['window_end'])
            for event in (old_event, new_event))
        if dependent and previous.get('lifecycle', 'active') == 'active':
            result[id].setdefault('retirement_history', []).append(dict(
                request_id=request_id, reason='NORMALIZATION_REVISED', at=moment,
                snapshot=copy.deepcopy({k:v for k,v in previous.items() if k != 'retirement_history'})))
            result[id]['lifecycle'] = 'archived'; changed.append(id)
    return result, sorted(changed)


def reconcile_windows(existing, incoming, users, policy, request_id, moment):
    """Plan only fully contained windows. Preserve exact old payloads on any change."""
    uuid.UUID(request_id); timestamp(moment)
    if not users or len(users) != len(set(users)):
        raise RepositoryError('INVALID_RECALCULATION_ACTORS')
    incoming_ids = set()
    keys = set()
    for window in incoming:
        validate_window(window)
        key = (window['user_id'], window['window_start'], window['window_end'])
        if (window['window_id'] in incoming_ids or key in keys or window['user_id'] not in users
                or not policy.start <= timestamp(window['window_start']) < timestamp(window['window_end']) <= policy.end):
            raise RepositoryError('INVALID_RECALCULATION_INPUT_SCOPE')
        old = existing.get(window['window_id'])
        if old and (old['user_id'],old['window_start'],old['window_end']) != key:
            raise RepositoryError('RECALCULATION_IDENTITY_CONFLICT')
        keys.add(key); incoming_ids.add(window['window_id'])
    result = copy.deepcopy(existing)
    changed = []
    for id, previous in result.items():
        if (previous['user_id'] in users
                and policy.start <= timestamp(previous['window_start']) < timestamp(previous['window_end']) <= policy.end
                and previous.get('lifecycle', 'active') == 'active' and id not in incoming_ids):
            previous.setdefault('retirement_history', []).append(dict(
                request_id=request_id, reason='OBSOLETE_IN_AUTHORIZED_SCOPE', at=moment,
                window=copy.deepcopy(existing[id])))
            previous['lifecycle'] = 'archived'; changed.append(id)
    for window in incoming:
        id = window['window_id']; old = result.get(id)
        if old and old.get('lifecycle', 'active') == 'active' and old['window'] == window:
            continue
        history = copy.deepcopy(old.get('retirement_history', [])) if old else []
        if old:
            history.append(dict(request_id=request_id, reason='RECALCULATED', at=moment,
                                window=copy.deepcopy(old['window'])))
        result[id] = dict(window=copy.deepcopy(window), user_id=window['user_id'],
                          window_start=window['window_start'], window_end=window['window_end'],
                          lifecycle='active', retirement_history=history)
        changed.append(id)
    return result, sorted(set(changed))
