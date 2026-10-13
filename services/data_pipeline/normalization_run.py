"""Ejecucion offline y estadisticas reproducibles; no contiene acceso a DB."""
import json
import hashlib
from collections import Counter,defaultdict
from pathlib import Path

from .adapters import SOURCES,file_sha256,read_dataset_batches
from .normalization import EmployeeDirectory,QualityError,VERSION,normalize_records


def normalize_dataset(directory,out=None):
    directory = Path(directory).resolve()
    out = Path(out).resolve() if out is not None else None
    if out and (out==directory or directory in out.parents):
        raise ValueError('OUTPUT_MUST_NOT_BE_INSIDE_INPUT')
    repo=Path(__file__).resolve().parents[2]
    if out and repo in out.parents and not any(base==out or base in out.parents for base in [repo/'data/processed',repo/'data/private']):
        raise ValueError('OUTPUT_MUST_BE_EXCLUDED_FROM_PUBLIC_GIT')
    if out:
        if out.exists():
            raise ValueError('OUTPUT_ALREADY_EXISTS')
        out.mkdir(parents=True)
    employee_path = directory/'employees_directory.csv'
    directory_hash = file_sha256(employee_path)
    employees = EmployeeDirectory.read(employee_path)
    input_hashes = {s.filename:file_sha256(directory/s.filename) for s in SOURCES}
    records = [record for batch in read_dataset_batches(directory,500) for record in batch.records]
    if any(record.file.sha256!=input_hashes[record.file.path.name] for record in records):
        raise ValueError('INPUT_CHANGED')
    counts = Counter()
    per_source = defaultdict(Counter)
    issues = Counter()
    issues_source = defaultdict(Counter)
    errors_unique = Counter()
    field_states = defaultdict(Counter)
    quality = defaultdict(Counter)
    examples = {}
    outputs = {}
    fingerprints = hashlib.sha256()
    if out:
        for status,name in [('processed','normalized.jsonl'),('rejected','quarantine.jsonl'),('duplicate','duplicates.jsonl')]:
            outputs[status]=(out/name).open('x',encoding='utf-8',newline='\n')
    try:
        for result in normalize_records(records,employees,directory_hash):
            status,source = result['status'],result['source']
            counts[status]+=1; per_source[source][status]+=1
            codes = {entry['code'] for entry in result['audit']}
            issues.update(codes); issues_source[source].update(codes)
            if status!='duplicate':
                errors_unique.update({entry['code'] for entry in result['audit'] if entry['severity']=='error'})
            for field,state in result['field_states'].items():
                field_states[field][state]+=1
            original=result['raw_payload']
            if 'MISSING_DESTINATION' in codes:
                quality[source]['missing_destination']+=1
            if 'INVALID_TIMESTAMP' in codes or 'MISSING_TIMESTAMP' in codes:
                quality[source]['invalid_timestamp']+=1
            value=original.get({'endpoint':'TAMANO','cloud':'sizeBytes','transfer':'Size'}[source])
            if isinstance(value,str) and value.strip().casefold()=='n/a':
                quality[source]['invalid_size_na']+=1
            original_actor=original.get({'endpoint':'USUARIO','cloud':'actorId','transfer':'Account'}[source])
            try:
                actor=employees.resolve(original_actor)
                if original_actor not in employees.source_aliases[actor][source]:
                    quality[source]['noncanonical_actor']+=1
            except QualityError:
                pass
            encoded=json.dumps(result,sort_keys=True,ensure_ascii=False,separators=(',',':'))+'\n'
            fingerprints.update(encoded.encode('utf-8'))
            if outputs:
                outputs[status].write(encoded)
            event_id=original.get({'endpoint':'id_evento','cloud':'recordId','transfer':'TransferId'}[source])
            if event_id in {'ENDPOINT-0001195','CLOUD-0000769','TRANSFER-0001812'} and status!='duplicate':
                examples[event_id]=result
            for kind,condition in [('quarantine',status=='rejected'),('missing_destination',status=='processed' and 'MISSING_DESTINATION' in codes),('duplicate',status=='duplicate')]:
                if condition and kind not in examples:
                    examples[kind]=result
    finally:
        for stream in outputs.values():
            stream.close()
    if file_sha256(employee_path)!=directory_hash:
        raise ValueError('DIRECTORY_CHANGED')
    if any(file_sha256(directory/name)!=digest for name,digest in input_hashes.items()):
        raise ValueError('INPUT_CHANGED')
    total=sum(counts.values())
    report={'provenance':'SYNTHETIC_GENERATED_NOT_OBSERVED','version':VERSION,'employee_count':len(employees.codes),
            'directory_sha256':directory_hash,'raw_received':total,'unique_events':total-counts['duplicate'],
            'exact_duplicates':counts['duplicate'],'normalized_valid':counts['processed'],'quarantine':counts['rejected'],
            'per_source':{s.code:{'raw':sum(per_source[s.code].values()),'processed':per_source[s.code]['processed'],
                'rejected':per_source[s.code]['rejected'],'duplicate':per_source[s.code]['duplicate']} for s in SOURCES},
            'incidents_all_occurrences':dict(sorted(issues.items())),
            'incidents_by_source':{s:dict(sorted(v.items())) for s,v in sorted(issues_source.items())},
            'fatal_incidents_unique':dict(sorted(errors_unique.items())),
            'field_states':{s:dict(v) for s,v in sorted(field_states.items())},
            'raw_quality_measured':{s:dict(v) for s,v in sorted(quality.items())},
            'logical_fingerprint_sha256':fingerprints.hexdigest(),
            'input_sha256':input_hashes,
            'database_writes':False}
    assert total==report['normalized_valid']+report['quarantine']+report['exact_duplicates']
    manifest_path=directory/'synthetic_manifest.json'
    if manifest_path.is_file():
        # Solo cantidades de calidad, nunca escenarios ni ground_truth.
        manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
        report['manifest_count_matches']={
            'raw':total==manifest['events_total_including_duplicate_rows'],
            'unique':report['unique_events']==manifest['events_unique'],
            'duplicates':report['exact_duplicates']==sum(manifest['intentional_duplicate_rows'].values())}
        report['manifest_quality_comparison']={s:{k:{'measured':quality[s][k],'manifest':expected[k],
             'matches':quality[s][k]==expected[k]} for k in ['missing_destination','invalid_timestamp','invalid_size_na','noncanonical_actor']}
             for s,expected in manifest['quality_counts_including_duplicates'].items()}
    if out:
        for name,content in [('quality_report.json',report),('examples.json',examples)]:
            with (out/name).open('x',encoding='utf-8') as stream:
                json.dump(content,stream,ensure_ascii=False,indent=2,sort_keys=True)
    return report
