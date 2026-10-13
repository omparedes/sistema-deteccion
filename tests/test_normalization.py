"""Oraculos HU02 offline, sin DB, etiquetas ni regeneracion de datos."""
import copy
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.data_pipeline.adapters import SourceFile,RawRecord,SOURCES
from services.data_pipeline.normalization import (EmployeeDirectory,QualityError,OPERATIONS,
    normalize_timestamp,normalize_size,normalize_destination,normalize_record,normalize_records)
from services.data_pipeline.normalized_contract import ContractError,validate_normalized
from services.data_pipeline.normalization_run import normalize_dataset

ROOT=Path(__file__).resolve().parents[1]


def directory():
    return EmployeeDirectory([{'EmployeeCode':'EMP018','AccountAlias':'u-018','TransferAlias':'staff-018'}])


def record(source='endpoint',row=1,changes=None):
    spec=next(s for s in SOURCES if s.code==source)
    payload={k:'' for k in spec.columns}
    data={
        'endpoint':{'id_evento':'E1','USUARIO':'u-018','FECHA':'01/09/2026 09:00','TAMANO':'20.48','UNIDAD':'MB','ACCION':'Editar','ARCHIVO':'sample.PY','RUTA_DESTINO':r'\\HOST\Path'},
        'cloud':{'recordId':'E1','actorId':'U-018','eventTime':'2026-09-01T09:00:00-05:00','sizeBytes':'8019605','activity':'DOWNLOAD','resourceName':'sample.csv','sourceType':'CORPORATE_REPOSITORY','destination':'HOST.internal'},
        'transfer':{'TransferId':'E1','Account':'STAFF-018','TimeStamp':'09/01/2026 09:00 AM','Size':'5180.66 KB','TransferMethod':'EMAIL_ATTACHMENT','Object':'sample.csv','Destination':'HOST.internal'}}
    payload.update(data[source]); payload.update(changes or {})
    source_file=SourceFile(Path(spec.filename),source,{'endpoint':'a','cloud':'b','transfer':'c'}[source]*64,100)
    return RawRecord(source_file,row,payload,spec.event_id_column)


class NormalizationTest(unittest.TestCase):
    def test_cli_normalize_never_reads_env_or_connects(self):
        from services.data_pipeline.cli import main
        with patch('services.data_pipeline.cli.load_settings',side_effect=AssertionError('env forbidden')),patch('psycopg.connect',side_effect=AssertionError('db forbidden')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['normalize','--input',str(ROOT/'data/generated/v2')]),0)

    def test_timestamp_regional_oracles(self):
        for source,value in [('endpoint','01/09/2026 09:00'),('transfer','09/01/2026 09:00 AM'),('cloud','2026-09-01T09:00:00-05:00')]:
            self.assertEqual(normalize_timestamp(source,value)['timestamp'],'2026-09-01T14:00:00Z')

    def test_iso_offsets_z_naive_and_fraction_preserved(self):
        for value in ['2026-09-01T14:00:00Z','2026-09-01T16:00:00+02:00','2026-09-01T09:00:00']:
            self.assertEqual(normalize_timestamp('cloud',value)['timestamp'],'2026-09-01T14:00:00Z')
        self.assertEqual(normalize_timestamp('cloud','2026-09-01T14:00:00.123Z')['timestamp'],'2026-09-01T14:00:00.123000Z')
        self.assertEqual(normalize_timestamp('cloud','2026-09-01T14:00:00Z')['lima_timestamp'],'2026-09-01T09:00:00-05:00')

    def test_regional_ambiguity_is_source_specific(self):
        self.assertTrue(normalize_timestamp('endpoint','09/01/2026 09:00')['timestamp'].startswith('2026-01-09'))
        self.assertTrue(normalize_timestamp('transfer','09/01/2026 09:00 AM')['timestamp'].startswith('2026-09-01'))
        with self.assertRaises(QualityError): normalize_timestamp('cloud','09/01/2026 09:00')
        with self.assertRaises(QualityError): normalize_timestamp('transfer','09/01/2026 09:00')

    def test_invalid_dates_and_offsets(self):
        for source,value in [('endpoint','31/09/2026 09:00'),('cloud','2026-02-30T10:00:00Z'),('cloud','2026-09-01T10:00:00+25:00'),('transfer','09/01/2026 13:00 PM'),('endpoint','fecha_desconocida')]:
            with self.assertRaises(QualityError): normalize_timestamp(source,value)

    def test_am_pm_midnight_noon(self):
        self.assertIn('T05:00',normalize_timestamp('transfer','09/01/2026 12:00 AM')['timestamp'])
        self.assertIn('T17:00',normalize_timestamp('transfer','09/01/2026 12:00 PM')['timestamp'])

    def test_binary_size_half_up_oracles(self):
        for value,unit,expected in [('1','KB',1024),('1.5','MB',1572864),('20.48','MB',21474836),('5180.66','KB',5304996),('8019605','bytes',8019605),('0.5','B',1),('0','B',0)]:
            self.assertEqual(normalize_size(value,unit)[0],expected)

    def test_invalid_missing_size_not_zero(self):
        for value,unit in [('N/A','B'),('','B'),('-1','KB'),('1','GB'),('1,5','MB'),('NaN','B'),('1',None)]:
            with self.assertRaises(QualityError): normalize_size(value,unit)

    def test_aliases_all_families_casefold(self):
        for alias in ['EMP018','emp018','u-018',' U-018 ','staff-018','STAFF-018']:
            self.assertEqual(directory().resolve(alias),'EMP018')

    def test_unknown_missing_and_ambiguous_identity(self):
        for value in ['someone',None,'']:
            with self.assertRaises(QualityError): directory().resolve(value)
        rows=[{'EmployeeCode':code,'AccountAlias':'shared','TransferAlias':code} for code in ['EMP001','EMP002']]
        with self.assertRaisesRegex(QualityError,'AMBIGUOUS_ACTOR'): EmployeeDirectory(rows).resolve('SHARED')

    def test_operations_catalogue_complete_and_unknown(self):
        for raw,expected in OPERATIONS.items():
            self.assertEqual(normalize_record(record(changes={'ACCION':raw.upper()}),directory())['normalized_event']['operation'],expected)
        result=normalize_record(record(changes={'ACCION':'unknown'}),directory())
        self.assertEqual(result['normalized_event']['operation'],'other')
        self.assertIn('UNKNOWN_OPERATION',[x['code'] for x in result['audit']])

    def test_destination_grammars_preserve_path_case(self):
        for raw,expected in [('HTTPS://HOST.Invalid/KeepCase?X=Y','https://host.invalid/KeepCase?X=Y'),(r'\\HOST\KeepCase',r'\\host\KeepCase'),(r'C:\KeepCase',r'C:\KeepCase'),('/KeepCase','/KeepCase'),('USB-SYNTHETIC-EMP018:/KeepCase','USB-SYNTHETIC-EMP018:/KeepCase'),('HOST.Internal','host.internal')]:
            self.assertEqual(normalize_destination(raw)[0],expected)

    def test_destination_absent_invalid_not_applicable_distinct(self):
        self.assertEqual(normalize_destination('')[1],'absent')
        self.assertEqual(normalize_record(record(source='cloud'),directory())['field_states']['source_path'],'not_applicable')
        self.assertEqual(normalize_destination('not_applicable')[1],'invalid')
        self.assertEqual(normalize_destination('???')[1],'invalid')
        self.assertEqual(normalize_destination('https://host:invalid/x')[1],'invalid')

    def test_optional_missing_destination_not_quarantine(self):
        result=normalize_record(record(changes={'RUTA_DESTINO':''}),directory())
        self.assertEqual(result['status'],'processed')
        self.assertIsNone(result['normalized_event']['destination_path'])
        self.assertIsNone(result['normalized_event']['destination_authorized'])

    def test_quarantine_collects_overlapping_reasons(self):
        result=normalize_record(record(changes={'FECHA':'invalid','TAMANO':'N/A','USUARIO':'unknown'}),directory())
        self.assertEqual(result['status'],'rejected')
        self.assertIsNone(result['normalized_event'])
        self.assertEqual(sum(x['severity']=='error' for x in result['audit']),3)

    def test_required_missing_and_malformed(self):
        for field in ['id_evento','FECHA','USUARIO','TAMANO','ACCION']:
            self.assertEqual(normalize_record(record(changes={field:''}),directory())['status'],'rejected')
        bad=record(); bad=RawRecord(bad.file,1,[],bad.event_id_column)
        self.assertEqual(normalize_record(bad,directory())['audit'][0]['code'],'MALFORMED_RECORD')

    def test_exact_duplicate_including_invalid_canonical(self):
        rows=[record(row=2),record(row=1)]
        results=list(normalize_records(rows,directory()))
        self.assertEqual([r['status'] for r in results],['processed','duplicate'])
        self.assertEqual(results[1]['duplicate_of'],results[0]['raw_reference'])
        results=list(normalize_records([record(row=i,changes={'FECHA':'invalid'}) for i in [1,2]],directory()))
        self.assertEqual([r['status'] for r in results],['rejected','duplicate'])

    def test_repeated_activity_different_ids_and_sources_not_duplicates(self):
        rows=[record(row=1),record(row=2,changes={'id_evento':'E2'}),record(source='cloud')]
        self.assertEqual([r['status'] for r in normalize_records(rows,directory())],['processed']*3)

    def test_conflicting_ids_quarantine_group_preserving_copies(self):
        rows=[record(row=1),record(row=2,changes={'TAMANO':'1'}),record(row=3)]
        results=list(normalize_records(rows,directory()))
        self.assertEqual([r['status'] for r in results],['rejected','rejected','duplicate'])
        self.assertTrue(all('CONFLICTING_EVENT_ID' in [a['code'] for a in r['audit']] for r in results))

    def test_payload_unchanged_and_process_deterministic(self):
        row=record(); before=copy.deepcopy(row.payload)
        first=list(normalize_records([row],directory(),'directory-sha'))
        second=list(normalize_records([row],directory(),'directory-sha'))
        self.assertEqual(first,second); self.assertEqual(row.payload,before)
        self.assertEqual(first[0]['raw_payload'],before)
        self.assertTrue(all('before' in a and 'after' in a and 'moment' in a for a in first[0]['audit']))

    def test_public_contract_validation_and_negative_cases(self):
        event=normalize_record(record(),directory())['normalized_event']
        self.assertEqual(validate_normalized(event),event)
        for changes in [{'file_size_bytes':True},{'file_size_bytes':-1},{'event_id':'bad'},{'timestamp':'2026-09-01'},{'is_synthetic':False},{'operation':'unknown'},{'scenario_type':'forbidden'}]:
            with self.assertRaises(ContractError): validate_normalized(dict(event,**changes))

    def test_v2_metrics_read_only_no_labels_or_network(self):
        original_open=Path.open
        def guarded_open(path,*args,**kwargs):
            if 'private_eval' in path.parts or path.name=='ground_truth.csv':
                raise AssertionError('labels forbidden')
            return original_open(path,*args,**kwargs)
        with patch('psycopg.connect',side_effect=AssertionError('database forbidden')),patch.object(Path,'open',guarded_open):
            report=normalize_dataset(ROOT/'data/generated/v2')
        self.assertEqual(report['raw_received'],20000)
        self.assertEqual(report['exact_duplicates'],358)
        self.assertEqual(report['unique_events'],19642)
        self.assertEqual(report['raw_received'],report['normalized_valid']+report['quarantine']+report['exact_duplicates'])
        self.assertTrue(all(report['manifest_count_matches'].values()))

    def test_output_rejects_input_directory_and_existing_folder(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp)
            with self.assertRaises(ValueError): normalize_dataset(folder,folder/'nested')
            with self.assertRaises(ValueError): normalize_dataset(ROOT/'data/generated/v2',folder)
        with self.assertRaisesRegex(ValueError,'EXCLUDED_FROM_PUBLIC_GIT'):
            normalize_dataset(ROOT/'data/generated/v2',ROOT/'normalization-public-export')


if __name__=='__main__': unittest.main()
