"""Transformaciones puras RAW sintetico V2 -> contrato V1; sin DB ni etiquetas."""
import csv
import hashlib
import json
import re
import uuid
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from pathlib import PureWindowsPath
from urllib.parse import urlsplit, urlunsplit

from .adapters import SOURCES
from .normalized_contract import load_contract, validate_normalized

VERSION = 'hu02-v1'
LIMA = timezone(timedelta(hours=-5), 'UTC-05:00')
FIELDS = {
    'endpoint':('id_evento','USUARIO','FECHA','TAMANO','ACCION','ARCHIVO','PC','RUTA_DESTINO'),
    'cloud':('recordId','actorId','eventTime','sizeBytes','activity','resourceName',None,'destination'),
    'transfer':('TransferId','Account','TimeStamp','Size','TransferMethod','Object','PCName','Destination')}
OPERATIONS = {'abrir':'file_access','open':'file_access','descarga':'file_download',
    'download':'file_download','descargado':'file_download','copia':'file_copy','copy':'file_copy',
    'editar':'file_modify','modify':'file_modify','subir':'web_upload','upload':'web_upload',
    'sincronizar':'cloud_sync','sync':'cloud_sync','adjuntar':'email_attachment',
    'email_attachment':'email_attachment','copia_usb':'usb_copy','usb_copy':'usb_copy'}


class QualityError(ValueError):
    pass


def blank(value):
    return value is None or isinstance(value,str) and not value.strip()


def payload_digest(payload):
    return hashlib.sha256(json.dumps(dict(payload),sort_keys=True,ensure_ascii=False,
                                    separators=(',',':')).encode('utf-8')).hexdigest()


class EmployeeDirectory:
    def __init__(self, rows):
        self.aliases = defaultdict(set)
        self.codes = set()
        self.source_aliases = defaultdict(lambda:defaultdict(set))
        for row in rows:
            code = row['EmployeeCode'].strip()
            if not code:
                raise QualityError('INVALID_DIRECTORY')
            self.codes.add(code)
            for source,field in [('endpoint','EmployeeCode'),('cloud','AccountAlias'),('transfer','TransferAlias')]:
                self.source_aliases[code][source].add(row[field].strip())
            for field in ('EmployeeCode','AccountAlias','TransferAlias'):
                alias = row[field].strip().casefold()
                if alias:
                    self.aliases[alias].add(code)

    def resolve(self, value):
        if blank(value):
            raise QualityError('MISSING_ACTOR')
        if not isinstance(value,str):
            raise QualityError('UNKNOWN_ACTOR')
        codes = self.aliases.get(value.strip().casefold(),set())
        if len(codes)>1:
            raise QualityError('AMBIGUOUS_ACTOR')
        if not codes:
            raise QualityError('UNKNOWN_ACTOR')
        return next(iter(codes))

    @classmethod
    def read(cls,path):
        with path.open(encoding='utf-8-sig',newline='') as stream:
            return cls(list(csv.DictReader(stream)))


def normalize_timestamp(source, value):
    if blank(value):
        raise QualityError('MISSING_TIMESTAMP')
    if not isinstance(value,str):
        raise QualityError('INVALID_TIMESTAMP')
    value = value.strip()
    try:
        if source not in FIELDS:
            raise ValueError()
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})?',value):
            parsed = datetime.fromisoformat(value.replace('Z','+00:00'))
        elif source=='endpoint':
            pattern = '%d/%m/%Y %H:%M:%S' if value.count(':')==2 else '%d/%m/%Y %H:%M'
            parsed = datetime.strptime(value,pattern)
        elif source=='transfer':
            match = re.fullmatch(r'(\d{2})/(\d{2})/(\d{4}) (\d{2}):(\d{2})(?::(\d{2}))? (AM|PM)',value,re.I)
            if not match:
                raise ValueError()
            month,day,year,hour,minute,second,period = match.groups()
            hour = int(hour)
            if not 1<=hour<=12:
                raise ValueError()
            hour = hour%12 + (12 if period.upper()=='PM' else 0)
            parsed = datetime(int(year),int(month),int(day),hour,int(minute),int(second or 0))
        else:
            raise ValueError()
    except ValueError:
        raise QualityError('INVALID_TIMESTAMP') from None
    local_assumed = parsed.tzinfo is None
    if local_assumed:
        parsed = parsed.replace(tzinfo=LIMA)
    interpreted = parsed.strftime('%z')
    return {'timestamp':parsed.astimezone(timezone.utc).isoformat().replace('+00:00','Z'),
            'lima_timestamp':parsed.astimezone(LIMA).isoformat(),
            'interpreted_zone':interpreted,'local_zone_assumed':local_assumed,'state':'known'}


def normalize_size(value,unit):
    if blank(value) or isinstance(value,str) and value.strip().casefold()=='n/a':
        raise QualityError('MISSING_SIZE')
    if not isinstance(value,str) or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)',value.strip()):
        raise QualityError('INVALID_SIZE')
    factors = {'b':1,'bytes':1,'kb':1024,'mb':1048576}
    if not isinstance(unit,str) or unit.strip().casefold() not in factors:
        raise QualityError('UNSUPPORTED_UNIT')
    try:
        with localcontext() as context:
            context.prec = max(28,len(value)+10)
            number = Decimal(value.strip())
            if number<0:
                raise QualityError('INVALID_SIZE')
            exact = number*factors[unit.strip().casefold()]
            rounded = exact.quantize(Decimal(1),rounding=ROUND_HALF_UP)
            return int(rounded), exact!=rounded
    except InvalidOperation:
        raise QualityError('INVALID_SIZE') from None


def normalize_destination(value):
    if blank(value):
        return None,'absent','MISSING_DESTINATION'
    if not isinstance(value,str):
        return None,'invalid','INVALID_DESTINATION'
    value = value.strip()
    if re.fullmatch(r'USB-[A-Za-z0-9_-]+:/.*',value,re.I):
        return value,'known',None
    if value.startswith('\\\\'):
        parts = value[2:].split('\\',1)
        if not parts[0] or len(parts)<2 or not parts[1]:
            return None,'invalid','INVALID_DESTINATION'
        return '\\\\'+parts[0].casefold()+'\\'+parts[1],'known',None
    if re.match(r'^[A-Za-z]:[\\/]',value) or value.startswith('/'):
        return value,'known',None
    try:
        parsed = urlsplit(value)
        if parsed.scheme.casefold() in {'http','https'} and parsed.hostname and not parsed.username and not parsed.password:
            host = parsed.hostname.casefold()
            if ':' in host:
                host = '['+host+']'
            if parsed.port is not None:
                host += ':'+str(parsed.port)
            return urlunsplit((parsed.scheme.casefold(),host,parsed.path,parsed.query,parsed.fragment)),'known',None
    except ValueError:
        pass
    if re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+',value):
        return value.casefold(),'known',None
    return None,'invalid','INVALID_DESTINATION'


def raw_reference(record):
    return f'csv-sha256:{record.file.sha256}:{record.row_number}'


def normalize_record(record,directory,directory_hash='',schema=None):
    source = record.file.source
    payload = dict(record.payload) if isinstance(record.payload,Mapping) else record.payload
    result = {'version':VERSION,'status':'rejected','raw_reference':raw_reference(record),
              'source':source,'file':record.file.path.name,'file_sha256':record.file.sha256,
              'record_number':record.row_number,'directory_sha256':directory_hash,
              'raw_payload':payload,'normalized_event':None,'duplicate_of':None,'audit':[],
              'field_states':{},'temporal':None}
    fatal = []
    def note(code,field,before,after=None,reject=False):
        result['audit'].append({'version':VERSION,'stage':'normalization','code':code,
                               'field':field,'before':before,'after':after,'severity':'error' if reject else 'warning',
                               'moment':result['temporal']['timestamp'] if result['temporal'] else None,
                               'moment_kind':'event_timestamp'})
        if reject:
            fatal.append(code)
    if source not in FIELDS or not isinstance(payload,dict):
        note('MALFORMED_RECORD','payload',payload,reject=True)
        return result
    id_key,actor_key,time_key,size_key,op_key,file_key,device_key,dest_key = FIELDS[source]
    event_id = payload.get(id_key)
    if blank(event_id) or not isinstance(event_id,str):
        note('MISSING_EVENT_ID',id_key,event_id,reject=True)
    actor = timestamp = size = None
    try:
        actor = directory.resolve(payload.get(actor_key))
        result['field_states']['actor']='known'
        if actor!=payload.get(actor_key):
            note('ACTOR_ALIAS_RESOLVED',actor_key,payload.get(actor_key),actor)
    except QualityError as error:
        result['field_states']['actor']='absent' if blank(payload.get(actor_key)) else 'invalid'
        note(str(error),actor_key,payload.get(actor_key),reject=True)
    try:
        result['temporal'] = normalize_timestamp(source,payload.get(time_key))
        timestamp = result['temporal']['timestamp']
        note('TIMESTAMP_TO_UTC',time_key,payload.get(time_key),timestamp)
        result['field_states']['timestamp']='known'
    except QualityError as error:
        result['field_states']['timestamp']='absent' if blank(payload.get(time_key)) else 'invalid'
        note(str(error),time_key,payload.get(time_key),reject=True)
    value = payload.get(size_key)
    unit = payload.get('UNIDAD') if source=='endpoint' else 'bytes'
    if source=='transfer':
        match = re.fullmatch(r'\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s+(\S+)\s*',value) if isinstance(value,str) else None
        if match:
            value,unit = match.groups()
        else:
            unit = None
    try:
        size,rounded = normalize_size(value,unit)
        result['field_states']['size']='known'
        note('SIZE_TO_BYTES',size_key,payload.get(size_key),size)
        if rounded:
            note('SIZE_ROUNDED',size_key,payload.get(size_key),size)
    except QualityError as error:
        result['field_states']['size']='absent' if blank(value) else 'invalid'
        note(str(error),size_key,payload.get(size_key),reject=True)
    original_op = payload.get(op_key)
    operation = OPERATIONS.get(original_op.strip().casefold(),'other') if isinstance(original_op,str) else 'other'
    if blank(original_op):
        note('MISSING_OPERATION',op_key,original_op,reject=True)
    elif not isinstance(original_op,str):
        note('INVALID_OPERATION',op_key,original_op,reject=True)
    elif operation=='other':
        note('UNKNOWN_OPERATION',op_key,original_op,'other')
    else:
        note('OPERATION_MAPPED',op_key,original_op,operation)
    destination,dest_state,dest_issue = normalize_destination(payload.get(dest_key))
    result['field_states']['destination']=dest_state
    if dest_issue:
        note(dest_issue,dest_key,payload.get(dest_key))
    elif destination!=payload.get(dest_key):
        note('DESTINATION_NORMALIZED',dest_key,payload.get(dest_key),destination)
    for entry in result['audit']:
        entry['moment'] = timestamp
        entry['moment_kind'] = 'event_timestamp'
    def optional(key):
        value = payload.get(key) if key else None
        return value.strip() if isinstance(value,str) and value.strip() else None
    original_source_path = optional('RUTA_ORIGEN') if source=='endpoint' else None
    source_path = normalize_destination(original_source_path)[0] if original_source_path else None
    result['field_states']['source_path']='known' if source_path else 'invalid' if original_source_path else 'absent' if source=='endpoint' else 'not_applicable'
    source_type = optional('sourceType')
    result['field_states']['source_type']='known' if source_type else 'not_applicable' if source!='cloud' else 'absent'
    name = optional(file_key)
    extension = PureWindowsPath(name).suffix.casefold().lstrip('.') if name else None
    if source_path!=original_source_path:
        note('SOURCE_PATH_NORMALIZED','RUTA_ORIGEN',payload.get('RUTA_ORIGEN'),source_path)
    if name:
        note('FILE_EXTENSION_EXTRACTED',file_key,payload.get(file_key),extension or None)
    if fatal:
        return result
    uid = uuid.uuid5(uuid.NAMESPACE_URL,VERSION+'|'+directory_hash+'|'+source+'|'+event_id+'|'+payload_digest(payload))
    event = {'event_id':str(uid),'source':source,'source_event_id':event_id,'timestamp':timestamp,
             'user_id':actor,'operation':operation,'file_size_bytes':size,'is_synthetic':True,
             'device_id':optional(device_key),'file_extension':extension or None,
             'source_type':source_type,
             'source_path':source_path,'destination_path':destination,'destination_type':
             'usb' if destination and destination.casefold().startswith('usb-') else 'uri' if destination and '://' in destination else 'path' if destination and (destination.startswith(('/','\\')) or re.match(r'^[A-Za-z]:',destination)) else 'host' if destination else None,
             'destination_authorized':None,'data_classification':optional('Classification') or optional('category'),
             'operation_result':optional('Result'),'raw_event_reference':result['raw_reference']}
    for entry in result['audit']:
        entry['moment'] = timestamp
        entry['moment_kind'] = 'event_timestamp'
    validate_normalized(event,schema)
    result['normalized_event']=event
    result['status']='processed'
    return result


def normalize_records(records,directory,directory_hash=''):
    """Canonizar por fuente/ID/payload antes de resolver estados finales."""
    records = sorted(records,key=lambda r:(r.file.source,r.file.sha256,r.row_number))
    schema = load_contract()
    groups = defaultdict(set)
    for record in records:
        if isinstance(record.payload,Mapping) and record.file.source in FIELDS:
            event_id = record.payload.get(FIELDS[record.file.source][0])
            if isinstance(event_id,str) and event_id.strip():
                groups[(record.file.source,event_id)].add(payload_digest(record.payload))
    canonicals = {}
    for record in records:
        result = normalize_record(record,directory,directory_hash,schema)
        event_id = record.payload.get(FIELDS[record.file.source][0]) if isinstance(record.payload,Mapping) and record.file.source in FIELDS else None
        if isinstance(event_id,str) and event_id.strip():
            digest = payload_digest(record.payload)
            key = (record.file.source,event_id,digest)
            if len(groups[(record.file.source,event_id)])>1:
                result['audit'].append({'version':VERSION,'stage':'deduplication','code':'CONFLICTING_EVENT_ID',
                    'field':'source_event_id','before':event_id,'after':None,'severity':'error'})
                result['status']='rejected'; result['normalized_event']=None
            if key in canonicals:
                result['status']='duplicate'; result['normalized_event']=None
                result['duplicate_of']=canonicals[key]
                result['audit'].append({'version':VERSION,'stage':'deduplication','code':'EXACT_DUPLICATE',
                    'field':'raw_reference','before':result['raw_reference'],'after':canonicals[key],'severity':'info'})
            else:
                canonicals[key]=result['raw_reference']
        for entry in result['audit']:
            entry.setdefault('moment',result['temporal']['timestamp'] if result['temporal'] else None)
            entry.setdefault('moment_kind','event_timestamp')
        yield result
