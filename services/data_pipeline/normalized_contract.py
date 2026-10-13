"""Validador stdlib del vocabulario utilizado por el contrato publico V1.

Rechaza cambios de keywords no soportadas; no sustituye un motor JSON Schema
general. Las salidas tambien se contrastan con Draft202012Validator en pruebas.
"""
import json
import re
import uuid
from datetime import datetime
from pathlib import Path

CONTRACT_PATH = Path(__file__).resolve().parents[2] / 'packages/contracts/normalized-event.schema.json'


class ContractError(ValueError):
    pass


def load_contract():
    schema = json.loads(CONTRACT_PATH.read_text(encoding='utf-8'))
    allowed = {'$schema','$id','title','type','additionalProperties','required','properties'}
    field_allowed = {'type','format','minLength','minimum','enum','const'}
    if set(schema)-allowed or any(set(node)-field_allowed for node in schema['properties'].values()):
        raise ContractError('UNSUPPORTED_CONTRACT_KEYWORD')
    return schema


def validate_normalized(event, schema=None):
    schema = schema or load_contract()
    if not isinstance(event,dict) or not set(schema['required']) <= set(event):
        raise ContractError('MISSING_CONTRACT_FIELD')
    if schema.get('additionalProperties') is False and set(event)-set(schema['properties']):
        raise ContractError('UNEXPECTED_CONTRACT_FIELD')
    for key,value in event.items():
        rule = schema['properties'][key]
        types = rule.get('type',[])
        types = [types] if isinstance(types,str) else types
        matches = {'string':isinstance(value,str),'null':value is None,
                   'integer':type(value) is int,'boolean':type(value) is bool}
        if types and not any(matches.get(t,False) for t in types):
            raise ContractError('INVALID_CONTRACT_TYPE:'+key)
        if 'enum' in rule and value not in rule['enum']:
            raise ContractError('INVALID_CONTRACT_ENUM:'+key)
        if 'const' in rule and (value != rule['const'] or type(value) is not type(rule['const'])):
            raise ContractError('INVALID_CONTRACT_CONST:'+key)
        if isinstance(value,str) and len(value)<rule.get('minLength',0):
            raise ContractError('EMPTY_CONTRACT_FIELD:'+key)
        if type(value) is int and value<rule.get('minimum',value):
            raise ContractError('INVALID_CONTRACT_MINIMUM:'+key)
        if rule.get('format')=='uuid':
            try:
                if str(uuid.UUID(value)) != value.lower():
                    raise ValueError()
            except (ValueError,AttributeError):
                raise ContractError('INVALID_CONTRACT_UUID') from None
        if rule.get('format')=='date-time':
            try:
                if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})',value):
                    raise ValueError()
                if datetime.fromisoformat(value.replace('Z','+00:00')).tzinfo is None:
                    raise ValueError()
            except (ValueError,TypeError):
                raise ContractError('INVALID_CONTRACT_TIMESTAMP') from None
    return event
