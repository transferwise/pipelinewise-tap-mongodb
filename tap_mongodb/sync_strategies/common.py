#!/usr/bin/env python3
import base64
import datetime
import time
import uuid
import decimal

import bson
import singer
import pytz
import tzlocal

from typing import Dict, Any, List
from bson import objectid, timestamp, datetime as bson_datetime
from singer import utils, metadata
from terminaltables import AsciiTable

from tap_mongodb.errors import MongoInvalidDateTimeException, SyncException, UnsupportedKeyTypeException

INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = False
UPDATE_BOOKMARK_PERIOD = 1000
COUNTS = {}
TIMES = {}
SCHEMA_COUNT = {}
SCHEMA_TIMES = {}


def write_schema(schema: Dict, row: Dict, stream: Dict) -> None:
    """
    Checks if the row and schema don't match and updates the latter accordingly, then write a singer schema message
    Args:
        schema: Json schema dictionary
        row: row from DB
        stream: dictionary with stream details
    """
    schema_build_start_time = time.time()
    if update_schema_from_row(schema, row):
        singer.write_message(singer.SchemaMessage(
            stream=calculate_destination_stream_name(stream),
            schema=schema,
            key_properties=['_id']))
        SCHEMA_COUNT[stream['tap_stream_id']] += 1
    SCHEMA_TIMES[stream['tap_stream_id']] += time.time() - schema_build_start_time


def calculate_destination_stream_name(stream: Dict) -> str:
    """
    Builds the right stream name to be written in singer messages
    Args:
        stream: stream dictionary

    Returns: str holding the stream name
    """
    s_md = metadata.to_map(stream['metadata'])
    if INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME:
        return "{}_{}".format(s_md.get((), {}).get('database-name'), stream['stream'])

    return stream['stream']


def get_stream_version(tap_stream_id: str, state: Dict) -> int:
    """
    Get the stream version by either extracting it from the state or generating a new one
    Args:
        tap_stream_id: stream ID to get version for
        state: state dictionary to extract version from if exists

    Returns: version as an integer

    """
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def class_to_string(key_value: Any, key_type: str) -> Any:
    """
    Converts specific types to string equivalent
    The supported types are: datetime, bson Timestamp, bytes, int, Int64, float, ObjectId, str and UUID
    Args:
        key_value: The value to convert to string
        key_type: the value type

    Returns: string equivalent of key value
    Raises: UnsupportedKeyTypeException if key_type is not supported
    """
    if key_type == 'datetime':
        timezone = tzlocal.get_localzone()
        local_datetime = timezone.localize(key_value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
        return utils.strftime(utc_datetime)

    if key_type == 'Timestamp':
        return '{}.{}'.format(key_value.time, key_value.inc)

    if key_type == 'bytes':
        return base64.b64encode(key_value).decode('utf-8')

    if key_type in ['int', 'Int64', 'float', 'ObjectId', 'str', 'UUID']:
        return str(key_value)

    raise UnsupportedKeyTypeException("{} is not a supported key type"
                                      .format(key_type))


def string_to_class(str_value: str, type_value: str) -> Any:
    """
    Converts the string value into the given type if supported.
    The supported types are: UUID, datetime, int, Int64, float, ObjectId, Timestamp, bytes, str
    Args:
        str_value: the string value to convert
        type_value: the value type

    Returns: converted string value
    Raises: UnsupportedKeyTypeException if key is unsupported
    """

    conversion = {
        'UUID': uuid.UUID,
        'datetime': singer.utils.strptime_with_tz,
        'int': int,
        'Int64': bson.int64.Int64,
        'float': str,
        'ObjectId': objectid.ObjectId,
        'Timestamp': lambda val: (lambda split_value=val.split('.'):
                                  bson.timestamp.Timestamp(int(split_value[0]), int(split_value[1]))),
        'bytes': lambda val: base64.b64decode(val.encode()),
        'str': str,
    }

    if type_value in conversion:
        return conversion[type_value](str_value)

    raise UnsupportedKeyTypeException("{} is not a supported key type"
                                      .format(type_value))


def safe_transform_datetime(value: datetime.datetime, path):
    """
    Safely transform datetime from local tz to UTC if applicable
    Args:
        value: datetime value to transform
        path:

    Returns: utc datetime as string

    """
    timezone = tzlocal.get_localzone()
    try:
        local_datetime = timezone.localize(value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
    except Exception as ex:
        if str(ex) == "year is out of range" and value.year == 0:
            # NB: Since datetimes are persisted as strings, it doesn't
            # make sense to blow up on invalid Python datetimes (e.g.,
            # year=0). In this case we're formatting it as a string and
            # passing it along down the pipeline.
            return "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:06d}Z".format(value.year,
                                                                              value.month,
                                                                              value.day,
                                                                              value.hour,
                                                                              value.minute,
                                                                              value.second,
                                                                              value.microsecond)
        raise MongoInvalidDateTimeException("Found invalid datetime at [{}]: {}".format(
            ".".join(map(str, path)),
            value))
    return utils.strftime(utc_datetime)


def transform_value(value: Any, path) -> Any:
    """
    transform values to json friendly ones
    Args:
        value: value to transform
        path:

    Returns: transformed value

    """
    conversion = {
        list: lambda val, pat: list(map(lambda v: transform_value(v[1], pat + [v[0]]), enumerate(val))),
        dict: lambda val, pat: {k: transform_value(v, pat + [k]) for k, v in val.items()},
        uuid.UUID: str,
        objectid.ObjectId: str,
        bson_datetime.datetime: safe_transform_datetime,
        timestamp.Timestamp: lambda val, _: utils.strftime(val.as_datetime()),
        bson.int64.Int64: lambda val, _: int(val),
        bytes: lambda val, _: base64.b64encode(val).decode('utf-8'),
        datetime.datetime: lambda val, _: utils.strftime(val),
        bson.decimal128.Decimal128: lambda val, _: val.to_decimal(),
        bson.regex.Regex: lambda val, _: dict(pattern=val.pattern, flags=val.flags),
        bson.code.Code: lambda val, _: dict(value=str(val), scope=str(val.scope)) if val.scope else str(val),
        bson.dbref.DBRef: lambda val, _: dict(id=str(val.id), collection=val.collection, database=val.database)


    }

    if isinstance(value, datetime.datetime):
        timezone = tzlocal.get_localzone()
        local_datetime = timezone.localize(value)
        value = local_datetime.astimezone(pytz.UTC)

    if type(value) in conversion:
        return conversion[type(value)](value, path)

    return value


def row_to_singer_record(stream: Dict,
                         row: Dict,
                         version: int,
                         time_extracted: datetime.datetime) -> singer.RecordMessage:
    """
    Transforms row to singer record message
    Args:
        stream: stream details
        row: DB row
        version: stream version
        time_extracted: Datetime when row was extracted

    Returns: Singer RecordMessage instance

    """
    try:
        row_to_persist = {k: transform_value(v, [k]) for k, v in row.items()
                          if type(v) not in [bson.min_key.MinKey, bson.max_key.MaxKey]}
    except MongoInvalidDateTimeException as ex:
        raise SyncException(
            "Error syncing collection {}, object ID {} - {}".format(stream["tap_stream_id"], row['_id'], ex))

    return singer.RecordMessage(
        stream=calculate_destination_stream_name(stream),
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)


def _add_to_any_of_for_datetime_types(schema: List) -> bool:
    """
    Add date-time type to schema when applicable
    Args:
        schema: anyOf schema

    Returns: True if schema has changed, False otherwise

    """
    has_date = False
    for field_schema_entry in schema:
        if field_schema_entry.get('format') == 'date-time':
            has_date = True
            break

    if not has_date:
        schema.insert(0, {"type": "string", "format": "date-time"})
        return True

    return False


def _add_to_any_of_for_decimal_types(schema: List) -> bool:
    """
    Add number type to schema when applicable
    Args:
        schema: anyOf schema

    Returns: True if schema has changed, False otherwise

    """
    has_date = False
    has_decimal = False

    for field_schema_entry in schema:
        if field_schema_entry.get('format') == 'date-time':
            has_date = True

        if field_schema_entry.get('type') == 'number' and not field_schema_entry.get('multipleOf'):
            field_schema_entry['multipleOf'] = decimal.Decimal('1e-34')
            return True

        if field_schema_entry.get('type') == 'number' and field_schema_entry.get('multipleOf'):
            has_decimal = True

    if not has_decimal:
        if has_date:
            schema.insert(1, {"type": "number", "multipleOf": decimal.Decimal('1e-34')})
        else:
            schema.insert(0, {"type": "number", "multipleOf": decimal.Decimal('1e-34')})
        return True

    return False


def _add_to_any_of_for_float_types(schema: List) -> bool:
    """
    Add number type to schema when applicable
    Args:
        schema: anyOf schema

    Returns: True if schema has changed, False otherwise

    """

    has_date = False
    has_float = False

    for field_schema_entry in schema:
        if field_schema_entry.get('format') == 'date-time':
            has_date = True
        if field_schema_entry.get('type') == 'number' and field_schema_entry.get('multipleOf'):
            field_schema_entry.pop('multipleOf')
            return True
        if field_schema_entry.get('type') == 'number' and not field_schema_entry.get('multipleOf'):
            has_float = True

    if not has_float:
        if has_date:
            schema.insert(1, {"type": "number"})
        else:
            schema.insert(0, {"type": "number"})
        return True

    return False


def _add_to_any_of_for_dict_type(schema: List, value: Dict) -> bool:
    """
    Add object type and subsequent changes to schema when applicable
    Args:
        schema: anyOf schema

    Returns: True if schema has changed, False otherwise

    """
    has_object = False

    # get pointer to object schema and see if it already existed
    object_schema = {"type": "object", "properties": {}}
    for field_schema_entry in schema:
        if field_schema_entry.get('type') == 'object':
            object_schema = field_schema_entry
            has_object = True

    # see if object schema changed
    if update_schema_from_row(object_schema, value):
        # if it changed and existed, it's reference was modified
        # if it changed and didn't exist, insert it
        if not has_object:
            schema.insert(-1, object_schema)

        return True

    return False


def _add_to_any_of_for_list_type(schema: List, value: List) -> bool:
    """
    Add array type to schema when applicable
    Args:
        schema: anyOf schema

    Returns: True if schema has changed, False otherwise

    """
    has_list = False

    # get pointer to list's anyOf schema and see if list schema already existed
    list_schema = {"type": "array", "items": {"anyOf": [{}]}}
    for field_schema_entry in schema:
        if field_schema_entry.get('type') == 'array':
            list_schema = field_schema_entry
            has_list = True
    anyof_schema = list_schema['items']['anyOf']

    # see if list schema changed
    list_entry_changed = False
    for list_entry in value:
        list_entry_changed = add_to_any_of(anyof_schema, list_entry) or list_entry_changed

    # if it changed and existed, it's reference was modified
    # if it changed and didn't exist, insert it
    if not has_list and list_entry_changed:
        schema.insert(-1, list_schema)

    return list_entry_changed


def add_to_any_of(schema: List, value: Any)-> bool:
    """
    Update types in to anyOf schema when applicable
    Args:
        schema: anyOf schema

    Returns: True if schema has changed, False otherwise

    """
    if isinstance(value, (bson_datetime.datetime, timestamp.Timestamp, datetime.datetime)):
        return _add_to_any_of_for_datetime_types(schema)

    if isinstance(value, bson.decimal128.Decimal128):
        return _add_to_any_of_for_decimal_types(schema)

    if isinstance(value, float):
        return _add_to_any_of_for_float_types(schema)

    if isinstance(value, dict):
        return _add_to_any_of_for_dict_type(schema, value)

    if isinstance(value, list):
        return _add_to_any_of_for_list_type(schema, value)

    return False


def update_schema_from_row(schema: Dict, row: Dict)->bool:
    """
    Updates json schema if needed depending on the row structure
    Args:
        schema: schema to update
        row: row to use

    Returns: True if schema has been updated, False otherwise

    """
    changed = False
    for field, value in row.items():
        if isinstance(value, (bson_datetime.datetime,
                              timestamp.Timestamp,
                              datetime.datetime,
                              bson.decimal128.Decimal128,
                              float,
                              dict,
                              list)):

            # get pointer to field's anyOf list
            if not schema.get('properties', {}).get(field):
                schema['properties'][field] = {'anyOf': [{}]}
            anyof_schema = schema['properties'][field]['anyOf']

            # add value's schema to anyOf list
            changed = add_to_any_of(anyof_schema, value) or changed

    return changed


def get_sync_summary(catalog)->str:
    """
    Builds a summary of sync for all streams
    Args:
        catalog: dictionary with all the streams details

    Returns: summary table as string

    """
    headers = [['database',
                'collection',
                'replication method',
                'total records',
                'write speed',
                'total time',
                'schemas written',
                'schema build duration',
                'percent building schemas']]

    rows = []
    for stream_id, stream_count in COUNTS.items():
        stream = [x for x in catalog['streams'] if x['tap_stream_id'] == stream_id][0]
        collection_name = stream.get("table_name")
        md_map = metadata.to_map(stream['metadata'])
        db_name = metadata.get(md_map, (), 'database-name')
        replication_method = metadata.get(md_map, (), 'replication-method')

        stream_time = TIMES[stream_id]
        schemas_written = SCHEMA_COUNT[stream_id]
        schema_duration = SCHEMA_TIMES[stream_id]

        if stream_time == 0:
            stream_time = 0.000001

        rows.append(
            [
                db_name,
                collection_name,
                replication_method,
                '{} records'.format(stream_count),
                '{:.1f} records/second'.format(stream_count / float(stream_time)),
                '{:.5f} seconds'.format(stream_time),
                '{} schemas'.format(schemas_written),
                '{:.5f} seconds'.format(schema_duration),
                '{:.2f}%'.format(100 * schema_duration / float(stream_time))
            ]
        )

    data = headers + rows
    table = AsciiTable(data, title='Sync Summary')

    return '\n\n' + table.table
