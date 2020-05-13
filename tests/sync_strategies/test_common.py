import time
import unittest
import uuid
from unittest.mock import patch

import bson
import decimal

from datetime import datetime

from bson import ObjectId, Timestamp, MinKey
from dateutil.tz import tzutc
from jsonschema import validate

import tap_mongodb.sync_strategies.common as common
from tap_mongodb.errors import UnsupportedKeyTypeException


class TestRowToSchemaMessage(unittest.TestCase):

    def test_write_schema(self):
        common.SCHEMA_COUNT['my_stream'] = 0
        common.SCHEMA_TIMES['my_stream'] = 0

        schema = {'type': 'object', 'properties': {}}
        row = {
            'key1': 1,
            'key2': 'abc',
            'key3': ['a', 'b'],
            'key4': {}
        }
        stream = {
            'tap_stream_id': 'my_stream'
        }

        common.write_schema(schema, row, stream)

        self.assertEqual({
            'type': 'object',
            'properties': {
                'key3': {
                    'anyOf': [{}]
                },
                'key4': {
                    'anyOf': [{}]
                }
            }
        }, schema)

    def test_update_schema_from_row_with_no_change(self):
        row = {
            "a_str": "hello",
            "a_list": ["foo", "bar", 1, 2, {"name": "nick"}],
            "an_object": {
                "a_nested_str": "baz",
                "a_nested_list": [1, 2, "hi"]
            }
        }

        schema = {"type": "object", "properties": {}}
        changed = common.update_schema_from_row(schema, row)
        self.assertFalse(changed)

        # another row that looks the same keeps changed false
        changed = common.update_schema_from_row(schema, row)
        self.assertFalse(changed)

        # a different looking row makes the schema change
        row = {"a_str": "hello",
               "a_date": bson.timestamp.Timestamp(1565897157, 1)}
        changed = common.update_schema_from_row(schema, row)
        self.assertTrue(changed)

        # the same (different) row again sets changed back to false
        changed = common.update_schema_from_row(schema, row)
        self.assertFalse(changed)

    def test_update_schema_from_row_with_simple_date(self):
        row = {"a_date": bson.timestamp.Timestamp(1565897157, 1)}
        schema = {"type": "object", "properties": {}}
        changed = common.update_schema_from_row(schema, row)

        expected = {"type": "object",
                    "properties": {
                        "a_date": {
                            "anyOf": [{"type": "string",
                                       "format": "date-time"},
                                      {}]
                        }
                    }
                    }
        self.assertTrue(changed)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_simple_decimal(self):
        row = {"a_decimal": bson.Decimal128(decimal.Decimal('1.34'))}
        schema = {"type": "object", "properties": {}}
        changed = common.update_schema_from_row(schema, row)

        expected = {
            "type": "object",
            "properties": {
                "a_decimal": {
                    "anyOf": [{"type": "number",
                               "multipleOf": decimal.Decimal('1e-34')},
                              {}]
                }
            }
        }
        self.assertTrue(changed)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_simple_float(self):
        row = {"a_float": 1.34}
        schema = {"type": "object", "properties": {}}
        changed = common.update_schema_from_row(schema, row)

        expected = {
            "type": "object",
            "properties": {
                "a_float": {
                    "anyOf": [{"type": "number"},
                              {}]
                }
            }
        }
        self.assertTrue(changed)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_decimal_then_float(self):
        decimal_row = {"a_field": bson.Decimal128(decimal.Decimal('1.34'))}
        float_row = {"a_field": 1.34}

        schema = {"type": "object", "properties": {}}

        changed_decimal = common.update_schema_from_row(schema, decimal_row)
        changed_float = common.update_schema_from_row(schema, float_row)

        expected = {
            "type": "object",
            "properties": {
                "a_field": {
                    "anyOf": [{"type": "number"},
                              {}]
                }
            }
        }

        self.assertTrue(changed_decimal)
        self.assertTrue(changed_float)

        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_float_then_decimal(self):
        float_row = {"a_field": 1.34}
        decimal_row = {"a_field": bson.Decimal128(decimal.Decimal('1.34'))}

        schema = {"type": "object", "properties": {}}

        changed_decimal = common.update_schema_from_row(schema, float_row)
        changed_float = common.update_schema_from_row(schema, decimal_row)

        expected = {
            "type": "object",
            "properties": {
                "a_field": {
                    "anyOf": [{"type": "number",
                               "multipleOf": decimal.Decimal('1e-34')},
                              {}]
                }
            }
        }

        self.assertTrue(changed_float)
        self.assertTrue(changed_decimal)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_float_then_float(self):
        float_row = {"a_field": 1.34}
        float_row_2 = {"a_field": 1.34}

        schema = {"type": "object", "properties": {}}

        changed_float = common.update_schema_from_row(schema, float_row)
        changed_float_2 = common.update_schema_from_row(schema, float_row_2)

        expected = {
            "type": "object",
            "properties": {
                "a_field": {
                    "anyOf": [{"type": "number"},
                              {}]
                }
            }
        }

        self.assertTrue(changed_float)
        self.assertFalse(changed_float_2)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_decimal_then_decimal(self):
        decimal_row = {"a_field": bson.Decimal128(decimal.Decimal('1.34'))}
        decimal_row_2 = {"a_field": bson.Decimal128(decimal.Decimal('1.34'))}

        schema = {"type": "object", "properties": {}}

        changed_decimal = common.update_schema_from_row(schema, decimal_row)
        changed_decimal_2 = common.update_schema_from_row(schema, decimal_row_2)

        expected = {
            "type": "object",
            "properties": {
                "a_field": {
                    "anyOf": [{"type": "number",
                               "multipleOf": decimal.Decimal('1e-34')},
                              {}]
                }
            }
        }

        self.assertTrue(changed_decimal)
        self.assertFalse(changed_decimal_2)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_decimal_and_date(self):
        date_row = {"a_field": bson.timestamp.Timestamp(1565897157, 1)}
        decimal_row = {"a_field": bson.Decimal128(decimal.Decimal('1.34'))}

        schema = {"type": "object", "properties": {}}

        changed_date = common.update_schema_from_row(schema, date_row)
        changed_decimal = common.update_schema_from_row(schema, decimal_row)

        expected = {
            "type": "object",
            "properties": {
                "a_field": {
                    "anyOf": [
                        {"type": "string",
                         "format": "date-time"},
                        {"type": "number",
                         "multipleOf": decimal.Decimal('1e-34')},
                        {}
                    ]
                }
            }
        }
        self.assertTrue(changed_date)
        self.assertTrue(changed_decimal)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_nested_data(self):
        date_row = {"foo": {"a_field": bson.timestamp.Timestamp(1565897157, 1)}}
        schema = {"type": "object", "properties": {}}

        changed = common.update_schema_from_row(schema, date_row)

        expected = {
            "type": "object",
            "properties": {
                "foo": {
                    "anyOf": [
                        {
                            "type": "object",
                            "properties": {
                                "a_field": {
                                    "anyOf": [
                                        {"type": "string",
                                         "format": "date-time"},
                                        {}
                                    ]
                                }
                            }
                        },
                        {}
                    ]
                }
            }
        }
        self.assertTrue(changed)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_date_and_nested_data(self):
        date_row = {"foo": bson.timestamp.Timestamp(1565897157, 1)}
        nested_row = {"foo": {"a_field": bson.timestamp.Timestamp(1565897157, 1)}}
        schema = {"type": "object", "properties": {}}

        changed_date = common.update_schema_from_row(schema, date_row)
        changed_nested = common.update_schema_from_row(schema, nested_row)

        expected = {
            "type": "object",
            "properties": {
                "foo": {
                    "anyOf": [
                        {
                            "type": "string",
                            "format": "date-time"
                        },
                        {
                            "type": "object",
                            "properties": {
                                "a_field": {
                                    "anyOf": [
                                        {"type": "string",
                                         "format": "date-time"},
                                        {}
                                    ]
                                }
                            }
                        },
                        {}
                    ]
                }
            }
        }
        self.assertTrue(changed_date)
        self.assertTrue(changed_nested)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_array_multiple_types(self):
        row = {
            "foo": [
                bson.timestamp.Timestamp(1565897157, 1),
                bson.Decimal128(decimal.Decimal('1.34'))
            ]
        }
        schema = {"type": "object", "properties": {}}
        changed = common.update_schema_from_row(schema, row)

        expected = {
            "type": "object",
            "properties": {
                "foo": {
                    "anyOf": [
                        {
                            "type": "array",
                            "items": {
                                "anyOf": [
                                    {
                                        "type": "string",
                                        "format": "date-time"
                                    },
                                    {
                                        "type": "number",
                                        "multipleOf": decimal.Decimal('1e-34')
                                    },
                                    {}
                                ]
                            }
                        },
                        {}
                    ]
                }
            }
        }
        self.assertTrue(changed)
        self.assertEqual(expected, schema)

    def test_update_schema_from_row_with_array_nested(self):
        row = {
            "foo": [
                [
                    bson.timestamp.Timestamp(1565897157, 1),
                    bson.Decimal128(decimal.Decimal('1.34'))
                ],
                {
                    "bar": bson.timestamp.Timestamp(1565897157, 1),
                    "bat": bson.Decimal128(decimal.Decimal('1.34'))
                }
            ]
        }
        row_2 = {
            "bar": "1",
            "foo": [
                ["bob", "roger"],
                {
                    "bar": "bob",
                    "bat": "roger"
                }
            ]
        }
        schema = {"type": "object", "properties": {}}
        changed = common.update_schema_from_row(schema, row)
        changed_2 = common.update_schema_from_row(schema, row_2)

        expected = {
            "type": "object",
            "properties": {
                "foo": {
                    "anyOf": [
                        {
                            "type": "array",
                            "items": {
                                "anyOf": [
                                    {
                                        "type": "array",
                                        "items": {
                                            "anyOf": [
                                                {
                                                    "type": "string",
                                                    "format": "date-time"
                                                },
                                                {
                                                    "type": "number",
                                                    "multipleOf": decimal.Decimal('1e-34')
                                                },
                                                {}
                                            ]
                                        }
                                    },
                                    {
                                        "type": "object",
                                        "properties": {
                                            "bar": {
                                                "anyOf": [
                                                    {
                                                        "type": "string",
                                                        "format": "date-time"
                                                    },
                                                    {}
                                                ]
                                            },
                                            "bat": {
                                                "anyOf": [
                                                    {
                                                        "type": "number",
                                                        "multipleOf": decimal.Decimal('1e-34')
                                                    },
                                                    {}
                                                ]
                                            }
                                        }
                                    },
                                    {}
                                ]
                            }
                        },
                        {}
                    ]
                }
            }
        }
        singer_row = {k: common.transform_value(v, [k]) for k, v in row_2.items()
                      if type(v) not in [bson.min_key.MinKey, bson.max_key.MaxKey]}

        decimal.getcontext().prec = 100000
        validate(instance=singer_row, schema=schema)

        self.assertTrue(changed)
        self.assertFalse(changed_2)
        self.assertEqual(expected, schema)

    def test_calculate_destination_stream_name_with_include_schema_True(self):
        """

        """
        stream = {
            'stream': 'myStream',
            'metadata': [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "database-name": "myDb",
                    }
                }
            ]
        }
        with patch('tap_mongodb.common.INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME') as constant_mock:
            constant_mock.return_value = True
            self.assertEqual('myDb_myStream', common.calculate_destination_stream_name(stream))

    def test_calculate_destination_stream_name_with_include_schema_False(self):
        """

        """
        stream = {
            'stream': 'myStream',
            'metadata': [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "database-name": "myDb",
                    }
                }
            ]
        }
        common.INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = False
        self.assertEqual('myStream', common.calculate_destination_stream_name(stream))

    def test_get_stream_version_with_none_version_returns_new_version(self):

        state = {
            'bookmarks': {
                'myStream': {}
            }
        }
        self.assertGreaterEqual(time.time()*1000, common.get_stream_version('myStream', state))

    def test_get_stream_version_with_defined_version_returns_the_same_version(self):

        state = {
            'bookmarks': {
                'myStream': {'version': 123}
            }
        }
        self.assertEqual(123, common.get_stream_version('myStream', state))

    def test_class_to_string_with_bson_Timestamp_should_return_concatenated_time(self):
        ts = bson.Timestamp(200000, 80)

        self.assertEqual('200000.80', common.class_to_string(ts, 'Timestamp'))

    def test_class_to_string_with_unsupported_type_raises_exception(self):
        with self.assertRaises(UnsupportedKeyTypeException):
            common.class_to_string('a', 'random type')

    def test_transform_value_with_datetime_should_return_utc_formatted_date(self):
        date = datetime(2020, 5, 13, 15, 00, 00)
        self.assertEqual('2020-05-13T12:00:00.000000Z', common.transform_value(date, None))

    def test_transform_value_with_bytes_should_return_decoded_string(self):
        b = b'Pythonnnn'

        self.assertEqual('UHl0aG9ubm5u', common.transform_value(b, None))

    def test_transform_value_with_UUID_should_return_str(self):
        uid = '123e4567-e89b-12d3-a456-426652340000'
        self.assertEqual(uid, common.transform_value(uuid.UUID(uid), None))

    def test_string_to_class_with_UUID(self):
        uid = '123e4567-e89b-12d3-a456-426652340000'
        self.assertEqual(uuid.UUID(uid), common.string_to_class(uid, 'UUID'))

    def test_string_to_class_with_formatted_utc_datetime(self):
        dt = '2020-05-10T12:01:50.000000Z'
        self.assertEqual(datetime(2020, 5, 10, 12, 1, 50, tzinfo=tzutc()), common.string_to_class(dt, 'datetime'))

    def test_string_to_class_with_ObjectId(self):
        ob = '0123456789ab0123456789ab'
        self.assertEqual(ObjectId('0123456789ab0123456789ab'), common.string_to_class(ob, 'ObjectId'))

    def test_string_to_class_with_Timestamp(self):
        ob = '3000.0'
        self.assertEqual(Timestamp(3000, 0), common.string_to_class(ob, 'Timestamp'))

    def test_string_to_class_with_unsupported_type_raises_exception(self):
        with self.assertRaises(UnsupportedKeyTypeException):
            common.string_to_class(1, 'some random type')

    def test_row_to_singer_record_successful_transformation(self):
        stream = {
            'stream': 'myStream',
            'metadata': [
                {
                    'breadcrumb': [],
                    'metadata': {}
                }
            ]
        }

        row = {
            '_id': ObjectId('0123456789ab0123456789ab'),
            'key1': 10,
            'key2': Timestamp(1589379991, 4696183),
            'key3': 1.5,
            'key4': MinKey()
        }
        dt = datetime(2020, 5, 13, 14, 10, 10, tzinfo=tzutc())

        result = common.row_to_singer_record(stream, row, 100, dt)

        self.assertEqual({
            'type': 'RECORD',
            'stream': 'myStream',
            'record': {
                '_id': '0123456789ab0123456789ab',
                'key1': 10,
                'key2': '2020-05-13T14:26:31.000000Z',
                'key3': 1.5
            },
            'version': 100,
            'time_extracted': '2020-05-13T14:10:10.000000Z'
        }, result.asdict())
