import unittest
import bson
import decimal
from jsonschema import validate

import tap_mongodb.sync_strategies.common as common

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

    def test_no_change(self):
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


    def test_simple_date(self):
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


    def test_simple_decimal(self):
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


    def test_simple_float(self):
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


    def test_decimal_then_float(self):
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


    def test_float_then_decimal(self):
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

    def test_float_then_float(self):
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


    def test_decimal_then_decimal(self):
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

    def test_decimal_and_date(self):
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


    def test_nested_data(self):
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

    def test_date_and_nested_data(self):
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

    def test_array_multiple_types(self):
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

    def test_array_nested(self):
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
        singer_row = {k:common.transform_value(v, [k]) for k, v in row_2.items()
                      if type(v) not in [bson.min_key.MinKey, bson.max_key.MaxKey]}


        decimal.getcontext().prec=100000
        validate(instance=singer_row, schema=schema)

        self.assertTrue(changed)
        self.assertFalse(changed_2)
        self.assertEqual(expected, schema)
