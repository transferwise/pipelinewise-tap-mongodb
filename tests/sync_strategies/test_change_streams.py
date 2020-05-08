import unittest
import bson

from unittest.mock import patch, Mock, PropertyMock
from pymongo.change_stream import CollectionChangeStream, ChangeStream
from pymongo.collection import Collection
from singer import RecordMessage

import tap_mongodb.sync_strategies.change_streams as change_streams
from tap_mongodb.sync_strategies import common


class TestChangeStreams(unittest.TestCase):

    def tearDown(self) -> None:
        common.SCHEMA_COUNT.clear()
        common.SCHEMA_TIMES.clear()

    def test_update_bookmarks(self):
        state = {
            'bookmarks': {
                'stream1': {},
                'stream2': {}
            }
        }

        new_state = change_streams.update_bookmarks(state, 'stream1', {'data': 'this is a token'})

        self.assertEqual({
            'bookmarks': {
                'stream1': {'token': {'data': 'this is a token'}},
                'stream2': {}
            }
        }, new_state)

    def test_get_buffer_rows_from_db(self):

        result = ['a', 'b', 'c']

        mock_enter = Mock()
        mock_enter.return_value = result

        mock_find = Mock().return_value
        mock_find.__enter__ = mock_enter
        mock_find.__exit__ = Mock()

        mock_coll = Mock(spec_set=Collection).return_value
        mock_coll.find.return_value = mock_find

        self.assertListEqual(
            result,
            list(change_streams.get_buffer_rows_from_db(mock_coll, {1,2,3}, None)))

        mock_enter.assert_called_once()

    @patch('tap_mongodb.sync_strategies.change_streams.singer.write_message')
    @patch('tap_mongodb.sync_strategies.change_streams.flush_buffer')
    def test_sync_collection(self, flush_buffer_mock, write_message_mock):
        common.SCHEMA_COUNT['mydb-stream1'] = 0
        common.SCHEMA_TIMES['mydb-stream1'] = 0
        common.COUNTS['mydb-stream1'] = 0

        messages = []

        flush_buffer_mock.side_effect = [1]
        write_message_mock.side_effect = lambda x: messages.append(x)

        state = {
            'bookmarks': {
                'mydb-stream1': {}
            }
        }

        stream = {
            'tap_stream_id': 'mydb-stream1',
            'stream': 'stream1',
            'metadata': [
                {
                    'breadcrumb': [],
                    'metadata': {
                        'database-name': 'mydb'
                    }
                }
            ]
        }

        change_mock1 = Mock(spec_set=ChangeStream, return_value={
            'operationType': 'insert',
            'fullDocument': {
                '_id': 'id1',
                'key1': 1,
                'key2': 'abc'
            }
        }).return_value

        change_mock2 = Mock(spec_set=ChangeStream, return_value={
            'operationType': 'update',
            'documentKey': {
                '_id': 'id2'
            }

        }).return_value

        change_mock3 = Mock(spec_set=ChangeStream, return_value={
            'operationType': 'delete',
            'documentKey': {
                '_id': 'id3'
            },
            'clusterTime': bson.timestamp.Timestamp(1588636800, 0) # datetime.datetime(2020, 5, 5, 3, 0, 0,0)
        }).return_value

        change_mock4 = Mock(spec_set=ChangeStream, return_value={
            'operationType': 'insert',
            'fullDocument': {
                '_id': 'id4',
                'key3': bson.timestamp.Timestamp(1588636800, 0),
            }
        }).return_value


        cursor_mock = Mock(spec_set=CollectionChangeStream).return_value
        type(cursor_mock).alive = PropertyMock(return_value=True)
        type(cursor_mock).resume_token = PropertyMock(side_effect=['token1','token2','token3','token4', 'token5'])
        cursor_mock.try_next.side_effect = [change_mock1, change_mock2, change_mock3, change_mock4, None]

        mock_enter = Mock()
        mock_enter.return_value = cursor_mock

        mock_watch = Mock().return_value
        mock_watch.__enter__ = mock_enter
        mock_watch.__exit__ = Mock()

        mock_coll = Mock(spec_set=Collection).return_value
        mock_coll.watch.return_value = mock_watch

        change_streams.sync_collection(mock_coll, stream, state, None)

        self.assertEqual({
            'bookmarks': {
                'mydb-stream1': {
                    'token': 'token5',
                }
            }
        }, state)

        self.assertListEqual([
            'ActivateVersionMessage',
            'RecordMessage',
            'RecordMessage',
            'SchemaMessage',
            'RecordMessage',
            'StateMessage',
        ], [msg.__class__.__name__ for msg in messages])

        self.assertListEqual([
            {'_id': 'id1', 'key1': 1, 'key2': 'abc'},
            {'_id': 'id3', '_sdc_deleted_at': '2020-05-05T00:00:00+00:00'},
            {'_id': 'id4', 'key3': '2020-05-05T00:00:00.000000Z'},
        ], [msg.record for msg in messages if isinstance(msg, RecordMessage)])

        self.assertEqual(common.COUNTS['mydb-stream1'], 4)

    @patch('tap_mongodb.sync_strategies.change_streams.singer.write_message')
    @patch('tap_mongodb.sync_strategies.change_streams.get_buffer_rows_from_db')
    def test_flush_buffer_with_3_rows_returns_3(self, get_rows_mock, write_message_mock):
        common.SCHEMA_COUNT['mydb-stream1'] = 0
        common.SCHEMA_TIMES['mydb-stream1'] = 0

        get_rows_mock.return_value = [
            {'_id': '1', 'key1': 1},
            {'_id': '2', 'key2': ['a', 'b']},
            {'_id': '3', 'key3': bson.timestamp.Timestamp(1588636800, 0)},
        ]

        buffer = {'1', '2', '3', '4'}
        schema = {'type': 'object', 'properties': {}}

        stream = {
            'tap_stream_id': 'mydb-stream1',
            'stream': 'stream1',
            'metadata': [
                {
                    'breadcrumb': [],
                    'metadata': {
                        'database-name': 'mydb'
                    }
                }
            ]
        }

        messages = []

        write_message_mock.side_effect = lambda x: messages.append(x)

        rows_saved = change_streams.flush_buffer(buffer, stream, Mock(spec_set=Collection),
                                                 None, 0, schema)

        self.assertEqual(3, rows_saved)

        self.assertListEqual([
            'RecordMessage',
            'RecordMessage',
            'SchemaMessage',
            'RecordMessage',
        ], [m.__class__.__name__ for m in messages])

        self.assertFalse(buffer)
