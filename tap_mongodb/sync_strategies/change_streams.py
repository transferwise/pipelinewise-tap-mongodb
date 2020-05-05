import copy
import time
import datetime
import singer

from typing import Set, Dict, Optional, Generator
from pymongo.collection import Collection

from tap_mongodb.sync_strategies import common

LOGGER = singer.get_logger('tap_mongodb')

SDC_DELETED_AT = "_sdc_deleted_at"
RESUME_TOKEN_KEY = 'token'
MAX_AWAIT_TIME_MS = 120000  # 2 minutes
MAX_UPDATE_BUFFER_LENGTH = 500

PIPELINE = [
    {'$match': {
        '$or': [
            {'operationType': 'insert'},
            {'operationType': 'update'},
            {'operationType': 'delete'}
        ]
    }
    }
]


def write_schema(schema, row, stream):
    schema_build_start_time = time.time()
    if common.row_to_schema(schema, row):
        singer.write_message(singer.SchemaMessage(
            stream=common.calculate_destination_stream_name(stream),
            schema=schema,
            key_properties=['_id']))
        common.SCHEMA_COUNT[stream['tap_stream_id']] += 1
    common.SCHEMA_TIMES[stream['tap_stream_id']] += time.time() - schema_build_start_time


def update_bookmarks(state: Dict, tap_stream_id: str, token: Dict) -> Dict:
    """
    Updates the stream state by re-setting the changeStream token
    Args:
        state: State dictionary
        tap_stream_id: stream ID
        token: resume token from changeStream

    Returns:
        state: updated state
    """
    state = singer.write_bookmark(state, tap_stream_id, RESUME_TOKEN_KEY, token)
    return state


def get_buffer_rows_from_db(collection: Collection,
                            update_buffer: Set,
                            stream_projection: Optional[Dict]
                            ) -> Generator:
    """
    Fetches the full documents of the IDs in the buffer from the DB
    Args:
        collection: MongoDB Collection instance
        update_buffer: A set of IDs whose documents needs to be fetched
        stream_projection: projection for the query
    Returns:
        generator: it can be empty
    """
    query = {'_id': {'$in': list(update_buffer)}}

    with collection.find(query, stream_projection) as cursor:
        yield from cursor


def sync_collection(collection: Collection,
                    stream: Dict,
                    state: Dict,
                    stream_projection: Optional[Dict]
                    ) -> None:
    """
    Syncs the records from the given collection using ChangeStreams
    Args:
        collection: MongoDB Collection instance
        stream: stream dictionary with all its details
        state: state dictionary
        stream_projection: projection for querying
    """
    tap_stream_id = stream['tap_stream_id']
    LOGGER.info('Starting LogBased sync for %s', tap_stream_id)

    # Write activate version message
    version = common.get_stream_version(tap_stream_id, state)
    singer.write_message(singer.ActivateVersionMessage(
        stream=common.calculate_destination_stream_name(stream),
        version=version
    ))

    rows_saved = 0
    start_time = time.time()

    update_buffer = set()

    schema = {"type": "object", "properties": {}}

    # Init a cursor to listen for changes from the last saved resume token
    # if there are no changes after MAX_AWAIT_TIME_MS, then we'll exit
    with collection.watch(
            PIPELINE,
            max_await_time_ms=MAX_AWAIT_TIME_MS,
            start_after=state.get('bookmarks', {}).get(tap_stream_id).get(RESUME_TOKEN_KEY)) as cursor:

        while cursor.alive:

            # Note that the ChangeStream's resume token may be updated
            # even when no changes are returned.
            token = cursor.resume_token

            change = cursor.try_next()

            # After MAX_AWAIT_TIME_MS has elapsed, the cursor will return None.
            # write state and exit
            if change is None:
                LOGGER.debug('No change streams after % s, updating bookmark and exiting...', MAX_AWAIT_TIME_MS)

                state = update_bookmarks(state, tap_stream_id, token)
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                break

            operation = change['operationType']

            if operation == 'insert':
                write_schema(schema, change['fullDocument'], stream)
                singer.write_message(common.row_to_singer_record(stream,
                                                                 change['fullDocument'],
                                                                 version,
                                                                 datetime.datetime.now(tz=datetime.timezone.utc)))
                rows_saved += 1

            elif operation == 'update':
                # update operation only return _id and updated fields in the row,
                # so saving _id for now until we fetch the document when it's time to flush the buffer
                update_buffer.add(change['documentKey']['_id'])

            elif operation == 'delete':
                # remove update from buffer if that document has been deleted
                update_buffer.discard(change['documentKey']['_id'])

                doc = {
                    SDC_DELETED_AT: change['clusterTime'].as_datetime().isoformat(),
                    '_id': change['documentKey']['_id']
                }

                write_schema(schema, doc, stream)

                # Delete ops only contain the _id of the row deleted
                singer.write_message(
                    common.row_to_singer_record(stream,
                                                doc,
                                                version,
                                                datetime.datetime.now(tz=datetime.timezone.utc))
                )
                rows_saved += 1

            state = update_bookmarks(state,
                                     tap_stream_id,
                                     token)

            # flush buffer if it has filled up or flush and write state every UPDATE_BOOKMARK_PERIOD messages

            if len(update_buffer) >= MAX_UPDATE_BUFFER_LENGTH or rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
                LOGGER.debug('Flushing update buffer ...')

                rows_saved += flush_buffer(update_buffer, stream, collection, stream_projection, version, schema)

                if rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
                    # write state
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        # flush buffer if finished with changeStreams
        rows_saved += flush_buffer(update_buffer, stream, collection, stream_projection, version, schema)

        common.COUNTS[tap_stream_id] += rows_saved
        common.TIMES[tap_stream_id] += time.time() - start_time

        LOGGER.info('Syncd %s records for %s', rows_saved, tap_stream_id)


def flush_buffer(buffer: Set, stream: Dict, collection: Collection,
                 stream_projection: Optional[Dict], version: int, schema: Dict):
    """
    Flush and reset the given buffer
    Args:
        collection: mongoDB Collection instance
        schema: json schema
        buffer: A set of rows to flush
        stream: stream whose rows to flush
        stream_projection: the stream projection
        version: stream version

    Returns:

    """
    rows_saved = 0

    # flush buffer before writing state
    for buffered_row in get_buffer_rows_from_db(collection,
                                                buffer,
                                                stream_projection
                                                ):
        write_schema(schema, buffered_row, stream)
        record_message = common.row_to_singer_record(stream,
                                                     buffered_row,
                                                     version,
                                                     datetime.datetime.now(tz=datetime.timezone.utc))
        singer.write_message(record_message)

        rows_saved += 1

    buffer.clear()

    return rows_saved
