#!/usr/bin/env python3
import copy
import json
import ssl
import sys
import singer

from typing import List, Dict, Optional
from pymongo import MongoClient
from json import JSONDecodeError

from pymongo.collection import Collection
from pymongo.database import Database
from singer import metadata, metrics, utils

import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table
import tap_mongodb.sync_strategies.incremental as incremental
import tap_mongodb.sync_strategies.change_streams as change_streams
from tap_mongodb.errors import InvalidReplicationMethodException, InvalidProjectionException

LOGGER = singer.get_logger('tap_mongodb')

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password',
    'database'
]

IGNORE_DBS = ['system', 'local', 'config']
ROLES_WITHOUT_FIND_PRIVILEGES = {
    'dbAdmin',
    'userAdmin',
    'clusterAdmin',
    'clusterManager',
    'clusterMonitor',
    'hostManager',
    'restore'
}
ROLES_WITH_FIND_PRIVILEGES = {
    'read',
    'readWrite',
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'dbOwner',
    'backup',
    'root'
}
ROLES_WITH_ALL_DB_FIND_PRIVILEGES = {
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'root'
}


def get_roles_with_find_privs(database: Database, user: Dict) -> List[Dict]:
    """
    Finds and returns all the user's roles that have find privileges.
    User is dictionary in the form:
     {
         '_id': <auth_db>.<user>,
         'db': <auth_db>,
         'mechanisms': ['SCRAM-SHA-1', 'SCRAM-SHA-256'],
         'roles': [{'db': 'admin', 'role': 'readWriteAnyDatabase'},
                   {'db': 'local', 'role': 'read'}],
         'user': <user>,
         'userId': <userId>
     }
    Args:
        database: MongoDB Database instance
        user: db user dictionary

    Returns: list of roles

    """
    roles = []

    for role in user.get('roles', []):
        if role.get('role') in ROLES_WITHOUT_FIND_PRIVILEGES:
            continue

        role_name = role['role']

        # roles with find privileges
        if role_name in ROLES_WITH_FIND_PRIVILEGES and role.get('db'):
            roles.append(role)

        # for custom roles, get the "sub-roles"
        else:
            role_info_list = database.command({'rolesInfo': {'role': role_name, 'db': database.name}})
            role_info = [r for r in role_info_list.get('roles', []) if r['role'] == role_name]

            if len(role_info) != 1:
                continue

            roles.extend([sub_role for sub_role in role_info[0].get('roles', [])
                          if sub_role.get('role') in ROLES_WITH_FIND_PRIVILEGES and sub_role.get('db')])

    return roles


def get_roles(database: Database, db_user: str) -> List[Dict]:
    """
    Get all user's roles with find privileges if user exists
    Args:
        database: MongoDB DB instance
        db_user: DB user name to get roles for

    Returns: List of roles found

    """

    # usersInfo Command  returns object in shape:
    # {
    #   < some_other_keys >
    #   'users': [
    #                {
    #                    '_id': < auth_db >. < user >,
    #                    'db': < auth_db >,
    #                    'mechanisms': ['SCRAM-SHA-1', 'SCRAM-SHA-256'],
    #                     'roles': [{'db': 'admin', 'role': 'readWriteAnyDatabase'},
    #                               {'db': 'local', 'role': 'read'}],
    #                     'user': < user >,
    #                     'userId': < userId >
    #                 }
    #           ]
    # }
    user_info = database.command({'usersInfo': db_user})

    users = [u for u in user_info.get('users') if u.get('user') == db_user]
    if len(users) != 1:
        LOGGER.warning('Could not find any users for %s', db_user)
        return []

    return get_roles_with_find_privs(database, users[0])


def get_databases(client: MongoClient, config: Dict) -> List[str]:
    """
    Get all the databases in the cluster that the user roles can read from
    Args:
        client: MongoDB client instance
        config: DB config

    Returns: List of db names

    """
    roles = get_roles(client[config['database']], config['user'])
    LOGGER.info('Roles: %s', roles)

    can_read_all = len([role for role in roles
                        if role['role'] in ROLES_WITH_ALL_DB_FIND_PRIVILEGES]) > 0

    if can_read_all:
        db_names = [d for d in client.list_database_names() if d not in IGNORE_DBS]
    else:
        db_names = [role['db'] for role in roles if role['db'] not in IGNORE_DBS]

    LOGGER.info('Datbases: %s', db_names)

    return db_names


def produce_collection_schema(collection: Collection) -> Dict:
    """
    Generate a schema/catalog from the collection details for discovery mode
    Args:
        collection: stream Collection

    Returns: collection catalog

    """
    collection_name = collection.name
    collection_db_name = collection.database.name

    is_view = collection.options().get('viewOn') is not None

    mdata = {}
    mdata = metadata.write(mdata, (), 'table-key-properties', ['_id'])
    mdata = metadata.write(mdata, (), 'database-name', collection_db_name)
    mdata = metadata.write(mdata, (), 'row-count', collection.estimated_document_count())
    mdata = metadata.write(mdata, (), 'is-view', is_view)

    # write valid-replication-key metadata by finding fields that have indexes on them.
    # cannot get indexes for views -- NB: This means no key-based incremental for views?
    if not is_view:
        valid_replication_keys = []
        coll_indexes = collection.index_information()
        # index_information() returns a map of index_name -> index_information
        for _, index_info in coll_indexes.items():
            # we don't support compound indexes
            if len(index_info.get('key')) == 1:
                index_field_info = index_info.get('key')[0]
                # index_field_info is a tuple of (field_name, sort_direction)
                if index_field_info:
                    valid_replication_keys.append(index_field_info[0])

        if valid_replication_keys:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', valid_replication_keys)

    return {
        'table_name': collection_name,
        'stream': collection_name,
        'metadata': metadata.to_list(mdata),
        'tap_stream_id': "{}-{}".format(collection_db_name, collection_name),
        'schema': {
            'type': 'object'
        }
    }


def do_discover(client: MongoClient, config: Dict):
    """
    Run discovery mode where the mongodb cluster is scanned and all the collections are turned into streams
    The result is dumped to stdout as json
    Args:
        client:MongoDB Client instance
        config: DB config
    """
    streams = []

    for db_name in get_databases(client, config):
        database = client[db_name]

        collection_names = database.list_collection_names()
        for collection_name in [c for c in collection_names
                                if not c.startswith("system.")]:

            collection = database[collection_name]
            is_view = collection.options().get('viewOn') is not None

            # Add support for views if needed here
            if is_view:
                continue

            LOGGER.info("Getting collection info for db: %s, collection: %s",
                        db_name, collection_name)
            streams.append(produce_collection_schema(collection))

    json.dump({'streams': streams}, sys.stdout, indent=2)


def is_stream_selected(stream: Dict) -> bool:
    """
    Checks the stream's metadata to see if stream is selected for sync
    Args:
        stream: stream dictionary

    Returns: True if selected, False otherwise

    """
    mdata = metadata.to_map(stream['metadata'])
    is_selected = metadata.get(mdata, (), 'selected')

    return is_selected is True


def get_streams_to_sync(streams: List[Dict], state: Dict) -> List:
    """
    Filter the streams list to return only those selected for sync
    Args:
        streams: list of all discovered streams
        state: streams state

    Returns: list of selected streams, ordered from streams without state to those with state

    """
    # get selected streams
    selected_streams = [stream for stream in streams if is_stream_selected(stream)]

    # prioritize streams that have not been processed
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        if state.get('bookmarks', {}).get(stream['tap_stream_id']):
            streams_with_state.append(stream)
        else:
            streams_without_state.append(stream)

    ordered_streams = streams_without_state + streams_with_state

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s['tap_stream_id'] == currently_syncing,
            ordered_streams))
        non_currently_syncing_streams = list(filter(lambda s: s['tap_stream_id'] != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        streams_to_sync = ordered_streams

    return streams_to_sync


def write_schema_message(stream: Dict):
    """
    Creates and writes a schema message to stdout
    Args:
        stream: stream catalog
    """
    singer.write_message(singer.SchemaMessage(
        stream=common.calculate_destination_stream_name(stream),
        schema=stream['schema'],
        key_properties=['_id']))


def load_stream_projection(stream: Dict) -> Optional[Dict]:
    """
    Looks for pre defined projection in the stream metadata
    Args:
        stream: stream catalog

    Returns: projection if found

    """
    md_map = metadata.to_map(stream['metadata'])
    stream_projection = metadata.get(md_map, (), 'pipelinewise-tap-mongodb.projection')
    if stream_projection == '' or stream_projection == '""' or not stream_projection:
        return None

    try:
        stream_projection = json.loads(stream_projection)
    except (JSONDecodeError, TypeError):
        err_msg = f"The projection: {stream_projection} for stream {stream['tap_stream_id']} is not valid json"
        raise InvalidProjectionException(err_msg)

    if stream_projection and stream_projection.get('_id') == 0:
        raise InvalidProjectionException(
            "Projection blacklists key property id for collection {}" \
                .format(stream['tap_stream_id']))

    return stream_projection


def clear_state_on_replication_change(stream: Dict, state: Dict) -> Dict:
    """
    Clears the given state if replication method of given stream has changed
    Args:
        stream: stream dictionary
        state: state

    Returns: new state

    """
    md_map = metadata.to_map(stream['metadata'])
    tap_stream_id = stream['tap_stream_id']

    # replication method changed
    current_replication_method = metadata.get(md_map, (), 'replication-method')
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (current_replication_method != last_replication_method):
        log_msg = 'Replication method changed from %s to %s, will re-replicate entire collection %s'
        LOGGER.info(log_msg, last_replication_method, current_replication_method, tap_stream_id)
        state = singer.reset_stream(state, tap_stream_id)

    # replication key changed
    if current_replication_method == 'INCREMENTAL':
        last_replication_key = singer.get_bookmark(state, tap_stream_id, 'replication_key_name')
        current_replication_key = metadata.get(md_map, (), 'replication-key')
        if last_replication_key is not None and (current_replication_key != last_replication_key):
            log_msg = 'Replication Key changed from %s to %s, will re-replicate entire collection %s'
            LOGGER.info(log_msg, last_replication_key, current_replication_key, tap_stream_id)
            state = singer.reset_stream(state, tap_stream_id)
        state = singer.write_bookmark(state, tap_stream_id, 'replication_key_name', current_replication_key)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', current_replication_method)

    return state


def sync_stream(client: MongoClient, stream: Dict, state: Dict):
    """
    Sync given stream
    Args:
        client: MongoDb client instance
        stream: stream to sync
        state: state
    """
    tap_stream_id = stream['tap_stream_id']

    common.COUNTS[tap_stream_id] = 0
    common.TIMES[tap_stream_id] = 0
    common.SCHEMA_COUNT[tap_stream_id] = 0
    common.SCHEMA_TIMES[tap_stream_id] = 0

    md_map = metadata.to_map(stream['metadata'])
    replication_method = metadata.get(md_map, (), 'replication-method')
    database_name = metadata.get(md_map, (), 'database-name')

    stream_projection = load_stream_projection(stream)

    # Emit a state message to indicate that we've started this stream
    state = clear_state_on_replication_change(stream, state)
    state = singer.set_currently_syncing(state, stream['tap_stream_id'])
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    write_schema_message(stream)
    common.SCHEMA_COUNT[tap_stream_id] += 1

    with metrics.job_timer('sync_table') as timer:
        timer.tags['database'] = database_name
        timer.tags['table'] = stream['table_name']

        collection = client[database_name][stream["table_name"]]

        if replication_method == 'LOG_BASED':
            change_streams.sync_collection(collection, stream, state, stream_projection)

        elif replication_method == 'FULL_TABLE':
            full_table.sync_collection(collection, stream, state, stream_projection)

        elif replication_method == 'INCREMENTAL':
            incremental.sync_collection(collection, stream, state, stream_projection)

        else:
            raise InvalidReplicationMethodException(
                replication_method, "Only FULL_TABLE, LOG_BASED, and INCREMENTAL replication methods are supported")

    state = singer.set_currently_syncing(state, None)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(client: MongoClient, catalog: Dict, state: Dict):
    """
    Syncs all the selected streams in the catalog
    Args:
        client: MongoDb client instance
        catalog: dictionary with all the streams details
        state: state
    """
    all_streams = catalog['streams']
    streams_to_sync = get_streams_to_sync(all_streams, state)

    for stream in streams_to_sync:
        sync_stream(client, stream, state)

    LOGGER.info(common.get_sync_summary(catalog))


def main_impl():
    """
    Main method
    """
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # Default SSL verify mode to true, give option to disable
    verify_mode = config.get('verify_mode', 'true') == 'true'
    use_ssl = config.get('ssl') == 'true'

    connection_params = {"host": config['host'],
                         "port": int(config['port']),
                         "username": config.get('user', None),
                         "password": config.get('password', None),
                         "authSource": config['database'],
                         "ssl": use_ssl,
                         "replicaSet": config.get('replica_set', None),
                         "readPreference": 'secondaryPreferred'
                         }

    # NB: "ssl_cert_reqs" must ONLY be supplied if `SSL` is true.
    if not verify_mode and use_ssl:
        connection_params["ssl_cert_reqs"] = ssl.CERT_NONE

    client = MongoClient(**connection_params)

    LOGGER.info('Connected to MongoDB host: %s, version: %s',
                config['host'],
                client.server_info().get('version', 'unknown'))

    common.INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = \
        (config.get('include_schemas_in_destination_stream_name') == 'true')

    if args.discover:
        do_discover(client, config)
    elif args.catalog:
        state = args.state or {}
        do_sync(client, args.catalog.to_dict(), state)


def main():
    """
    Main
    """
    try:
        main_impl()
    except Exception as exc:
        LOGGER.exception(exc)
        raise exc
