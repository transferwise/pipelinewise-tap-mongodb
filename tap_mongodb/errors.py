class InvalidReplicationMethodException(Exception):
    """Exception for errors related to replication methods"""

    def __init__(self, replication_method, message=None):
        msg = f"Invalid replication method {replication_method}!"

        if message is not None:
            msg = f'{msg} {message}'

        super(InvalidReplicationMethodException, self).__init__(msg)


class InvalidProjectionException(Exception):
    """Raised if projection blacklists _id"""

class UnsupportedReplicationKeyTypeException(Exception):
    """Raised if key type is unsupported"""

class MongoAssertionException(Exception):
    """Raised if Mongo exhibits incorrect behavior"""

class MongoInvalidDateTimeException(Exception):
    """Raised if we find an invalid date-time that we can't handle"""

class SyncException(Exception):
    """Raised if we find an invalid date-time that we can't handle"""
