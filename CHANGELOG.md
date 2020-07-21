# Changelog

## 1.0.1
   * Fix case where resume tokens has extra properties that are not json serializable by saving `_data` only. 

## 1.0.0
   * This is a fork of [Singer's tap-mongodb version 2.0.0](https://github.com/singer-io/tap-mongodb).
   * Oplog replication has been replaced with [MongoDB ChangeStreams](https://docs.mongodb.com/manual/changeStreams/).
   * Custom logging configuration.
