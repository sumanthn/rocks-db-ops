# rocks-db-ops
Rocks DB for counting
Counting of uniques & storing is well solved using HLL + Redis
However persisting it in disk is also required for long term storage
In this solution RocksDB is used to embed HLL (as ```byte[]```)
Store & merge operations are handled in a layer above the DB
```Db``` a shard of RocksDB with support for add/fetch HLL
```UniqueUsersDbStoreV2``` Hash the key , select a shard & insert into RocksDB
Can init number of shards. 
Tested(```UniqueUsersGenerator``) using a data generator and comparing stored uniques aganist the retrieved HLL counts. 

Usage
Use a day key with prefix and a day and upload HLL (uniques)
Example:
regionId:<day> = set(uniqueUsers)
Day alone can be used too
day = set(uniqueUsers)
Multiple sets can be used to update the unique set , all are merged upon read