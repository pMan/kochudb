[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Maven Build](https://github.com/pMan/kochudb/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/pMan/kochudb/actions/workflows/build.yml)

# kochudb
KochuDB is LSMTree based level-compacted single-threaded key-value store.

Read more about KochuDB on [Medium](https://medium.com/@pracho/building-an-lsm-tree-based-data-store-in-java-part-1-d46adab464ab)

```
:::::::::::::::::::::::::::::::::::::::::::::::::::
::                                               ::
::   Welcome to      _           _____  ____     ::
::   | |/ /         | |         |  __ \|  _ \    ::
::   | ' / ___   ___| |__  _   _| |  | | |_) |   ::
::   |  < / _ \ / __| '_ \| | | | |  | |  _ <    ::
::   | . \ (_) | (__| | | | |_| | |__| | |_) |   ::
::   |_|\_\___/ \___|_| |_|\__,_|_____/|____/    ::
::                               Version 0.0.1   ::
:::::::::::::::::::::::::::::::::::::::::::::::::::
```

## Design
Implemented based on Log-Structured Merge (LSM) tree.

- In-Memory data is stored in `Skiplist` of byte arrays, which is periodically flushed to disk using a flusher thread.
- A `Deque` is used for skiplist rolling, which is consumed and emptied by the flusher thread.
- Data on disk is persisted into SSTables. SSTables consist of an index file and data file organized into levels.
- SSTable store `byte[]` of serialized objects in data files, while index file stores references to corresponding data file entries.
- Each level may contain multiple `SSTable` files.
- SSTables in one level are compacted and promoted into next higher level by a `Compaction Thread` which implements `Leveled Compaction` strategy.
- Compaction thread runs periodically checking against the compaction criteria to begin a fresh compaction.
- Keys are restricted to 256 bytes long `String` types, where as values can be any `Serializable` object of size 4MB.

## How to run
#### Clone and build kochudb
```
git clone https://github.com/pMan/kochudb.git
cd kochudb
mvn clean package -DskipTests
```
#### Run server
```
cd kochudb-server
java -jar target/kochudb-server*.jar
```

#### Run cli client
```
java -jar cli-client/target/cli-client*.jar
```
## Possible improvements
* __Bloomfilter__ - for lookup optimization
* __Write-Ahead Log__ - to improve durability
* __Sparse indexes__ - for search optimization
* __Data Compression__ - for storage efficiency

### Disclaimer
Not production-ready, not meant to be.
