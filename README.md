[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Maven Build](https://github.com/pMan/kochudb/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/pMan/kochudb/actions/workflows/build.yml)

# kochudb
KochuDB is an LSMTree-based level-compacted thread-safe persistent key-value store.

Read more about KochuDB on (outdated) [Medium](https://medium.com/@pracho/building-an-lsm-tree-based-data-store-in-java-part-1-d46adab464ab) posts.

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
Use the shell scripts (`build-all.sh`, `start-server.sh`, `start-client.sh` in that order) on Unix systems, or run manually by using below commands.
#### Clone and build kochudb
```
> git clone https://github.com/pMan/kochudb.git
> cd kochudb
> mvn clean install -DskipTests
```
#### Run server
```
> java -jar kochudb-server/target/kochudb-server*.jar
```


#### Load test data
```
> java -jar cli-client/target/cli-client*.jar load
```

#### Run interactive cli client
```
> java -jar cli-client/target/cli-client*.jar
<< KochuDB CLI client >>
Usage help:
        set <key> <val>
        get <key>
        del <key>

> Type "bye" to exit
>
> get rafting
this is the house that Jack built and this is the judge that kept the maiden sowing his corn that tossed the house that Jack built
>
> set key1 value1
ok
>
> bye
Client closed

Shutting down client
```
## Possible improvements
* __Bloomfilter__ - for lookup optimization
* __Write-Ahead Log__ - to improve durability
* __Sparse indexes__ - for search optimization
* __Data Compression__ - for storage efficiency

### Disclaimer
Not production-ready, not meant to be.
