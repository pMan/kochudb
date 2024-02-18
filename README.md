[![Maven Build](https://github.com/pMan/kochudb/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/pMan/kochudb/actions/workflows/build.yml)

# kochudb
KochuDB is LSMTree based level-compacted single-threaded key-value store

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

## Possible improvements
* __Bloomfilter__ - for lookup optimization
* __Write-Ahead Log__ - to improve durability
* __Sparse indexes__ - for search optimization
* __Data Compression__ - for storage efficiency

### Disclaimer
Not production-ready, not meant to be.
