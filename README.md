# kochudb
KochuDB is LSMTree based level-compacted single-threaded key-value store

## Design
Implemented based on Log-Structured Merge tree.

- In-Memory data is stored in `Skiplist` based data structures, which is periodically flushed to disk using a flusher thread.
- A `Deque` is used for skiplist rolling, which is consumed and emptied by the flusher thread.
- Data on disk is persisted into SSTables. SSTables consist of an index file and data file organized into levels.
- SSTable store `byte[]` of serialized objects in data files, while index file stores references to corresponding data file for each key.
- Each level may contain multiple `SSTable` files.
- SSTables in one level are compacted and promoted into next higher level by a `Compaction Thread` which implements `Leveled Compaction` strategy.
- Compaction thread runs periodically checking against the compaction criteria to begin a fresh compaction.
- Keys are restricted to 256 bytes long `String` types, where as values can be any `Serializable` object of size `1 << 32`.

## Possible improvements
* __Bloomfilter__ - for lookup optimization
* __Write-Ahead Log__ - to impreove durability
* __Sparse indexes__ - for higher compaction levels
