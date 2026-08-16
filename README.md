# rkvs
Key value store written in Rust. This project grew out of a desire to better understand NoSQL storage systems in contrast to relational databases, especially in situations where the data has the structure of a tree and operations consist mostly of reading, appending, but not modifying (except for maybe deleting) data. The main application we are thinking of is LTANS-type systems for long time storage of documents, where the documents are stored in a separate storage system and the nodes of the timestamped hash trees are stored in our key-value store.

## API
The API supports 3 operations:
- get: retrieve the value corresponding to a key
- put: set the value corresponding to a key
- delete: delete the value corresponding to a key

## Current status and future development
- As of now the TCP layer is implemented with an async event loop using Tokio and a custom protocol. The protocol is well tested, the async event loop has some tests using Tokio's mocking functionality and fakes for the storage engine. We have also implemented functionality for reusing connections with timeout mechanisms, i.e. the server drops a connection if the client is idle for a certain time.
    - Future work will at least consist of expanding test coverage. Single requests per connections are reasonably well tested, but resuing connections is not.
- The storage engine consists of the following components:
    - An in memory component (memtable) which internally consists of a hash map together with a write-ahead-log (WAL) for crash recovery. This is almost fully tested, a few edge cases for a corrupted WAL still remain to be tested.
    - String-sorted tables (SSTables) for on-disk persistence. If the memtable becomes too large, it is flushed and written into a segment of the SSTable.
        - Currently the flushing of the memtable is implemented and basic cases tested. We still need to test edge cases, especially for failed writes.
        - We have designed for level-based compaction, i.e. there is a level 0 with overlapping SSTable segments (the memtable flushes) and higher levels where the segments are partitioned, i.e. non-overlapping. Compaction will merge segments (from oldest to newest) from level i into level i+1. Implementing the merge operations as well as the compaction background task (and testing) is the next big step.
    - The storage engine is multithreaded
        - The memtable has a separate writer working thread that first writes to the wal and then the internal hash map, which is protected by a RwLock.
        - For reading the memtable has an internal thread pool of threads that read from the hash map (via the RwLock)
        - The SSTable has a slightly more sophisticated setup
            - Each reader thread receives a snapshot (a vector of Arcs) of all the levels and the segment files contained therein
            - The thread doing background compaction first writes all the new segment files, before swapping them with the old files. The old files are scheduled for deletion, but the background task counts (through an Arc) how many threads are still reading the old files and deletion only happens after this count is 0.
            - Reading from the segment files is thread-safe, since we use the read_at (i.e. pread) function to read from  an offset within the files, which does not use/move the file cursor
- Once the storage layer is complete, we are planning on doing some benchmarking as well as researching optimizations of data structures for the use case of storing (hash) tree structures.
    - This could include using arena allocation for the memtable instead of using a hash map or caching strategies.


