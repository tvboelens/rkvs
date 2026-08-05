## Storage engine
This consists of 3 parts.
1. MemTable
2. Write ahead log (WAL)
3. String Sorted table (SSTable)/LSM-Tree

### MemTable
- This is the lowest level component. It contains all the keys and values that were last written (both puts and deletes).
- When calling `get()` the MemTable is searched first and only is the key is not found the search continues in the SSTables.
- If the MemTable is full then it gets compacted to an SSTable and the MemTable is cleared again.
- We need a special value for deleted keys (i.e. a tombstone record). One possibility is to let the MemTable consists of a HashMap with string keys and `Option<String>` values and `None` signifies a deleted key
    - We might also want to store the LSN with the value, since we also want to persist it to the SSTable
#### Compaction
- If size is exceeded
- Once compaction is finished clear the old WAL files, i.e. all segment files before the active segment
- Compaction creates a new segment in L0, the caller of `Memtable::compact()` (probably the storage engine) is responsible for adding it to L0 of the LSM tree
### Recovery
- Get latest LSN from SSTable
- Then go through WAL to recover, just start from the LSN in the SSTable and apply each operation to MemTable
- So in more detail
    - Check if SSTable files exist (separate function)
        - Yes -> get largest LSN
        - No -> largest LSN = 0
    - Check if WAL files exist
        - No -> new memtable, no recovery
        - Yes -> check if segments exist containing newer LSNs
            - No -> new memtable, no recovery
            - Yes -> recover

### WAL
Append-only log that contains all the write operations in order. Rotate after flush of MemTable. 
Used for crash recovery, i.e. when starting replay all the operations from WAL and perform all the
write operations on the MemTable in order.
#### WAL Segments (files)
- LSN (u64) of each entry is byte offset from start
- Logical log is 4 GB (= 2^32 bytes), so log number is high part (first 32 bits) of LSN
- One file per segment
- Filename constists of 3 32 bit unsigned ints -> hex string of length 24 (1 byte is two hex chars)
    - Timeline (no multi-version support, so this will be 0 always)
    - Logical log number
    - Segment number -> each logical log divided in n segments, so 0,...,n-1 -> this equals LSN / segment_size 
- WAL should have exact size (if full) and this should be power of 2 bytes (since it must divide 2^32)
- We allow WAL entries to be broken up
    - But not the headers?
    - Would need padding at the end
    - would need to have a header that indicates where the first (new) entry starts. That way if we delete an older segment we know where the first record to recover is located
#### WAL Entry
Have the following actions:
- Put
    - This can both mean setting a new key-value pair or updating an existing one
- Delete
Which fields should be in the entry?
- LSN
- Previous LSN?
- Type -> see above
- Transaction ID -> Probabably not necessary, since we will do last-write-wins
- pageId? -> Have to check what the differences between SQL and KV-stores are with respect to pages
- before/after?


### SSTable/LSM-tree
1. This is the on-disk layer
2. Keys are sorted
3. SSTable itself consists of levels, which consist of segments (one file per segment)
4. Memtable full -> flush to new segment
    1. Currently unclear how I want to do this
    2. Also need to add the new segment to level 0.
5. If after MemTable flush level 0 is full, compact
    1. Need some way to notify the compaction background task of this
5. Levels
    1. Level 0 has overlapping segments
    2. Higher levels (PartitionedLevel) have non-overlapping segments
    3. Have target/max size, next level has about 10x target size
6. Compaction
    1. Determine which segments to merge into higher level
        1. L0 -> all segments
        2. higher levels -> From oldest to newest segment, keep going until target size not exceeded
    2. Merge
        1. If merging level i-1 into i. Check which segments of level i overlap with those of i-1 we want to merge
        2. Then form intervals and merge per interval
    3. Check if the level we merged into exceeds target size, if yes repeat
    4. Can periodically check if a higher level exceeds target size (e.g. in case a previous compaction failed)
7. structs and thread safety
    1. SsTable is the orchestrator
    2. CompactionBt is the compaction background task
    3. Segment is a segment of a level
    4. SsTableLevel represents a level of the SSTable -> Use a generic and trait to distinguish between a level containing overlapping and a level containing non-overlapping segments
    5. LevelContainer: this is the struct that guarantees thread safety, it contains the following
        1. An Arc pointing to an OverlappingLevel
        2. A Vec of Arcs pointing to a PartitionedLevel
    6. Thread safety
        1. LevelContainer is behind a RwLock
        2. Reading threads access the container, and clone the list, then read from the levels
        3. The compaction background task first clones the list, creates the new segments and a new list and then performs a swap.
        4. In this way the reading threads see a snapshot
        5. After completing compaction bt schedules the old segments for deletion and periodically checks the reference count to see whether no reading thread is accessing the segment and it can delete the file
6. Lookup
    1. Look in newest segment and keep going back until you find it
    2. If not found in level i, go to level i+1.
    2. There is something like a Bloom filter which makes it more effective to scan whether a key is contained in a segment -> deal with this later
7. On-disk format: seems frequently to be like this
    1. data block: key, LSN and value (prepended by length)
        1. Or maybe just store the last LSN in the footer
        2. And maybe I also want checksums?
            1. Partial write when compacting
            2. OTOH the write is complete when the magic number is appended, so maybe that is enough?
    2. Index block: key and offset (key prepended by length) -> possibly sparse
    3. Bloom filter (the bit array)
    4. Footer -> needs to have fixed amount of bytes so that we can search it from the end
        1. Index offset (start of index block)
        2. Bloom filter offset
        3. Bloom filter size
        4. magic number for validation
    5. Why this sequence? Because this is the logical order for flushing from memtable to file
    6. Need to decide a tombstone value
    7. How does searching work?
        1. Do binary search, but for this load the index into memory
        2. So find closest index and next index and search this range, if key not found move on the previous segment

### TCP Layer
- io::Error means that either connection error or unexpected EOF
    - Can we check if connection error? If we can and lost connection, just move on (log?)
    - In latter case can check if we received a correlation id and send back an error, else just drop the socket (log?)
- All other errors demand a response
    - Wrong magic bytes -> close connection
    - Wrong version
    - Wrong type
    - Missing value for put
    - Invalid bytes for payload (no ascii/utf8)
    - unknown flags
    - storage engine errors
- Connections
    - handled by both the cancellation token (in the server) and the connection manager
        - Connection manager should store list (map) of active connections
        - When shutting down the server should wait for connection manager to finish and connection manager waits for all the connections to finish.
        - Once a connection finishes the manager does an await on the task handle to make sure the task is really over before (and maybe handle panics in the task) before removing the connection from the list
    - Closing connections
        - Via cancellation token -> do tokio select
        - Problem: what if the connection just hangs, i.e. client is lagging with sending bytes?
            - use timeouts
            - first timeout can be longer or maybe make it configurable as parameter in the start function
                - If the whole request does not complete or if it has not started?
                    - First one is easy, just do timeout
                    - second one i do not know how to do this in a select, but an easy solution would be to do a timeout on receiving the headers, since these consist of 20 to 30 bytes, but then should receive the headers and payload directly in the start method, not in recv_tcp_request
            - Second timeout for headers -> here can do completion time
            - third timeout for payload -> here can do completion time
#### TCP Protocol
- Header Length header -> u32 
- magic bytes 72 6B 76 73 ("rkvs")
- Correlation id -> Rust has libraries for this
- protocol version (in case I decide to change the protocol): u8
- type -> u8
    - Put -> 0
    - Delete -> 1
    - Get -> 2
    - Heartbeat/Ping -> 3
- flags -> u16
- optional headers
    - none planned at this moment, but need to have an id of some sorts
- Payload length -> u32
- payload: raw bytes in little endian order
    - key length -> u32
    - key bytes -> ascii
    - value length (only with put) -> u32
    - value bytes -> ascii
#### Response
- Length header
- Correlation id
- Return code -> u8 should be enough
    - 0 for ok, other values for ec
- Payload
    - Value if Ok and present
    - Error message if present
