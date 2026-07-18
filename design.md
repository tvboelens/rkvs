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
#### Compaction
- If size is exceeded
- Once compaction is finished, rollover WAL and clear the old WAL files
- Compaction creates a new segment in L0, the caller of `Memtable::compact()` (probably the storage engine) is responsible for adding it to L0 of the LSM tree
### Recovery
- Get latest LSN from SSTable
- Then go through WAL to recover, just start from the LSN in the SSTable and apply each operation to MemTable



### WAL
Append-only log that contains all the write operations in order. Rotate after flush of MemTable. 
Used for crash recovery, i.e. when starting replay all the operations from WAL and perform all the
write operations on the MemTable in order.
#### WAL Segments (files)
- LSN (u64) of each entry is byte offset from start
- Logical log is 4 GB (= 2^32 bytes), so log number is high part (first 32 bits) of LSN
- One file per segment
- Filename constists of 3 32 bit unsigned ints
    - Timeline (no multi-version support, so this will be 0 always)
    - Logical log number
    - Segment number -> each logical log divided in n segments, so 0,...,n-1
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

### Threading
- WAL is single threaded, access only through the write worker of the storage engine
- So usually I would say that each thread could access the storage engine
- So the server would have an event loop
    - Read tcp request and then spawn a task that handles the request and sends the response


### SSTable/LSM-tree
1. This is the on-disk layer
2. Keys are sorted (maybe with an index)
3. SSTable itself consists of segments (one file per segment)
4. Memtable full -> new segment
5. In intervals do compaction
    1. create new segment that is merged from older segments -> for each key take the newest value
    2. delete the old segments
5. Levels -> different approaches
    1. L0 always has overlapping segments and here searching has to start in the newest and go to oldest (could theoretically also split segments across ranges)
    2. Then the question becomes if we want to have more levels than L1. These have non-overlapping segments.
        1. If only L1, merging can become complicated, since we have to merge L0 into L1.
        2. With multiple levels you can just "push up" and in later compaction remove keys that are in segments in higher levels. Only for moving L0 to L1 one needs to merge everything all at once because of the overlapping segments.
6. Compaction in case of multiple levels
    1. Merging L0 to L1 is the usual merge sort
    2. In higher levels, say level `i` need to check if key is in level `j` for `j<i`. If yes, then do not include in result.
        1. Just using L1 is not enough
            1. No, because we might miss L1 being pushed up to L2
            2. To prevent this we would have to compact every level before compacting L0 into L1, which would probably lead to bad performance
        2. So a method for compacting that takes the following arguments
            1. The level to be compacted
            2. The levels to check for presence of keys
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
8. So should have the following structs:
    1. SegmentIndex
        1. No need for the file, just load the index into memory
        2. Can probably use a simple hashmap internally, i.e. map key (string) to offset (u64)
    2. Segment
        1. file descriptor
        2. Index
        3. What should the filename be?
            1. `*.sst` but what should `*` be?
            2. probably some kind of epoch + segment number, i.e. after compaction epoch increases by one
        4. There is also the possibility to split up the data block in smaller blocks which can be loaded into memory as a sort of cache
    3. SSTable
        1. Holds the list of segments
            1. As levels and each level is a Vec
            2. Maybe L1 separately and the other levels in a Vec (lowest level first)
        2. Searching
            1. Only if key not found in memtable
            2. L1 is special, but apart from that start at L1 and continue until Ln and if not found return None
            3. In L1 there is overlap, so here we would have to search all segments, but in the other levels we know which segment should contain the key (if it is there) provided we implement this well.
        3. Compaction
            1. Create new file and list (one entry or multiple, depends on if we can create one segment file or need to split)
            2. Delete old files
            3. swap lists (before deleting?)
            4. Implementation
                1. Level 1 has overlapping segments 
                2. Compact -> L2 no overlapping segments
                3. But what if L2 already has segments?
                    1. One possibility -> push L2 up to L3 and compact again (at a later stage maybe)
                        1. so this would mean looking in the lower levels if a key is present and if yes drop it from this level
                        2. Maybe only L1? we should not trust the memtable, but we assume that level i does not contain any keys contained in level j for j=2,...,i-1, then if we remove all keys that are contained in L1
                        3. Best approach seems from bottom to top
                            1. Merge sort L1 to create new L2.
                            2. Then from old L2 to Ln do:
                                1. Write new LiSj until it is full, if so create LiSj+1.
                                2. When writing only include key if it is not in L1 (i.e. in new L2)
                    2. Other possibility is to have only 3 levels, L0 is memtable, L1 are the overlapping segments and L2 the non-overlapping
                        1. First merge L1 and then merge again with L2?
                        2. I think I like this aproach less, since we have to do two merges
        4. If memtable flushes, needs to be notified
        5. Regardless of everything: Implement the merge and specify policy independently
            1. ⁠First implement merge, which takes a list of segments to merge. We need to know which level the segments have and which ones are more recent
            2. ⁠⁠or one method for merging with overlapping and one method for non overlapping. In the latter case the key ranges can be used instead of recency. 
            3. ⁠⁠So then the policy becomes deciding which segments to merge and when.

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
