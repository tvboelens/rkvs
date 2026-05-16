## Storage engine
This consists of 3 parts.
1. MemTable
2. Write ahead log (WAL)
3. String Sorted table (SSTable)

### MemTable
This is the simplest component. It contains all the keys and values that were last written (both puts and deletes).
When calling `get()` the MemTable is searched first and only is the key is not found the search continues in the SSTable.
If the MemTable is full then it gets flushed to an SSTable and the MemTable is cleared again.



### WAL
Append-only log that contains all the write operations in order. Rotate after flush of MemTable. 
Used for crash recovery, i.e. when starting replay all the operations from WAL and perform all the
write operations on the MemTable in order.
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


### SSTable
TODO

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
