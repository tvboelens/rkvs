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

### SSTable
TODO
