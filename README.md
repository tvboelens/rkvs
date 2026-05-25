# rkvs
Key value store written in Rust. This project grew out of a desire to better understand NoSQL storage systems in contrast to relational databases, especially in situations where the data has the structure of a tree and operations consist mostly of reading, appending, but not modifying (except for maybe deleting) data. The main application we are thinking of is LTANS-type systems for long time storage of documents, where the documents are stored in a separate storage system and the nodes of the timestamped hash trees are stored in our key-value store.

## API
The API supports 3 operations:
- get: retrieve the value corresponding to a key
- put: set the value corresponding to a key
- delete: delete the value corresponding to a key

## Current status and future development
- As of now the TCP layer is implemented with an async event loop using tokio (single request per socket/connection) and a custom protocol. The protocol is well tested, the async event loop has some tests using Tokio's mocking functionality and fakes for the storage engine.
    - Future work will at least consist of expanding test coverage. We might also consider reusing connections, i.e. multiple requests per connection.
- The storage engine consists of a multithreaded hash map (memtable) and we have started implementing a write-ahead-log (WAL). We are planning the following steps:
    - Finishing the WAL
    - Implementing the string-sorted table including compaction
    - Optimizing the data structures of the memtable for storage of (timestamped) hash trees.
