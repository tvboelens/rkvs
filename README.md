# rkvs
Key value store written in Rust. This project grew out of a desire to better understand NoSQL storage systems in contrast to relational databases, especially in situations where the data has the structure of a tree and operations consist mostly of reading, appending, but not modifying (except for maybe deleting) data. The main application we are thinking of is LTANS-type systems for long time storage of documents, where the documents are stored in a separate storage system and the timestamped hash trees are stored in our key-value store.

## API
The API supports 3 operations:
- get: retrieve the value corresponding to a key
- set: set the value corresponding to a key
- delete: delete the value corresponding to a key

## Development steps
- We will start with a simple in memory key-value store (MemTable), where the keys are sorted and lookups are done via binary search.
- Next we will implement on-disk persistence. This will at least involve a write-ahead-log (WAL) and most likely String Sorted Tables (SSTables).
- We will also need some way to interact with the storage system. We will start with a CLI and add a TCP networking layer once persistence is established.
