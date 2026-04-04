pub mod memtable;

pub struct StorageEngine {
    memtable: memtable::MemTable,
}

impl StorageEngine {
    pub fn get(&self, key: &String) -> Option<String> {
        /* This will have to get more sophisticated once we move to on-disk persistence */
        self.memtable.get(key)
    }
}
