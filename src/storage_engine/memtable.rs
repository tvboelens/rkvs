use std::collections::HashMap;
use std::sync::RwLock;

pub struct MemTable {
    table: RwLock<HashMap<String, String>>,
}

impl MemTable {
    pub fn new() -> Self {
        let map = HashMap::new();
        MemTable {
            table: RwLock::new(map),
        }
    }

    pub fn put(&mut self, key: String, value: String) -> Option<String> {
        let mut table = self.table.write().unwrap();
        table.insert(key, value)
    }

    pub fn get(&self, key: &String) -> Option<String> {
        let table = self.table.read().unwrap();
        table.get(key).cloned()
    }

    pub fn delete(&mut self, key: &String) -> Option<String> {
        let mut table = self.table.write().unwrap();
        table.remove(key)
    }

    pub fn len(&self) -> usize {
        let table = self.table.read().unwrap();
        table.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn put_get() {
        let mut table = MemTable::new();
        let key = String::from("key");
        let key_read = key.clone();
        let value = String::from("value");
        let val_read = value.clone();
        let result = table.put(key, value);
        assert!(result.is_none());
        let len = table.len();
        assert_eq!(len, 1);
        let res = table.get(&key_read);
        assert!(res.is_some());
        assert_eq!(res.unwrap(), val_read);
    }
}
