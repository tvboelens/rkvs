use std::collections::HashMap;
use std::sync::RwLock;

pub struct MemTable {
    table: RwLock<HashMap<String, String>>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        let map = HashMap::new();
        MemTable {
            table: RwLock::new(map),
            size: 0,
        }
    }

    pub fn put(&mut self, key: String, value: String) -> Option<String> {
        let mut table = self.table.write().unwrap();
        match table.insert(key, value) {
            Some(v) => Some(v),
            None => {
                self.size = self.size + 1;
                None
            }
        }
    }

    pub fn get(&self, key: &String) -> Option<String> {
        let table = self.table.read().unwrap();
        match table.get(key) {
            Some(v) => Some(v.clone()),
            None => None,
        }
    }

    pub fn delete(&mut self, key: &String) -> Option<String> {
        let mut table = self.table.write().unwrap();
        match table.remove(key) {
            Some(v) => {
                self.size = self.size - 1;
                Some(v)
            }
            None => None,
        }
    }

    pub fn size(&self) -> usize {
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
        let size = table.size();
        assert_eq!(size, 1);
        let res = table.get(&key_read);
        assert!(res.is_some());

        assert_eq!(res.unwrap(), val_read);
    }
}
