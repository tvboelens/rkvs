use std::collections::HashMap;

pub struct MemTable {
    table: HashMap<String, String>,
    size: usize,
}

impl MemTable {
    pub fn put(&mut self, key: String, value: String) -> Option<String> {
        match self.table.insert(key, value) {
            Some(v) => Some(v),
            None => {
                self.size = self.size + 1;
                None
            }
        }
    }

    pub fn get_copy(&self, key: &String) -> Option<String> {
        match self.table.get(key) {
            Some(v) => Some(v.clone()),
            None => None,
        }
    }

    pub fn get(&self, key: &String) -> Option<&String> {
        self.table.get(key)
    }

    pub fn delete(&mut self, key: &String) -> Option<String> {
        match self.table.remove(key) {
            Some(v) => {
                self.size = self.size - 1;
                Some(v)
            }
            None => None,
        }
    }

    pub fn size(&self) -> usize {
        self.size.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn put_get() {
        let mut table = MemTable {
            table: HashMap::new(),
            size: 0,
        };
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

        assert_eq!(res.unwrap(), &val_read);
    }
}
