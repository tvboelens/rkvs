pub struct MemTable {
    table: std::collections::HashMap<String, String>,
    size: usize
}

impl MemTable {
    pub fn put(mut self, key: String, value: String) -> Option<String> {
        match self.table.insert(key, value) {
            Some(v) => Some(v),
            None => { self.size = self.size+1; None }
        } 
    }

    pub fn get_copy(&self, key: &String) -> Option<String> {
        match self.table.get(key) {
            Some(v) => Some(v.clone()),
            None => None
        }
    }

    pub fn get(&self, key: &String) -> Option<&String> {
        self.table.get(key) 
    }

    pub fn delete(mut self, key: &String) -> Option<String> {
        self.table.remove(key)
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
