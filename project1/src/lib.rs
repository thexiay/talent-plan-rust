use std::collections::BTreeMap;

#[derive(Default)]
pub struct KvStore {
    map: BTreeMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            map: BTreeMap::default(),
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).map(|s| s.to_owned())
    }

    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}

#[cfg(test)]
mod tests {}
