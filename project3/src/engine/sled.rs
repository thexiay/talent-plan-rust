use std::sync::Arc;

use crate::KvsEngine;

use sled::{Db, IVec, Tree};

struct SledStore {
    tree: Db,
}

impl KvsEngine for SledStore {
    fn open(path: &std::path::Path) -> crate::Result<Self> where Self: Sized {
        let tree = sled::open(path)?;
        Ok(SledStore{
            tree
        })
    }

    fn set(&mut self, key: String, value: String) -> crate::Result<()> {
        self.tree.insert(key, value.as_str())?;
        Ok(())
    }

    fn get(&mut self, key: String) -> crate::Result<Option<String>> {
        Ok(self.tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&mut self, key: String) -> crate::Result<()> {
        self.tree.remove(key)?;
        Ok(())
    }
}