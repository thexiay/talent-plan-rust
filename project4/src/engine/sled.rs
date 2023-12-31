use std::sync::Arc;

use crate::{error::ErrorCode, KvsEngine};

use sled::{Db, IVec, Tree};

#[derive(Clone)]
pub struct SledStore {
    tree: Db,
}

impl KvsEngine for SledStore {
    fn open(path: &std::path::Path) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let tree = sled::open(path)?;
        Ok(SledStore { tree })
    }

    fn set(&self, key: String, value: String) -> crate::Result<()> {
        self.tree.insert(key, value.as_str())?;
        self.tree.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> crate::Result<Option<String>> {
        Ok(self
            .tree
            .get(key)?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()?)
    }

    fn remove(&self, key: String) -> crate::Result<()> {
        self.tree.remove(key)?.ok_or(ErrorCode::RmKeyNotFound)?;
        self.tree.flush()?;
        Ok(())
    }
}
