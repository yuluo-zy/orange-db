use anyhow::Result;

use std::path::Path;
use std::sync::Arc;
use crate::store::Store;
use crate::tree::Tree;


/// 数据表结构, 用于数据分区
pub struct Table {
    tree: Arc<Tree>,
    store: Arc<Store>
}

/// 配置数据表的选项
pub struct TableOptions {
    path: Path
}

impl Table {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let tree = Arc::new(Tree{});
        let store = Arc::new(Store{});
        Ok(Table {
            tree,
            store
        })
    }
    // pub async fn get(&self, key: &[u8] ) -> Result<Option<[u8]>> {
    //     Ok(None)
    // }
    // pub async fn put(&self, key: &[u8], data: &[u8]) -> Result<()>{
    //     Ok(())
    // }
    // pub async fn delete(&self, key: &[u8]) -> Result<()> {
    //     Ok(())
    // }
    // pub async fn close(&self) -> Result<()>{Ok(())}

}