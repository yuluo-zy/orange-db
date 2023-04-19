fn main() {
    // let path = tempdir().unwrap();
    // let table = Table::open(&path, TableOptions::default())?;
    // let key = vec![1];
    // let val1 = vec![2];
    // let val2 = vec![3];
    // // Simple CRUD operations.
    // table.put(&key, 1, &val1)?;
    // table.delete(&key, 2)?;
    // table.put(&key, 3, &val2)?;
    // assert_eq!(table.get(&key, 1)?, Some(val1));
    // assert_eq!(table.get(&key, 2)?, None);
    // assert_eq!(table.get(&key, 3)?, Some(val2.clone()));
    // let guard = table.pin();
    // // Get the value without copy.
    // assert_eq!(guard.get(&key, 3)?, Some(val2.as_slice()));
    // // Iterate the tree page by page.
    // let mut pages = guard.pages();
    // while let Some(page) = pages.next()? {
    //     for (k, v) in page {
    //         println!("{:?} {:?}", k, v);
    //     }
    // }
    // Ok(())
}

// page：页面存储中的最小数据单元
// page addr：页面存储中页面的唯一逻辑地址
// node：页面链
// node id：节点的唯一标识
// node table：将节点id映射到节点中第一页的地址