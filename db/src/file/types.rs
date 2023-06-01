use std::collections::HashMap;
use std::sync::Arc;
use rustc_hash::{FxHashMap, FxHashSet};
use crate::page::base::PageInfo;

pub(crate) struct PageHandle {
    pub(crate) offset: u32,
    pub(crate) size: u32
}

struct PageMeta {
    index: u32,
    info: PageInfo,
    handle: PageHandle
}

#[derive(Clone)]
pub(crate) struct PageGroup {
    dealloc_pages: FixedBitmap,
    active_size: usize,
    meta: Arc<PageGroupMeta>
}

pub(crate) struct PageGroupIterator {
    file_id: u32,
    index: usize,
    active_pages: Vec<(u32, u32)>
}

pub(crate) struct PageGroupMeta {
    pub(crate) group_id: u32,
    pub(crate) file_id: u32,
    base_offset: u64,
    page_table_offset: u64,
    meta_block_end: u64,
    page_meta_map: FxHashMap<u32,PageMeta>
}

#[derive(Clone)]
pub(crate) struct FileMeta {
    pub(crate) file_id: u32,
    pub(crate) file_size: usize,
    pub(crate) block_size: usize,
    pub(crate) referenced_groups: FxHashSet<u32>,

    pub(crate) checksum_type: ChecksumType,
    pub(crate) compression: Compression,
    pub(crate) page_groups: FxHashMap<u32, Arc<PageGroupMeta>>
}

pub(crate) struct FileInfo {
    up1: u32,
    up2: u32,

    meta: Arc<FileMeta>
}