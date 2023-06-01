use std::collections::BTreeMap;
use crate::file::checksum::ChecksumType;
use crate::file::compression::Compression;
use crate::page::base::PageInfo;
use crate::store::Options;

struct IndexBlock {
    pub(crate) page_offsets: BTreeMap<u64, (u64, PageInfo)>,
    pub(crate) meta_page_table: Option<u64>,
}
#[derive(Default)]
struct IndexBlockBuilder {
    index_block: IndexBlock,
}

pub(crate) struct CommonFileBuilder {
    group_id: u32,
    compression: Compression,
    checksum: ChecksumType,

    index: IndexBlockBuilder,
    page_table: PageTable,
}

