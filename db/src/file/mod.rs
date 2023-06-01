mod file_reader;
mod types;
mod checksum;
mod compression;
mod file_builder;

pub(crate) mod constant {
    pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4096;
    pub(crate) const IO_BUFFER_SIZE: usize = 8 << 20;
    pub(crate) const FILE_MAGIC: u64 = 0x179394; // 操作系统中文件 魔数是一个特殊的固定值，用于标识文件格式或特定的文件类型
}

pub(crate) mod facade {

}