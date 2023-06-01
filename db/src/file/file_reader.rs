use std::alloc::Layout;
use std::io::SeekFrom;
use crate::utils::atomic::Count;
use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};


#[derive(Copy, Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct BlockHandle {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}


struct FileReader<R> where
    R: AsyncSeekExt+  AsyncRead + Unpin{
    reader: R,
    use_direct: bool,
    align_size: usize,
    // 数据对齐的字节数
    file_size: usize,
    // 文件大小
    read_bytes: Count, // 已经读取的字节大小
}

impl<R> FileReader<R> where R: AsyncSeekExt+  AsyncRead + Unpin  {
    /// 创建 基本 文件读取器
    pub fn from(
        reader: R,
        use_direct: bool,
        align_size: usize,
        file_size: usize) -> Self {
        Self {
            reader,
            use_direct,
            align_size,
            file_size,
            read_bytes: Count::default(),
        }
    }

    /// 从指定偏移量的页面上准确读取指定数量的字节。
    pub async fn read_exact_at(&mut self, buf: &mut [u8], req_offset: u64) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        };

        if !self.use_direct {
            self.reader.seek(SeekFrom::Start(req_offset)).await?;
            self.reader.read_exact(buf).await?;
        };

        Ok(())
    }

    pub async fn read_block(&mut self, block_handle: BlockHandle) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; block_handle.length as usize];
        self.read_exact_at(&mut buf, block_handle.offset).await?;
        Ok(buf)
    }

    #[inline]
    pub(crate) fn total_read_bytes(&self) -> u64 {
        self.read_bytes.get()
    }
}

#[inline]
pub(crate) fn floor_to_block_lo_pos(pos: usize, align: usize) -> usize {
    pos - (pos & (align - 1))
}

#[inline]
pub(crate) fn ceil_to_block_hi_pos(pos: usize, align: usize) -> usize {
    ((pos + align - 1) / align) * align
}


pub(crate) struct AlignBuffer {
    data: std::ptr::NonNull<u8>,
    layout: Layout,
    size: usize,
}

impl AlignBuffer {
    pub(crate) fn new(n: usize, align: usize) -> Self {
        assert!(n > 0);
        let size = ceil_to_block_hi_pos(n, align);
        let layout = Layout::from_size_align(size, align).expect("Invalid layout");
        let data = unsafe {
            // Safety: it is guaranteed that layout size > 0.
            std::ptr::NonNull::new(std::alloc::alloc(layout)).expect("The memory is exhausted")
        };
        Self { data, layout, size }
    }

    #[inline]
    fn len(&self) -> usize {
        self.size
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.size) }
    }

    pub(crate) fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.size) }
    }
}

impl Drop for AlignBuffer {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Send for AlignBuffer {}

/// # Safety
///
/// [`AlignBuffer`] is [`Send`] since all accesses to the inner buf are
/// guaranteed that the aliases do not overlap.
unsafe impl Sync for AlignBuffer {}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_floor_to_block_lo_pos() {
        // Test with different alignments
        assert_eq!(floor_to_block_lo_pos(5000, 4096), 4096);
        assert_eq!(floor_to_block_lo_pos(12345, 1024), 12288);

    }
    #[test]
    fn test_align_buffer() {
        // 创建一个 AlignBuffer 实例
        let align_size = 16;
        let buf_size = 32;
        let mut align_buffer = AlignBuffer::new(buf_size, align_size);

        // 验证长度是否正确
        assert_eq!(align_buffer.len(), buf_size);

        // 获取可变的字节切片
        let mut mutable_bytes = align_buffer.as_bytes_mut();
        assert_eq!(mutable_bytes.len(), buf_size);

        // 修改字节切片中的数据
        mutable_bytes[0] = 1;
        mutable_bytes[1] = 2;
        mutable_bytes[2] = 3;

        // 获取不可变的字节切片
        let bytes = align_buffer.as_bytes();
        assert_eq!(bytes.len(), buf_size);

        // 验证数据是否正确修改
        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[1], 2);
        assert_eq!(bytes[2], 3);
    }
}