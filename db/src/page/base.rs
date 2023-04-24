use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::{fmt, slice};
use anyhow::Result;

/// Page format {
///     epoch      : 6 bytes 世代用来追踪事务
///     flags      : 1 bytes  用来标明是是否是叶子节点 和 数据节点
///     chain_len  : 1 bytes
///     chain_next : 8 bytes
///     content    : multiple bytes 内容具体存储
/// }
const PAGE_HEADER_LEN: usize = 6;
const PAGE_CONTENT_LEN: usize = 16;
const PAGE_EPOCH_MAX: u64 = (1 << 48) - 1;

/// 针对 page的数据指针
#[derive(Clone, Copy)]
pub struct PagePtr {
    //头指针
    ptr: NonNull<u8>,
    //内存块长度
    len: usize,
}

impl PagePtr {
    pub fn new(ptr: NonNull<u8>, len: usize) -> Self {
        Self {
            ptr,
            len,
        }
    }
    pub fn kind(&self) -> PageKind { self.flags().kind() }
    pub fn tier(&self) -> PageTier { self.flags().tier() }

    pub fn set_flags(&mut self, flags: PageFlag) { unsafe { self.flags_ptr().write(flags.0) } }
    pub fn flags(&self) -> PageFlag { unsafe { PageFlag(self.flags_ptr().read()) } }
    pub fn set_epoch(&mut self, epoch: u64) {
        let data = epoch.to_le();
        let ptr = &data as *const u64 as *const u8;
        unsafe { ptr.copy_to_nonoverlapping(self.epoch_ptr(), PAGE_HEADER_LEN) }
    }
    pub fn epoch(&self) -> u64 {
        unsafe {
            let data = self.epoch_ptr() as *mut u64;
            let val = u64::from_le(data.read());
            val & PAGE_EPOCH_MAX
        }
    }
    pub fn chain_len(&self) -> u8 { unsafe { self.chain_len_ptr().read() } }
    pub fn set_chain_len(&mut self, len: u8) { unsafe { self.chain_len_ptr().write(len) } }

    pub fn chain_next(&self) -> u64 { unsafe { self.chain_next_ptr().read() } }
    pub fn set_chain_next(&mut self, address: u64) { unsafe { self.chain_next_ptr().write(address) }; }

    pub(crate) fn size(&self) -> usize {
        self.len
    }

    pub(crate) fn data<'a>(&self) -> &'a [u8] { unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) } }

    pub(super) fn content<'a>(&self) -> &'a [u8] { unsafe { slice::from_raw_parts(self.content_ptr(), self.content_size()) } }

    pub(super) fn content_mut<'a>(&mut self) -> &'a mut [u8] { unsafe { slice::from_raw_parts_mut(self.content_ptr(), self.content_size()) } }

    pub fn content_size(&self) -> usize { self.len - PAGE_CONTENT_LEN }

    /// Returns the page info.
    pub(crate) fn info(&self) -> PageInfo {
        let meta = unsafe { self.as_ptr().cast::<u64>().read() };
        let next = self.chain_next();
        let size = self.len;
        PageInfo { meta, next, size }
    }
}

impl PagePtr {
    fn as_ptr(&self) -> *mut u8 { self.ptr.as_ptr() }
    fn epoch_ptr(&self) -> *mut u8 { self.as_ptr() }
    unsafe fn flags_ptr(&self) -> *mut u8 { self.as_ptr().add(PAGE_HEADER_LEN) }
    unsafe fn chain_len_ptr(&self) -> *mut u8 { self.as_ptr().add(PAGE_HEADER_LEN + 1) }
    unsafe fn chain_next_ptr(&self) -> *mut u64 { self.as_ptr().cast::<u64>().add(1) }
    unsafe fn content_ptr(&self) -> *mut u8 { self.as_ptr().add(PAGE_CONTENT_LEN) }
}

/// 针对 page 的可变引用
pub struct PageMut<'a> {
    ptr: PagePtr,
    _marker: PhantomData<&'a mut ()>,
}

impl<'a> PageMut<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Self {
        unsafe {
            let ptr = NonNull::new_unchecked(buf.as_mut_ptr());
            PagePtr::new(ptr, buf.len()).into()
        }
    }
}

impl<'a> Deref for PageMut<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<'a> DerefMut for PageMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ptr
    }
}

// impl<'a> fmt::Debug for PageMut<'a> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         // self.ptr.fmt(f)
//     }
// }

impl<'a> From<&'a mut [u8]> for PageMut<'a> {
    fn from(buf: &'a mut [u8]) -> Self {
        PageMut::new(buf)
    }
}

impl<'a> From<PagePtr> for PageMut<'a> {
    fn from(ptr: PagePtr) -> Self {
        PageMut {
            ptr,
            _marker: PhantomData,
        }
    }
}

/// 针对 page 不可变引用
#[derive(Copy, Clone)]
pub struct PageRef<'a> {
    ptr: PagePtr,
    _marker: PhantomData<&'a mut ()>,
}


impl<'a> PageRef<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        unsafe {
            let ptr = NonNull::new_unchecked(buf.as_ptr() as *mut _);
            PagePtr::new(ptr, buf.len()).into()
        }
    }
}

impl<'a> Deref for PageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

// impl<'a> fmt::Debug for PageRef<'a> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         self.ptr.fmt(f)
//     }
// }

impl<'a> From<&'a [u8]> for PageRef<'a> {
    fn from(buf: &'a [u8]) -> Self {
        PageRef::new(buf)
    }
}

impl<'a> From<PagePtr> for PageRef<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self {
            ptr,
            _marker: PhantomData,
        }
    }
}

impl<'a> From<PageMut<'a>> for PageRef<'a> {
    fn from(buf: PageMut<'a>) -> Self {
        buf.ptr.into()
    }
}

/// page 构建对象
pub struct PageBuild {
    kind: PageKind,
    tier: PageTier,
}

impl PageBuild {
    pub fn new(kind: PageKind, tier: PageTier) -> Self {
        Self {
            kind,
            tier,
        }
    }

    pub fn build(&self, page: &mut PageMut<'_>) {
        let flags = PageFlag::new(self.kind, self.tier);
        page.set_flags(flags);
        page.set_epoch(0);
        page.set_chain_len(1);
        page.set_chain_next(0);
    }
}

/// page的内容对象
pub struct PageInfo {
    meta: u64,
    next: u64,
    size: usize,
}

impl PageInfo {
    #[inline]
    pub(crate) fn from_raw(meta: u64, next: u64, size: usize) -> Self {
        PageInfo { meta, next, size }
    }

    /// Returns the page tier.
    #[inline]
    pub(crate) fn tier(&self) -> PageTier {
        self.flags().tier()
    }

    /// Returns the page kind
    #[inline]
    pub(crate) fn kind(&self) -> PageKind {
        self.flags().kind()
    }

    /// Returns the page epoch.
    #[inline]
    pub(crate) fn epoch(&self) -> u64 {
        self.meta & PAGE_EPOCH_MAX
    }

    /// Returns the address of the next page.
    #[inline]
    pub(crate) fn chain_next(&self) -> u64 {
        self.next
    }

    /// Returns the length of the chain.
    #[inline]
    pub(crate) fn chain_len(&self) -> u8 {
        (self.meta >> ((PAGE_HEADER_LEN + 1) * 8)) as u8
    }

    /// Returns the page size.
    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub(crate) fn value(&self) -> (u64, u64) {
        (self.meta, self.next)
    }

    #[inline]
    fn flags(&self) -> PageFlag {
        PageFlag((self.meta >> (PAGE_HEADER_LEN * 8)) as u8)
    }
}

pub struct PageFlag(u8);

impl PageFlag {
    pub fn new(kind: PageKind, tier: PageTier) -> Self { Self(kind as u8 | tier as u8) }
    pub fn kind(&self) -> PageKind { self.0.into() }
    pub fn tier(&self) -> PageTier { self.0.into() }
}

/// page 的 种类
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PageTier {
    Leaf = PAGE_TIER_LEAF,
    Inner = PAGE_TIER_INNER,
}

const PAGE_TIER_MASK: u8 = 0b0000_0001;
const PAGE_TIER_LEAF: u8 = 0b0000_0000;
const PAGE_TIER_INNER: u8 = 0b0000_0001;

impl PageTier {
    pub(crate) fn is_leaf(&self) -> bool {
        self == &Self::Leaf
    }

    pub(crate) fn is_inner(&self) -> bool {
        self == &Self::Inner
    }
}

impl From<u8> for PageTier {
    fn from(value: u8) -> Self {
        match value & PAGE_TIER_MASK {
            PAGE_TIER_LEAF => Self::Leaf,
            PAGE_TIER_INNER => Self::Inner,
            _ => unreachable!(),
        }
    }
}

/// A list of possible page kinds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PageKind {
    Data = PAGE_KIND_DATA,
    Split = PAGE_KIND_SPLIT,
}

const PAGE_KIND_MASK: u8 = 0b0000_1110;
const PAGE_KIND_DATA: u8 = 0b0000_0000;
const PAGE_KIND_SPLIT: u8 = 0b0000_0010;

impl PageKind {
    pub(crate) fn is_data(&self) -> bool {
        self == &Self::Data
    }

    pub(crate) fn is_split(&self) -> bool {
        self == &Self::Split
    }
}

impl From<u8> for PageKind {
    fn from(value: u8) -> Self {
        match value & PAGE_KIND_MASK {
            PAGE_KIND_DATA => Self::Data,
            PAGE_KIND_SPLIT => Self::Split,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{alloc, Layout};
    use super::*;

    pub(crate) fn alloc_page(size: usize) -> Box<[u8]> {
        let layout = Layout::from_size_align(size, 8).unwrap();
        unsafe {
            let ptr = alloc(layout);
            let buf = slice::from_raw_parts_mut(ptr, layout.size());
            Box::from_raw(buf)
        }
    }

    #[test]
    fn page() {
        let mut buf = alloc_page(PAGE_CONTENT_LEN + 1);
        let mut page = PageMut::new(buf.as_mut());
        {
            let builder = PageBuild::new(PageKind::Data, PageTier::Leaf);
            builder.build(&mut page);
            assert!(page.tier().is_leaf());
            assert!(page.kind().is_data());
        }
        // 测试析构变量
        {
            let builder = PageBuild::new(PageKind::Split, PageTier::Inner);
            builder.build(&mut page);
            assert!(page.tier().is_inner());
            assert!(page.kind().is_split());
        }

        assert_eq!(page.epoch(), 0);
        page.set_epoch(1);
        assert_eq!(page.epoch(), 1);
        assert_eq!(page.chain_len(), 1);
        page.set_chain_len(2);
        assert_eq!(page.chain_len(), 2);
        assert_eq!(page.chain_next(), 0);
        page.set_chain_next(3);
        assert_eq!(page.chain_next(), 3);
        assert_eq!(page.size(), PAGE_CONTENT_LEN + 1);
        assert_eq!(page.data().len(), PAGE_CONTENT_LEN + 1);
        assert_eq!(page.content().len(), 1);
        assert_eq!(page.content_mut().len(), 1);
    }
}
