use std::marker::PhantomData;
use std::{mem, slice};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::ops::{Deref, Range};
use crate::page::base::{PageBuild, PageKind, PageMut, PageRef, PageTier};
use crate::page::codec::{Codec, Decoder, Encoder};
use crate::page::data::{Index, Key, Value};
use crate::page::iter::{ItemIter, RewindableIterator, SeekableIterator, SliceIter};

pub(crate) struct SortedPageBuilder<I> {
    base: PageBuild,
    iter: Option<I>,
    num_items: usize,
    content_size: usize,
}

impl<I, K, V> SortedPageBuilder<I>
    where
        I: RewindableIterator<Item = (K, V)>,
        K: SortedPageKey,
        V: SortedPageValue,
{
    /// Creates a [`SortedPageBuilder`] that will build a page with the given
    /// metadata.
    pub(crate) fn new(tier: PageTier, kind: PageKind) -> Self {
        Self {
            base: PageBuild::new(kind, tier),
            iter: None,
            num_items: 0,
            content_size: 0,
        }
    }

    /// Creates a [`SortedPageBuilder`] that will build a page from the given
    /// iterator.
    pub(crate) fn with_iter(mut self, mut iter: I) -> Self {
        // key和 data的数据空间
        for (k, v) in &mut iter {
            self.num_items += 1;
            self.content_size += k.encode_size() + v.encode_size();
        }
        // 添加 对应的 索引位置
        self.content_size += self.num_items * mem::size_of::<u32>();
        // We use `u32` to store item offsets, so the content size must not exceed
        // `u32::MAX`.
        assert!(self.content_size <= u32::MAX as usize);
        self.iter = Some(iter);
        self
    }

    /// Returns the size of the page that will be built.
    pub(crate) fn size(&self) -> usize {
        self.base.size(self.content_size)
    }

    pub(crate) fn build(mut self, page: &mut PageMut<'_>) {
        assert!(page.size() >= self.size());
        self.base.build(page);
        if let Some(iter) = self.iter.as_mut() {
            unsafe {
                let mut buf = SortedPageBuf::new(page.content_mut(), self.num_items);
                iter.rewind();
                for (k, v) in iter {
                    buf.add(k, v);
                }
            }
        }
    }
}

impl<K, V> SortedPageBuilder<ItemIter<(K, V)>>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{

    pub(crate) fn with_item(self, item: (K, V)) -> Self {
        self.with_iter(ItemIter::new(item))
    }
}

impl<'a, K, V> SortedPageBuilder<SliceIter<'a, (K, V)>>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{
    pub(crate) fn with_slice(self, slice: &'a [(K, V)]) -> Self {
        self.with_iter(SliceIter::new(slice))
    }
}

// 用来进行写pageRef的内容 使用 Encode来完成写入操作
struct SortedPageBuf<K, V> {
    offsets: Encoder,
    payload: Encoder,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> SortedPageBuf<K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{
    unsafe fn new(content: &mut [u8], num_items: usize) -> Self {
        let offsets_size = num_items * mem::size_of::<u32>();
        // 把整块内存 分为两块 [存储偏移量, 存储key + value]
        let (offsets, payload) = content.split_at_mut(offsets_size);
        Self {
            offsets: Encoder::new(offsets),
            payload: Encoder::new(payload),
            _marker: PhantomData,
        }
    }

    unsafe fn add(&mut self, key: K, value: V) {
        // 把整块内存 分为两块 [存储偏移量, 存储key + value]
        let offset = self.offsets.len() + self.payload.offset(); // 游标和buf头的偏移
        self.offsets.put_u32(offset as u32); // 将写入位置 放置在 索引区
        key.encode_to(&mut self.payload);
        value.encode_to(&mut self.payload);
    }
}

/// An immutable reference to a sorted page.
#[derive(Clone)]
pub(crate) struct SortedPageRef<'a, K, V> {
    page: PageRef<'a>,
    content: &'a [u8],
    offsets: &'a [u32],
    _marker: PhantomData<(K, V)>,
}

impl<'a, K, V> SortedPageRef<'a, K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{
    pub(crate) fn new(page: PageRef<'a>) -> Self {
        let content = page.content();
        let offsets = unsafe { // 索引位置
            let ptr = content.as_ptr() as *const u32;
            let len = if content.is_empty() {
                0
            } else {
                let size = u32::from_le(ptr.read());
                size as usize / mem::size_of::<u32>()
            };
            slice::from_raw_parts(ptr, len)
        };
        Self {
            page,
            content,
            offsets,
            _marker: PhantomData,
        }
    }

    /// Returns the number of items in the page.
    pub(crate) fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Returns the item at the given index.
    pub(crate) fn get(&self, index: usize) -> Option<(K, V)> {
        if let Some(item) = self.item(index) {
            let mut dec = Decoder::new(item);
            unsafe {
                let k = K::decode_from(&mut dec);
                let v = V::decode_from(&mut dec);
                Some((k, v))
            }
        } else {
            None
        }
    }

    /// 返回页面中目标的排名。如果找到该值，则返回 [`Result::Ok`]，其中包含匹配项的索引。
    /// 如果有多个匹配项，则可以返回任何一个匹配项。如果找不到该值，则返回 [`Result::Err`]，其中包含可以在保持排序顺序的同时插入匹配项的索引。
    pub(crate) fn rank<Q: ?Sized>(&self, target: &Q) -> Result<usize, usize>
        where
            K: Borrow<Q>,
            Q: Ord,
    {
        // 二分查找内容
        let mut left = 0;
        let mut right = self.len();
        while left < right {
            let mid = (left + right) / 2;
            let key = unsafe {
                let item = self.item(mid).unwrap();
                let mut dec = Decoder::new(item);
                K::decode_from(&mut dec)
            };
            match key.borrow().cmp(target) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(left)
    }

    /// Finds a separator to split the page into two halves.
    ///
    /// If a split separator is found, returns [`Option::Some`] with the split
    /// separator, an iterator over items before the separator, and another
    /// iterator over items at or after the separator.
    #[allow(clippy::type_complexity)]
    pub(crate) fn into_split_iter(
        self,
    ) -> Option<(
        K,
        SortedPageRangeIter<'a, K, V>,
        SortedPageRangeIter<'a, K, V>,
    )> {
        let len = self.len();
        if let Some((mid, _)) = self.get(len / 2) {
            let sep = mid.as_split_separator();
            let index = match self.rank(&sep) {
                Ok(i) => i,
                Err(i) => i,
            };
            if index > 0 {
                let left_iter = SortedPageRangeIter::new(self.clone(), 0..index);
                let right_iter = SortedPageRangeIter::new(self, index..len);
                return Some((sep, left_iter, right_iter));
            }
        }
        None
    }

    fn item(&self, index: usize) -> Option<&[u8]> {
        if let Some(offset) = self.item_offset(index) {
            let next_offset = self.item_offset(index + 1).unwrap_or(self.content.len());
            Some(&self.content[offset..next_offset])
        } else {
            None
        }
    }

    fn item_offset(&self, index: usize) -> Option<usize> {
        self.offsets.get(index).map(|v| u32::from_le(*v) as usize)
    }
}

impl<'a, K, V> Deref for SortedPageRef<'a, K, V> {
    type Target = PageRef<'a>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<'a, K, V, T> From<T> for SortedPageRef<'a, K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
        T: Into<PageRef<'a>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

#[derive(Clone)]
pub(crate) struct SortedPageIter<'a, K, V> {
    page: SortedPageRef<'a, K, V>,
    next: usize,
}

impl<'a, K, V> SortedPageIter<'a, K, V> {
    /// Creates a [`SortedPageIter`] over items in the given page.
    pub(crate) fn new(page: SortedPageRef<'a, K, V>) -> Self {
        Self { page, next: 0 }
    }
}

impl<'a, K, V, T> From<T> for SortedPageIter<'a, K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
        T: Into<SortedPageRef<'a, K, V>>,
{
    fn from(page: T) -> Self {
        Self::new(page.into())
    }
}

impl<'a, K, V> Iterator for SortedPageIter<'a, K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.page.get(self.next) {
            self.next += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl<'a, K, V> RewindableIterator for SortedPageIter<'a, K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{
    fn rewind(&mut self) {
        self.next = 0;
    }
}

impl<'a, V> SeekableIterator<Key<'_>> for SortedPageIter<'a, Key<'_>, V>
    where
        V: SortedPageValue,
{
    fn seek(&mut self, target: &Key<'_>) -> bool {
        match self.page.rank(target) {
            Ok(i) => {
                self.next = i;
                true
            }
            Err(i) => {
                self.next = i;
                false
            }
        }
    }
}

impl<'a, V> SeekableIterator<[u8]> for SortedPageIter<'a, &'a [u8], V>
    where
        V: SortedPageValue,
{
    fn seek(&mut self, target: &[u8]) -> bool {
        match self.page.rank(target) {
            Ok(i) => {
                self.next = i;
                true
            }
            Err(i) => {
                self.next = i;
                false
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct SortedPageRangeIter<'a, K, V> {
    page: SortedPageRef<'a, K, V>,
    range: Range<usize>,
    index: usize,
}

impl<'a, K, V> SortedPageRangeIter<'a, K, V> {
    /// Creates a [`SortedPageRangeIter`] over a range of items in the given
    /// page.
    pub(crate) fn new(page: SortedPageRef<'a, K, V>, range: Range<usize>) -> Self {
        let index = range.start;
        Self { page, range, index }
    }
}

impl<'a, K, V> Iterator for SortedPageRangeIter<'a, K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.range.end {
            if let Some(item) = self.page.get(self.index) {
                self.index += 1;
                return Some(item);
            }
        }
        None
    }
}

impl<'a, K, V> RewindableIterator for SortedPageRangeIter<'a, K, V>
    where
        K: SortedPageKey,
        V: SortedPageValue,
{
    fn rewind(&mut self) {
        self.index = self.range.start;
    }
}

/// Required methods for keys in a sorted page.
pub(crate) trait SortedPageKey: Codec + Clone + Ord {
    /// Returns the raw part of the key.
    fn as_raw(&self) -> &[u8];

    /// Returns a key that can be used as a split separator.
    fn as_split_separator(&self) -> Self;
}

/// Required methods for values in a sorted page.
pub(crate) trait SortedPageValue: Codec + Clone {}

impl<T> SortedPageValue for T where T: Codec + Clone {}

impl Codec for &[u8] {
    fn encode_size(&self) -> usize {
        mem::size_of::<u32>() + self.len()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u32(self.len() as u32);
        enc.put_slice(self);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let len = dec.get_u32() as usize;
        dec.get_slice(len)
    }
}

impl SortedPageKey for &[u8] {
    fn as_raw(&self) -> &[u8] {
        self
    }

    fn as_split_separator(&self) -> Self {
        self
    }
}

impl Codec for Key<'_> {
    fn encode_size(&self) -> usize {
        self.raw.encode_size() + mem::size_of::<u64>()
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        self.raw.encode_to(enc);
        enc.put_u64(self.lsn);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let raw = Codec::decode_from(dec);
        let lsn = dec.get_u64();
        Self::new(raw, lsn)
    }
}

impl SortedPageKey for Key<'_> {
    fn as_raw(&self) -> &[u8] {
        self.raw
    }

    fn as_split_separator(&self) -> Self {
        // Avoid splitting on the same raw key.
        Key::new(self.raw, u64::MAX)
    }
}

/// These values are persisted to disk, don't change them.
const VALUE_KIND_PUT: u8 = 0;
const VALUE_KIND_DELETE: u8 = 1;

impl Codec for Value<'_> {
    fn encode_size(&self) -> usize {
        1 + match self {
            Self::Put(v) => v.len(),
            Self::Delete => 0,
        }
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        match self {
            Value::Put(v) => {
                enc.put_u8(VALUE_KIND_PUT);
                enc.put_slice(v);
            }
            Value::Delete => enc.put_u8(VALUE_KIND_DELETE),
        }
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let kind = dec.get_u8();
        match kind {
            VALUE_KIND_PUT => Self::Put(dec.get_slice(dec.remaining())),
            VALUE_KIND_DELETE => Self::Delete,
            _ => unreachable!(),
        }
    }
}

impl Codec for Index {
    fn encode_size(&self) -> usize {
        mem::size_of::<u64>() * 2
    }

    unsafe fn encode_to(&self, enc: &mut Encoder) {
        enc.put_u64(self.id);
        enc.put_u64(self.epoch);
    }

    unsafe fn decode_from(dec: &mut Decoder) -> Self {
        let id = dec.get_u64();
        let epoch = dec.get_u64();
        Self::new(id, epoch)
    }
}