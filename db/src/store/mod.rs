mod manifest;
mod meta;
mod page_store;

/// Options to configure a page store.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct Options {
    /// The capacity of the write buffer. It should be power of two.
    ///
    /// Default: 128MB
    pub write_buffer_capacity: u32,

    /// The maxmum number of write buffers.
    ///
    /// If there exists too many write buffers, writing will be stalled until at
    /// leaset one write buffer is flushed.
    ///
    /// Default: 8
    pub max_write_buffers: usize,

    /// If true, use O_DIRECT to read/write page files.
    ///
    /// Default: false
    pub use_direct_io: bool,

    /// If true, no space reclamation.
    ///
    /// Default: false
    pub disable_space_reclaiming: bool,

    /// The max percentage of the space amplification.
    ///
    /// The space amplification is defined as the amount (in percentage) of
    /// additional storage needed to store a single byte of data in the
    /// database.
    ///
    /// Default: 100
    pub max_space_amplification_percent: usize,

    /// The high watermark of the used storage space of the database.
    ///
    /// Default: u64::MAX
    pub space_used_high: u64,

    /// Target file size for compaction.
    ///
    /// Default: 64MB
    pub file_base_size: usize,

    /// The capacity of the page read cache in bytes.
    ///
    /// Default: 8 Mib
    pub cache_capacity: usize,

    /// The estimated average `charge` associated with cache entries.
    ///
    /// Default: 8 Kib
    ///
    /// This is a critical configuration parameter for good performance for page
    /// read cache, because having a table size that is fixed at creation
    /// time greatly reduces the required synchronization between threads.
    ///
    /// - If the estimate is substantially too low (e.g. less than half the true
    ///   average) then metadata space overhead with be substantially higher
    ///   (e.g. 200 bytes per entry rather than 100). This can slightly reduce
    ///   cache hit rates, and slightly reduce access times due to the larger
    ///   working memory size.
    /// - If the estimate is substantially too high (e.g. 25% higher than the
    ///   true average) then there might not be sufficient slots in the hash
    ///   table for both efficient operation and capacity utilization (hit
    ///   rate). The cache will evict entries to prevent load factors that could
    ///   dramatically affect lookup times, instead letting the hit rate suffer
    ///   by not utilizing the full capacity.
    pub cache_estimated_entry_charge: usize,

    /// The capacity of file_reader cache.
    ///
    /// Default: 5000 file_readers.
    pub cache_file_reader_capacity: u64,

    /// Whether report error when there is no enough memory for the page cache.
    ///
    /// Default: false
    pub cache_strict_capacity_limit: bool,

    /// Insert warm pages into PageCache during flush if true.
    ///
    /// Default: true
    pub prepopulate_cache_on_flush: bool,

    /// Compression method during flush new file.
    /// include hot rewrite.
    ///
    /// Default: Snappy.
    // pub compression_on_flush: Compression,
    //
    // /// Compression method during compact cold file.
    // ///
    // /// Default: Zstd(Level3).
    // pub compression_on_cold_compact: Compression,
    //
    // /// ChecksumType for each page.
    // ///
    // /// Default: NONE.
    // pub page_checksum_type: ChecksumType,

    /// PhotonDB will flush all write buffers on DB close, if there are
    /// unpersisted data. The flush can be skip to speed up DB close, but
    /// unpersisted data WILL BE LOST.
    ///
    /// Default: false
    pub avoid_flush_during_shutdown: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            write_buffer_capacity: 128 << 20,
            max_write_buffers: 8,
            use_direct_io: false,
            disable_space_reclaiming: false,
            max_space_amplification_percent: 100,
            space_used_high: u64::MAX,
            file_base_size: 64 << 20,
            cache_capacity: 8 << 20,
            cache_estimated_entry_charge: 8 << 10,
            cache_file_reader_capacity: 5000,
            cache_strict_capacity_limit: false,
            prepopulate_cache_on_flush: true,
            // compression_on_flush: Compression::SNAPPY,
            // compression_on_cold_compact: Compression::ZSTD,
            // page_checksum_type: ChecksumType::NONE,
            avoid_flush_during_shutdown: false,
        }
    }
}

/// Options that control manual flush operations.
#[derive(Clone, Debug)]
pub struct FlushOptions {
    /// If true, the flush will wait until the flush is done.
    ///
    /// Default: true
    wait: bool,

    /// If true, then flush will start processing regardless of whether there is
    /// a write stall during the flush process.
    ///
    /// Default: false
    allow_write_stall: bool,
}

impl Default for FlushOptions {
    fn default() -> Self {
        FlushOptions {
            wait: true,
            allow_write_stall: false,
        }
    }
}

// pub(crate) struct PageStore {
//     options: Options,
//     table: PageTable,
//
//     version_owner: Arc<VersionOwner>,
//     page_files: Arc<PageFiles<E>>,
//     manifest: Arc<Mutex<Manifest<E>>>,
//
//     job_stats: Arc<AtomicJobStats>,
//     writebuf_stats: Arc<AtomicWritebufStats>,
//
//     jobs: Vec<E::JoinHandle<()>>,
//     shutdown: ShutdownNotifier,
// }