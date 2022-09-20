use tokio::sync::Semaphore;

use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::Mutex as aMutex;

use std::collections::HashSet;
use std::hash::Hash;

pub const RESERVE: u8 = 10;
pub const TRY_RESERVE: u8 = 20;
pub const ALLOCATE: u8 = 30;
pub const RELEASE: u8 = 40;

pub struct Block {
    mem: Arc<RustBuffer>,
    ptr: *mut u8,
    len: usize,
    child: bool,
}

unsafe impl Sync for Block {}
unsafe impl Send for Block {}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        unsafe {std::slice::from_raw_parts(self.ptr, self.len)}
    }
}

impl Block {
    pub fn as_ref_static(&self) -> &'static [u8] {
        unsafe {std::slice::from_raw_parts(self.ptr, self.len)}
    }

    pub fn trim(&mut self, size: usize) {
        self.len = match self.len.cmp(&size){
            Ordering::Less => unreachable!(),
            Ordering::Greater => size,
            Ordering::Equal => size
        };
    }

    #[allow(clippy::len_without_is_empty)] //is_empty() does not make that much sense here
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn chunk(self, off: usize, blocksz: usize) -> ChunkIter {
        if self.child {
            unreachable!();
        }
        ChunkIter::new(self, off, blocksz)
    }

    pub fn chunks(self, blocksz: usize) -> Vec<(usize, Block)> {
        let iter = self.chunk(0, blocksz);
        iter.collect()
    }
}

pub struct ChunkIter {
    block: Block,
    pos: usize,
    blocksz: usize,
}

impl ChunkIter {
    fn new(block: Block, pos: usize, blocksz: usize) -> Self {
        ChunkIter{block, pos, blocksz}
    }
}

impl Iterator for ChunkIter {
    type Item = (usize, Block);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.block.len() || self.blocksz == 0 {
            return None
        }

        let ptr = unsafe {self.block.mem.raw.ptr.add(self.pos)};
        let end = (self.pos+self.blocksz).min(self.block.len());
        let len = end-self.pos;

        let b = Block {
            mem: self.block.mem.clone(),
            ptr,
            len,
            child: true,
        };

        let off = self.pos;
        self.pos += self.blocksz;
        Some((off, b))
    }
}


#[derive(Clone)]
pub struct ItemsAllocator<T> {
    items: Arc<aMutex<HashSet<T>>>,
    guard: Arc<Semaphore>,
    limit: usize,
}

impl <T> ItemsAllocator<T> 
where T: 'static+PartialEq+Hash+Eq+Send+Clone {
    pub fn new(limit: usize) -> Self {
        ItemsAllocator {
            items: Arc::new(aMutex::new(HashSet::new())),
            guard: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    pub async fn reserve(&self, t: T) {
        if self.limit > 0 {
            let mut hs = self.items.lock().await;
            if !hs.insert(t) {
                return;
            }
            drop(hs);
            tracing::trace!(mem_op="item_reserve", permits=self.guard.available_permits());
            let permit = self.guard.acquire().await.unwrap();
            permit.forget();
        }
    }

    pub async fn release_back(&self, t: T, reason: &str) {
        let mut hs = self.items.lock().await;
        if !hs.remove(&t) {
            return;
        }
        drop(hs);
        tracing::trace!(mem_op="item_release", reason=reason, permits=self.guard.available_permits());
        self.guard.add_permits(1);
    }
}

type AllocCallback = Box<dyn Fn(u8, usize) + Sync + Send>;

struct AllocatorInner {
    guard: Arc<Semaphore>,
    callback: Option<AllocCallback>,
}

#[derive(Clone)]
pub struct Allocator {
    inner: Arc<AllocatorInner>,
    limit: usize,
}

impl Allocator {
    pub fn new(limit: usize) -> Self {
        Allocator::with_callback_opt(limit, None)
    }

    pub fn with_callback(limit: usize, callback: AllocCallback) -> Self {
        Self::with_callback_opt(limit, Some(callback))
    }

    fn with_callback_opt(limit: usize, callback: Option<AllocCallback>) -> Self {
        let limit = limit * 1024 * 1024;

        let inner = AllocatorInner {
            guard: Arc::new(Semaphore::new(limit)),
            callback,
        };

        Allocator {
            inner: Arc::new(inner),
            limit,
        }
    }

    pub async fn alloc_vec(&self, sz: usize) -> Vec<u8> {
        if self.limit > 0 {
            tracing::trace!(mem_op="alloc_vec", sz=sz, permits=self.inner.guard.available_permits());
            let permit = self.inner.guard.acquire_many(sz as u32).await.unwrap();
            permit.forget();
            self.callback(ALLOCATE, sz);
        }

        vec![0; sz as usize]
    }

    pub async fn alloc(&self, sz: usize) -> Buffer {
        let vec = self.alloc_vec(sz).await;

        Buffer::from_vec(vec)
    }

    pub async fn alloc_block(&self, sz: usize) -> Block {
        let buf = self.alloc(sz).await;
        self.to_block(buf)
    }

    pub fn empty_block(&self) -> Block {
        self.to_block(Buffer::default())
    }

    pub fn to_block(&self, ffibuf: Buffer) -> Block {
        let (ptr, len) = (ffibuf.ptr, ffibuf.len);

        let rsbuf = RustBuffer {
            raw: ffibuf,
            ator: self.clone(),
        };

        Block {
            mem: Arc::new(rsbuf),
            ptr,
            len,
            child: false,
        }
    }

    fn callback(&self, op: u8, sz: usize) {
        if let Some(f) = self.inner.callback.as_ref() {
            f(op, sz)
        }
    }

    pub fn try_reserve(&self, sz: usize) -> bool {
        let acquired = match self.inner.guard.try_acquire_many(sz as u32) {
            Ok(permit) => {
                permit.forget();
                self.callback(TRY_RESERVE, sz);
                true
            },
            Err(_) => {
                false
            }
        };

        tracing::trace!(mem_op="try_reserve", sz=sz, permits=self.inner.guard.available_permits(), status=acquired);
        acquired
    }

    pub async fn reserve(&self, sz: usize) {
        if self.limit > 0 {
            tracing::trace!(mem_op="reserve", sz=sz, permits=self.inner.guard.available_permits());
            let permit = self.inner.guard.acquire_many(sz as u32).await.unwrap();
            permit.forget();
            self.callback(RESERVE, sz);
        }        
    }

    pub fn release_back(&self, sz: usize, reason: &str) {
        if sz == 0 {
            return
        }
        tracing::trace!(mem_op="release", reason=reason, sz=sz, permits=self.inner.guard.available_permits());
        self.inner.guard.add_permits(sz);
        self.callback(RELEASE, sz);
    }

    pub fn free(&self, buf: Buffer) {
        let vec = buf.to_vec();

        self.free_vec(vec);
    }

    pub fn free_vec(&self, vec: Vec<u8>) {
        let permits = vec.capacity();

        if self.limit > 0 {
            tracing::trace!(mem_op="free_vec", sz=permits, permits=self.inner.guard.available_permits());
            self.inner.guard.add_permits(permits);
            self.callback(RELEASE, permits);
        }
    }
}

struct RustBuffer {
    raw: Buffer,
    ator: Allocator,
}

impl Drop for RustBuffer {
    fn drop(&mut self) {
        if !self.raw.ptr.is_null() {
            let v = self.raw.to_vec();
            drop(v);

            self.ator.release_back(self.raw.cap, "rsbuf dealloc");
            self.raw.nullify();
        }
    }
}

#[repr(C)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

unsafe impl Sync for Buffer {}
unsafe impl Send for Buffer {}

impl Default for Buffer {
    fn default() -> Self {
        Buffer {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

impl Buffer {
    pub fn from_vec(mut v: Vec<u8>) -> Buffer {
        let cap = v.capacity();
        if cap == 0 {
            Default::default()
        } else {
            let len = v.len();
            let ptr = v.as_mut_ptr();

            std::mem::forget(v);
            Buffer {
                ptr,
                len,
                cap,
            }
        }
    }

    pub fn from_string(s: String) -> Buffer {
        let v = s.into_bytes();
        Buffer::from_vec(v)
    }

    pub fn alloc(sz: usize) -> Buffer {
        let v = vec![0u8; sz];
        Buffer::from_vec(v)
    }

    pub fn free(buf: Buffer) {
        let _ = buf.to_vec();
    }

    pub fn to_vec(&self) -> Vec<u8> {
        if self.cap == 0 {
            Vec::new()
        } else {
            unsafe{
                Vec::from_raw_parts(self.ptr, self.len, self.cap)
            }
        }
    }

    fn nullify(&mut self) {
        self.ptr = std::ptr::null_mut();
        self.len = 0;
        self.cap = 0;
    }
}

#[derive(Clone)]
pub struct GoBuffer {
    pub ptr: *mut u8,
    pub len: usize,
}

unsafe impl Sync for GoBuffer {}
unsafe impl Send for GoBuffer {}

impl Default for GoBuffer {
    fn default() -> Self {
        GoBuffer {
            ptr: std::ptr::null_mut(),
            len: 0,
        }
    }
}

impl GoBuffer {
    pub fn from_ptr(ptr: *mut u8, len: usize) -> GoBuffer {
        GoBuffer { ptr, len }
    }

    pub unsafe fn incr_cpy(&self, src: *const u8, count: usize) -> GoBuffer {
        self.ptr.copy_from(src, count);
        let ptr = self.ptr.add(count);
        GoBuffer { ptr , len: self.len - count }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use super::*;
    use tokio::runtime::Runtime;
    use tracing::{self, Level};

    const MB1: usize = 1024*1024;

    #[derive(Default)]
    struct AllocStats {
        try_reserve: AtomicUsize,
        reserve: AtomicUsize,
        alloc: AtomicUsize,
        release: AtomicUsize,
    }

    fn create_callback_obj() -> (Arc<AllocStats>, AllocCallback) {
        let alloc_stats = Arc::new(AllocStats::default());

        let stats = alloc_stats.clone();
        let callback = move |op, sz| {
            match op {
                TRY_RESERVE => {stats.try_reserve.fetch_add(sz, Ordering::SeqCst);},
                RESERVE => {stats.reserve.fetch_add(sz, Ordering::SeqCst);},
                ALLOCATE => {stats.alloc.fetch_add(sz, Ordering::SeqCst);},
                RELEASE => {stats.release.fetch_add(sz, Ordering::SeqCst);},
                _ => {},
            };
        };

        (alloc_stats, Box::new(callback))
    }


    #[test]
    fn mem_try_reserve() {
        let (stats, callback) = create_callback_obj();

        let ator = Allocator::with_callback(1, callback);

        assert!(ator.try_reserve(100));
        assert!(!ator.try_reserve(MB1));

        assert_eq!(100, stats.try_reserve.load(Ordering::SeqCst));
    }

    #[test]
    fn mem_alloc_vec() {
        let rt = Arc::new(Runtime::new().unwrap());
        let (stats, callback) = create_callback_obj();
        let ator = Allocator::with_callback(1, callback);

        {
            let _ = rt.block_on(ator.alloc_vec(MB1));

            assert!(!ator.try_reserve(1024*1024));
            assert_eq!(MB1, stats.alloc.load(Ordering::SeqCst));
            assert_eq!(0, stats.try_reserve.load(Ordering::SeqCst));
        }

        ator.release_back(MB1 as usize, "test");
        assert!(ator.try_reserve(MB1));
        assert_eq!(MB1, stats.release.load(Ordering::SeqCst));
    }

    #[test]
    fn mem_alloc_block() {
        let rt = Arc::new(Runtime::new().unwrap());
        let (stats, callback) = create_callback_obj();
        let ator = Allocator::with_callback(1, callback);

        {
            let _block = rt.block_on(ator.alloc_block(MB1));

            assert!(!ator.try_reserve(1024*1024));
            assert_eq!(MB1, stats.alloc.load(Ordering::SeqCst));
            assert_eq!(0, stats.try_reserve.load(Ordering::SeqCst));
        }

        assert!(ator.try_reserve(MB1));
        assert_eq!(MB1, stats.release.load(Ordering::SeqCst));
    }

    #[test]
    fn mem_block_active_ref() {
        let rt = Arc::new(Runtime::new().unwrap());
        let (stats, callback) = create_callback_obj();
        let ator = Allocator::with_callback(1, callback);

        let block = rt.block_on(ator.alloc_block(MB1));
        {
            let chunk_blocks = block.chunks(128);
            assert_eq!(chunk_blocks.len(), MB1/128);
            assert_eq!(chunk_blocks[0].1.len(), 128);

            assert!(!ator.try_reserve(MB1));
            assert_eq!(MB1, stats.alloc.load(Ordering::SeqCst));
        }

        // No mem should be released
        assert_eq!(MB1, stats.release.load(Ordering::SeqCst));

        assert!(ator.try_reserve(MB1));
        assert_eq!(MB1, stats.release.load(Ordering::SeqCst));
        assert_eq!(MB1, stats.try_reserve.load(Ordering::SeqCst));
    }

    #[test]
    fn mem_block_chunks() {
        let ator = Allocator::new(1);

        let s = b"1234567890";
    
        let expected_1 = &["1","2","3","4","5","6","7","8","9","0"][..];
        let expected_2 = &["12", "34", "56", "78", "90"][..];
        let expected_3 = &["123", "456", "789", "0"][..];
        let expected_10 = &["1234567890"][..];
        let expected_15 = &["1234567890"][..];

        let expected = [(1,expected_1),(2,expected_2), (3, expected_3), (10, expected_10), (15, expected_15)];

        for (blocksz, exp) in expected.iter() {
            let vec = Vec::from(&s[..]);
            let buf = Buffer::from_vec(vec);
            let block = ator.to_block(buf);

            let mut i = 0;
            for (_, b) in block.chunks(*blocksz) {
                assert_eq!(exp[i], std::str::from_utf8(b.as_ref()).unwrap());
                i += 1;
            }
            assert_eq!(exp.len(), i);
        }
    }

    #[test]
    fn buffer_simple() {
        let v = vec![0; 1024];
        let buf = Buffer::from_vec(v);
        assert_eq!(buf.len, 1024);
        assert!(buf.len <= buf.cap);
        assert_ne!(buf.ptr, std::ptr::null_mut());
        Buffer::free(buf);
    }

    #[test]
    fn buffer_empty() {
        let v: Vec<u8> = Vec::new();
        let buf = Buffer::from_vec(v);
        assert_eq!(buf.len, 0);
        assert_eq!(buf.cap, 0);
        assert_eq!(buf.ptr, std::ptr::null_mut());
        Buffer::free(buf);
    }

    #[test]
    fn buffer_alloc() {
        let buf = Buffer::from_string("0123456789abcdef".to_string());
        let alloc = Allocator::new(20*1024*1024);
        let block = alloc.to_block(buf);
        for (count, (_, chunk)) in block.chunk(0, 4).enumerate() {
            if count == 0 {
                assert_eq!(chunk.as_ref_static(), "0123".as_bytes());
            } else if count == 1{
                assert_eq!(chunk.as_ref_static(), "4567".as_bytes());
            }
        }
    }

    #[test]
    fn gobuffer_simple() {
        let mut v = vec![0_u8; 1024];
        let buf = GoBuffer::from_ptr(v.as_mut_ptr(), 1024);
        let v1 = vec![1_u8; 512];
        let v2 = vec![2_u8; 512];
        unsafe {
            let buf1 = buf.incr_cpy(v1.as_ptr(), 512);
            let _ = buf1.incr_cpy(v2.as_ptr(), 512);
        }
        assert_eq!(v, [&v1[..], &v2[..]].concat());
    }

}
