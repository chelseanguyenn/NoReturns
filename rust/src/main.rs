//! Shopping Sessions pipeline in Rust, implementing ../rules.md.
//!
//! Reads  $DATA_DIR    (default /data)
//! Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
//!
//! Stages:
//!  1. mmap every CSV, cut the bodies (header skipped) into ~2 MB chunks at '\n'
//!     boundaries, and parse the chunks on NCPU threads (default 8) with a
//!     hand-written parser for the rules' dialect. Each thread interns the user
//!     IDs it sees into thread-local integers.
//!  2. Merge the thread-local IDs into one sorted list: the global integer of an
//!     ID is its byte-wise rank, so the smallest integer in a set is its name.
//!  3. Array-based union-find over the links (root = smallest rank).
//!  4. Dedup events on event_id (hash-sharded, exact byte compare), keep the
//!     copy with the smallest ingest_ts, and keep that only if it is valid.
//!  5. Sort events by (person, ts) and clicks by (person, ts, click_id); scan
//!     each person's events into sessions and user-days, credit each session by
//!     a binary search over the person's clicks, and count assists from merged
//!     per-campaign click intervals.
//!  6. Write Parquet (parquet crate, low-level column writers).
//!
//! Click dedup is skipped on purpose: copies of a click have identical values
//! (rules.md), so a duplicate can change neither the credited click (same ts,
//! same click_id, same campaign) nor the set of assisting campaigns.

use memchr::{memchr, memchr_iter};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

type S = &'static [u8];
type Hs = ahash::RandomState;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NSHARD: usize = 256;
const CHUNK: usize = 2 << 20;

fn hasher() -> Hs {
    ahash::RandomState::with_seeds(0x243f6a88, 0x85a308d3, 0x13198a2e, 0x03707344)
}

// ------------------------------------------------------------------ timing
struct Timer {
    t0: Instant,
    last: Instant,
}
impl Timer {
    fn new() -> Self {
        let now = Instant::now();
        Timer { t0: now, last: now }
    }
    fn stage(&mut self, name: &str) {
        let now = Instant::now();
        eprintln!(
            "[timing] {:<26} {:7.3}s   total {:7.3}s",
            name,
            (now - self.last).as_secs_f64(),
            (now - self.t0).as_secs_f64()
        );
        self.last = now;
    }
}

// ------------------------------------------------------------------ timestamps
#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 { Some(a as i64 * 10 + c as i64) } else { None }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's algorithm).
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 }
        }
    }
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ` or F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, to epoch ms.
#[inline]
fn parse_iso(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n != 20 && n != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    // Year 0000 is not a calendar year (Python's datetime rejects it too).
    if y == 0 || !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let ms = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000;
    if n == 20 {
        return if b[19] == b'Z' { Some(ms) } else { None };
    }
    let sign = b[19];
    if (sign != b'+' && sign != b'-') || b[22] != b':' {
        return None;
    }
    let oh = d2(b, 20)?;
    let om = d2(b, 23)?;
    if oh > 23 || om > 59 {
        return None;
    }
    let off = (oh * 60 + om) * 60_000;
    Some(if sign == b'+' { ms - off } else { ms + off })
}

/// ASCII digits only, any length: the value, saturated to i64::MAX (outside every window).
#[inline]
fn parse_digits(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig <= 19 {
                v = v * 10 + x as u64;
            }
        }
    }
    if sig > 19 || v > i64::MAX as u64 { Some(i64::MAX) } else { Some(v as i64) }
}

/// Reference parser (the tests check TsParser against it).
#[cfg_attr(not(test), allow(dead_code))]
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.len() >= 5 && b[4] == b'-' { parse_iso(b) } else { parse_digits(b) }
}

/// Eight ASCII digits (b[0] most significant) to their value, or None (SWAR, exact).
#[inline(always)]
fn parse8(b: &[u8]) -> Option<u64> {
    let w = u64::from_le_bytes(b[..8].try_into().unwrap());
    // byte in 0x30..=0x3F, and byte + 6 still in 0x30..=0x3F: exactly '0'..='9'
    if w & 0xF0F0_F0F0_F0F0_F0F0 != 0x3030_3030_3030_3030
        || w.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0 != 0x3030_3030_3030_3030
    {
        return None;
    }
    let mut v = w - 0x3030_3030_3030_3030;
    v = v.wrapping_mul(10).wrapping_add(v >> 8);
    v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32;
    Some(v)
}

/// Per-thread timestamp parser: same results as parse_ts / parse_iso, with
/// fast paths for the common forms. F1/F2 cache the day number of each
/// validated `YYYY-MM-DD` (a window spans ~21 dates); F3 of exactly 13
/// digits is parsed 8 digits at a time.
struct TsParser {
    date_key: [u64; 128], // bytes 0..8 of the date ("YYYY-MM-"), 0 = empty
    date_days: [i64; 128],
}

impl TsParser {
    fn new() -> Self {
        TsParser { date_key: [0; 128], date_days: [0; 128] }
    }

    #[inline]
    fn iso(&mut self, b: &[u8]) -> Option<i64> {
        let n = b.len();
        if n != 20 && n != 25 {
            return None;
        }
        if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let dd = d2(b, 8)?; // 0..=99
        let key = u64::from_le_bytes(b[..8].try_into().unwrap());
        let days = if self.date_key[dd as usize] == key {
            self.date_days[dd as usize]
        } else {
            let y = d2(b, 0)? * 100 + d2(b, 2)?;
            let mo = d2(b, 5)?;
            if y == 0 || !(1..=12).contains(&mo) || dd < 1 || dd > days_in_month(y, mo) {
                return None;
            }
            let days = days_from_civil(y, mo, dd);
            self.date_key[dd as usize] = key;
            self.date_days[dd as usize] = days;
            days
        };
        let h = d2(b, 11)?;
        let mi = d2(b, 14)?;
        let s = d2(b, 17)?;
        if h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let ms = ((days * 24 + h) * 60 + mi) * 60_000 + s * 1000;
        if n == 20 {
            return if b[19] == b'Z' { Some(ms) } else { None };
        }
        let sign = b[19];
        if (sign != b'+' && sign != b'-') || b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        Some(if sign == b'+' { ms - off } else { ms + off })
    }

    #[inline]
    fn ts(&mut self, b: &[u8]) -> Option<i64> {
        if b.len() == 13 {
            // 13 digits: digits 0..8 and 5..13 (overlapping) cover them all
            let hi = parse8(&b[0..8]);
            let lo = parse8(&b[5..13]);
            // not all digits: not F3, and too short for F1/F2
            return match (hi, lo) {
                (Some(h), Some(l)) => Some((h * 100_000 + l % 100_000) as i64),
                _ => None,
            };
        }
        if b.len() >= 5 && b[4] == b'-' { self.iso(b) } else { parse_digits(b) }
    }
}

#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    match parse_digits(b) {
        Some(v) if (1..=10_000_000).contains(&v) => Some(v as u32),
        _ => None,
    }
}

// ------------------------------------------------------------------ CSV dialect
/// Split one line (without its '\n') into exactly N fields, unquoting. False if malformed.
#[inline]
fn split_fields<const N: usize>(line: S, out: &mut [S; N]) -> bool {
    let mut start = 0;
    let mut k = 0;
    for pos in memchr_iter(b',', line) {
        if k == N - 1 {
            return false;
        }
        out[k] = &line[start..pos];
        k += 1;
        start = pos + 1;
    }
    if k != N - 1 {
        return false;
    }
    out[k] = &line[start..];
    if memchr(b'"', line).is_some() {
        for f in out.iter_mut() {
            if memchr(b'"', f).is_some() {
                let n = f.len();
                if n < 2 || f[0] != b'"' || f[n - 1] != b'"' || memchr(b'"', &f[1..n - 1]).is_some() {
                    return false;
                }
                *f = &f[1..n - 1];
            }
        }
    }
    true
}

/// Calls f on every non-empty line; only '\n' ends a line. (An empty line is
/// malformed for every table, so skipping it is the same as dropping it.)
#[inline]
fn for_lines(c: S, mut f: impl FnMut(S)) {
    let n = c.len();
    let mut p = 0;
    while p < n {
        let e = memchr(b'\n', &c[p..]).map_or(n, |i| p + i);
        if e > p {
            f(&c[p..e]);
        }
        p = e + 1;
    }
}

const fn splat(b: u8) -> u64 {
    b as u64 * 0x0101_0101_0101_0101
}

/// High bit set in every byte of v that is zero (exact, no false positives).
#[inline(always)]
fn zero_bytes(v: u64) -> u64 {
    let t = (v & 0x7f7f_7f7f_7f7f_7f7f).wrapping_add(0x7f7f_7f7f_7f7f_7f7f);
    !(t | v | 0x7f7f_7f7f_7f7f_7f7f)
}

/// The quote rule, given the line's quote count q > 0: every field that is at
/// least 2 bytes and starts and ends with '"' is enclosed and loses its quotes;
/// the line is well-formed iff those enclosing quotes are all its quotes
/// (q == 2 * enclosed). Any other quote anywhere makes q larger.
#[inline(always)]
fn unquote_counted<const N: usize>(f: &mut [S; N], q: usize) -> bool {
    let mut enclosed = 0;
    for x in f.iter_mut() {
        let n = x.len();
        if n >= 2 && x[0] == b'"' && x[n - 1] == b'"' {
            *x = &x[1..n - 1];
            enclosed += 1;
        }
    }
    q == 2 * enclosed
}

/// One SWAR pass (8 bytes at a time) over a chunk of whole lines, finding
/// ',', '\n' and '"' together: calls f with the N unquoted fields of every
/// well-formed line. Same dialect as split_fields: only '\n' ends a line,
/// every comma separates fields, and a wrong field count, a quote anywhere
/// except around a whole field, or an empty line drops the line.
#[inline]
fn for_records<const N: usize>(c: S, mut f: impl FnMut(&[S; N])) {
    let mut fields: [S; N] = [&[]; N];
    let (mut commas, mut fstart, mut q, mut lstart) = (0usize, 0usize, 0usize, 0usize);
    let n = c.len();
    macro_rules! on {
        ($pos:expr) => {{
            let pos = $pos;
            match c[pos] {
                b',' => {
                    if commas < N - 1 {
                        fields[commas] = &c[fstart..pos];
                    }
                    commas += 1;
                    fstart = pos + 1;
                }
                b'"' => q += 1,
                _ => {
                    if commas == N - 1 {
                        fields[N - 1] = &c[fstart..pos];
                        if q == 0 || unquote_counted(&mut fields, q) {
                            f(&fields);
                        }
                    }
                    commas = 0;
                    q = 0;
                    fstart = pos + 1;
                    lstart = pos + 1;
                }
            }
        }};
    }
    let mut i = 0;
    while i + 8 <= n {
        let w = u64::from_le_bytes(c[i..i + 8].try_into().unwrap());
        let mut m = zero_bytes(w ^ splat(b',')) | zero_bytes(w ^ splat(b'\n')) | zero_bytes(w ^ splat(b'"'));
        while m != 0 {
            on!(i + (m.trailing_zeros() / 8) as usize);
            m &= m - 1;
        }
        i += 8;
    }
    while i < n {
        if matches!(c[i], b',' | b'\n' | b'"') {
            on!(i);
        }
        i += 1;
    }
    // a last line without '\n' (lstart == n: there is none)
    if lstart < n && commas == N - 1 {
        fields[N - 1] = &c[fstart..];
        if q == 0 || unquote_counted(&mut fields, q) {
            f(&fields);
        }
    }
}

// ------------------------------------------------------------------ files
fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                name.ends_with(".csv") && !name.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> S {
    let f = File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    // Linux: MAP_POPULATE reads the whole file in at map time (the files are
    // mapped in parallel), so the parse never stops on a page fault. On the
    // Linux test bed this made cold-cache runs as fast as warm ones.
    #[cfg(target_os = "linux")]
    let m = unsafe { memmap2::MmapOptions::new().populate().map(&f) };
    #[cfg(not(target_os = "linux"))]
    let m = unsafe { memmap2::Mmap::map(&f) };
    let m = m.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

/// Everything after the header line.
fn body(data: S) -> S {
    match memchr(b'\n', data) {
        Some(i) => &data[i + 1..],
        None => &[],
    }
}

/// A chunk is a plain byte range [a, b) of a file body, cut without reading
/// the data; it owns the lines that *start* inside it (see lines_of).
#[derive(Clone, Copy)]
struct Chunk {
    kind: Kind,
    body: S,
    a: usize,
    b: usize,
}

fn cut_chunks(body: S, kind: Kind, out: &mut Vec<Chunk>) {
    let n = body.len();
    let mut a = 0;
    while a < n {
        let b = (a + CHUNK).min(n);
        out.push(Chunk { kind, body, a, b });
        a = b;
    }
}

/// The whole lines starting in [c.a, c.b): from the first line start >= a to
/// the first line start >= b (line starts are 0 and every position after a
/// '\n'). Consecutive chunks therefore partition the body's lines exactly.
fn lines_of(c: &Chunk) -> S {
    let (body, n) = (c.body, c.body.len());
    let start_at = |x: usize| if x == 0 { 0 } else if x >= n { n } else { memchr(b'\n', &body[x - 1..]).map_or(n, |i| x + i) };
    let (s, e) = (start_at(c.a), start_at(c.b));
    if s < e { &body[s..e] } else { &[] }
}

// ------------------------------------------------------------------ parse stage
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Ev,
    Ck,
    Lk,
}

/// One preallocated row array per event_id shard, shared by the parse
/// threads: each reserves blocks of BLK slots with one atomic add and writes
/// its rows there once. Unused slots of a thread's last blocks are written as
/// holes (all zero; a real row has hi != 0). Rows that do not fit go to the
/// thread's overflow vector for that shard.
struct RowBuf {
    ptr: *mut EvRow,
    cap: usize,
    next: AtomicUsize,
}
unsafe impl Sync for RowBuf {}
unsafe impl Send for RowBuf {}
const BLK: usize = 128;
const HOLE: EvRow = EvRow { lo: 0, hi: 0, ing: 0, ts: 0, user: 0, price: 0, cat: 0, valid: false, tid: 0 };

impl RowBuf {
    fn new(cap: usize) -> Self {
        let mut v: Vec<EvRow> = Vec::with_capacity(cap); // untouched pages cost nothing
        let ptr = v.as_mut_ptr();
        std::mem::forget(v);
        RowBuf { ptr, cap, next: AtomicUsize::new(0) }
    }
    /// The slots written so far (rows and holes). Call only after every writer is done.
    fn filled(&self) -> &mut [EvRow] {
        let n = self.next.load(Ordering::Acquire).min(self.cap);
        unsafe { std::slice::from_raw_parts_mut(self.ptr, n) }
    }
}

#[derive(Clone, Copy)]
struct EvRow {
    lo: u64, // event_id packed exactly (see pack); over 15 bytes: Out::ev_long
    hi: u64,
    ing: i64,
    ts: u32, // ts - window_start (valid rows only)
    user: u32,
    price: u32,
    cat: u16,
    valid: bool,
    tid: u8,
}

#[derive(Clone, Copy)]
struct CkRow {
    cid: S,
    ts: i64,
    user: u32,
    camp: u32,
}

struct Out {
    ev: Vec<Vec<EvRow>>, // per shard: rows that did not fit in its RowBuf
    ev_long: Vec<(S, EvRow)>, // event_ids over 15 bytes (exact, slow path)
    ck: Vec<CkRow>,
    lk: Vec<(u32, u32)>,
    new_keys: Vec<(u32, S)>, // global ids this thread created
    keys: Vec<S>,            // thread-local (fallback) ids
}

struct Ctx<'a> {
    ws: i64,
    we: i64,
    cws: i64,
    products: &'a HashMap<S, u16, Hs>,
    unknown: u16,
    campaigns: &'a HashMap<S, u32, Hs>,
}

/// Packs an ID of at most 15 bytes exactly: the bytes, then the length in the
/// top byte. Equal packs mean equal IDs, and a real pack is never (_, 0).
#[inline]
fn pack(s: &[u8]) -> (u64, u64) {
    let mut b = [0u8; 16];
    b[..s.len()].copy_from_slice(s);
    b[15] = s.len() as u8;
    (u64::from_le_bytes(b[..8].try_into().unwrap()), u64::from_le_bytes(b[8..].try_into().unwrap()))
}

/// 64-bit mix of a packed ID (low bits index tables, high bits pick partitions).
#[inline]
fn mix(lo: u64, hi: u64) -> u64 {
    let h = (lo ^ hi.wrapping_mul(0x9E37_79B9_7F4A_7C15)).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^ (h >> 29)
}

#[repr(C)]
struct ASlot {
    hi: std::sync::atomic::AtomicU64, // 0 = empty, BUSY = being written
    lo: std::sync::atomic::AtomicU64,
    id: std::sync::atomic::AtomicU64,
}
const BUSY: u64 = u64::MAX; // never a real pack: its length byte is at most 15

/// One lock-free interning table shared by all parse threads: packed short
/// IDs -> global ids, linear probing, slots claimed by CAS on `hi`. Fixed
/// capacity: once `limit` ids exist it refuses new ones (None) and callers
/// fall back to a thread-local map, so any number of IDs stays correct.
struct SharedTab {
    slots: Box<[ASlot]>,
    mask: usize,
    count: AtomicUsize,
    limit: usize,
}

impl SharedTab {
    fn new(cap_pow2: usize) -> Self {
        // Zeroed memory is a valid table of empty atomics, and the OS hands it
        // out lazily, so an oversized table costs only the pages it touches.
        let layout = std::alloc::Layout::array::<ASlot>(cap_pow2).unwrap();
        let slots = unsafe {
            let p = std::alloc::alloc_zeroed(layout) as *mut ASlot;
            assert!(!p.is_null(), "out of memory");
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(p, cap_pow2))
        };
        SharedTab { slots, mask: cap_pow2 - 1, count: AtomicUsize::new(0), limit: cap_pow2 / 2 }
    }

    /// The id of (lo, hi), inserting it if new (then calls on_new with the id).
    /// None if it is new and the table is at its limit.
    #[inline]
    fn get_or_insert(&self, lo: u64, hi: u64, on_new: impl FnOnce(u32)) -> Option<u32> {
        let mut i = (mix(lo, hi) as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            let mut h = s.hi.load(Ordering::Acquire);
            if h == 0 {
                if self.count.load(Ordering::Relaxed) >= self.limit {
                    return None;
                }
                match s.hi.compare_exchange(0, BUSY, Ordering::Acquire, Ordering::Acquire) {
                    Ok(_) => {
                        let id = self.count.fetch_add(1, Ordering::Relaxed) as u32;
                        s.lo.store(lo, Ordering::Relaxed);
                        s.id.store(id as u64, Ordering::Relaxed);
                        s.hi.store(hi, Ordering::Release); // publishes lo and id
                        on_new(id);
                        return Some(id);
                    }
                    Err(cur) => h = cur,
                }
            }
            while h == BUSY {
                std::hint::spin_loop();
                h = s.hi.load(Ordering::Acquire);
            }
            if h == hi && s.lo.load(Ordering::Relaxed) == lo {
                return Some(s.id.load(Ordering::Relaxed) as u32);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Read-only lookup, for after the parse.
    fn find(&self, lo: u64, hi: u64) -> Option<u32> {
        let mut i = (mix(lo, hi) as usize) & self.mask;
        loop {
            let s = &self.slots[i];
            let h = s.hi.load(Ordering::Acquire);
            if h == 0 {
                return None;
            }
            if h == hi && s.lo.load(Ordering::Relaxed) == lo {
                return Some(s.id.load(Ordering::Relaxed) as u32);
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Marks a thread-local id (an ID the shared table did not take).
const LOCAL: u32 = 1 << 31;

struct Worker<'a> {
    ctx: &'a Ctx<'a>,
    tid: u8,
    tsp: TsParser,
    shared: &'a SharedTab,
    map: HashMap<S, u32, Hs>, // fallback: long IDs, or the shared table at its limit
    out: Out,
    pend: Vec<Pend>, // user IDs waiting to be interned, in batches
    bufs: &'a [RowBuf],
    cur: Vec<(usize, usize)>, // per shard: this thread's current block [next, end)
}

/// A deferred user-ID lookup and the row field it fills.
#[derive(Clone, Copy)]
struct Pend {
    lo: u64,
    hi: u64,
    key: S,
    at: u32, // EV_AT | shard, or CK_AT
    ix: u32, // row index in that vector
}
const EV_AT: u32 = 1 << 31;
const CK_AT: u32 = 0;
const BATCH: usize = 32;

impl<'a> Worker<'a> {
    #[inline]
    /// A global id, or LOCAL | a thread-local id (resolved after the parse).
    fn intern(&mut self, k: S) -> u32 {
        if k.len() <= 15 {
            let (lo, hi) = pack(k);
            let new_keys = &mut self.out.new_keys;
            if let Some(g) = self.shared.get_or_insert(lo, hi, |g| new_keys.push((g, k))) {
                return g;
            }
        }
        self.intern_local(k)
    }

    /// The fallback path of intern (long IDs, or the shared table full).
    fn intern_local(&mut self, k: S) -> u32 {
        let keys = &mut self.out.keys;
        LOCAL
            | *self.map.entry(k).or_insert_with(|| {
                keys.push(k);
                (keys.len() - 1) as u32
            })
    }

    /// Resolves the pending lookups back to back: they are independent, so
    /// their cache misses overlap instead of stalling each row in turn.
    #[inline(never)]
    fn flush(&mut self) {
        let pend = std::mem::take(&mut self.pend);
        for p in &pend {
            let new_keys = &mut self.out.new_keys;
            let key = p.key;
            let u = match self.shared.get_or_insert(p.lo, p.hi, |g| new_keys.push((g, key))) {
                Some(g) => g,
                None => self.intern_local(key),
            };
            if p.at & EV_AT != 0 {
                // the slot is in a block this thread reserved: only it writes there
                unsafe { (*self.bufs[(p.at & !EV_AT) as usize].ptr.add(p.ix as usize)).user = u };
            } else {
                self.out.ck[p.ix as usize].user = u;
            }
        }
        self.pend = pend;
        self.pend.clear();
    }

    #[inline]
    fn defer(&mut self, key: S, at: u32, ix: u32) {
        let (lo, hi) = pack(key);
        self.pend.push(Pend { lo, hi, key, at, ix });
        if self.pend.len() == BATCH {
            self.flush();
        }
    }

    fn event(&mut self, f: &[S; 7]) {
        let [eid, uid, ts, ing, typ, pid, price] = *f;
        if eid.is_empty() {
            return;
        }
        let c = self.ctx;
        let ing = self.tsp.iso(ing).unwrap_or(i64::MAX);
        let mut row =
            EvRow { lo: 0, hi: 0, ing, ts: 0, user: 0, price: 0, cat: 0, valid: false, tid: self.tid };
        // Validity of this copy (rules: validate the kept copy, then drop late events).
        'v: {
            if uid.is_empty() || pid.is_empty() {
                break 'v;
            }
            let Some(t) = self.tsp.ts(ts) else { break 'v };
            if t < c.ws || t >= c.we || ing.saturating_sub(t) > DAY {
                break 'v;
            }
            let p = match typ {
                b"view" | b"cart" => 0,
                b"purchase" => match parse_price(price) {
                    Some(p) => p,
                    None => break 'v,
                },
                _ => break 'v,
            };
            row.valid = true;
            row.ts = (t - c.ws) as u32;
            row.price = p;
            row.cat = *c.products.get(pid).unwrap_or(&c.unknown);
        }
        // user IDs of short-event_id rows are interned in batches (see flush)
        let defer_user = row.valid && uid.len() <= 15 && eid.len() <= 15;
        if row.valid && !defer_user {
            row.user = self.intern(uid);
        }
        if eid.len() <= 15 {
            (row.lo, row.hi) = pack(eid);
            let shard = (mix(row.lo, row.hi) >> 56) as usize;
            let (mut a, mut b) = self.cur[shard];
            if a == b {
                let buf = &self.bufs[shard];
                let st = buf.next.fetch_add(BLK, Ordering::Relaxed);
                (a, b) = (st.min(buf.cap), (st + BLK).min(buf.cap));
            }
            if a < b {
                unsafe { self.bufs[shard].ptr.add(a).write(row) };
                self.cur[shard] = (a + 1, b);
                if defer_user {
                    self.defer(uid, EV_AT | shard as u32, a as u32);
                }
            } else {
                // shard buffer full (estimate too small): overflow, interned now
                self.cur[shard] = (a, b);
                if defer_user {
                    row.user = self.intern(uid);
                }
                self.out.ev[shard].push(row);
            }
        } else {
            self.out.ev_long.push((eid, row));
        }
    }

    fn click(&mut self, f: &[S; 4]) {
        let [cid, uid, camp, ts] = *f;
        if cid.is_empty() || uid.is_empty() {
            return;
        }
        let c = self.ctx;
        let Some(t) = self.tsp.ts(ts) else { return };
        if t < c.cws || t >= c.we {
            return;
        }
        let Some(&camp) = c.campaigns.get(camp) else { return };
        if uid.len() <= 15 {
            self.out.ck.push(CkRow { cid, ts: t, user: 0, camp });
            let ix = (self.out.ck.len() - 1) as u32;
            self.defer(uid, CK_AT, ix);
        } else {
            let user = self.intern(uid);
            self.out.ck.push(CkRow { cid, ts: t, user, camp });
        }
    }

    fn link(&mut self, f: &[S; 2]) {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let a = self.intern(f[0]);
        let b = self.intern(f[1]);
        self.out.lk.push((a, b));
    }
}

fn read_lookup(path: &Path) -> Vec<(S, S)> {
    let mut rows = Vec::new();
    if !path.is_file() {
        return rows;
    }
    for_lines(body(map_file(path)), |line| {
        let mut f: [S; 2] = [&[]; 2];
        if split_fields(line, &mut f) && !f[0].is_empty() {
            rows.push((f[0], f[1]));
        }
    });
    rows
}

// ------------------------------------------------------------------ sessions
#[derive(Clone, Copy)]
struct Ev {
    key: u64, // person << tsh | (ts - ws)
    val: u64, // price << 16 | category
}

#[derive(Clone, Copy)]
struct Ck {
    key: u64, // person << csh | (ts - cws)
    camp: u32,
    cid: S,
}

struct Sess {
    start: i64,
    day: usize,
}

#[derive(Clone, Copy)]
struct UdRow {
    person: u32,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

struct Acc {
    ud: Vec<UdRow>,
    sess: Vec<i64>,
    purch: Vec<i64>,
    rev: Vec<i64>,
    assist: Vec<i64>,
}

fn bits_for(span: i64) -> u32 {
    64 - (span.max(1) as u64).leading_zeros()
}

/// Category with the highest revenue; a tie goes to the smallest id (= byte-wise smallest name).
fn top_category(cat_rev: &[(u16, i64)]) -> Option<u16> {
    let mut top: Option<(u16, i64)> = None;
    for &(c, r) in cat_rev {
        if top.is_none_or(|(tc, tr)| r > tr || (r == tr && c < tc)) {
            top = Some((c, r));
        }
    }
    top.map(|t| t.0)
}

// ------------------------------------------------------------------ parquet
mod pq {
    use bytes::Bytes;
    use parquet::basic::Compression;
    use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    pub enum Col {
        Str(Vec<&'static [u8]>),
        OptStr(Vec<Option<&'static [u8]>>),
        I32(Vec<i32>),
        I64(Vec<i64>),
    }

    pub fn write(path: &Path, schema: &str, cols: Vec<Col>) -> parquet::errors::Result<()> {
        let schema = Arc::new(parse_message_type(schema)?);
        // No dictionaries (user_id is unique per row) and no statistics: less
        // work per value; any encoding and file size are fine for the outputs.
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .build(),
        );
        let mut w = SerializedFileWriter::new(File::create(path)?, schema, props)?;
        let mut rg = w.next_row_group()?;
        let mut it = cols.into_iter();
        while let Some(mut col) = rg.next_column()? {
            match it.next().expect("schema/column mismatch") {
                Col::Str(v) => {
                    let v: Vec<ByteArray> = v.into_iter().map(|s| ByteArray::from(Bytes::from_static(s))).collect();
                    col.typed::<ByteArrayType>().write_batch(&v, None, None)?;
                }
                Col::OptStr(v) => {
                    let def: Vec<i16> = v.iter().map(|x| x.is_some() as i16).collect();
                    let vals: Vec<ByteArray> =
                        v.into_iter().flatten().map(|s| ByteArray::from(Bytes::from_static(s))).collect();
                    col.typed::<ByteArrayType>().write_batch(&vals, Some(&def), None)?;
                }
                Col::I32(v) => {
                    col.typed::<Int32Type>().write_batch(&v, None, None)?;
                }
                Col::I64(v) => {
                    col.typed::<Int64Type>().write_batch(&v, None, None)?;
                }
            }
            col.close()?;
        }
        rg.close()?;
        w.close()?;
        Ok(())
    }
}

const UD_SCHEMA: &str = "message user_daily {
  REQUIRED BYTE_ARRAY user_id (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 events;
  REQUIRED INT64 revenue_cents;
  OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CD_SCHEMA: &str = "message campaign_daily {
  REQUIRED BYTE_ARRAY campaign_id (UTF8);
  REQUIRED BYTE_ARRAY channel (UTF8);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 sessions;
  REQUIRED INT64 purchasing_sessions;
  REQUIRED INT64 revenue_cents;
  REQUIRED INT64 assisted_sessions;
}";

fn fresh_dir(p: &Path) {
    if p.is_dir() {
        let _ = std::fs::remove_dir_all(p);
    }
    std::fs::create_dir_all(p).unwrap_or_else(|e| panic!("mkdir {}: {e}", p.display()));
}

// ------------------------------------------------------------------ main
fn main() {
    let mut tm = Timer::new();
    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let (data, outdir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (PathBuf::from(env("DATA_DIR", "/data")), PathBuf::from(env("OUTPUT_DIR", "/output")))
    };
    let nthreads: usize = env("NCPU", "8").parse().ok().filter(|&n: &usize| n > 0).unwrap_or(8).min(256);
    rayon::ThreadPoolBuilder::new().num_threads(nthreads).build_global().unwrap();

    // manifest
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(data.join("manifest.json")).expect("read manifest.json"))
            .expect("parse manifest.json");
    let win = |k: &str| -> i64 {
        let s = manifest[k].as_str().unwrap_or_else(|| panic!("manifest {k}"));
        parse_iso(s.as_bytes()).unwrap_or_else(|| panic!("manifest {k}: bad timestamp {s}"))
    };
    let (ws, we) = (win("window_start"), win("window_end"));
    let cws = ws - WEEK;

    // lookups: categories get ids in byte order, so comparing ids compares names
    let prod_rows = read_lookup(&data.join("products.csv"));
    let mut cat_names: Vec<S> = prod_rows.iter().map(|r| r.1).filter(|c| !c.is_empty()).collect();
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    assert!(cat_names.len() < u16::MAX as usize, "too many categories");
    let cat_id = |c: S| cat_names.binary_search(&c).unwrap() as u16;
    let unknown = cat_id(b"unknown");
    let mut products: HashMap<S, u16, Hs> = HashMap::with_capacity_and_hasher(prod_rows.len(), hasher());
    for &(p, c) in &prod_rows {
        products.insert(p, if c.is_empty() { unknown } else { cat_id(c) });
    }
    let mut camp_names: Vec<S> = Vec::new();
    let mut channels: Vec<S> = Vec::new();
    let mut campaigns: HashMap<S, u32, Hs> = HashMap::with_hasher(hasher());
    for (c, ch) in read_lookup(&data.join("campaigns.csv")) {
        if let std::collections::hash_map::Entry::Vacant(e) = campaigns.entry(c) {
            e.insert(camp_names.len() as u32);
            camp_names.push(c);
            channels.push(ch);
        }
    }
    let organic = camp_names.len();
    camp_names.push(b"organic");
    channels.push(b"organic");
    let ncamp = camp_names.len();

    // chunks
    let mut paths: Vec<(PathBuf, Kind)> = Vec::new();
    for (sub, kind) in [("events", Kind::Ev), ("clicks", Kind::Ck), ("links", Kind::Lk)] {
        paths.extend(csv_files(&data.join(sub)).into_iter().map(|p| (p, kind)));
    }
    let datas: Vec<S> = paths.par_iter().map(|(p, _)| map_file(p)).collect();
    // byte ranges only: no data is read here (workers find their line ends)
    let mut chunks: Vec<Chunk> = datas
        .par_iter()
        .zip(&paths)
        .flat_map_iter(|(&d, &(_, kind))| {
            let mut v = Vec::new();
            cut_chunks(body(d), kind, &mut v);
            v
        })
        .collect();
    chunks.sort_by_key(|c| std::cmp::Reverse(c.b - c.a));
    tm.stage("setup/lookups");

    // ---------------------------------------------------------- 1. parse
    let ctx = Ctx { ws, we, cws, products: &products, unknown, campaigns: &campaigns };
    // Sized from the input: generated data has about 1 distinct ID per 15-20
    // lines (~65 bytes each). The table takes up to lines/8 IDs (its limit is
    // half its slots); past that, threads fall back to local maps (still exact).
    // Every slot page gets touched, so oversizing costs real memory.
    let total_bytes: usize = chunks.iter().map(|c| c.b - c.a).sum();
    let shared = SharedTab::new((total_bytes / 65 / 4).next_power_of_two().clamp(1 << 16, 1 << 30));
    // Row buffers sized from the event bytes (lines are ~70 bytes; this assumes
    // 55, plus slack for partly used blocks); overflow keeps it exact anyway.
    let ev_bytes: usize = chunks.iter().filter(|c| c.kind == Kind::Ev).map(|c| c.b - c.a).sum();
    let per_shard = ev_bytes / 55 / NSHARD + BLK * nthreads * 2 + 1024;
    let bufs: Vec<RowBuf> = (0..NSHARD).map(|_| RowBuf::new(per_shard)).collect();
    let next = AtomicUsize::new(0);
    let outs: Vec<Out> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let (ctx, next, chunks, shared, bufs) = (&ctx, &next, &chunks, &shared, &bufs);
                sc.spawn(move || {
                    let mut w = Worker {
                        ctx,
                        tid: t as u8,
                        tsp: TsParser::new(),
                        shared,
                        pend: Vec::with_capacity(BATCH),
                        bufs,
                        cur: vec![(0, 0); NSHARD],
                        map: HashMap::with_hasher(hasher()),
                        out: Out {
                            ev: (0..NSHARD).map(|_| Vec::new()).collect(),
                            ev_long: Vec::new(),
                            ck: Vec::new(),
                            lk: Vec::new(),
                            new_keys: Vec::new(),
                            keys: Vec::new(),
                        },
                    };
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(ch) = chunks.get(i) else { break };
                        let c = lines_of(ch);
                        match ch.kind {
                            Kind::Ev => for_records(c, |f| w.event(f)),
                            Kind::Ck => for_records(c, |f| w.click(f)),
                            Kind::Lk => for_records(c, |f| w.link(f)),
                        }
                    }
                    w.flush();
                    for (sh, &(a, b)) in w.cur.iter().enumerate() {
                        for i in a..b {
                            unsafe { w.bufs[sh].ptr.add(i).write(HOLE) };
                        }
                    }
                    w.out
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    tm.stage("read/parse csv");

    // ---------------------------------------------------------- 2. global IDs
    // Global ids come from the shared table; thread-local (fallback) IDs are
    // looked up there first (another thread may have inserted them just
    // before the table reached its limit) and otherwise get new global ids.
    let mut gnames: Vec<S> = vec![&[]; shared.count.load(Ordering::Relaxed)];
    for o in &outs {
        for &(g, k) in &o.new_keys {
            gnames[g as usize] = k;
        }
    }
    let mut extra: HashMap<S, u32, Hs> = HashMap::with_hasher(hasher());
    let mut local_gid: Vec<Vec<u32>> = Vec::with_capacity(outs.len());
    for o in &outs {
        let mut v = Vec::with_capacity(o.keys.len());
        for &k in &o.keys {
            let found = if k.len() <= 15 {
                let (lo, hi) = pack(k);
                shared.find(lo, hi)
            } else {
                None
            };
            v.push(found.unwrap_or_else(|| {
                *extra.entry(k).or_insert_with(|| {
                    gnames.push(k);
                    (gnames.len() - 1) as u32
                })
            }));
        }
        local_gid.push(v);
    }
    drop(extra);
    drop(shared);
    let nids = gnames.len();
    if std::env::var_os("VERBOSE").is_some() {
        eprintln!(
            "ids: {} distinct, {} via thread-local fallback",
            nids,
            outs.iter().map(|o| o.keys.len()).sum::<usize>()
        );
    }
    // byte-wise rank of every global id
    #[derive(Clone, Copy)]
    struct K {
        pre: u128,
        s: S,
        g: u32,
    }
    #[inline]
    fn prefix(s: S) -> u128 {
        let mut b = [0u8; 16];
        let n = s.len().min(16);
        b[..n].copy_from_slice(&s[..n]);
        u128::from_be_bytes(b)
    }
    let mut ks: Vec<K> = gnames.par_iter().enumerate().map(|(g, &s)| K { pre: prefix(s), s, g: g as u32 }).collect();
    ks.par_sort_unstable_by(|a, b| a.pre.cmp(&b.pre).then_with(|| a.s.cmp(b.s)));
    let names: Vec<S> = ks.par_iter().map(|k| k.s).collect();
    let mut rank_g: Vec<u32> = vec![0; nids];
    for (r, k) in ks.iter().enumerate() {
        rank_g[k.g as usize] = r as u32;
    }
    drop(ks);
    drop(gnames);
    let gid = |t: usize, u: u32| -> usize {
        if u & LOCAL == 0 { u as usize } else { local_gid[t][(u & !LOCAL) as usize] as usize }
    };
    tm.stage("intern ids");

    // ---------------------------------------------------------- 3. union-find
    let mut parent: Vec<u32> = (0..nids as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for (t, o) in outs.iter().enumerate() {
        for &(a, b) in &o.lk {
            let ra = find(&mut parent, rank_g[gid(t, a)]);
            let rb = find(&mut parent, rank_g[gid(t, b)]);
            if ra < rb {
                parent[rb as usize] = ra;
            } else if rb < ra {
                parent[ra as usize] = rb;
            }
        }
    }
    // parent[i] <= i always holds, so one ascending pass resolves every root.
    for i in 0..nids {
        let p = parent[i] as usize;
        parent[i] = parent[p];
    }
    // person (the root's rank, i.e. an index into names) of every global id
    let person_g: Vec<u32> = rank_g.par_iter().map(|&r| parent[r as usize]).collect();
    let person_l: Vec<Vec<u32>> =
        local_gid.par_iter().map(|v| v.iter().map(|&g| person_g[g as usize]).collect()).collect();
    let person_of = |t: usize, u: u32| -> u32 {
        if u & LOCAL == 0 { person_g[u as usize] } else { person_l[t][(u & !LOCAL) as usize] }
    };
    drop(rank_g);
    drop(parent);
    tm.stage("links (union-find)");

    // ---------------------------------------------------------- 4. events dedup
    let tsh = bits_for(we - ws);
    let csh = bits_for(we - cws);
    assert!(bits_for(nids as i64) + csh.max(tsh) <= 64, "window too long for packed sort keys");
    assert!(tsh <= 32, "event window over 49 days (EvRow keeps ts - window_start in a u32)");
    let mut overflow: Vec<Vec<Vec<EvRow>>> = (0..NSHARD).map(|_| Vec::new()).collect();
    let mut cks_raw: Vec<Vec<CkRow>> = Vec::with_capacity(nthreads);
    let mut ev_long: Vec<(S, EvRow)> = Vec::new();
    for o in outs {
        for (s, v) in o.ev.into_iter().enumerate() {
            if !v.is_empty() {
                overflow[s].push(v);
            }
        }
        cks_raw.push(o.ck);
        ev_long.extend(o.ev_long);
    }
    let to_ev = |r: &EvRow| -> Ev {
        let p = person_of(r.tid as usize, r.user) as u64;
        Ev { key: (p << tsh) | r.ts as u64, val: ((r.price as u64) << 16) | r.cat as u64 }
    };
    // Surviving events go straight into person-range buckets, so each bucket
    // can be sorted on its own and the sessions stage can take whole buckets.
    let nb = nthreads * 16;
    let bucket_of = |p: u64| (p as u128 * nb as u128 / nids.max(1) as u128) as usize;
    let mut ev_parts: Vec<Vec<Vec<Ev>>> = bufs
        .par_iter()
        .zip(overflow.par_iter())
        .map(|(buf, over)| {
            let mut joined: Vec<EvRow>;
            let rows: &mut [EvRow] = if over.is_empty() {
                buf.filled()
            } else {
                joined = buf.filled().to_vec();
                for v in over {
                    joined.extend_from_slice(v);
                }
                &mut joined
            };
            rows.sort_unstable_by_key(|r| (r.hi, r.lo, r.ing)); // holes (hi == 0) sort first
            let per = rows.len() / nb + rows.len() / (4 * nb) + 4;
            let mut out: Vec<Vec<Ev>> = (0..nb).map(|_| Vec::with_capacity(per)).collect();
            // first row of each event_id run = the copy with the smallest ingest_ts
            for (i, r) in rows.iter().enumerate() {
                if r.valid && r.hi != 0 && (i == 0 || rows[i - 1].hi != r.hi || rows[i - 1].lo != r.lo) {
                    let e = to_ev(r);
                    out[bucket_of(e.key >> tsh)].push(e);
                }
            }
            out
        })
        .collect();
    // event_ids over 15 bytes: exact dedup by string (normally none)
    if !ev_long.is_empty() {
        ev_long.sort_by(|a, b| a.0.cmp(b.0).then(a.1.ing.cmp(&b.1.ing)));
        let mut out: Vec<Vec<Ev>> = (0..nb).map(|_| Vec::new()).collect();
        for k in 0..ev_long.len() {
            if (k == 0 || ev_long[k].0 != ev_long[k - 1].0) && ev_long[k].1.valid {
                let e = to_ev(&ev_long[k].1);
                out[bucket_of(e.key >> tsh)].push(e);
            }
        }
        ev_parts.push(out);
    }
    tm.stage("dedup/validate events");

    // ---------------------------------------------------------- 5. sort
    let evb: Vec<Vec<Ev>> = (0..nb)
        .into_par_iter()
        .map(|b| {
            let mut v: Vec<Ev> = Vec::with_capacity(ev_parts.iter().map(|sp| sp[b].len()).sum());
            for sp in &ev_parts {
                v.extend_from_slice(&sp[b]);
            }
            v.sort_unstable_by_key(|e| e.key);
            v
        })
        .collect();
    std::mem::forget(ev_parts);
    let mut cks: Vec<Ck> = cks_raw
        .par_iter()
        .enumerate()
        .flat_map_iter(|(t, v)| {
            v.iter().map(move |c| Ck {
                key: ((person_of(t, c.user) as u64) << csh) | (c.ts - cws) as u64,
                camp: c.camp,
                cid: c.cid,
            })
        })
        .collect();
    drop(cks_raw);
    cks.par_sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| a.cid.cmp(b.cid)));
    tm.stage("sort events/clicks");

    // ---------------------------------------------------------- 6. sessions
    let d0 = ws.div_euclid(DAY);
    let nd = if we > ws { ((we - 1).div_euclid(DAY) - d0 + 1) as usize } else { 1 };
    let tmask = (1u64 << tsh) - 1;
    let cmask = (1u64 << csh) - 1;
    let new_acc = || Acc {
        ud: Vec::new(),
        sess: vec![0; ncamp * nd],
        purch: vec![0; ncamp * nd],
        rev: vec![0; ncamp * nd],
        assist: vec![0; ncamp * nd],
    };
    let accs: Vec<Acc> = (0..nb)
        .into_par_iter()
        .fold(new_acc, |mut acc, part| {
            let evs: &[Ev] = &evb[part]; // whole persons, sorted by (person, ts)
            let (lo, hi) = (0, evs.len());
            if hi == 0 {
                return acc;
            }
            let first_p = evs[lo].key >> tsh;
            let mut ci = cks.partition_point(|c| (c.key >> csh) < first_p);
            let mut sess: Vec<Sess> = Vec::new();
            let mut pos: Vec<usize> = vec![0; nd + 1];
            let mut cat_rev: Vec<(u16, i64)> = Vec::new();
            let mut tmp: Vec<(u32, i64)> = Vec::new();
            let cts = |c: &Ck| (c.key & cmask) as i64 + cws;
            let mut i = lo;
            while i < hi {
                let p = evs[i].key >> tsh;
                let mut pe = i;
                while pe < hi && (evs[pe].key >> tsh) == p {
                    pe += 1;
                }
                while ci < cks.len() && (cks[ci].key >> csh) < p {
                    ci += 1;
                }
                let mut ce = ci;
                while ce < cks.len() && (cks[ce].key >> csh) == p {
                    ce += 1;
                }
                let clicks = &cks[ci..ce];

                // sessions; user-days are contiguous since sessions come in start order
                sess.clear();
                let mut ud = UdRow { person: p as u32, day: 0, sessions: 0, events: 0, revenue: 0, top: None };
                let mut ud_day = usize::MAX;
                let mut k = i;
                while k < pe {
                    let start = (evs[k].key & tmask) as i64 + ws;
                    let day = (start.div_euclid(DAY) - d0) as usize;
                    if day != ud_day {
                        if ud_day != usize::MAX {
                            ud.top = top_category(&cat_rev);
                            cat_rev.clear();
                            acc.ud.push(ud);
                        }
                        ud_day = day;
                        ud.day = (d0 + day as i64) as i32;
                        ud.sessions = 0;
                        ud.events = 0;
                        ud.revenue = 0;
                    }
                    let mut prev = start;
                    let mut cnt = 0i64;
                    let mut rev = 0i64;
                    let mut has_p = false;
                    while k < pe {
                        let t = (evs[k].key & tmask) as i64 + ws;
                        if t - prev > GAP {
                            break;
                        }
                        prev = t;
                        cnt += 1;
                        let price = (evs[k].val >> 16) as i64;
                        if price > 0 {
                            has_p = true;
                            rev += price;
                            let cat = (evs[k].val & 0xffff) as u16;
                            match cat_rev.iter_mut().find(|x| x.0 == cat) {
                                Some(x) => x.1 += price,
                                None => cat_rev.push((cat, price)),
                            }
                        }
                        k += 1;
                    }
                    ud.sessions += 1;
                    ud.events += cnt;
                    ud.revenue += rev;
                    sess.push(Sess { start, day });

                    // credit: latest click at or before the start (ties: smallest click_id), within 7 days
                    let j = clicks.partition_point(|c| cts(c) <= start);
                    let mut camp = organic;
                    if j > 0 {
                        let t = cts(&clicks[j - 1]);
                        if start - t <= WEEK {
                            let i0 = clicks[..j].partition_point(|c| cts(c) < t);
                            camp = clicks[i0].camp as usize;
                        }
                    }
                    let ix = camp * nd + day;
                    acc.sess[ix] += 1;
                    acc.purch[ix] += has_p as i64;
                    acc.rev[ix] += rev;
                }
                ud.top = top_category(&cat_rev);
                cat_rev.clear();
                acc.ud.push(ud);

                // assists: a click at t assists sessions starting in [t, t + 7d];
                // per campaign, merge those intervals and count sessions per day.
                if !clicks.is_empty() {
                    let mut d = 0;
                    for (s_ix, s) in sess.iter().enumerate() {
                        while d <= s.day {
                            pos[d] = s_ix;
                            d += 1;
                        }
                    }
                    while d <= nd {
                        pos[d] = sess.len();
                        d += 1;
                    }
                    tmp.clear();
                    tmp.extend(clicks.iter().map(|c| (c.camp, cts(c))));
                    tmp.sort_unstable();
                    let mut a = 0;
                    while a < tmp.len() {
                        let camp = tmp[a].0;
                        let lo_t = tmp[a].1;
                        let mut hi_t = lo_t;
                        let mut b = a + 1;
                        while b < tmp.len() && tmp[b].0 == camp && tmp[b].1 - hi_t <= WEEK {
                            hi_t = tmp[b].1;
                            b += 1;
                        }
                        let hi_t = hi_t + WEEK;
                        let slo = sess.partition_point(|s| s.start < lo_t);
                        let shi = sess.partition_point(|s| s.start <= hi_t);
                        if slo < shi {
                            for dd in sess[slo].day..=sess[shi - 1].day {
                                let c = shi.min(pos[dd + 1]) as i64 - slo.max(pos[dd]) as i64;
                                if c > 0 {
                                    acc.assist[camp as usize * nd + dd] += c;
                                }
                            }
                        }
                        a = b;
                    }
                }
                i = pe;
            }
            acc
        })
        .collect();
    drop(evb);
    drop(cks);
    tm.stage("sessions/attribution");

    // ---------------------------------------------------------- 7. outputs
    let mut tot = new_acc();
    for a in &accs {
        for (dst, src) in
            [(&mut tot.sess, &a.sess), (&mut tot.purch, &a.purch), (&mut tot.rev, &a.rev), (&mut tot.assist, &a.assist)]
        {
            for (x, y) in dst.iter_mut().zip(src) {
                *x += y;
            }
        }
    }
    let ud_dir = outdir.join("user_daily");
    let cd_dir = outdir.join("campaign_daily");
    fresh_dir(&ud_dir);
    fresh_dir(&cd_dir);

    // user_daily: up to nthreads files, written in parallel
    let total_ud: usize = accs.iter().map(|a| a.ud.len()).sum();
    let per = total_ud.div_ceil(nthreads).max(1);
    let mut groups: Vec<Vec<&UdRow>> = vec![Vec::new()];
    for r in accs.iter().flat_map(|a| a.ud.iter()) {
        if groups.last().unwrap().len() >= per {
            groups.push(Vec::new());
        }
        groups.last_mut().unwrap().push(r);
    }
    groups.par_iter().enumerate().for_each(|(gi, rows)| {
        use pq::Col;
        let cols = vec![
            Col::Str(rows.iter().map(|r| names[r.person as usize]).collect()),
            Col::I32(rows.iter().map(|r| r.day).collect()),
            Col::I64(rows.iter().map(|r| r.sessions).collect()),
            Col::I64(rows.iter().map(|r| r.events).collect()),
            Col::I64(rows.iter().map(|r| r.revenue).collect()),
            Col::OptStr(rows.iter().map(|r| r.top.map(|c| cat_names[c as usize])).collect()),
        ];
        pq::write(&ud_dir.join(format!("part-{gi:05}.parquet")), UD_SCHEMA, cols).expect("write user_daily");
    });

    let mut cd: (Vec<S>, Vec<S>, Vec<i32>, Vec<i64>, Vec<i64>, Vec<i64>, Vec<i64>) = Default::default();
    for c in 0..ncamp {
        for d in 0..nd {
            let ix = c * nd + d;
            if tot.sess[ix] > 0 || tot.assist[ix] > 0 {
                cd.0.push(camp_names[c]);
                cd.1.push(channels[c]);
                cd.2.push((d0 + d as i64) as i32);
                cd.3.push(tot.sess[ix]);
                cd.4.push(tot.purch[ix]);
                cd.5.push(tot.rev[ix]);
                cd.6.push(tot.assist[ix]);
            }
        }
    }
    {
        use pq::Col;
        let cols =
            vec![Col::Str(cd.0), Col::Str(cd.1), Col::I32(cd.2), Col::I64(cd.3), Col::I64(cd.4), Col::I64(cd.5), Col::I64(cd.6)];
        pq::write(&cd_dir.join("part-00000.parquet"), CD_SCHEMA, cols).expect("write campaign_daily");
    }
    tm.stage("write parquet");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leak(v: Vec<u8>) -> S {
        Box::leak(v.into_boxed_slice())
    }

    /// Reference: line by line with split_fields.
    fn reference<const N: usize>(chunk: S) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for_lines(chunk, |l| {
            let mut f: [S; N] = [&[]; N];
            if split_fields(l, &mut f) {
                out.push(f.iter().map(|x| x.to_vec()).collect());
            }
        });
        out
    }

    fn fast<const N: usize>(chunk: S) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for_records(chunk, |f: &[S; N]| out.push(f.iter().map(|x| x.to_vec()).collect()));
        out
    }

    /// Every line of up to 9 bytes over {a , " \r space}, alone and all in one chunk.
    fn check<const N: usize>() {
        let alpha = [b'a', b',', b'"', b'\r', b' '];
        let mut lines: Vec<Vec<u8>> = vec![vec![]];
        let mut frontier: Vec<Vec<u8>> = vec![vec![]];
        for _ in 0..9 {
            let mut next = Vec::new();
            for l in &frontier {
                for &c in &alpha {
                    let mut m = l.clone();
                    m.push(c);
                    next.push(m);
                }
            }
            lines.extend(next.iter().cloned());
            frontier = next;
        }
        let mut all = Vec::new();
        for l in &lines {
            for nl in [false, true] {
                let mut c = l.clone();
                if nl {
                    c.push(b'\n');
                }
                let c = leak(c);
                assert_eq!(fast::<N>(c), reference::<N>(c), "line {:?} N={N}", String::from_utf8_lossy(c));
            }
            all.extend_from_slice(l);
            all.push(b'\n');
        }
        let all = leak(all);
        assert_eq!(fast::<N>(all), reference::<N>(all));
        let trimmed = &all[..all.len() - 1]; // last line without '\n'
        assert_eq!(fast::<N>(trimmed), reference::<N>(trimmed));
    }

    #[test]
    fn chunks_partition_lines() {
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        for len in 0..300usize {
            for step in 1..24usize {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let density = (x % 5) as u64 + 1;
                let body: Vec<u8> = (0..len)
                    .map(|i| {
                        let r = (x.wrapping_mul(i as u64 + 1) >> 33) % 7;
                        if r < density { b'\n' } else { b'a' }
                    })
                    .collect();
                let body = leak(body);
                let mut whole = Vec::new();
                let mut a = 0;
                while a < len {
                    let b = (a + step).min(len);
                    let part = lines_of(&Chunk { kind: Kind::Ev, body, a, b });
                    // each part is whole lines: empty, or starts at a line start
                    let off = part.as_ptr() as usize - body.as_ptr() as usize;
                    assert!(part.is_empty() || off == 0 || body[off - 1] == b'\n');
                    whole.extend_from_slice(part);
                    a = b;
                }
                assert_eq!(whole, body, "len={len} step={step}");
            }
        }
    }

    #[test]
    fn ts_parser_matches_reference() {
        let mut p = TsParser::new();
        let mut cases: Vec<String> = Vec::new();
        for y in ["2024", "2025", "2027", "2028", "2100", "2000", "0000", "1970", "9999", "２０26"] {
            for m in ["01", "02", "03", "04", "09", "10", "12", "13", "00", "1a"] {
                for d in ["01", "09", "10", "28", "29", "30", "31", "32", "00", "3 "] {
                    for t in ["00:00:00", "23:59:59", "24:00:00", "12:60:00", "12:00:60", "1x:00:00"] {
                        for z in ["Z", "z", "+00:00", "-00:00", "+23:59", "+24:00", "+05:60", "-10:00", "+0200", "", "ZZ"] {
                            cases.push(format!("{y}-{m}-{d}T{t}{z}"));
                        }
                    }
                }
            }
        }
        for s in ["1791071890000", "0001791071890000", "86400000", "1791071890000.0", "179107189000a", "a791071890000",
                  "17910718900001", "179107189000", "0000000000000", "9999999999999", "1.79e12", "-5", "", " 1791071890000",
                  "1791071890000\r", "999999999999999999999", "0x00000000000", "2026-10-03 23:58:10Z", "2026-10-03t23:58:10Z"] {
            cases.push(s.to_string());
        }
        // exercise the date cache with repeats and with different years on the same day
        let mut all = cases.clone();
        all.extend(cases.iter().rev().cloned());
        for c in &all {
            let b = c.as_bytes();
            assert_eq!(p.ts(b), parse_ts(b), "ts {c:?}");
            assert_eq!(p.iso(b), parse_iso(b), "iso {c:?}");
        }
        for v in [0u64, 1, 12345678, 99999999, 10000000, 87654321] {
            assert_eq!(parse8(format!("{v:08}").as_bytes()), Some(v));
        }
    }

    #[test]
    fn records_match_split_fields_2() {
        check::<2>();
    }
    #[test]
    fn records_match_split_fields_4() {
        check::<4>();
    }
    #[test]
    fn records_match_split_fields_7() {
        check::<7>();
    }
}
