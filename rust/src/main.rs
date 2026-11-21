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

#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.len() >= 5 && b[4] == b'-' { parse_iso(b) } else { parse_digits(b) }
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
    let m = unsafe { memmap2::Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
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

fn cut_chunks(body: S, kind: Kind, out: &mut Vec<(Kind, S)>) {
    let n = body.len();
    let mut p = 0;
    while p < n {
        let mut e = (p + CHUNK).min(n);
        if e < n {
            e = memchr(b'\n', &body[e..]).map_or(n, |i| e + i + 1);
        }
        out.push((kind, &body[p..e]));
        p = e;
    }
}

// ------------------------------------------------------------------ parse stage
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Ev,
    Ck,
    Lk,
}

#[derive(Clone, Copy)]
struct EvRow {
    h: u64,
    eid: S,
    ing: i64,
    ts: i64,
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
    ev: Vec<Vec<EvRow>>,
    ck: Vec<CkRow>,
    lk: Vec<(u32, u32)>,
    keys: Vec<S>,
}

struct Ctx<'a> {
    ws: i64,
    we: i64,
    cws: i64,
    products: &'a HashMap<S, u16, Hs>,
    unknown: u16,
    campaigns: &'a HashMap<S, u32, Hs>,
    h: Hs,
}

struct Worker<'a> {
    ctx: &'a Ctx<'a>,
    tid: u8,
    map: HashMap<S, u32, Hs>,
    out: Out,
}

impl<'a> Worker<'a> {
    #[inline]
    fn intern(&mut self, k: S) -> u32 {
        let keys = &mut self.out.keys;
        *self.map.entry(k).or_insert_with(|| {
            keys.push(k);
            (keys.len() - 1) as u32
        })
    }

    fn event(&mut self, line: S) {
        let mut f: [S; 7] = [&[]; 7];
        if !split_fields(line, &mut f) {
            return;
        }
        let [eid, uid, ts, ing, typ, pid, price] = f;
        if eid.is_empty() {
            return;
        }
        let c = self.ctx;
        let ing = parse_iso(ing).unwrap_or(i64::MAX);
        let mut row =
            EvRow { h: c.h.hash_one(eid), eid, ing, ts: 0, user: 0, price: 0, cat: 0, valid: false, tid: self.tid };
        // Validity of this copy (rules: validate the kept copy, then drop late events).
        'v: {
            if uid.is_empty() || pid.is_empty() {
                break 'v;
            }
            let Some(t) = parse_ts(ts) else { break 'v };
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
            row.ts = t;
            row.price = p;
            row.cat = *c.products.get(pid).unwrap_or(&c.unknown);
            row.user = self.intern(uid);
        }
        self.out.ev[(row.h >> 56) as usize].push(row);
    }

    fn click(&mut self, line: S) {
        let mut f: [S; 4] = [&[]; 4];
        if !split_fields(line, &mut f) {
            return;
        }
        let [cid, uid, camp, ts] = f;
        if cid.is_empty() || uid.is_empty() {
            return;
        }
        let c = self.ctx;
        let Some(t) = parse_ts(ts) else { return };
        if t < c.cws || t >= c.we {
            return;
        }
        let Some(&camp) = c.campaigns.get(camp) else { return };
        let user = self.intern(uid);
        self.out.ck.push(CkRow { cid, ts: t, user, camp });
    }

    fn link(&mut self, line: S) {
        let mut f: [S; 2] = [&[]; 2];
        if !split_fields(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
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
    use parquet::file::properties::WriterProperties;
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
        let props = Arc::new(WriterProperties::builder().set_compression(Compression::UNCOMPRESSED).build());
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
    let mut chunks: Vec<(Kind, S)> = Vec::new();
    for (sub, kind) in [("events", Kind::Ev), ("clicks", Kind::Ck), ("links", Kind::Lk)] {
        for p in csv_files(&data.join(sub)) {
            cut_chunks(body(map_file(&p)), kind, &mut chunks);
        }
    }
    chunks.sort_by_key(|&(_, c)| std::cmp::Reverse(c.len()));
    tm.stage("setup/lookups");

    // ---------------------------------------------------------- 1. parse
    let ctx = Ctx { ws, we, cws, products: &products, unknown, campaigns: &campaigns, h: hasher() };
    let next = AtomicUsize::new(0);
    let outs: Vec<Out> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let (ctx, next, chunks) = (&ctx, &next, &chunks);
                sc.spawn(move || {
                    let mut w = Worker {
                        ctx,
                        tid: t as u8,
                        map: HashMap::with_hasher(hasher()),
                        out: Out {
                            ev: (0..NSHARD).map(|_| Vec::new()).collect(),
                            ck: Vec::new(),
                            lk: Vec::new(),
                            keys: Vec::new(),
                        },
                    };
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&(kind, c)) = chunks.get(i) else { break };
                        match kind {
                            Kind::Ev => for_lines(c, |l| w.event(l)),
                            Kind::Ck => for_lines(c, |l| w.click(l)),
                            Kind::Lk => for_lines(c, |l| w.link(l)),
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
    // Sort every (thread, local id) key once, with a 16-byte big-endian prefix
    // inline so most comparisons never touch the string; one scan then gives
    // each distinct ID its byte-wise rank.
    #[derive(Clone, Copy)]
    struct K {
        pre: u128,
        s: S,
        t: u32,
        l: u32,
    }
    #[inline]
    fn prefix(s: S) -> u128 {
        let mut b = [0u8; 16];
        let n = s.len().min(16);
        b[..n].copy_from_slice(&s[..n]);
        u128::from_be_bytes(b)
    }
    let mut ks: Vec<K> = outs
        .par_iter()
        .enumerate()
        .flat_map_iter(|(t, o)| {
            o.keys.iter().enumerate().map(move |(l, &s)| K { pre: prefix(s), s, t: t as u32, l: l as u32 })
        })
        .collect();
    ks.par_sort_unstable_by(|a, b| a.pre.cmp(&b.pre).then_with(|| a.s.cmp(b.s)));
    let mut rank: Vec<Vec<u32>> = outs.iter().map(|o| vec![0u32; o.keys.len()]).collect();
    let mut names: Vec<S> = Vec::new();
    for (i, k) in ks.iter().enumerate() {
        if i == 0 || k.pre != ks[i - 1].pre || k.s != ks[i - 1].s {
            names.push(k.s);
        }
        rank[k.t as usize][k.l as usize] = (names.len() - 1) as u32;
    }
    drop(ks);
    let nids = names.len();
    if std::env::var_os("VERBOSE").is_some() {
        eprintln!("ids: {} thread-local keys, {} distinct", outs.iter().map(|o| o.keys.len()).sum::<usize>(), nids);
    }
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
    for (o, r) in outs.iter().zip(&rank) {
        for &(a, b) in &o.lk {
            let ra = find(&mut parent, r[a as usize]);
            let rb = find(&mut parent, r[b as usize]);
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
    let person: Vec<Vec<u32>> =
        rank.par_iter().map(|r| r.iter().map(|&g| parent[g as usize]).collect()).collect();
    drop(rank);
    drop(parent);
    tm.stage("links (union-find)");

    // ---------------------------------------------------------- 4. events dedup
    let tsh = bits_for(we - ws);
    let csh = bits_for(we - cws);
    assert!(bits_for(nids as i64) + csh.max(tsh) <= 64, "window too long for packed sort keys");
    let mut shards: Vec<Vec<Vec<EvRow>>> = (0..NSHARD).map(|_| Vec::with_capacity(nthreads)).collect();
    let mut cks_raw: Vec<Vec<CkRow>> = Vec::with_capacity(nthreads);
    for o in outs {
        for (s, v) in o.ev.into_iter().enumerate() {
            shards[s].push(v);
        }
        cks_raw.push(o.ck);
    }
    let ev_parts: Vec<Vec<Ev>> = shards
        .into_par_iter()
        .map(|parts| {
            let mut rows: Vec<EvRow> = parts.concat();
            drop(parts);
            rows.sort_unstable_by_key(|r| (r.h, r.ing));
            let mut out = Vec::with_capacity(rows.len());
            let mut emit = |r: &EvRow| {
                if r.valid {
                    let p = person[r.tid as usize][r.user as usize] as u64;
                    out.push(Ev { key: (p << tsh) | (r.ts - ws) as u64, val: ((r.price as u64) << 16) | r.cat as u64 });
                }
            };
            let mut i = 0;
            while i < rows.len() {
                let mut j = i + 1;
                let mut same = true;
                while j < rows.len() && rows[j].h == rows[i].h {
                    same &= rows[j].eid == rows[i].eid;
                    j += 1;
                }
                if same {
                    emit(&rows[i]); // the copy with the smallest ingest_ts
                } else {
                    // 64-bit hash collision: group exactly by event_id.
                    let g = &mut rows[i..j];
                    g.sort_by(|a, b| a.eid.cmp(b.eid).then(a.ing.cmp(&b.ing)));
                    for k in 0..g.len() {
                        if k == 0 || g[k].eid != g[k - 1].eid {
                            emit(&g[k]);
                        }
                    }
                }
                i = j;
            }
            out
        })
        .collect();
    let mut evs: Vec<Ev> = ev_parts.concat();
    drop(ev_parts);
    tm.stage("dedup/validate events");

    // ---------------------------------------------------------- 5. sort
    evs.par_sort_unstable_by_key(|e| e.key);
    let mut cks: Vec<Ck> = cks_raw
        .par_iter()
        .enumerate()
        .flat_map_iter(|(t, v)| {
            let pt = &person[t];
            v.iter().map(move |c| Ck {
                key: ((pt[c.user as usize] as u64) << csh) | (c.ts - cws) as u64,
                camp: c.camp,
                cid: c.cid,
            })
        })
        .collect();
    drop(cks_raw);
    drop(person);
    cks.par_sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| a.cid.cmp(b.cid)));
    tm.stage("sort events/clicks");

    // ---------------------------------------------------------- 6. sessions
    let d0 = ws.div_euclid(DAY);
    let nd = if we > ws { ((we - 1).div_euclid(DAY) - d0 + 1) as usize } else { 1 };
    let tmask = (1u64 << tsh) - 1;
    let cmask = (1u64 << csh) - 1;
    let n = evs.len();
    let nparts = nthreads * 16;
    let mut bounds = vec![0usize];
    for k in 1..nparts {
        let b = k * n / nparts;
        if b == 0 || b >= n {
            continue;
        }
        let prev = evs[b - 1].key >> tsh;
        let b = evs.partition_point(|e| (e.key >> tsh) <= prev);
        if b > *bounds.last().unwrap() && b < n {
            bounds.push(b);
        }
    }
    if n > 0 {
        bounds.push(n);
    }
    let new_acc = || Acc {
        ud: Vec::new(),
        sess: vec![0; ncamp * nd],
        purch: vec![0; ncamp * nd],
        rev: vec![0; ncamp * nd],
        assist: vec![0; ncamp * nd],
    };
    let accs: Vec<Acc> = (0..bounds.len().saturating_sub(1))
        .into_par_iter()
        .fold(new_acc, |mut acc, part| {
            let (lo, hi) = (bounds[part], bounds[part + 1]);
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
    drop(evs);
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
