//! Transition-matrix loader (FSM1 binary, served from a Fastly KV Store).
//!
//! The matrix is produced by `scripts/scoring/train.py` (JSON on disk / FOS for
//! Python eval) and pushed to a Fastly **KV Store** (resource-link name
//! `scoring_matrix`, key `matrix`) in a compact binary form — `FSM1` — by the
//! backend's KV writers (`backend/scoring/matrix.py::serialize_kv`). We fetch +
//! decode it lazily on first use and cache it for the Wasm instance lifetime.
//!
//! Why binary (not JSON as before): the matrix is sparse (~38 K nonzero pairs
//! over ~8 K routes) but the JSON is ~1.85 MB, slow to fetch AND slow to parse
//! into nested `HashMap`s on a COLD Wasm instance — and Fastly Compute is
//! instance-per-request, so *every* L2 request is cold. FSM1 is ~5.6× smaller
//! and decodes in one pass into flat arrays (no per-entry allocation); lookups
//! are binary searches. Only the fields the scorer reads (version, vocab_size,
//! counts, row_totals) are encoded — `categories`/`anchors` are dropped.
//!
//! Why KV (not `include_bytes!`): the matrix is per-tenant; KV lets the backend
//! push a retrained matrix via the Fastly API with no Rust/Fastly toolchain.
//! There is no embedded fallback any more — a KV miss / parse error degrades to
//! the empty default (vocab_size 0 → L2 disabled), same effect as the old empty
//! `matrix.default.json` placeholder.
//!
//! Wire format (little-endian) — MUST stay byte-for-byte in lockstep with the
//! Python encoder `serialize_kv`. The cross-language fixture
//! (`tests::parse_fsm1_cross_lang_fixture` here ↔ `test_serialize_kv_byte_exact`
//! in `tests/scoring/test_matrix.py`) pins the exact bytes.
//!
//! ```text
//!   magic[4]="FSM1" | fmt_ver u8=1 | vocab_size u32 | n_routes u32 |
//!   ver_len u16 + version[ver_len] |
//!   str_off[n_routes+1] u32 (cumulative) | str_blob (routes sorted by bytes) |
//!   row_total[n_routes] uvarint u64 | row_off[n_routes+1] uvarint u32 |
//!   pairs[]: per row, ascending col_id uvarint + count uvarint u64
//! ```

use fastly::kv_store::{KVStore, KVStoreError};
use std::cmp::Ordering;
use std::sync::OnceLock;

/// KV Store resource-link name (linked to the scoring service at provision
/// time) and the key the matrix is stored under. MUST match
/// `MATRIX_RESOURCE_LINK_NAME` / `MATRIX_KEY_NAME` on the Python side.
const MATRIX_STORE: &str = "scoring_matrix";
const MATRIX_KEY: &str = "matrix";
const FSM1_MAGIC: &[u8; 4] = b"FSM1";

/// Decoded transition matrix. Backed by flat arrays (no `HashMap`): a route's
/// id is its index in the sorted string table, so `prev`/`current` resolve via
/// binary search and counts via a CSR row lookup. Built once per KV fetch.
#[derive(Default)]
pub struct TransitionMatrix {
    version: String,
    vocab_size: u32,
    /// `n_routes + 1` cumulative byte offsets into `str_blob` (route id = index).
    str_off: Vec<u32>,
    /// Concatenated UTF-8 route paths, sorted ascending by raw bytes.
    str_blob: Vec<u8>,
    /// Per route id: total outgoing transition count (0 if route is curr-only).
    row_total: Vec<u64>,
    /// `n_routes + 1` cumulative pair indices (CSR row pointers).
    row_off: Vec<u32>,
    /// Destination route ids, ascending within each row.
    col_ids: Vec<u32>,
    /// Transition counts, parallel to `col_ids`.
    counts: Vec<u64>,
}

impl TransitionMatrix {
    pub fn vocab_size(&self) -> u32 {
        self.vocab_size
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    fn num_routes(&self) -> usize {
        self.str_off.len().saturating_sub(1)
    }

    /// True when the matrix has any transition rows (mirrors the old
    /// `!row_totals.is_empty()` gate that drives L2's fail-closed behaviour).
    pub fn has_rows(&self) -> bool {
        !self.col_ids.is_empty()
    }

    fn route_bytes(&self, id: usize) -> &[u8] {
        // Safe: parse_fsm1 validates str_off is monotonic and bounded by str_blob.
        let a = self.str_off[id] as usize;
        let b = self.str_off[id + 1] as usize;
        &self.str_blob[a..b]
    }

    /// Resolve a route path to its id via binary search over the sorted string
    /// table. `None` when the route isn't in the matrix.
    pub fn route_id(&self, path: &str) -> Option<u32> {
        let target = path.as_bytes();
        let (mut lo, mut hi) = (0usize, self.num_routes());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.route_bytes(mid).cmp(target) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Some(mid as u32),
            }
        }
        None
    }

    pub fn row_total(&self, route_id: u32) -> u64 {
        self.row_total.get(route_id as usize).copied().unwrap_or(0)
    }

    /// Transition count for `prev_id → cur_id` (0 if the pair was never seen).
    pub fn transition_count(&self, prev_id: u32, cur_id: u32) -> u64 {
        let p = prev_id as usize;
        // row_off has n_routes+1 entries; prev_id < n_routes guaranteed by caller
        // (it came from route_id), but guard anyway to stay panic-free.
        let (Some(&start), Some(&end)) = (self.row_off.get(p), self.row_off.get(p + 1)) else {
            return 0;
        };
        let (start, end) = (start as usize, end as usize);
        let cols = &self.col_ids[start..end];
        match cols.binary_search(&cur_id) {
            Ok(i) => self.counts[start + i],
            Err(_) => 0,
        }
    }
}

static MATRIX_CACHE: OnceLock<TransitionMatrix> = OnceLock::new();

/// LEB128 unsigned-varint reader over a byte cursor. All reads are
/// bounds-checked and return `None` on truncation/overflow so a malformed KV
/// value degrades to the empty default rather than panicking the hot path.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        let b = self.take(2)?;
        Some(u16::from_le_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Option<u32> {
        let b = self.take(4)?;
        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn uvarint(&mut self) -> Option<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            if shift >= 64 {
                return None; // malformed: more bytes than a u64 can hold
            }
            let byte = self.u8()?;
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
        }
    }
}

/// Decode an FSM1 byte buffer. Returns `None` on any malformed input (bad magic,
/// unknown version, truncation, non-monotonic offsets) — the caller maps that to
/// the empty default so the request path never panics.
fn parse_fsm1(buf: &[u8]) -> Option<TransitionMatrix> {
    let mut r = Reader { buf, pos: 0 };
    if r.take(4)? != FSM1_MAGIC {
        return None;
    }
    if r.u8()? != 1 {
        return None;
    }
    let vocab_size = r.u32()?;
    let n_routes = r.u32()? as usize;
    let ver_len = r.u16()? as usize;
    let version = std::str::from_utf8(r.take(ver_len)?).ok()?.to_string();

    let mut str_off = Vec::with_capacity(n_routes + 1);
    for _ in 0..=n_routes {
        str_off.push(r.u32()?);
    }
    // Offsets must be monotonic and bounded so route_bytes slicing is panic-free.
    if str_off.windows(2).any(|w| w[0] > w[1]) {
        return None;
    }
    let blob_len = *str_off.last()? as usize;
    let str_blob = r.take(blob_len)?.to_vec();

    let mut row_total = Vec::with_capacity(n_routes);
    for _ in 0..n_routes {
        row_total.push(r.uvarint()?);
    }

    let mut row_off = Vec::with_capacity(n_routes + 1);
    for _ in 0..=n_routes {
        row_off.push(u32::try_from(r.uvarint()?).ok()?);
    }
    if row_off.windows(2).any(|w| w[0] > w[1]) {
        return None;
    }
    let total_pairs = *row_off.last()? as usize;

    let mut col_ids = Vec::with_capacity(total_pairs);
    let mut counts = Vec::with_capacity(total_pairs);
    for _ in 0..total_pairs {
        col_ids.push(u32::try_from(r.uvarint()?).ok()?);
        counts.push(r.uvarint()?);
    }

    Some(TransitionMatrix {
        version,
        vocab_size,
        str_off,
        str_blob,
        row_total,
        row_off,
        col_ids,
        counts,
    })
}

/// Fetch the matrix bytes from the KV Store, returning an empty `Vec` on any
/// miss (store unlinked / key absent / KV error). Never panics — a KV hiccup
/// must not 5xx live traffic; an empty buffer parses to the empty default
/// (vocab_size 0 → L2 disabled).
fn fetch_matrix_bytes() -> Vec<u8> {
    match KVStore::open(MATRIX_STORE) {
        Ok(Some(store)) => match store.lookup(MATRIX_KEY) {
            Ok(mut resp) => return resp.take_body_bytes(),
            Err(KVStoreError::ItemNotFound) => {
                eprintln!("[scoring/dbg] [ERROR] Matrix item not found in KVStore under key '{}'", MATRIX_KEY);
            }
            Err(e) => {
                eprintln!("[scoring/dbg] [ERROR] KVStore lookup error for key '{}': {:?}", MATRIX_KEY, e);
            }
        },
        Ok(None) => {
            eprintln!("[scoring/dbg] [ERROR] KVStore matrix_store is None (unlinked or unavailable)");
        }
        Err(e) => {
            eprintln!("[scoring/dbg] [ERROR] Failed to open KVStore '{}': {:?}", MATRIX_STORE, e);
        }
    }
    Vec::new()
}

/// Lazily fetch + decode the matrix, caching it for the Wasm instance lifetime.
/// Returns `None` when the result is empty (vocab_size 0) — the request handler
/// treats that as "L2 disabled, fall back to L1 only". A parse failure also maps
/// to the empty default (never a panic) so a malformed KV value can't take down
/// scoring for live traffic.
pub fn load_matrix() -> Option<&'static TransitionMatrix> {
    let m = MATRIX_CACHE.get_or_init(|| {
        let bytes = fetch_matrix_bytes();
        match parse_fsm1(&bytes) {
            Some(parsed) => parsed,
            None => {
                if !bytes.is_empty() {
                    eprintln!("[scoring/dbg] [ERROR] Failed to parse FSM1 matrix bytes");
                }
                TransitionMatrix::default()
            }
        }
    });
    if m.vocab_size == 0 {
        None
    } else {
        Some(m)
    }
}

#[cfg(test)]
mod test_support {
    //! FSM1 encoder used only by tests — mirrors `serialize_kv` in
    //! `backend/scoring/matrix.py` so test matrices round-trip through the real
    //! parser.
    use super::*;
    use std::collections::BTreeMap;

    fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                out.push(byte | 0x80);
            } else {
                out.push(byte);
                break;
            }
        }
    }

    /// Encode `(prev, &[(curr, count)])` rows into FSM1. `row_total` is the row's
    /// count-sum (override after decode with `set_row_total`).
    pub fn encode(version: &str, vocab_size: u32, rows: &[(&str, &[(&str, u64)])]) -> Vec<u8> {
        let mut routes: BTreeMap<Vec<u8>, ()> = BTreeMap::new();
        for (prev, dests) in rows {
            routes.insert(prev.as_bytes().to_vec(), ());
            for (c, _) in *dests {
                routes.insert(c.as_bytes().to_vec(), ());
            }
        }
        let sorted: Vec<Vec<u8>> = routes.into_keys().collect();
        let rid = |s: &str| sorted.iter().position(|r| r.as_slice() == s.as_bytes()).unwrap() as u32;
        let n = sorted.len();

        let mut out = Vec::new();
        out.extend_from_slice(FSM1_MAGIC);
        out.push(1);
        out.extend_from_slice(&vocab_size.to_le_bytes());
        out.extend_from_slice(&(n as u32).to_le_bytes());
        out.extend_from_slice(&(version.len() as u16).to_le_bytes());
        out.extend_from_slice(version.as_bytes());

        let mut blob = Vec::new();
        let mut offs = vec![0u32];
        for r in &sorted {
            blob.extend_from_slice(r);
            offs.push(blob.len() as u32);
        }
        for o in &offs {
            out.extend_from_slice(&o.to_le_bytes());
        }
        out.extend_from_slice(&blob);

        let mut totals = vec![0u64; n];
        let mut row_pairs: Vec<Vec<(u32, u64)>> = vec![Vec::new(); n];
        for (prev, dests) in rows {
            let pid = rid(prev) as usize;
            let mut sum = 0u64;
            let mut v: Vec<(u32, u64)> = dests.iter().map(|(c, cnt)| (rid(c), *cnt)).collect();
            for (_, cnt) in &v {
                sum += *cnt;
            }
            v.sort();
            totals[pid] = sum;
            row_pairs[pid] = v;
        }
        for t in &totals {
            write_uvarint(&mut out, *t);
        }
        let mut row_off = vec![0u32];
        let mut pairs = Vec::new();
        for v in &row_pairs {
            for (cid, cnt) in v {
                write_uvarint(&mut pairs, u64::from(*cid));
                write_uvarint(&mut pairs, *cnt);
            }
            row_off.push(row_off.last().unwrap() + v.len() as u32);
        }
        for o in &row_off {
            write_uvarint(&mut out, u64::from(*o));
        }
        out.extend_from_slice(&pairs);
        out
    }
}

#[cfg(test)]
impl TransitionMatrix {
    /// Build a matrix from `(prev, &[(curr, count)])` rows by encoding to FSM1
    /// then decoding — exercises the real parser. Used by `scorer.rs` tests.
    pub fn from_counts(version: &str, vocab_size: u32, rows: &[(&str, &[(&str, u64)])]) -> Self {
        let bytes = test_support::encode(version, vocab_size, rows);
        parse_fsm1(&bytes).expect("test fixture must encode+parse")
    }

    /// Override a route's row_total (tests simulate specific probabilities where
    /// row_total ≠ the count-sum, mirroring the old `row_totals.insert`).
    pub fn set_row_total(&mut self, path: &str, val: u64) {
        if let Some(id) = self.route_id(path) {
            self.row_total[id as usize] = val;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CROSS-LANGUAGE CONTRACT: byte-identical to `SMALL_MATRIX_FSM1_HEX` in
    /// `tests/scoring/test_matrix.py::test_serialize_kv_byte_exact`. If the FSM1
    /// wire format changes, both fixtures update together — or a build breaks.
    const CROSS_LANG_FIXTURE_HEX: &str = "46534d310103000000030000000b00746573742d66736d312d6100000000050000000a000000130000002f636172742f686f6d652f70726f6475637473001402000002030005020f0002";

    #[test]
    fn parse_fsm1_cross_lang_fixture() {
        let bytes = hex::decode(CROSS_LANG_FIXTURE_HEX).unwrap();
        let m = parse_fsm1(&bytes).expect("fixture must parse");
        assert_eq!(m.vocab_size(), 3);
        assert_eq!(m.version(), "test-fsm1-a");

        let cart = m.route_id("/cart").unwrap();
        let home = m.route_id("/home").unwrap();
        let products = m.route_id("/products").unwrap();
        // Routes sort by bytes: /cart=0, /home=1, /products=2.
        assert_eq!((cart, home, products), (0, 1, 2));

        assert_eq!(m.row_total(home), 20);
        assert_eq!(m.row_total(products), 2);
        assert_eq!(m.row_total(cart), 0); // /cart is curr-only

        assert_eq!(m.transition_count(home, products), 15);
        assert_eq!(m.transition_count(home, cart), 5);
        assert_eq!(m.transition_count(products, cart), 2);
        assert_eq!(m.transition_count(home, home), 0); // pair never seen
        assert_eq!(m.route_id("/nope"), None);
        assert!(m.has_rows());
    }

    #[test]
    fn from_counts_round_trips() {
        let mut m = TransitionMatrix::from_counts(
            "test-v1",
            10,
            &[("/home", &[("/products", 15), ("/cart", 5)])],
        );
        let home = m.route_id("/home").unwrap();
        let products = m.route_id("/products").unwrap();
        assert_eq!(m.vocab_size(), 10);
        assert_eq!(m.row_total(home), 20); // sum of the row
        assert_eq!(m.transition_count(home, products), 15);

        m.set_row_total("/home", 9999);
        assert_eq!(m.row_total(home), 9999);
    }

    #[test]
    fn empty_buffer_parses_to_none() {
        // KV miss → empty bytes → parse fails → default (vocab 0) → load None.
        assert!(parse_fsm1(&[]).is_none());
        let def = TransitionMatrix::default();
        assert_eq!(def.vocab_size(), 0);
        assert!(!def.has_rows());
        assert_eq!(def.route_id("/x"), None);
    }

    #[test]
    fn malformed_bytes_never_panic() {
        // Truncated / garbage inputs must return None, not panic.
        assert!(parse_fsm1(b"FSM1").is_none());
        assert!(parse_fsm1(b"FSM1\x01\x00").is_none());
        assert!(parse_fsm1(b"XXXX\x01").is_none());
        let bytes = hex::decode(CROSS_LANG_FIXTURE_HEX).unwrap();
        for cut in 0..bytes.len() {
            // Any prefix-truncation parses to None or a value, never panics.
            let _ = parse_fsm1(&bytes[..cut]);
        }
    }
}
