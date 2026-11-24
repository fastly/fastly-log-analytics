//! HyperLogLog++ distinct counts for summaries: precision 12, sparse below
//! [`SPARSE_MAX`] entries (precision 25, near-exact) and dense above, md5 hash as in fla
//! `backend/utils/hll.py`.
//!
//! A sparse entry is `idx25 << 6 | r`, where `idx25` is the hash's top 25 bits and `r` the rank
//! of its low 39 bits (1..=39). It converts to exactly the dense register a direct insert would
//! set, so merging is order-independent and gives the same bytes whichever path a value took.
//!
//! Blob: `[2, 12, 0, varint n, varint entry deltas...]` (sparse) or `[2, 12, 1, 4096 registers]`
//! (dense). `frontend/lib/cloud-engine/dashboard/hll.ts` reads and writes the same bytes.

use md5::{Digest, Md5};

pub const VERSION: u8 = 2;
pub const P: u8 = 12;
pub const M: usize = 1 << P;
/// Sparse precision.
pub const SP: u32 = 25;
/// Sparse entries above this convert to dense (4 KiB either way in memory).
pub const SPARSE_MAX: usize = 1024;
const LOW: u32 = 64 - SP;
const KIND_SPARSE: u8 = 0;
const KIND_DENSE: u8 = 1;

/// fla `backend/utils/hll.py`: md5 of the UTF-8 item, first 8 bytes little-endian.
pub fn hash(item: &str) -> u64 {
    let digest = Md5::digest(item.as_bytes());
    u64::from_le_bytes(digest[..8].try_into().expect("md5 is 16 bytes"))
}

/// The sparse entry of a hash.
#[inline]
pub fn entry(h: u64) -> u32 {
    let idx = (h >> LOW) as u32;
    let bits = 64 - (h & ((1u64 << LOW) - 1)).leading_zeros();
    let r = if bits <= 1 { LOW } else { LOW + 1 - bits };
    (idx << 6) | r
}

#[inline]
fn idx25(e: u32) -> u32 {
    e >> 6
}

/// Dense `(register, rank)` of a sparse entry, as fla's rank `min(lz + 1, 64 - p)` would give.
#[inline]
fn dense(e: u32) -> (usize, u8) {
    let register = (e >> (6 + SP - u32::from(P))) as usize;
    let w = idx25(e) & ((1 << (SP - u32::from(P))) - 1);
    let rank = if w != 0 {
        (SP - u32::from(P) + 1) - (32 - w.leading_zeros())
    } else {
        (SP - u32::from(P)) + (e & 63)
    };
    (register, rank as u8)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Hll {
    /// Sorted entries, one per `idx25`, keeping its highest rank.
    Sparse(Vec<u32>),
    Dense(Box<[u8; M]>),
}

impl Default for Hll {
    fn default() -> Self {
        Self::Sparse(Vec::new())
    }
}

/// Sorts and keeps one entry per `idx25`, the highest.
fn canonical(entries: &mut Vec<u32>) {
    entries.sort_unstable();
    entries.dedup_by(|later, kept| {
        if idx25(*later) == idx25(*kept) {
            *kept = *later;
            true
        } else {
            false
        }
    });
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(blob: &[u8], at: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let b = *blob.get(*at)?;
        *at += 1;
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

impl Hll {
    /// A sketch of `entries` (any order, duplicates allowed).
    pub fn from_entries(mut entries: Vec<u32>) -> Self {
        canonical(&mut entries);
        let mut hll = Self::Sparse(entries);
        hll.densify_if_full();
        hll
    }

    pub fn add_hash(&mut self, h: u64) {
        self.add_entry(entry(h));
    }

    pub fn add_entry(&mut self, e: u32) {
        match self {
            Self::Dense(regs) => {
                let (r, rank) = dense(e);
                regs[r] = regs[r].max(rank);
            }
            Self::Sparse(entries) => {
                match entries.binary_search_by_key(&idx25(e), |x| idx25(*x)) {
                    Ok(at) => entries[at] = entries[at].max(e),
                    Err(at) => entries.insert(at, e),
                }
                self.densify_if_full();
            }
        }
    }

    fn densify_if_full(&mut self) {
        if let Self::Sparse(entries) = self {
            if entries.len() > SPARSE_MAX {
                let mut regs = Box::new([0u8; M]);
                for &e in entries.iter() {
                    let (r, rank) = dense(e);
                    regs[r] = regs[r].max(rank);
                }
                *self = Self::Dense(regs);
            }
        }
    }

    pub fn is_dense(&self) -> bool {
        matches!(self, Self::Dense(_))
    }

    pub fn merge(&mut self, other: &Hll) {
        match other {
            Self::Sparse(theirs) => self.merge_entries(theirs),
            Self::Dense(theirs) => self.merge_registers(theirs),
        }
    }

    fn merge_registers(&mut self, theirs: &[u8; M]) {
        if let Self::Sparse(entries) = self {
            let mut regs = Box::new(*theirs);
            for &e in entries.iter() {
                let (r, rank) = dense(e);
                regs[r] = regs[r].max(rank);
            }
            *self = Self::Dense(regs);
            return;
        }
        let Self::Dense(regs) = self else {
            unreachable!()
        };
        for (a, b) in regs.iter_mut().zip(theirs.iter()) {
            *a = (*a).max(*b);
        }
    }

    /// Merges sorted canonical entries.
    fn merge_entries(&mut self, theirs: &[u32]) {
        match self {
            Self::Dense(regs) => {
                for &e in theirs {
                    let (r, rank) = dense(e);
                    regs[r] = regs[r].max(rank);
                }
            }
            Self::Sparse(mine) => {
                if theirs.is_empty() {
                    return;
                }
                // Both sides ascend by idx25: count the new ones, grow once, merge from the back.
                let (mut a, mut fresh) = (0, 0);
                for &y in theirs {
                    while a < mine.len() && idx25(mine[a]) < idx25(y) {
                        a += 1;
                    }
                    if a == mine.len() || idx25(mine[a]) != idx25(y) {
                        fresh += 1;
                    }
                }
                let (mut a, mut b) = (mine.len(), theirs.len());
                mine.resize(a + fresh, 0);
                let mut at = mine.len();
                while b > 0 {
                    at -= 1;
                    let y = theirs[b - 1];
                    match a.checked_sub(1).map(|i| mine[i]) {
                        Some(x) if idx25(x) > idx25(y) => {
                            mine[at] = x;
                            a -= 1;
                        }
                        Some(x) if idx25(x) == idx25(y) => {
                            mine[at] = x.max(y);
                            a -= 1;
                            b -= 1;
                        }
                        _ => {
                            mine[at] = y;
                            b -= 1;
                        }
                    }
                }
                self.densify_if_full();
            }
        }
    }

    /// Merges a blob from [`Hll::to_bytes`]; false (and no change) if it is not one.
    pub fn merge_bytes(&mut self, blob: &[u8]) -> bool {
        match blob {
            [VERSION, P, KIND_DENSE, regs @ ..] if regs.len() == M => {
                self.merge_registers(regs.try_into().expect("length checked"));
                true
            }
            [VERSION, P, KIND_SPARSE, ..] => match Self::from_bytes(blob) {
                Some(Self::Sparse(entries)) => {
                    self.merge_entries(&entries);
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Sparse(entries) => {
                let mut out = Vec::with_capacity(4 + entries.len() * 3);
                out.extend([VERSION, P, KIND_SPARSE]);
                put_varint(&mut out, entries.len() as u64);
                let mut prev = 0;
                for &e in entries {
                    put_varint(&mut out, u64::from(e - prev));
                    prev = e;
                }
                out
            }
            Self::Dense(regs) => {
                let mut out = Vec::with_capacity(3 + M);
                out.extend([VERSION, P, KIND_DENSE]);
                out.extend_from_slice(&regs[..]);
                out
            }
        }
    }

    pub fn from_bytes(blob: &[u8]) -> Option<Self> {
        match blob {
            [VERSION, P, KIND_DENSE, regs @ ..] if regs.len() == M => {
                Some(Self::Dense(Box::new(regs.try_into().ok()?)))
            }
            [VERSION, P, KIND_SPARSE, ..] => {
                let mut at = 3;
                let n = usize::try_from(get_varint(blob, &mut at)?).ok()?;
                if n > SPARSE_MAX {
                    return None;
                }
                let mut entries = Vec::with_capacity(n);
                let mut prev = 0u64;
                for i in 0..n {
                    let delta = get_varint(blob, &mut at)?;
                    if i > 0 && delta == 0 {
                        return None;
                    }
                    prev = prev.checked_add(delta)?;
                    let e = u32::try_from(prev).ok().filter(|&e| idx25(e) < 1 << SP)?;
                    if e & 63 == 0
                        || e & 63 > LOW
                        || entries.last().is_some_and(|&l| idx25(l) == idx25(e))
                    {
                        return None;
                    }
                    entries.push(e);
                }
                (at == blob.len()).then_some(Self::Sparse(entries))
            }
            _ => None,
        }
    }

    pub fn count(&self) -> u64 {
        let estimate = match self {
            Self::Sparse(entries) => {
                let m = f64::from(1u32 << SP);
                m * (m / (m - entries.len() as f64)).ln()
            }
            Self::Dense(regs) => {
                let m = M as f64;
                let sum: f64 = regs.iter().map(|&r| 2f64.powi(-i32::from(r))).sum();
                let mut estimate = 0.7213 / (1.0 + 1.079 / m) * m * m / sum;
                if estimate <= 2.5 * m {
                    let zeros = regs.iter().filter(|&&r| r == 0).count();
                    if zeros > 0 {
                        estimate = m * (m / zeros as f64).ln();
                    }
                }
                estimate
            }
        };
        compute_common::num::round_ties_even(estimate) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direct(hashes: &[u64]) -> [u8; M] {
        let mut regs = [0u8; M];
        for &h in hashes {
            let width = 64 - u32::from(P);
            let r = (h >> width) as usize;
            let bits = 64 - (h & ((1u64 << width) - 1)).leading_zeros();
            let rank = if bits <= 1 { width } else { width + 1 - bits } as u8;
            regs[r] = regs[r].max(rank);
        }
        regs
    }

    fn hashes(n: usize, seed: u64) -> Vec<u64> {
        (0..n).map(|i| hash(&format!("{seed}-{i}"))).collect()
    }

    #[test]
    fn sparse_converts_to_the_registers_a_direct_insert_sets() {
        let mut edge = vec![
            0,
            1,
            u64::MAX,
            1u64 << 39,
            (1u64 << 39) - 1,
            0xfff0_0000_0000_0001,
            0x0008_0000_0000_0000,
        ];
        edge.extend(hashes(3000, 1));
        let mut hll = Hll::default();
        for &h in &edge {
            hll.add_hash(h);
        }
        assert_eq!(hll, Hll::Dense(Box::new(direct(&edge))));
    }

    #[test]
    fn merge_is_order_independent_across_sparse_and_dense() {
        let parts: Vec<Vec<u64>> = [10, 900, 300, 2000, 1]
            .iter()
            .enumerate()
            .map(|(i, &n)| hashes(n, i as u64))
            .collect();
        let sketch = |hs: &[u64]| {
            let mut h = Hll::default();
            hs.iter().for_each(|&x| h.add_hash(x));
            h
        };
        let all: Vec<u64> = parts.concat();
        let mut want = Hll::default();
        all.iter().for_each(|&x| want.add_hash(x));
        for order in [
            [0, 1, 2, 3, 4],
            [4, 3, 2, 1, 0],
            [3, 0, 4, 2, 1],
            [2, 1, 0, 4, 3],
        ] {
            let mut got = Hll::default();
            for i in order {
                assert!(got.merge_bytes(&sketch(&parts[i]).to_bytes()));
            }
            assert_eq!(got.to_bytes(), want.to_bytes(), "{order:?}");
        }
        // Two sparse halves whose union passes the threshold become dense.
        let mut a = sketch(&parts[1]);
        a.merge(&sketch(&parts[2]));
        assert!(a.is_dense());
        assert_eq!(a, sketch(&[parts[1].clone(), parts[2].clone()].concat()));
    }

    #[test]
    fn small_sets_count_exactly_and_large_ones_within_error() {
        for n in [0, 1, 2, 14, 177, 1000] {
            let mut h = Hll::default();
            hashes(n, 7).into_iter().for_each(|x| h.add_hash(x));
            assert!(!h.is_dense());
            assert_eq!(h.count(), n as u64);
        }
        for n in [2_000, 20_000, 200_000] {
            let mut h = Hll::default();
            hashes(n, 9).into_iter().for_each(|x| h.add_hash(x));
            let err = (h.count() as f64 - n as f64).abs() / n as f64;
            assert!(err < 0.05, "{n}: {}", h.count());
        }
    }

    #[test]
    fn bytes_round_trip_and_reject_foreign_blobs() {
        for n in [0, 5, 1500] {
            let mut h = Hll::default();
            hashes(n, 3).into_iter().for_each(|x| h.add_hash(x));
            let bytes = h.to_bytes();
            assert_eq!(Hll::from_bytes(&bytes), Some(h.clone()));
        }
        let mut h = Hll::default();
        h.add_hash(hash("192.0.2.1"));
        let bytes = h.to_bytes();
        assert!(bytes.len() <= 3 + 1 + 5);
        let mut v1 = vec![1, 8];
        v1.extend([0u8; 256]);
        assert!(Hll::from_bytes(&v1).is_none());
        let mut target = h.clone();
        assert!(!target.merge_bytes(&v1));
        assert_eq!(target, h);
        assert!(Hll::from_bytes(&bytes[..bytes.len() - 1]).is_none());
        assert!(Hll::from_bytes(&[VERSION, P, KIND_DENSE, 0]).is_none());
        let mut wide = vec![VERSION, P, KIND_SPARSE, 1];
        put_varint(&mut wide, u64::from(u32::MAX - 62));
        assert!(Hll::from_bytes(&wide).is_none(), "idx25 past 25 bits");
        assert!(!target.merge_bytes(&wide));
    }
}
