//! Mergeable latency histograms for summaries: [`BUCKETS`] log-spaced buckets over
//! `[100 µs, 60 s)` (about 11% wide, so a quantile read at a bucket's geometric middle is within
//! about ±5.3%), plus underflow and overflow counts and the exact min and max. Values are integer
//! microseconds. Merging adds counts, so any split of the rows merges to the same histogram.
//!
//! Blob: `[1, varint under, varint over, zigzag min, zigzag max, (u8 bucket, varint count)...]`
//! with buckets ascending and only non-zero ones present; an empty histogram is not written.
//! `frontend/lib/cloud-engine/dashboard/histogram.ts` reads the same bytes.

pub const VERSION: u8 = 1;
pub const BUCKETS: usize = 128;
/// Bucket `i` holds `[BOUNDS[i], BOUNDS[i + 1])`: `round(100 * 600000^(i / 128))` µs.
pub const BOUNDS: [i64; BUCKETS + 1] = [
    100, 111, 123, 137, 152, 168, 187, 207, 230, 255, 283, 314, 348, 386, 429, 475, 528, 585, 649,
    721, 800, 887, 984, 1092, 1212, 1344, 1492, 1655, 1836, 2038, 2261, 2508, 2783, 3088, 3426,
    3802, 4218, 4680, 5193, 5761, 6393, 7093, 7870, 8732, 9688, 10749, 11927, 13233, 14683, 16291,
    18076, 20055, 22252, 24690, 27394, 30395, 33724, 37418, 41517, 46065, 51110, 56709, 62920,
    69813, 77460, 85944, 95358, 105804, 117393, 130252, 144520, 160350, 177914, 197402, 219025,
    243016, 269636, 299171, 331941, 368301, 408643, 453405, 503070, 558174, 619315, 687153, 762422,
    845935, 938597, 1041408, 1155480, 1282048, 1422480, 1578294, 1751176, 1942995, 2155825,
    2391967, 2653976, 2944685, 3267237, 3625121, 4022206, 4462786, 4951627, 5494013, 6095811,
    6763528, 7504385, 8326393, 9238442, 10250393, 11373191, 12618977, 14001223, 15534875, 17236519,
    19124557, 21219404, 23543715, 26122624, 28984019, 32158843, 35681427, 39589865, 43926422,
    48737993, 54076608, 60000000,
];
const UNDER: u8 = 254;
const OVER: u8 = 255;
/// `min`/`max` are clamped to `±LIMIT` so their zigzag fits a JS number exactly; slots are not.
const LIMIT: i64 = (1 << 52) - 1;

/// The bucket of `v`, or underflow/overflow.
#[inline]
pub fn slot(v: i64) -> u8 {
    if v < BOUNDS[0] {
        UNDER
    } else if v >= BOUNDS[BUCKETS] {
        OVER
    } else {
        (BOUNDS.partition_point(|&b| b <= v) - 1) as u8
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hist {
    pub under: u64,
    pub over: u64,
    pub min: i64,
    pub max: i64,
    /// Ascending, non-zero.
    pub buckets: Vec<(u8, u64)>,
}

impl Default for Hist {
    fn default() -> Self {
        Self {
            under: 0,
            over: 0,
            min: i64::MAX,
            max: i64::MIN,
            buckets: Vec::new(),
        }
    }
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

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Reads a [`Hist::to_bytes`] blob into `buckets`: `([under, over], [min, max], buckets used)`,
/// or `None` unless it is a well-formed, non-empty histogram.
fn parse(blob: &[u8], buckets: &mut [(u8, u64); BUCKETS]) -> Option<([u64; 2], [i64; 2], usize)> {
    if blob.first() != Some(&VERSION) {
        return None;
    }
    let mut at = 1;
    let counts = [get_varint(blob, &mut at)?, get_varint(blob, &mut at)?];
    let range = [
        unzigzag(get_varint(blob, &mut at)?),
        unzigzag(get_varint(blob, &mut at)?),
    ];
    let mut n = 0;
    while at < blob.len() {
        let s = blob[at];
        at += 1;
        let c = get_varint(blob, &mut at)?;
        if usize::from(s) >= BUCKETS || c == 0 || (n > 0 && buckets[n - 1].0 >= s) {
            return None;
        }
        buckets[n] = (s, c);
        n += 1;
    }
    ((n > 0 || counts != [0, 0]) && range[0] <= range[1]).then_some((counts, range, n))
}

impl Hist {
    pub fn add(&mut self, v: i64) {
        self.add_slot(v, slot(v));
    }

    /// Adds `v`, whose [`slot`] the caller already knows.
    #[inline]
    pub fn add_slot(&mut self, v: i64, s: u8) {
        let v = v.clamp(-LIMIT, LIMIT);
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.add_count(s, 1);
    }

    #[inline]
    fn add_count(&mut self, s: u8, n: u64) {
        match s {
            UNDER => self.under = self.under.saturating_add(n),
            OVER => self.over = self.over.saturating_add(n),
            _ => match self.buckets.binary_search_by_key(&s, |b| b.0) {
                Ok(at) => self.buckets[at].1 = self.buckets[at].1.saturating_add(n),
                Err(at) => self.buckets.insert(at, (s, n)),
            },
        }
    }

    pub fn count(&self) -> u64 {
        self.buckets
            .iter()
            .fold(self.under.saturating_add(self.over), |a, b| {
                a.saturating_add(b.1)
            })
    }

    pub fn is_empty(&self) -> bool {
        self.under == 0 && self.over == 0 && self.buckets.is_empty()
    }

    pub fn merge(&mut self, other: &Hist) {
        if !other.is_empty() {
            self.merge_parts(
                [other.under, other.over],
                [other.min, other.max],
                &other.buckets,
            );
        }
    }

    /// Merges a blob from [`Hist::to_bytes`] without decoding it into a [`Hist`]; false (and no
    /// change) if it is not one.
    pub fn merge_bytes(&mut self, blob: &[u8]) -> bool {
        let mut buckets = [(0, 0); BUCKETS];
        let Some((counts, range, n)) = parse(blob, &mut buckets) else {
            return false;
        };
        self.merge_parts(counts, range, &buckets[..n]);
        true
    }

    fn merge_parts(&mut self, [under, over]: [u64; 2], [min, max]: [i64; 2], theirs: &[(u8, u64)]) {
        self.min = self.min.min(min);
        self.max = self.max.max(max);
        self.under = self.under.saturating_add(under);
        self.over = self.over.saturating_add(over);
        // Both sides ascend: count the new slots, grow once, then merge from the back in place.
        let mine = &self.buckets;
        let (mut a, mut fresh) = (0, 0);
        for &(s, _) in theirs {
            while a < mine.len() && mine[a].0 < s {
                a += 1;
            }
            if a == mine.len() || mine[a].0 != s {
                fresh += 1;
            }
        }
        let (mut a, mut b) = (self.buckets.len(), theirs.len());
        self.buckets.resize(a + fresh, (0, 0));
        let mut at = self.buckets.len();
        while b > 0 {
            at -= 1;
            let y = theirs[b - 1];
            match a.checked_sub(1).map(|i| self.buckets[i]) {
                Some(x) if x.0 > y.0 => {
                    self.buckets[at] = x;
                    a -= 1;
                }
                Some(x) if x.0 == y.0 => {
                    self.buckets[at] = (x.0, x.1.saturating_add(y.1));
                    a -= 1;
                    b -= 1;
                }
                _ => {
                    self.buckets[at] = y;
                    b -= 1;
                }
            }
        }
    }

    /// `None` when empty: an empty histogram is written as null.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        if self.is_empty() {
            return None;
        }
        let mut out = Vec::with_capacity(12 + self.buckets.len() * 3);
        out.push(VERSION);
        put_varint(&mut out, self.under);
        put_varint(&mut out, self.over);
        put_varint(&mut out, zigzag(self.min));
        put_varint(&mut out, zigzag(self.max));
        for &(s, n) in &self.buckets {
            out.push(s);
            put_varint(&mut out, n);
        }
        Some(out)
    }

    pub fn from_bytes(blob: &[u8]) -> Option<Self> {
        let mut buckets = [(0, 0); BUCKETS];
        let ([under, over], [min, max], n) = parse(blob, &mut buckets)?;
        Some(Hist {
            under,
            over,
            min,
            max,
            buckets: buckets[..n].to_vec(),
        })
    }

    /// The `q` quantile (nearest rank), read at its bucket's geometric middle and clamped to
    /// `[min, max]`; underflow reads as `min` and overflow as `max`.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let n = self.count();
        if n == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * n as f64).ceil() as u64).clamp(1, n);
        let mut seen = self.under;
        if rank <= seen {
            return Some(self.min as f64);
        }
        for &(s, c) in &self.buckets {
            seen += c;
            if rank <= seen {
                let (lo, hi) = (BOUNDS[s as usize] as f64, BOUNDS[s as usize + 1] as f64);
                return Some((lo * hi).sqrt().clamp(self.min as f64, self.max as f64));
            }
        }
        Some(self.max as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_follow_the_bounds() {
        assert_eq!(slot(-5), UNDER);
        assert_eq!(slot(99), UNDER);
        assert_eq!(slot(100), 0);
        assert_eq!(slot(110), 0);
        assert_eq!(slot(111), 1);
        assert_eq!(slot(59_999_999), 127);
        assert_eq!(slot(60_000_000), OVER);
        for i in 0..BUCKETS {
            assert_eq!(slot(BOUNDS[i]), i as u8);
            assert_eq!(slot(BOUNDS[i + 1] - 1), i as u8);
            assert!(BOUNDS[i + 1] as f64 / BOUNDS[i] as f64 <= 1.12);
        }
    }

    #[test]
    fn merging_any_split_gives_the_same_histogram_and_bytes() {
        let values: Vec<i64> = (0..5000i64)
            .map(|i| (i * 15_485_863) % 70_000_000 - 50)
            .collect();
        let mut whole = Hist::default();
        values.iter().for_each(|&v| whole.add(v));
        for split in [1, 17, 2500, 4999] {
            let (a, b) = values.split_at(split);
            let mut x = Hist::default();
            a.iter().for_each(|&v| x.add(v));
            let mut y = Hist::default();
            b.iter().for_each(|&v| y.add(v));
            let mut merged = Hist::from_bytes(&y.to_bytes().unwrap()).unwrap();
            merged.merge(&x);
            assert_eq!(merged, whole);
            assert_eq!(merged.to_bytes(), whole.to_bytes());
        }
        assert_eq!(whole.count(), 5000);
        assert_eq!(whole.min, -50);
        assert_eq!(whole.max, *values.iter().max().unwrap());
        assert!(whole.under > 0 && whole.over > 0);
    }

    #[test]
    fn merging_bytes_matches_merging_histograms_and_rejects_bad_blobs() {
        let parts: Vec<Hist> = (0..6i64)
            .map(|p| {
                let mut h = Hist::default();
                (0..300i64).for_each(|i| h.add((i * 7_919 + p * 104_729) % (2_000 * (p + 1)) * 31));
                h
            })
            .collect();
        let mut by_value = Hist::default();
        let mut by_bytes = Hist::default();
        for h in &parts {
            by_value.merge(h);
            assert!(by_bytes.merge_bytes(&h.to_bytes().unwrap()));
            assert_eq!(by_bytes, by_value);
        }
        let before = by_bytes.clone();
        let bytes = parts[0].to_bytes().unwrap();
        assert!(!by_bytes.merge_bytes(&bytes[..bytes.len() - 1]));
        assert!(!by_bytes.merge_bytes(&[VERSION, 0, 0, 0, 0]));
        assert_eq!(by_bytes, before);
    }

    #[test]
    fn matches_the_reference_vector() {
        // Shared with frontend/lib/cloud-engine/dashboard/histogram.test.ts.
        let mut h = Hist::default();
        [50, 100, 150, 1_000, 1_000, 70_000_000]
            .iter()
            .for_each(|&v| h.add(v));
        assert_eq!(
            hex::encode(h.to_bytes().unwrap()),
            "0101016480f6e042000103011602"
        );
        assert_eq!(h.quantile(0.5), Some((137f64 * 152.0).sqrt()));
        assert_eq!(h.quantile(0.0), Some(50.0));
        assert_eq!(h.quantile(1.0), Some(70_000_000.0));
        // Saturated garbage keeps min and max within what a JS number holds exactly.
        let mut wild = Hist::default();
        wild.add(i64::MIN);
        wild.add(i64::MAX);
        assert_eq!(
            hex::encode(wild.to_bytes().unwrap()),
            "010101fdffffffffffff0ffeffffffffffff0f"
        );
    }

    #[test]
    fn quantiles_stay_within_the_bucket_error() {
        let mut h = Hist::default();
        let values: Vec<i64> = (1..=10_000).map(|i| 1_000 + i * 37).collect();
        values.iter().for_each(|&v| h.add(v));
        for q in [0.1, 0.5, 0.95, 0.99] {
            let exact = values[((q * values.len() as f64).ceil() as usize).max(1) - 1] as f64;
            let got = h.quantile(q).unwrap();
            assert!((got - exact).abs() / exact < 0.055, "{q}: {got} vs {exact}");
        }
        assert_eq!(h.quantile(0.0), Some(1_037.0), "clamped to the exact min");
        let mut tiny = Hist::default();
        tiny.add(5);
        tiny.add(90_000_000);
        assert_eq!(tiny.quantile(0.5), Some(5.0));
        assert_eq!(tiny.quantile(1.0), Some(90_000_000.0));
        assert_eq!(Hist::default().quantile(0.5), None);
        assert_eq!(Hist::default().to_bytes(), None);
    }

    #[test]
    fn bytes_reject_malformed_blobs() {
        let mut h = Hist::default();
        h.add(150);
        h.add(150);
        h.add(4_000);
        let bytes = h.to_bytes().unwrap();
        assert_eq!(Hist::from_bytes(&bytes), Some(h));
        assert!(Hist::from_bytes(&bytes[..bytes.len() - 1]).is_none());
        assert!(Hist::from_bytes(&[2]).is_none());
        assert!(Hist::from_bytes(&[VERSION, 0, 0, 0, 0]).is_none());
        let mut swapped = bytes.clone();
        swapped.extend([3, 1]);
        assert!(Hist::from_bytes(&swapped).is_none(), "buckets must ascend");
    }
}
