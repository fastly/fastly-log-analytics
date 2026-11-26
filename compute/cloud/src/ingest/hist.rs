//! Mergeable latency histograms for summaries: [`BUCKETS`] log-spaced buckets over
//! `[100 µs, 60 s)` (~2% wide, matching DDSketch `GAMMA = 1.02` so interior quantiles read
//! at a bucket's geometric middle are within ~1%, and exact `rank == 1` / `rank == n` read
//! the exact `min` / `max`), plus underflow and overflow counts. Values are integer microseconds.
//! Merging adds counts, so any split of the rows merges to the same histogram.
//!
//! V2 blob: `[2, varint under, varint over, zigzag min, zigzag max, (varint bucket, varint count)...]`
//! V1 blob (legacy, decoded on read): `[1, varint under, varint over, zigzag min, zigzag max, (u8 bucket, varint count)...]`
//! with buckets ascending and only non-zero ones present; an empty histogram is not written.
//! `frontend/lib/cloud-engine/dashboard/histogram.ts` reads and writes the same bytes.

pub const VERSION: u8 = 2;
pub const BUCKETS: usize = 672;
const VERSION_V1: u8 = 1;
const BUCKETS_V1: usize = 128;

/// Legacy v1 bounds: `round(100 * 600000^(i / 128))` µs, decoded on read for backward compatibility.
const BOUNDS_V1: [i64; BUCKETS_V1 + 1] = [
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

/// Bucket `i` holds `[BOUNDS[i], BOUNDS[i + 1])`: `round(100 * 600000^(i / 672))` µs.
pub const BOUNDS: [i64; BUCKETS + 1] = [
    100, 102, 104, 106, 108, 110, 113, 115, 117, 120, 122, 124, 127, 129, 132, 135, 137, 140, 143,
    146, 149, 152, 155, 158, 161, 164, 167, 171, 174, 178, 181, 185, 188, 192, 196, 200, 204, 208,
    212, 216, 221, 225, 230, 234, 239, 244, 249, 254, 259, 264, 269, 274, 280, 286, 291, 297, 303,
    309, 315, 322, 328, 335, 341, 348, 355, 362, 369, 377, 384, 392, 400, 408, 416, 424, 433, 441,
    450, 459, 468, 478, 487, 497, 507, 517, 528, 538, 549, 560, 571, 582, 594, 606, 618, 630, 643,
    656, 669, 682, 696, 710, 724, 739, 753, 768, 784, 800, 816, 832, 848, 865, 883, 900, 918, 937,
    955, 975, 994, 1014, 1034, 1055, 1076, 1098, 1119, 1142, 1165, 1188, 1212, 1236, 1261, 1286,
    1312, 1338, 1365, 1392, 1420, 1448, 1477, 1507, 1537, 1567, 1599, 1631, 1663, 1697, 1731, 1765,
    1800, 1836, 1873, 1911, 1949, 1988, 2028, 2068, 2109, 2152, 2195, 2238, 2283, 2329, 2375, 2423,
    2471, 2521, 2571, 2623, 2675, 2729, 2783, 2839, 2896, 2953, 3013, 3073, 3134, 3197, 3261, 3326,
    3393, 3460, 3530, 3600, 3672, 3746, 3820, 3897, 3975, 4054, 4135, 4218, 4302, 4388, 4476, 4566,
    4657, 4750, 4845, 4942, 5041, 5141, 5244, 5349, 5456, 5565, 5677, 5790, 5906, 6024, 6144, 6267,
    6393, 6520, 6651, 6784, 6919, 7058, 7199, 7343, 7490, 7639, 7792, 7948, 8107, 8269, 8434, 8603,
    8775, 8950, 9129, 9312, 9498, 9688, 9882, 10079, 10281, 10487, 10696, 10910, 11128, 11351,
    11578, 11809, 12045, 12286, 12532, 12783, 13038, 13299, 13565, 13836, 14113, 14395, 14683,
    14976, 15276, 15581, 15893, 16211, 16535, 16865, 17203, 17547, 17897, 18255, 18620, 18993,
    19372, 19760, 20155, 20558, 20969, 21388, 21816, 22252, 22697, 23151, 23614, 24086, 24568,
    25059, 25560, 26071, 26593, 27124, 27667, 28220, 28784, 29360, 29947, 30546, 31156, 31779,
    32415, 33063, 33724, 34399, 35086, 35788, 36504, 37233, 37978, 38737, 39512, 40302, 41108,
    41930, 42768, 43624, 44496, 45386, 46293, 47219, 48163, 49126, 50108, 51110, 52132, 53175,
    54238, 55323, 56429, 57557, 58708, 59882, 61079, 62301, 63546, 64817, 66113, 67435, 68784,
    70159, 71562, 72993, 74452, 75941, 77460, 79009, 80588, 82200, 83843, 85520, 87230, 88974,
    90753, 92568, 94419, 96307, 98233, 100197, 102201, 104244, 106329, 108455, 110624, 112836,
    115092, 117393, 119741, 122135, 124577, 127068, 129609, 132201, 134844, 137540, 140291, 143096,
    145957, 148876, 151853, 154889, 157986, 161145, 164368, 167654, 171007, 174426, 177914, 181472,
    185100, 188801, 192577, 196427, 200355, 204362, 208448, 212616, 216867, 221204, 225627, 230139,
    234741, 239434, 244222, 249106, 254087, 259167, 264350, 269636, 275027, 280527, 286136, 291858,
    297694, 303646, 309718, 315911, 322228, 328671, 335243, 341947, 348784, 355759, 362872, 370128,
    377529, 385078, 392778, 400632, 408643, 416815, 425149, 433650, 442322, 451166, 460188, 469390,
    478776, 488349, 498114, 508074, 518234, 528596, 539166, 549947, 560944, 572161, 583602, 595271,
    607174, 619315, 631699, 644330, 657214, 670356, 683760, 697433, 711379, 725603, 740112, 754912,
    770007, 785404, 801109, 817128, 833467, 850133, 867132, 884471, 902157, 920196, 938597, 957365,
    976508, 996034, 1015951, 1036266, 1056987, 1078122, 1099680, 1121670, 1144098, 1166976,
    1190310, 1214112, 1238389, 1263152, 1288410, 1314173, 1340451, 1367254, 1394594, 1422480,
    1450924, 1479937, 1509529, 1539714, 1570502, 1601905, 1633937, 1666609, 1699934, 1733926,
    1768598, 1803962, 1840034, 1876828, 1914356, 1952636, 1991681, 2031506, 2072128, 2113562,
    2155825, 2198932, 2242902, 2287751, 2333497, 2380157, 2427751, 2476296, 2525812, 2576318,
    2627833, 2680379, 2733976, 2788644, 2844406, 2901283, 2959296, 3018470, 3078827, 3140391,
    3203186, 3267237, 3332569, 3399206, 3467177, 3536506, 3607222, 3679352, 3752924, 3827967,
    3904511, 3982585, 4062221, 4143448, 4226301, 4310809, 4397008, 4484930, 4574611, 4666084,
    4759387, 4854555, 4951627, 5050639, 5151631, 5254643, 5359714, 5466887, 5576202, 5687704,
    5801435, 5917440, 6035764, 6156455, 6279559, 6405125, 6533201, 6663839, 6797088, 6933003,
    7071634, 7213038, 7357270, 7504385, 7654442, 7807500, 7963618, 8122858, 8285282, 8450954,
    8619939, 8792303, 8968113, 9147439, 9330350, 9516919, 9707219, 9901324, 10099310, 10301255,
    10507238, 10717340, 10931644, 11150232, 11373191, 11600609, 11832574, 12069177, 12310512,
    12556672, 12807754, 13063857, 13325081, 13591528, 13863304, 14140513, 14423266, 14711673,
    15005846, 15305902, 15611958, 15924134, 16242552, 16567336, 16898616, 17236519, 17581180,
    17932732, 18291313, 18657065, 19030131, 19410656, 19798790, 20194685, 20598497, 21010383,
    21430505, 21859028, 22296119, 22741951, 23196698, 23660537, 24133652, 24616226, 25108451,
    25610518, 26122624, 26644970, 27177761, 27721206, 28275517, 28840912, 29417613, 30005846,
    30605841, 31217833, 31842063, 32478774, 33128218, 33790647, 34466323, 35155509, 35858476,
    36575500, 37306861, 38052846, 38813748, 39589865, 40381501, 41188967, 42012579, 42852660,
    43709538, 44583551, 45475041, 46384357, 47311855, 48257900, 49222861, 50207118, 51211056,
    52235069, 53279558, 54344932, 55431610, 56540017, 57670587, 58823764, 60000000,
];
const UNDER: u16 = u16::MAX - 1;
const OVER: u16 = u16::MAX;
/// `min`/`max` are clamped to `±LIMIT` so their zigzag fits a JS number exactly; slots are not.
const LIMIT: i64 = (1 << 52) - 1;

/// The bucket of `v`, or underflow/overflow.
#[inline]
pub fn slot(v: i64) -> u16 {
    if v < BOUNDS[0] {
        UNDER
    } else if v >= BOUNDS[BUCKETS] {
        OVER
    } else {
        (BOUNDS.partition_point(|&b| b <= v) - 1) as u16
    }
}

/// One non-zero log-spaced histogram bucket: 2-byte slot plus 48-bit saturating count (`8` bytes total).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bucket {
    slot: u16,
    c0: u16,
    c1: u16,
    c2: u16,
}

const MAX_BUCKET_COUNT: u64 = (1u64 << 48) - 1;

impl Bucket {
    const ZERO: Self = Self {
        slot: 0,
        c0: 0,
        c1: 0,
        c2: 0,
    };

    #[inline]
    pub fn new(slot: u16, count: u64) -> Self {
        let c = count.min(MAX_BUCKET_COUNT);
        Self {
            slot,
            c0: c as u16,
            c1: (c >> 16) as u16,
            c2: (c >> 32) as u16,
        }
    }

    #[inline]
    pub fn slot(self) -> u16 {
        self.slot
    }

    #[inline]
    pub fn count(self) -> u64 {
        u64::from(self.c0) | (u64::from(self.c1) << 16) | (u64::from(self.c2) << 32)
    }

    #[inline]
    fn add_saturating(&mut self, n: u64) {
        let sum = self.count().saturating_add(n).min(MAX_BUCKET_COUNT);
        self.c0 = sum as u16;
        self.c1 = (sum >> 16) as u16;
        self.c2 = (sum >> 32) as u16;
    }
}

#[inline]
fn grow_buckets(buckets: &mut Vec<Bucket>, min_cap: usize) {
    if min_cap <= buckets.capacity() {
        return;
    }
    let doubled = buckets.capacity().saturating_mul(2).max(4);
    let target = doubled.max(min_cap);
    if target > BUCKETS {
        buckets.reserve_exact(
            BUCKETS
                .saturating_sub(buckets.len())
                .max(min_cap - buckets.len()),
        );
    } else {
        buckets.reserve_exact(target - buckets.len());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hist {
    pub under: u64,
    pub over: u64,
    pub min: i64,
    pub max: i64,
    /// Ascending, non-zero.
    pub buckets: Vec<Bucket>,
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

/// Reads a [`Hist::to_bytes`] blob (v2 or legacy v1) into `buckets`: `([under, over], [min, max], buckets used)`,
/// or `None` unless it is a well-formed, non-empty histogram.
fn parse(blob: &[u8], buckets: &mut [Bucket; BUCKETS]) -> Option<([u64; 2], [i64; 2], usize)> {
    let ver = *blob.first()?;
    if ver != VERSION && ver != VERSION_V1 {
        return None;
    }
    let mut at = 1;
    let counts = [get_varint(blob, &mut at)?, get_varint(blob, &mut at)?];
    let range = [
        unzigzag(get_varint(blob, &mut at)?),
        unzigzag(get_varint(blob, &mut at)?),
    ];
    let mut n = 0;
    let mut prev_raw: Option<u64> = None;
    while at < blob.len() {
        let raw_s = if ver == VERSION_V1 {
            let b = *blob.get(at)?;
            at += 1;
            u64::from(b)
        } else {
            get_varint(blob, &mut at)?
        };
        let c = get_varint(blob, &mut at)?;
        let limit = if ver == VERSION_V1 {
            BUCKETS_V1
        } else {
            BUCKETS
        };
        if raw_s as usize >= limit || c == 0 || prev_raw.is_some_and(|p| p >= raw_s) {
            return None;
        }
        prev_raw = Some(raw_s);
        let mapped_s = if ver == VERSION_V1 {
            let i = raw_s as usize;
            let mid = ((BOUNDS_V1[i] as f64) * (BOUNDS_V1[i + 1] as f64))
                .sqrt()
                .round() as i64;
            slot(mid)
        } else {
            raw_s as u16
        };
        if n > 0 && buckets[n - 1].slot() == mapped_s {
            buckets[n - 1].add_saturating(c);
        } else {
            buckets[n] = Bucket::new(mapped_s, c);
            n += 1;
        }
    }
    ((n > 0 || counts != [0, 0]) && range[0] <= range[1]).then_some((counts, range, n))
}

impl Hist {
    pub fn add(&mut self, v: i64) {
        self.add_slot(v, slot(v));
    }

    /// Adds `v`, whose [`slot`] the caller already knows.
    #[inline]
    pub fn add_slot(&mut self, v: i64, s: u16) {
        let v = v.clamp(-LIMIT, LIMIT);
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.add_count(s, 1);
    }

    #[inline]
    fn add_count(&mut self, s: u16, n: u64) {
        match s {
            UNDER => self.under = self.under.saturating_add(n),
            OVER => self.over = self.over.saturating_add(n),
            _ => match self.buckets.binary_search_by_key(&s, |b| b.slot()) {
                Ok(at) => self.buckets[at].add_saturating(n),
                Err(at) => {
                    let next_len = self.buckets.len() + 1;
                    grow_buckets(&mut self.buckets, next_len);
                    self.buckets.insert(at, Bucket::new(s, n));
                }
            },
        }
    }

    pub fn count(&self) -> u64 {
        self.buckets
            .iter()
            .fold(self.under.saturating_add(self.over), |a, b| {
                a.saturating_add(b.count())
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
        let mut buckets = [Bucket::ZERO; BUCKETS];
        let Some((counts, range, n)) = parse(blob, &mut buckets) else {
            return false;
        };
        self.merge_parts(counts, range, &buckets[..n]);
        true
    }

    fn merge_parts(&mut self, [under, over]: [u64; 2], [min, max]: [i64; 2], theirs: &[Bucket]) {
        self.min = self.min.min(min);
        self.max = self.max.max(max);
        self.under = self.under.saturating_add(under);
        self.over = self.over.saturating_add(over);
        // Both sides ascend: count the new slots, grow once (capped at BUCKETS), then merge in place.
        let mine = &self.buckets;
        let (mut a, mut fresh) = (0, 0);
        for b in theirs {
            let s = b.slot();
            while a < mine.len() && mine[a].slot() < s {
                a += 1;
            }
            if a == mine.len() || mine[a].slot() != s {
                fresh += 1;
            }
        }
        let (mut a, mut b) = (self.buckets.len(), theirs.len());
        let target = a + fresh;
        grow_buckets(&mut self.buckets, target);
        self.buckets.resize(target, Bucket::ZERO);
        let mut at = self.buckets.len();
        while b > 0 {
            at -= 1;
            let mut y = theirs[b - 1];
            match a.checked_sub(1).map(|i| self.buckets[i]) {
                Some(x) if x.slot() > y.slot() => {
                    self.buckets[at] = x;
                    a -= 1;
                }
                Some(x) if x.slot() == y.slot() => {
                    y.add_saturating(x.count());
                    self.buckets[at] = y;
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
        let mut out = Vec::with_capacity(12 + self.buckets.len() * 4);
        out.push(VERSION);
        put_varint(&mut out, self.under);
        put_varint(&mut out, self.over);
        put_varint(&mut out, zigzag(self.min));
        put_varint(&mut out, zigzag(self.max));
        for b in &self.buckets {
            put_varint(&mut out, u64::from(b.slot()));
            put_varint(&mut out, b.count());
        }
        Some(out)
    }

    pub fn from_bytes(blob: &[u8]) -> Option<Self> {
        let mut buckets = [Bucket::ZERO; BUCKETS];
        let ([under, over], [min, max], n) = parse(blob, &mut buckets)?;
        Some(Hist {
            under,
            over,
            min,
            max,
            buckets: buckets[..n].to_vec(),
        })
    }

    /// The `q` quantile (nearest rank), returning exact `min`/`max` for boundary ranks
    /// (`rank == 1` or `rank == n`), and reading interior ranks at the bucket's geometric
    /// middle clamped to `[min, max]`.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let n = self.count();
        if n == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * n as f64).ceil() as u64).clamp(1, n);
        if rank == 1 {
            return Some(self.min as f64);
        }
        if rank == n {
            return Some(self.max as f64);
        }
        let mut seen = self.under;
        if rank <= seen {
            return Some(self.min as f64);
        }
        for b in &self.buckets {
            seen += b.count();
            if rank <= seen {
                let s = b.slot() as usize;
                let (lo, hi) = (BOUNDS[s] as f64, BOUNDS[s + 1] as f64);
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
        assert_eq!(slot(101), 0);
        assert_eq!(slot(102), 1);
        assert_eq!(slot(59_999_999), (BUCKETS - 1) as u16);
        assert_eq!(slot(60_000_000), OVER);
        for i in 0..BUCKETS {
            assert_eq!(slot(BOUNDS[i]), i as u16);
            assert_eq!(slot(BOUNDS[i + 1] - 1), i as u16);
            assert!(BOUNDS[i + 1] as f64 / BOUNDS[i] as f64 <= 1.03);
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
            "0201016480f6e042000114017402"
        );
        assert_eq!(h.quantile(0.5), Some((149f64 * 152.0).sqrt()));
        assert_eq!(h.quantile(0.0), Some(50.0));
        assert_eq!(h.quantile(1.0), Some(70_000_000.0));
        // Saturated garbage keeps min and max within what a JS number holds exactly.
        let mut wild = Hist::default();
        wild.add(i64::MIN);
        wild.add(i64::MAX);
        assert_eq!(
            hex::encode(wild.to_bytes().unwrap()),
            "020101fdffffffffffff0ffeffffffffffff0f"
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
            assert!((got - exact).abs() / exact < 0.015, "{q}: {got} vs {exact}");
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
        assert!(Hist::from_bytes(&[3, 1, 0, 0, 0]).is_none());
        assert!(Hist::from_bytes(&[VERSION, 0, 0, 0, 0]).is_none());
        let mut swapped = bytes.clone();
        swapped.extend([3, 1]);
        assert!(Hist::from_bytes(&swapped).is_none(), "buckets must ascend");
    }

    #[test]
    fn exact_min_and_max_order_statistics_and_high_precision_quantiles() {
        // When rank == 1 (min) or rank == n (max), exact min/max are returned instead of bucket midpoint.
        let mut small = Hist::default();
        for _ in 0..15 {
            small.add(2_500_000);
        }
        small.add(10_111_570);
        assert_eq!(small.quantile(0.0), Some(2_500_000.0));
        assert_eq!(small.quantile(0.99), Some(10_111_570.0));
        assert_eq!(small.quantile(1.0), Some(10_111_570.0));

        // Across 10k samples, v2 (672 buckets, ~2% width) keeps interior quantiles within ~1.4%.
        let mut h = Hist::default();
        let values: Vec<i64> = (1..=10_000).map(|i| 1_000 + i * 37).collect();
        values.iter().for_each(|&v| h.add(v));
        for q in [0.1, 0.5, 0.95, 0.99] {
            let exact = values[((q * values.len() as f64).ceil() as usize).max(1) - 1] as f64;
            let got = h.quantile(q).unwrap();
            assert!((got - exact).abs() / exact < 0.015, "{q}: {got} vs {exact}");
        }
        assert_eq!(VERSION, 2);
    }

    #[test]
    fn reads_legacy_v1_blob_and_merges_with_v2() {
        let legacy_v1 = hex::decode("0101016480f6e042000103011602").unwrap();
        let mut h = Hist::from_bytes(&legacy_v1).expect("v1 blob decodes");
        assert_eq!(h.count(), 6);
        assert_eq!(h.min, 50);
        assert_eq!(h.max, 70_000_000);
        assert!(h.merge_bytes(&legacy_v1));
        assert_eq!(h.count(), 12);
        let roundtrip = Hist::from_bytes(&h.to_bytes().unwrap()).unwrap();
        assert_eq!(roundtrip, h);
    }

    #[test]
    fn compact_bucket_layout_and_bounded_capacity_at_full_672_slots() {
        assert_eq!(std::mem::size_of::<Bucket>(), 8);
        assert_eq!(std::mem::align_of::<Bucket>(), 2);

        let mut via_add = Hist::default();
        for &b in BOUNDS.iter().take(BUCKETS) {
            via_add.add(b);
        }
        assert_eq!(via_add.buckets.len(), BUCKETS);
        assert!(
            via_add.buckets.capacity() <= BUCKETS,
            "add capacity {} exceeded BUCKETS {}",
            via_add.buckets.capacity(),
            BUCKETS
        );

        let mut via_merge = Hist::default();
        let half_a = {
            let mut h = Hist::default();
            for i in (0..BUCKETS).step_by(2) {
                h.add(BOUNDS[i]);
            }
            h.to_bytes().unwrap()
        };
        let half_b = {
            let mut h = Hist::default();
            for i in (1..BUCKETS).step_by(2) {
                h.add(BOUNDS[i]);
            }
            h.to_bytes().unwrap()
        };
        assert!(via_merge.merge_bytes(&half_a));
        assert!(via_merge.merge_bytes(&half_b));
        assert_eq!(via_merge.buckets.len(), BUCKETS);
        assert!(
            via_merge.buckets.capacity() <= BUCKETS,
            "merge capacity {} exceeded BUCKETS {}",
            via_merge.buckets.capacity(),
            BUCKETS
        );
        assert_eq!(via_merge.to_bytes(), via_add.to_bytes());
    }
}
