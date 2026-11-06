use arrow_array::{
    builder::{
        BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
    },
    cast::AsArray,
    types::{Float64Type, Int32Type, Int64Type, TimestampMillisecondType},
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding, ZstdLevel},
    file::{
        metadata::{KeyValue, SortingColumn},
        properties::WriterProperties,
    },
    schema::types::ColumnPath,
};
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt,
    io::{BufRead, BufReader, Read},
    sync::{Arc, OnceLock},
};

/// A `HashMap` with a keyed, DoS-resistant hasher that is much cheaper than SipHash. Callers must
/// not depend on its iteration order.
pub(crate) type FastMap<K, V> = HashMap<K, V, ahash::RandomState>;

pub const SCHEMA_VERSION: &str = "v3";
pub const MAX_DECOMPRESSED_BYTES: u64 = 48 * 1024 * 1024;
pub const MAX_LINE_BYTES: u64 = 256 * 1024;
/// A segmented raw object may decompress to at most this much; segments skip up to it.
pub const MAX_OBJECT_DECOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;
/// Decompressed bytes between two calls of a segment read's or measurement's `stop` check.
pub const CHECKPOINT_BYTES: u64 = 1024 * 1024;

/// A decompressed byte range of a raw object. A line belongs to the segment holding its first
/// byte, so segments that tile `[0, ∞)` read every line exactly once.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Segment {
    pub start: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<u64>,
}

impl Segment {
    pub const WHOLE: Self = Self {
        start: 0,
        end: None,
    };
}
pub(crate) const ROUTING_TOLERANCE_S: i64 = 24 * 3600;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Kind {
    Utf8,
    Int64,
    Float64,
    Bool,
    TimestampMs,
}

pub struct Column {
    pub id: &'static str,
    pub kind: Kind,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConvertError {
    Corrupt,
    TooLarge,
    /// The caller's `stop` check asked to stop; nothing was converted.
    Stopped,
}

impl ConvertError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Corrupt => "corrupt_gzip",
            Self::TooLarge => "decompressed_too_large",
            Self::Stopped => "worker_budget_stopped",
        }
    }
}

pub fn columns() -> &'static [Column] {
    static COLUMNS: OnceLock<Vec<Column>> = OnceLock::new();
    COLUMNS.get_or_init(|| {
        crate::catalog::data()["fields"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|field| {
                let id: &'static str = field["id"].as_str()?;
                let kind = match field["duckdb_type"].as_str()? {
                    "VARCHAR" => Kind::Utf8,
                    "BOOLEAN" => Kind::Bool,
                    "TIMESTAMP" => Kind::TimestampMs,
                    "FLOAT" | "DOUBLE" => Kind::Float64,
                    _ => Kind::Int64,
                };
                Some(Column { id, kind })
            })
            .collect()
    })
}

pub fn schema() -> SchemaRef {
    RequestSchema::base().arrow()
}

/// A custom (or CMCD) field's column: added after the catalog columns, in name order.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CustomColumn {
    pub name: String,
    pub kind: Kind,
}

/// The request table's columns for one service: every catalog column, then each custom field
/// that is committed or enabled (master plan 8B-R). A disabled committed field stays a null
/// column, so the schema never shrinks once a field is locked.
pub fn custom_columns(log_fields: &Value) -> Vec<CustomColumn> {
    let catalog: Vec<&str> = columns().iter().map(|c| c.id).collect();
    let mut custom: Vec<CustomColumn> = log_fields["custom_fields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|f| f["committed"] == true || f["enabled"] != false)
        .filter_map(|field| {
            let name = field["name"].as_str()?;
            let valid = !name.is_empty()
                && name
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
                && !catalog.contains(&name);
            let normalized = crate::catalog::normalize_custom_field(field);
            let kind = match normalized["duckdb_type"].as_str()? {
                "VARCHAR" => Kind::Utf8,
                "BIGINT" | "INTEGER" => Kind::Int64,
                "DOUBLE" | "FLOAT" => Kind::Float64,
                "BOOLEAN" => Kind::Bool,
                _ => return None,
            };
            valid.then(|| CustomColumn {
                name: name.to_owned(),
                kind,
            })
        })
        .collect();
    custom.sort_by(|a, b| a.name.cmp(&b.name));
    custom.dedup_by(|a, b| a.name == b.name);
    custom
}

pub struct RequestSchema {
    kinds: Vec<Kind>,
    index: FastMap<String, usize>,
    /// `index` inverted: the field name at each column, if any.
    names: Vec<Option<String>>,
    arrow: SchemaRef,
    custom: Vec<CustomColumn>,
    hash: String,
}

impl RequestSchema {
    /// The catalog alone: a service with no custom fields.
    pub fn base() -> &'static RequestSchema {
        static BASE: OnceLock<RequestSchema> = OnceLock::new();
        BASE.get_or_init(|| RequestSchema::new(Vec::new()))
    }

    pub fn for_fields(log_fields: &Value) -> Self {
        Self::new(custom_columns(log_fields))
    }

    pub fn new(mut custom: Vec<CustomColumn>) -> Self {
        custom.sort_by(|a, b| a.name.cmp(&b.name));
        custom.dedup_by(|a, b| a.name == b.name);
        let catalog = columns();
        let mut kinds: Vec<Kind> = catalog.iter().map(|c| c.kind).collect();
        let mut index: FastMap<String, usize> = catalog
            .iter()
            .enumerate()
            .map(|(i, c)| (c.id.to_owned(), i))
            .collect();
        let mut fields: Vec<Field> = arrow_schema(catalog)
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        custom.retain(|c| !index.contains_key(&c.name));
        for column in &custom {
            index.insert(column.name.clone(), kinds.len());
            kinds.push(column.kind);
            fields.push(Field::new(&column.name, data_type(column.kind), true));
        }
        let hash = {
            use sha2::{Digest, Sha256};
            let mut digest = Sha256::new();
            digest.update(format!(
                "{SCHEMA_VERSION}|{ROUTING_TOLERANCE_S}|{MAX_LINE_BYTES}|{MAX_DECOMPRESSED_BYTES}|"
            ));
            for column in catalog {
                digest.update(format!("{}:{:?};", column.id, column.kind));
            }
            for column in &custom {
                digest.update(format!("custom:{}:{:?};", column.name, column.kind));
            }
            hex::encode(&digest.finalize()[..8])
        };
        let mut names = vec![None; kinds.len()];
        for (name, &i) in &index {
            names[i] = Some(name.clone());
        }
        Self {
            kinds,
            names,
            index,
            arrow: Arc::new(Schema::new(fields)),
            custom,
            hash,
        }
    }

    /// Identity of everything that decides which rows land in which window file, and how: the
    /// Parquet columns and their types, the routing tolerance and the line cap. Ingest plans and
    /// workers must agree on it, so a release or custom-field change can never reuse a plan's
    /// output keys.
    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn custom(&self) -> &[CustomColumn] {
        &self.custom
    }

    pub fn arrow(&self) -> SchemaRef {
        self.arrow.clone()
    }
}

fn data_type(kind: Kind) -> DataType {
    match kind {
        Kind::Utf8 => DataType::Utf8,
        Kind::Int64 => DataType::Int64,
        Kind::Float64 => DataType::Float64,
        Kind::Bool => DataType::Boolean,
        Kind::TimestampMs => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
    }
}

pub(crate) fn arrow_schema(columns: &[Column]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| Field::new(column.id, data_type(column.kind), true))
            .collect::<Vec<_>>(),
    ))
}

enum Builder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Bool(BooleanBuilder),
    Timestamp(TimestampMillisecondBuilder),
}

impl Builder {
    fn new(kind: Kind) -> Self {
        match kind {
            Kind::Utf8 => Self::Utf8(StringBuilder::new()),
            Kind::Int64 => Self::Int64(Int64Builder::new()),
            Kind::Float64 => Self::Float64(Float64Builder::new()),
            Kind::Bool => Self::Bool(BooleanBuilder::new()),
            Kind::TimestampMs => {
                Self::Timestamp(TimestampMillisecondBuilder::new().with_timezone("UTC"))
            }
        }
    }

    fn push(&mut self, value: Option<&Raw<'_>>) {
        match (self, value) {
            (Self::Utf8(b), Some(Raw::Str(s))) => b.append_value(s),
            (Self::Utf8(b), Some(Raw::Num(n))) => b.append_value(n.to_string()),
            (Self::Utf8(b), Some(Raw::Bool(v))) => {
                b.append_value(if *v { "true" } else { "false" })
            }
            (Self::Utf8(b), _) => b.append_null(),
            (Self::Int64(b), value) => b.append_option(value.and_then(integer)),
            (Self::Float64(b), value) => b.append_option(value.and_then(float)),
            (Self::Bool(b), value) => b.append_option(value.and_then(boolean)),
            (Self::Timestamp(b), value) => b.append_option(value.and_then(timestamp_ms)),
        }
    }

    /// No coercion: a value of the wrong JSON type becomes null. Timestamps are epoch
    /// milliseconds.
    fn push_strict(&mut self, value: Option<&Value>) {
        match self {
            Self::Utf8(b) => b.append_option(value.and_then(Value::as_str)),
            Self::Int64(b) => b.append_option(value.and_then(strict_integer)),
            Self::Float64(b) => b.append_option(
                value
                    .and_then(|v| v.as_number().and_then(serde_json::Number::as_f64))
                    .filter(|v| v.is_finite()),
            ),
            Self::Bool(b) => b.append_option(value.and_then(Value::as_bool)),
            Self::Timestamp(b) => b.append_option(value.and_then(strict_integer)),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Timestamp(b) => Arc::new(b.finish()),
        }
    }
}

fn strict_integer(value: &Value) -> Option<i64> {
    let number = value.as_number()?;
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|v| i64::try_from(v).ok()))
}

fn number_integer(n: &Number) -> Option<i64> {
    n.as_i64()
        .or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok()))
}

fn integer(value: &Raw<'_>) -> Option<i64> {
    match value {
        Raw::Num(n) => number_integer(n),
        Raw::Str(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn float(value: &Raw<'_>) -> Option<f64> {
    let parsed = match value {
        Raw::Num(n) => n.as_f64(),
        Raw::Str(s) => s.trim().parse().ok(),
        _ => None,
    };
    parsed.filter(|v| v.is_finite())
}

fn boolean(value: &Raw<'_>) -> Option<bool> {
    match value {
        Raw::Bool(b) => Some(*b),
        Raw::Num(n) => match n.as_i64() {
            Some(0) => Some(false),
            Some(1) => Some(true),
            _ => None,
        },
        Raw::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Text timestamps are parsed; integers (`ts_ms`) are epoch milliseconds.
fn timestamp_ms(value: &Raw<'_>) -> Option<i64> {
    match value {
        Raw::Str(s) => parse_ms(s),
        Raw::Num(n) => number_integer(n),
        _ => None,
    }
}

/// A log line's field value, decoded without building a `serde_json::Value`. Strings borrow
/// from the line unless they contain escapes; arrays and objects are validated and dropped.
#[derive(Clone)]
enum Raw<'a> {
    Str(Cow<'a, str>),
    Num(Number),
    Bool(bool),
    Null,
    Other,
}

impl<'de> de::Deserialize<'de> for Raw<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(RawVisitor)
    }
}

struct RawVisitor;

impl<'de> Visitor<'de> for RawVisitor {
    type Value = Raw<'de>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
        Ok(Raw::Str(Cow::Borrowed(v)))
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        Ok(Raw::Str(Cow::Owned(v.to_owned())))
    }
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
        Ok(Raw::Str(Cow::Owned(v)))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
        Ok(Raw::Num(v.into()))
    }
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
        Ok(Raw::Num(v.into()))
    }
    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
        Ok(Number::from_f64(v).map_or(Raw::Null, Raw::Num))
    }
    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
        Ok(Raw::Bool(v))
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Raw::Null)
    }
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Raw::Null)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Raw::Other)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(Raw::Other)
    }
}

/// Decodes one JSON object or positional array line into per-column slots. Like `serde_json::Map`,
/// a repeated key keeps its last value; unknown keys or extra array elements are validated and skipped.
struct RowSeed<'s, 'de> {
    schema: &'s RequestSchema,
    slots: &'s mut [Option<Raw<'de>>],
}

impl<'de> DeserializeSeed<'de> for RowSeed<'_, 'de> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for RowSeed<'_, 'de> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object or positional array")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut next = 0;
        while let Some(index) = map.next_key_seed(KeySeed {
            schema: self.schema,
            next,
        })? {
            match index {
                Some(i) => {
                    next = i + 1;
                    self.slots[i] = Some(map.next_value()?);
                }
                None => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let mut i = 0;
        while i < self.slots.len() {
            let Some(value) = seq.next_element()? else {
                return Ok(());
            };
            self.slots[i] = Some(value);
            i += 1;
        }
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(())
    }
}

/// Logged lines follow column order, so the column after the previous key is tried before the
/// hash lookup.
struct KeySeed<'s> {
    schema: &'s RequestSchema,
    next: usize,
}

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = Option<usize>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for KeySeed<'_> {
    type Value = Option<usize>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a field name")
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        if self.schema.names.get(self.next).and_then(Option::as_deref) == Some(v) {
            return Ok(Some(self.next));
        }
        Ok(self.schema.index.get(v).copied())
    }
}

/// A line validated as UTF-8 once skips serde_json's per-string check. Any other line keeps the
/// byte reader, which also accepts invalid UTF-8 inside skipped values.
fn decode_row<'de>(
    schema: &RequestSchema,
    line: &'de [u8],
    slots: &mut [Option<Raw<'de>>],
) -> bool {
    match std::str::from_utf8(line) {
        Ok(text) => decode_from(serde_json::Deserializer::from_str(text), schema, slots),
        Err(_) => decode_from(serde_json::Deserializer::from_slice(line), schema, slots),
    }
}

fn decode_from<'de, R: serde_json::de::Read<'de>>(
    mut deserializer: serde_json::Deserializer<R>,
    schema: &RequestSchema,
    slots: &mut [Option<Raw<'de>>],
) -> bool {
    RowSeed { schema, slots }
        .deserialize(&mut deserializer)
        .is_ok()
        && deserializer.end().is_ok()
}

/// Parses v3's `%Y-%m-%dT%H:%M:%S%z` (`+0000`) as well as RFC 3339 (`Z`, `+00:00`).
pub fn parse_ms(value: &str) -> Option<i64> {
    let value = value.trim();
    let bytes = value.as_bytes();
    let mut buf = [0u8; 64];
    let normalized: String;
    let value = if bytes.len() >= 5
        && matches!(bytes[bytes.len() - 5], b'+' | b'-')
        && bytes[bytes.len() - 4..].iter().all(u8::is_ascii_digit)
        && value.is_char_boundary(value.len() - 2)
    {
        let split = value.len() - 2;
        if value.len() < buf.len() {
            buf[..split].copy_from_slice(&bytes[..split]);
            buf[split] = b':';
            buf[split + 1..value.len() + 1].copy_from_slice(&bytes[split..]);
            std::str::from_utf8(&buf[..value.len() + 1]).ok()?
        } else {
            let (head, minutes) = value.split_at(split);
            normalized = format!("{head}:{minutes}");
            normalized.as_str()
        }
    } else {
        value
    };
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    i64::try_from(parsed.unix_timestamp_nanos() / 1_000_000).ok()
}

struct WindowBuilders {
    columns: Vec<Builder>,
    rows: u64,
}

pub struct ObjectRows {
    /// Window start → rows. For the RUM stream these are `client_vitals` rows.
    pub windows: BTreeMap<i64, RecordBatch>,
    /// RUM `client_errors` rows by window start; always empty for requests.
    pub errors: BTreeMap<i64, RecordBatch>,
    pub rows: u64,
    pub lines_read: u64,
    pub bad_lines: u64,
    /// The first rejected line, cut to `BAD_SAMPLE_BYTES`.
    pub bad_sample: Option<String>,
    pub decompressed: u64,
}

const BAD_SAMPLE_BYTES: usize = 2048;

pub(crate) fn sample_of(line: &[u8]) -> String {
    String::from_utf8_lossy(&line[..line.len().min(BAD_SAMPLE_BYTES)]).into_owned()
}

pub(crate) fn window_of(seconds: i64, window_s: i64) -> i64 {
    seconds - seconds.rem_euclid(window_s)
}

/// The second a row with time `ms` routes by: its own, unless it has none or it is more than
/// `ROUTING_TOLERANCE_S` from the object's time.
pub(crate) fn route(ms: Option<i64>, object_time_s: i64) -> i64 {
    ms.map(|ms| ms.div_euclid(1000))
        .filter(|s| (s - object_time_s).abs() <= ROUTING_TOLERANCE_S)
        .unwrap_or(object_time_s)
}

/// One non-empty line of a raw object: trimmed, or the first `MAX_LINE_BYTES` of one too long.
pub(crate) enum Line<'a> {
    Good(&'a [u8]),
    TooLong(&'a [u8]),
}

impl<'a> Line<'a> {
    pub(crate) fn good(&self) -> Option<&'a [u8]> {
        match self {
            Self::Good(line) => Some(line),
            Self::TooLong(_) => None,
        }
    }
    pub(crate) fn bytes(&self) -> &'a [u8] {
        match self {
            Self::Good(line) | Self::TooLong(line) => line,
        }
    }
}

/// Feeds every non-empty line of a zstd object to `on_line`, without holding more than one
/// line; returns the decompressed size. More than `max_decompressed` (at most
/// `MAX_DECOMPRESSED_BYTES`) is `TooLarge`.
pub(crate) fn read_lines(
    compressed: &[u8],
    max_decompressed: u64,
    on_line: impl FnMut(Line<'_>),
) -> Result<u64, ConvertError> {
    read_segment(
        compressed,
        Segment::WHOLE,
        max_decompressed,
        &|| false,
        on_line,
    )
}

/// Like [`read_lines`] for the lines starting inside `segment`; returns their size. Lines before
/// it are decompressed and skipped, and reading stops at the first line starting past its end.
/// `stop` is checked every `CHECKPOINT_BYTES` decompressed; when it answers true the read ends
/// with `Stopped`.
pub(crate) fn read_segment(
    compressed: &[u8],
    segment: Segment,
    max_decompressed: u64,
    stop: &dyn Fn() -> bool,
    on_line: impl FnMut(Line<'_>),
) -> Result<u64, ConvertError> {
    if !compressed.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Err(ConvertError::Corrupt);
    }
    let decoder =
        zstd::stream::read::Decoder::new(compressed).map_err(|_| ConvertError::Corrupt)?;
    read_lines_from(decoder, segment, max_decompressed, stop, on_line)
}

/// The decompressed size of a zstd object; more than `max` is `TooLarge`. `stop` is
/// checked every `CHECKPOINT_BYTES`, as in [`read_segment`].
pub fn decompressed_len(
    compressed: &[u8],
    max: u64,
    stop: &dyn Fn() -> bool,
) -> Result<u64, ConvertError> {
    if !compressed.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Err(ConvertError::Corrupt);
    }
    let decoder =
        zstd::stream::read::Decoder::new(compressed).map_err(|_| ConvertError::Corrupt)?;
    let mut decoder = decoder.take(max.saturating_add(1));
    let mut buffer = vec![0u8; 64 * 1024];
    let mut copied = 0u64;
    let mut checkpoint = CHECKPOINT_BYTES;
    loop {
        let n = match decoder.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(ConvertError::Corrupt),
        };
        copied += n as u64;
        if copied >= checkpoint {
            if stop() {
                return Err(ConvertError::Stopped);
            }
            checkpoint = copied + CHECKPOINT_BYTES;
        }
    }
    if copied > max {
        return Err(ConvertError::TooLarge);
    }
    Ok(copied)
}

fn read_lines_from<R: Read>(
    inner: R,
    segment: Segment,
    max_decompressed: u64,
    stop: &dyn Fn() -> bool,
    mut on_line: impl FnMut(Line<'_>),
) -> Result<u64, ConvertError> {
    let max_decompressed = max_decompressed.min(MAX_DECOMPRESSED_BYTES);
    let ceiling = if segment == Segment::WHOLE {
        max_decompressed
    } else {
        MAX_OBJECT_DECOMPRESSED_BYTES
    };
    let decoder = inner.take(ceiling + 1);
    let mut reader = BufReader::new(decoder);
    let mut offset = 0u64;
    let mut held = 0u64;
    let mut checkpoint = CHECKPOINT_BYTES;
    let mut line = Vec::new();
    let mut sink = Vec::new();
    loop {
        if segment.end.is_some_and(|end| offset >= end) {
            return Ok(held);
        }
        let skip = offset < segment.start;
        line.clear();
        let read = (&mut reader)
            .take(MAX_LINE_BYTES + 1)
            .read_until(b'\n', &mut line)
            .map_err(|_| ConvertError::Corrupt)?;
        if read == 0 {
            return Ok(held);
        }
        let mut add = |n: usize| {
            offset += n as u64;
            if !skip {
                held += n as u64;
            }
            if held > max_decompressed || offset > ceiling {
                return Err(ConvertError::TooLarge);
            }
            if offset >= checkpoint {
                if stop() {
                    return Err(ConvertError::Stopped);
                }
                checkpoint = offset + CHECKPOINT_BYTES;
            }
            Ok(())
        };
        add(read)?;
        if line.len() as u64 > MAX_LINE_BYTES && line.last() != Some(&b'\n') {
            if !skip {
                on_line(Line::TooLong(&line));
            }
            loop {
                sink.clear();
                let n = (&mut reader)
                    .take(MAX_LINE_BYTES)
                    .read_until(b'\n', &mut sink)
                    .map_err(|_| ConvertError::Corrupt)?;
                add(n)?;
                if n == 0 || sink.last() == Some(&b'\n') {
                    break;
                }
            }
            continue;
        }
        let trimmed = line.trim_ascii();
        if !skip && !trimmed.is_empty() {
            on_line(Line::Good(trimmed));
        }
    }
}

/// Rows route by their own `ts_ms`, or by `timestamp` when a line has no usable `ts_ms` (raw
/// objects logged before `ts_ms` existed). A row without either, or one wildly
/// outside the object's upload time, routes by the object's upload minute so no row is dropped
/// and a corrupt clock cannot demand an unbounded number of output files.
pub fn convert_object(
    gzip: &[u8],
    window_s: i64,
    object_time_s: i64,
) -> Result<ObjectRows, ConvertError> {
    convert_object_capped(
        gzip,
        window_s,
        object_time_s,
        MAX_DECOMPRESSED_BYTES,
        RequestSchema::base(),
    )
}

/// Like [`convert_object`] into `schema`'s columns, with a lower decompressed-byte ceiling (at
/// most `MAX_DECOMPRESSED_BYTES`); exceeding it returns `TooLarge`.
pub fn convert_object_capped(
    gzip: &[u8],
    window_s: i64,
    object_time_s: i64,
    max_decompressed: u64,
    schema: &RequestSchema,
) -> Result<ObjectRows, ConvertError> {
    convert_object_range(
        gzip,
        window_s,
        object_time_s,
        Segment::WHOLE,
        max_decompressed,
        schema,
        &|| false,
    )
}

struct EnumDecodeMaps {
    c_speed: BTreeMap<&'static str, &'static str>,
    p_type: BTreeMap<&'static str, &'static str>,
    p_desc: BTreeMap<&'static str, &'static str>,
}

fn enum_decode_maps() -> &'static EnumDecodeMaps {
    static MAPS: OnceLock<EnumDecodeMaps> = OnceLock::new();
    MAPS.get_or_init(|| {
        let invert = |table_name: &str| -> BTreeMap<&'static str, &'static str> {
            let data = crate::catalog::data();
            let obj = data["enum_encodings"][table_name]
                .as_object()
                .or_else(|| data[table_name].as_object());
            obj.into_iter()
                .flatten()
                .filter_map(|(full_str, code_val)| Some((code_val.as_str()?, full_str.as_str())))
                .collect()
        };
        EnumDecodeMaps {
            c_speed: invert("CONN_SPEED_ENCODE"),
            p_type: invert("PROXY_TYPE_ENCODE"),
            p_desc: invert("PROXY_DESC_ENCODE"),
        }
    })
}

/// Looks up a 1-character VCL enum code (`CONN_SPEED_ENCODE`, `PROXY_TYPE_ENCODE`,
/// `PROXY_DESC_ENCODE`) in `catalog.json` and returns its `'static` canonical string.
pub fn lookup_enum_decode(field_id: &str, value: &str) -> Option<&'static str> {
    let maps = enum_decode_maps();
    match field_id {
        "c_speed" => maps.c_speed.get(value).copied(),
        "p_type" => maps.p_type.get(value).copied(),
        "p_desc" => maps.p_desc.get(value).copied(),
        _ => None,
    }
}

/// Decodes a 1-character VCL enum code (`CONN_SPEED_ENCODE`, `PROXY_TYPE_ENCODE`,
/// `PROXY_DESC_ENCODE`) back to its canonical string when present in `catalog.json`.
pub fn decode_enum_value<'a>(field_id: &str, value: &'a str) -> &'a str {
    lookup_enum_decode(field_id, value).unwrap_or(value)
}

/// Like [`convert_object_capped`] for the lines starting in `segment`; `max_decompressed` caps
/// their bytes, and `decompressed` reports them.
pub fn convert_object_range(
    gzip: &[u8],
    window_s: i64,
    object_time_s: i64,
    segment: Segment,
    max_decompressed: u64,
    schema: &RequestSchema,
    stop: &dyn Fn() -> bool,
) -> Result<ObjectRows, ConvertError> {
    let timestamp_index = schema.index.get("timestamp").copied();
    let ts_ms_index = schema.index.get("ts_ms").copied();
    let c_speed_index = schema.index.get("c_speed").copied();
    let p_type_index = schema.index.get("p_type").copied();
    let p_desc_index = schema.index.get("p_desc").copied();
    let mut windows: BTreeMap<i64, WindowBuilders> = BTreeMap::new();
    let mut bad_lines = 0u64;
    let mut lines_read = 0u64;
    let mut bad_sample = None;
    let decompressed = read_segment(gzip, segment, max_decompressed, stop, |line| {
        lines_read += 1;
        let Some(trimmed) = line.good() else {
            bad_lines += 1;
            bad_sample.get_or_insert_with(|| sample_of(line.bytes()));
            return;
        };
        let mut stack_slots: [Option<Raw<'_>>; 128] = std::array::from_fn(|_| None);
        let mut heap_slots: Vec<Option<Raw<'_>>> = Vec::new();
        let slots: &mut [Option<Raw<'_>>] = if schema.kinds.len() <= stack_slots.len() {
            &mut stack_slots[..schema.kinds.len()]
        } else {
            heap_slots.resize_with(schema.kinds.len(), || None);
            &mut heap_slots
        };
        if !decode_row(schema, trimmed, slots) {
            bad_lines += 1;
            bad_sample.get_or_insert_with(|| sample_of(trimmed));
            return;
        }
        for (field_id, idx_opt) in [
            ("c_speed", c_speed_index),
            ("p_type", p_type_index),
            ("p_desc", p_desc_index),
        ] {
            if let Some(idx) = idx_opt {
                let decoded_opt = match &slots[idx] {
                    Some(Raw::Str(s)) => lookup_enum_decode(field_id, s),
                    _ => None,
                };
                if let Some(decoded) = decoded_opt {
                    slots[idx] = Some(Raw::Str(Cow::Borrowed(decoded)));
                }
            }
        }
        if trimmed.first() == Some(&b'[') {
            if let (Some(ts_idx), Some(ms_idx)) = (timestamp_index, ts_ms_index) {
                if matches!(slots[ts_idx], None | Some(Raw::Null))
                    && !matches!(slots[ms_idx], None | Some(Raw::Null))
                {
                    slots[ts_idx] = slots[ms_idx].clone();
                }
            }
        }
        let slot_ms =
            |index: Option<usize>| index.and_then(|i| slots[i].as_ref().and_then(timestamp_ms));
        let routed = route(
            slot_ms(ts_ms_index).or_else(|| slot_ms(timestamp_index)),
            object_time_s,
        );
        push_row(
            windows
                .entry(window_of(routed, window_s))
                .or_insert_with(|| WindowBuilders {
                    columns: schema
                        .kinds
                        .iter()
                        .map(|&kind| Builder::new(kind))
                        .collect(),
                    rows: 0,
                }),
            slots,
        );
    })?;
    let mut rows = 0;
    let mut batches = BTreeMap::new();
    for (start, mut builders) in windows {
        rows += builders.rows;
        let arrays = builders.columns.iter_mut().map(Builder::finish).collect();
        batches.insert(
            start,
            RecordBatch::try_new(schema.arrow(), arrays).map_err(|_| ConvertError::Corrupt)?,
        );
    }
    Ok(ObjectRows {
        windows: batches,
        errors: BTreeMap::new(),
        rows,
        lines_read,
        bad_lines,
        bad_sample,
        decompressed,
    })
}

fn push_row(window: &mut WindowBuilders, slots: &[Option<Raw<'_>>]) {
    for (builder, value) in window.columns.iter_mut().zip(slots) {
        builder.push(value.as_ref());
    }
    window.rows += 1;
}

/// The order window files are written in: low-cardinality dimensions (`host`, `status`, `cache`,
/// `method`) first so dictionary and RLE encoding compress repeated runs, then `ts_ms` and
/// `timestamp`, ascending, nulls last.
pub const SORT_COLUMNS: [&str; 6] = ["host", "status", "cache", "method", "ts_ms", "timestamp"];

/// Low-cardinality sort order for RUM (`client_vitals` and `client_errors`) window files, followed
/// by `timestamp`.
pub const RUM_SORT_COLUMNS: [&str; 9] = [
    "metric_name",
    "metric_rating",
    "device",
    "browser",
    "os",
    "country",
    "pop",
    "tls",
    "timestamp",
];

/// Writes one window file with its rows sorted by [`SORT_COLUMNS`] (or [`RUM_SORT_COLUMNS`] for
/// RUM schemas), in the batches' schema. The sort is stable over the batches' order, so the same
/// batches always give the same bytes.
pub fn write_parquet(batches: &[&RecordBatch]) -> Result<Vec<u8>, String> {
    let schema = batches.first().map_or_else(schema, |b| b.schema());
    let cols: &[&str] =
        if schema.index_of("metric_name").is_ok() || schema.index_of("error_message").is_ok() {
            &RUM_SORT_COLUMNS
        } else {
            &SORT_COLUMNS
        };
    let sort: Vec<usize> = cols
        .iter()
        .filter_map(|name| schema.index_of(name).ok())
        .collect();
    let sorted = sort_rows(&schema, batches, &sort)?;
    let sorting = sort
        .iter()
        .map(|&i| SortingColumn {
            column_idx: i as i32,
            descending: false,
            nulls_first: false,
        })
        .collect();
    write_with(
        schema,
        &sorted.iter().collect::<Vec<_>>(),
        vec![KeyValue::new(
            "flp_schema".into(),
            SCHEMA_VERSION.to_string(),
        )],
        Some(sorting),
    )
}

enum SortCol<'a> {
    Utf8(Vec<&'a StringArray>),
    Int64(Vec<&'a Int64Array>),
    TimestampMs(Vec<&'a TimestampMillisecondArray>),
}

impl SortCol<'_> {
    fn cmp(&self, (b1, i1): (usize, usize), (b2, i2): (usize, usize)) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match self {
            Self::Utf8(cols) => match (cols[b1].is_valid(i1), cols[b2].is_valid(i2)) {
                (true, true) => cols[b1].value(i1).cmp(cols[b2].value(i2)),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            },
            Self::Int64(cols) => match (cols[b1].is_valid(i1), cols[b2].is_valid(i2)) {
                (true, true) => cols[b1].value(i1).cmp(&cols[b2].value(i2)),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            },
            Self::TimestampMs(cols) => match (cols[b1].is_valid(i1), cols[b2].is_valid(i2)) {
                (true, true) => cols[b1].value(i1).cmp(&cols[b2].value(i2)),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            },
        }
    }
}

fn sort_rows(
    schema: &SchemaRef,
    batches: &[&RecordBatch],
    sort: &[usize],
) -> Result<Option<RecordBatch>, String> {
    let mut order: Vec<(usize, usize)> = batches
        .iter()
        .enumerate()
        .flat_map(|(b, batch)| (0..batch.num_rows()).map(move |i| (b, i)))
        .collect();
    if order.is_empty() {
        return Ok(None);
    }
    let cols: Vec<SortCol<'_>> = sort
        .iter()
        .filter_map(|&c| match schema.field(c).data_type() {
            DataType::Utf8 => Some(SortCol::Utf8(
                batches.iter().map(|b| b.column(c).as_string()).collect(),
            )),
            DataType::Int64 => Some(SortCol::Int64(
                batches
                    .iter()
                    .map(|b| b.column(c).as_primitive::<Int64Type>())
                    .collect(),
            )),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Some(SortCol::TimestampMs(
                batches
                    .iter()
                    .map(|b| b.column(c).as_primitive::<TimestampMillisecondType>())
                    .collect(),
            )),
            _ => None,
        })
        .collect();
    order.sort_by(|&a, &b| {
        for col in &cols {
            let ord = col.cmp(a, b);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(c, field)| take(field.data_type(), batches, c, &order))
        .collect();
    RecordBatch::try_new(schema.clone(), arrays)
        .map(Some)
        .map_err(|_| "sort_batch".into())
}

fn take(
    data_type: &DataType,
    batches: &[&RecordBatch],
    c: usize,
    order: &[(usize, usize)],
) -> ArrayRef {
    fn pick<'a, A: Array, T>(
        arrays: &'a [&'a A],
        order: &'a [(usize, usize)],
        value: impl Fn(&'a A, usize) -> T + 'a,
    ) -> impl Iterator<Item = Option<T>> + 'a {
        order
            .iter()
            .map(move |&(b, i)| arrays[b].is_valid(i).then(|| value(arrays[b], i)))
    }
    match data_type {
        DataType::Utf8 => {
            let arrays: Vec<&StringArray> =
                batches.iter().map(|b| b.column(c).as_string()).collect();
            Arc::new(pick(&arrays, order, StringArray::value).collect::<StringArray>())
        }
        DataType::Int64 => {
            let arrays: Vec<&Int64Array> = batches
                .iter()
                .map(|b| b.column(c).as_primitive::<Int64Type>())
                .collect();
            Arc::new(pick(&arrays, order, Int64Array::value).collect::<Int64Array>())
        }
        DataType::Int32 => {
            let arrays: Vec<&Int32Array> = batches
                .iter()
                .map(|b| b.column(c).as_primitive::<Int32Type>())
                .collect();
            Arc::new(pick(&arrays, order, Int32Array::value).collect::<Int32Array>())
        }
        DataType::Float64 => {
            let arrays: Vec<&Float64Array> = batches
                .iter()
                .map(|b| b.column(c).as_primitive::<Float64Type>())
                .collect();
            Arc::new(pick(&arrays, order, Float64Array::value).collect::<Float64Array>())
        }
        DataType::Boolean => {
            let arrays: Vec<&BooleanArray> =
                batches.iter().map(|b| b.column(c).as_boolean()).collect();
            Arc::new(pick(&arrays, order, BooleanArray::value).collect::<BooleanArray>())
        }
        _ => {
            let arrays: Vec<&TimestampMillisecondArray> = batches
                .iter()
                .map(|b| b.column(c).as_primitive::<TimestampMillisecondType>())
                .collect();
            Arc::new(
                pick(&arrays, order, TimestampMillisecondArray::value)
                    .collect::<TimestampMillisecondArray>()
                    .with_timezone("UTC"),
            )
        }
    }
}

/// [`RequestSchema::hash`] of the catalog-only schema.
pub fn schema_hash() -> &'static str {
    RequestSchema::base().hash()
}

/// Strict conversion for operational logs: rows are JSON objects, unknown fields are ignored and
/// type mismatches become null.
pub(crate) fn rows_to_parquet(
    columns: &[Column],
    rows: &[Map<String, Value>],
) -> Result<Vec<u8>, String> {
    let mut builders: Vec<Builder> = columns.iter().map(|c| Builder::new(c.kind)).collect();
    for row in rows {
        for (builder, column) in builders.iter_mut().zip(columns) {
            builder.push_strict(row.get(column.id));
        }
    }
    let schema = arrow_schema(columns);
    let arrays = builders.iter_mut().map(Builder::finish).collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|_| "record_batch")?;
    write_batches(schema, &[&batch])
}

fn write_batches(schema: SchemaRef, batches: &[&RecordBatch]) -> Result<Vec<u8>, String> {
    write_with_metadata(
        schema,
        batches,
        vec![KeyValue::new(
            "flp_schema".into(),
            SCHEMA_VERSION.to_string(),
        )],
    )
}

pub(crate) fn encode_record_batch(
    schema: SchemaRef,
    batch: &RecordBatch,
    metadata: Vec<KeyValue>,
) -> Result<Vec<u8>, String> {
    write_with_metadata(schema, &[batch], metadata)
}

/// Every Parquet output is ZSTD(1).
pub(crate) fn write_with_metadata(
    schema: SchemaRef,
    batches: &[&RecordBatch],
    metadata: Vec<KeyValue>,
) -> Result<Vec<u8>, String> {
    write_with(schema, batches, metadata, None)
}

fn write_with(
    schema: SchemaRef,
    batches: &[&RecordBatch],
    metadata: Vec<KeyValue>,
    sorting: Option<Vec<SortingColumn>>,
) -> Result<Vec<u8>, String> {
    let level = ZstdLevel::try_new(1).map_err(|_| "parquet_zstd_level")?;
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .set_max_row_group_row_count(Some(65_536))
        .set_statistics_truncate_length(Some(64))
        .set_key_value_metadata(Some(metadata))
        .set_sorting_columns(sorting);
    for field in schema.fields() {
        let name = field.name().as_str();
        let path = ColumnPath::from(name);
        match (name, field.data_type()) {
            ("timestamp", DataType::Timestamp(_, _)) => {
                builder = builder
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path, Encoding::DELTA_BINARY_PACKED);
            }
            ("ttfb" | "metric_value" | "metric_delta", DataType::Float64) => {
                builder = builder
                    .set_column_dictionary_enabled(path.clone(), false)
                    .set_column_encoding(path, Encoding::BYTE_STREAM_SPLIT);
            }
            ("fastly_req_id" | "req_id" | "tcp_info" | "_extra", DataType::Utf8) => {
                builder = builder.set_column_dictionary_enabled(path, false);
            }
            _ => {}
        }
    }
    let properties = builder.build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), schema, Some(properties)).map_err(|_| "parquet_writer")?;
    for batch in batches {
        writer.write(batch).map_err(|_| "parquet_write")?;
    }
    writer.into_inner().map_err(|_| "parquet_finish".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: i64 = 1_700_000_040;

    fn line(fields: &str) -> Vec<u8> {
        format!("{{\"ts_ms\":{},{fields}}}", BASE * 1000 + 7).into_bytes()
    }

    fn with_bytes(prefix: &str, raw: &[u8], suffix: &str) -> Vec<u8> {
        let mut out = line(prefix);
        out.pop();
        out.extend_from_slice(raw);
        out.extend_from_slice(suffix.as_bytes());
        out.push(b'}');
        out
    }

    /// Valid and invalid UTF-8 in known and skipped values, escapes, malformed strings,
    /// repeated and out-of-order keys, and integer limits.
    fn corpus() -> Vec<(Vec<u8>, bool)> {
        vec![
            (line(r#""ip":"a","url":"/café/é","zz":"ü""#), true),
            (
                with_bytes(r#""ip":"b","zz":""#, b"\xff\xfe", r#"","status":200"#),
                true,
            ),
            (with_bytes(r#""ip":"#, b"\"\xff\"", ""), false),
            (with_bytes(r#""ip":"c","#, b"\"\xff\":1", ""), false),
            (
                line(r#""ip":"d","url":"/a\"b\\c\u0041\n","ua":"\ud83d\ude00""#),
                true,
            ),
            (line(r#""\u0069p":"escaped-key","\u0074tl":1.5"#), true),
            (line(r#""ip":"\ud800""#), false),
            (line(r#""ip":"a	b""#), false),
            (line(r#""ip":"\x""#), false),
            (line(r#""zz":"\q","ip":"e""#), false),
            (line(r#""ip":"unterminated"#), false),
            (
                line(r#""ip":"first","status":404,"ip":"last","status":" 503 ""#),
                true,
            ),
            (
                line(r#""url":"/z","status":201,"ipx":"no","ip":"f","host":"h","timestamp":null"#),
                true,
            ),
            (
                line(r#""ip":"g","status":9223372036854775807,"elapsed":9223372036854775808"#),
                true,
            ),
            (
                line(r#""ip":"h","status":-9223372036854775808,"elapsed":18446744073709551616"#),
                true,
            ),
            (
                line(r#""ip":"i","status":1.0,"elapsed":1e2,"edge":1,"is_ipv6":"false""#),
                true,
            ),
            (line(r#""ip":"j","zz":1e400"#), true),
            (line(r#""ip":"k","status":1e400"#), false),
            (line(r#""ip":"l"} "#), false),
        ]
    }

    fn show(value: &Option<Raw<'_>>) -> String {
        match value {
            None => "absent".into(),
            Some(Raw::Str(s)) => format!("s:{s}"),
            Some(Raw::Num(n)) => format!("n:{n}"),
            Some(Raw::Bool(b)) => format!("b:{b}"),
            Some(Raw::Null) => "null".into(),
            Some(Raw::Other) => "other".into(),
        }
    }

    #[test]
    fn decode_fast_paths_match_byte_reader_with_hash_lookup() {
        let mut prior = RequestSchema::new(Vec::new());
        prior.names.clear();
        let schema = RequestSchema::base();
        for (line, ok) in corpus() {
            let mut fast = vec![None; schema.kinds.len()];
            let mut slow = vec![None; schema.kinds.len()];
            let fast_ok = decode_row(schema, &line, &mut fast);
            let slow_ok = decode_from(
                serde_json::Deserializer::from_slice(&line),
                &prior,
                &mut slow,
            );
            let text = String::from_utf8_lossy(&line);
            assert_eq!((fast_ok, slow_ok), (ok, ok), "{text}");
            if ok {
                let fast: Vec<_> = fast.iter().map(show).collect();
                let slow: Vec<_> = slow.iter().map(show).collect();
                assert_eq!(fast, slow, "{text}");
            }
        }
    }

    #[test]
    fn convert_keeps_values_and_counts_bad_lines() {
        let lines = corpus();
        let mut text = Vec::new();
        for (line, _) in &lines {
            text.extend_from_slice(line);
            text.push(b'\n');
        }
        let data = zstd::stream::encode_all(&text[..], 1).unwrap();
        let rows = convert_object(&data, 60, BASE).unwrap();
        let bad = lines.iter().filter(|(_, ok)| !ok).count() as u64;
        assert_eq!(
            (rows.lines_read, rows.bad_lines, rows.rows),
            (lines.len() as u64, bad, lines.len() as u64 - bad)
        );
        assert_eq!(rows.windows.keys().copied().collect::<Vec<_>>(), [BASE]);
        let batch = &rows.windows[&BASE];
        let s = |name: &str| -> Vec<Option<String>> {
            let col = batch.column_by_name(name).unwrap().as_string::<i32>();
            col.iter().map(|v| v.map(str::to_owned)).collect()
        };
        let i = |name: &str| -> Vec<Option<i64>> {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_primitive::<Int64Type>();
            col.iter().collect()
        };
        let b = |name: &str| -> Vec<Option<bool>> {
            batch
                .column_by_name(name)
                .unwrap()
                .as_boolean()
                .iter()
                .collect()
        };
        let some = |v: &str| Some(v.to_owned());
        assert_eq!(
            s("ip"),
            [
                some("a"),
                some("b"),
                some("d"),
                some("escaped-key"),
                some("last"),
                some("f"),
                some("g"),
                some("h"),
                some("i"),
                some("j"),
            ]
        );
        assert_eq!(
            s("url"),
            [
                some("/café/é"),
                None,
                some("/a\"b\\cA\n"),
                None,
                None,
                some("/z"),
                None,
                None,
                None,
                None,
            ]
        );
        assert_eq!(s("ua")[2], some("\u{1f600}"));
        assert_eq!(s("host")[5], some("h"));
        assert_eq!(
            i("status"),
            [
                None,
                Some(200),
                None,
                None,
                Some(503),
                Some(201),
                Some(i64::MAX),
                Some(i64::MIN),
                None,
                None,
            ]
        );
        assert_eq!(i("elapsed")[6..9], [None, None, None]);
        let ttl = batch
            .column_by_name("ttl")
            .unwrap()
            .as_primitive::<Float64Type>();
        assert_eq!(ttl.iter().nth(3), Some(Some(1.5)));
        assert_eq!((b("edge")[8], b("is_ipv6")[8]), (Some(true), Some(false)));
        let ts = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        assert_eq!(ts.null_count(), ts.len());
    }
}
