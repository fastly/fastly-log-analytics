//! Saved filter views, shared by every user of the install like the self-hosted `/api/views`.
//! One KV record per service, `views:<service id>`, holds the list; writes go through a
//! compare-and-swap so two browsers saving at once both land.

use crate::admin::Record;
use crate::auth::{Error, Role};
use crate::http::{Request, Response};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const MAX_VIEWS: usize = 200;
const MAX_NAME_CHARS: usize = 120;
const MAX_FILTERS_BYTES: usize = 32 * 1024;
const MAX_FIELD_CHARS: usize = 200;
const DEFAULT_LIMIT: usize = 500;
const MAX_LIMIT: usize = 2000;

/// The subset of the runtime a view store needs, so the logic is testable without a Compute host.
pub trait ViewStore {
    fn get(&self, key: &str) -> Result<Option<Record>, String>;
    fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String>;
    fn random(&self) -> Result<[u8; 32], String>;
    fn now(&self) -> u64;
}

impl<R: crate::shell::Runtime> ViewStore for R {
    fn get(&self, key: &str) -> Result<Option<Record>, String> {
        self.admin_get(key)
    }
    fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String> {
        self.admin_cas(key, generation, value)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        crate::shell::Runtime::random(self)
    }
    fn now(&self) -> u64 {
        crate::shell::Runtime::now(self)
    }
}

/// The wire shape of the self-hosted `SavedViewRecord`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SavedView {
    pub id: String,
    pub service_id: String,
    pub name: String,
    pub filters_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewInput {
    #[serde(default)]
    id: Option<String>,
    service_id: String,
    name: String,
    filters_json: String,
    #[serde(default)]
    time_range_type: Option<String>,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default)]
    page: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
}

pub fn key(service_id: &str) -> String {
    format!("views:{service_id}")
}

fn parse_views(value: &Value) -> Vec<SavedView> {
    serde_json::from_value(value["views"].clone()).unwrap_or_default()
}

fn load<S: ViewStore>(
    store: &S,
    service_id: &str,
) -> Result<(Option<String>, Vec<SavedView>), Error> {
    match store.get(&key(service_id)).map_err(|_| Error::Storage)? {
        Some(record) => Ok((Some(record.generation), parse_views(&record.value))),
        None => Ok((None, Vec::new())),
    }
}

pub const PII_FILTER_KEYS: &[&str] = &[
    "ip",
    "client_ip",
    "c_ip",
    "ip_address",
    "remote_addr",
    "oip",
    "x-forwarded-for",
    "x_forwarded_for",
    "true-client-ip",
    "true_client_ip",
    "fastly-client-ip",
    "fastly_client_ip",
    "cookie_session",
    "cid",
    "client_id",
    "edge_sid",
    "sid",
    "session_id",
];

const PII_NONWORD_KEYS: &[&str] = &[
    "ip",
    "clientip",
    "cip",
    "ipaddress",
    "remoteaddr",
    "oip",
    "xforwardedfor",
    "trueclientip",
    "fastlyclientip",
    "cookiesession",
    "cid",
    "clientid",
    "edgesid",
    "sid",
    "sessionid",
];

fn is_pii_ws_or_ctrl(ch: char) -> bool {
    let u = ch as u32;
    u <= 0x1f
        || u == 0x20
        || u == 0x7f
        || u == 0x0085
        || u == 0x00a0
        || u == 0x1680
        || u == 0x180e
        || (0x2000..=0x200d).contains(&u)
        || matches!(
            u,
            0x2028 | 0x2029 | 0x202f | 0x205f | 0x2060 | 0x3000 | 0xfeff
        )
        || ch.is_whitespace()
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn decode_pii_url_once(s: &str) -> String {
    if !s.contains('%') {
        return s.to_owned();
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_nibble(bytes[i + 1]), hex_nibble(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn decode_pii_url_twice(s: &str) -> String {
    let first = decode_pii_url_once(s);
    if !first.contains('%') {
        return first;
    }
    decode_pii_url_once(&first)
}

fn normalize_pii_unicode(s: &str) -> String {
    if s.is_ascii() {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\u{0130}' => out.push('I'),
            '\u{0307}' => {}
            '\u{212a}' => out.push('K'),
            _ => out.push(ch),
        }
    }
    out
}

fn camel_to_snake_lower(s: &str) -> String {
    // Normalize IPv4/IPv6/IPV4/IPV6 -> Ipv4/Ipv6 before camelCase splitting.
    let bytes = s.as_bytes();
    let mut pre = String::with_capacity(s.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if i + 3 < bytes.len()
            && bytes[i] == b'I'
            && bytes[i + 1] == b'P'
            && matches!(bytes[i + 2], b'v' | b'V')
            && matches!(bytes[i + 3], b'4' | b'6')
        {
            pre.push_str("Ipv");
            pre.push(bytes[i + 3] as char);
            i += 4;
        } else {
            let ch = s[i..].chars().next().unwrap();
            pre.push(ch);
            i += ch.len_utf8();
        }
    }
    let chars: Vec<char> = pre.chars().collect();
    let mut out = String::with_capacity(pre.len() + 8);
    for idx in 0..chars.len() {
        let cur = chars[idx];
        if idx > 0 {
            let prev = chars[idx - 1];
            let next_is_lower = chars.get(idx + 1).is_some_and(char::is_ascii_lowercase);
            let split_1 =
                (prev.is_ascii_lowercase() || prev.is_ascii_digit()) && cur.is_ascii_uppercase();
            let split_2 = prev.is_ascii_uppercase() && cur.is_ascii_uppercase() && next_is_lower;
            if split_1 || split_2 {
                out.push('_');
            }
        }
        out.push(cur.to_ascii_lowercase());
    }
    out
}

/// O(N) iterative bracket stripping matching `console_access.py::_strip_pii_brackets`.
fn strip_pii_brackets(s: &str) -> String {
    if !s.contains('[') {
        return s.to_owned();
    }
    let mut stack: Vec<String> = vec![String::with_capacity(s.len())];
    for ch in s.chars() {
        match ch {
            '[' => stack.push(String::from("[")),
            ']' => {
                if stack.len() > 1 {
                    stack.pop();
                } else {
                    stack[0].push(']');
                }
            }
            _ => stack.last_mut().unwrap().push(ch),
        }
    }
    stack.concat()
}

fn normalize_pii_ipv(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if i + 4 < bytes.len()
            && (bytes[i].is_ascii_lowercase() || bytes[i] == b'_')
            && &bytes[i + 1..i + 4] == b"ipv"
            && matches!(bytes[i + 4], b'4' | b'6')
            && (i + 5 == bytes.len() || bytes[i + 5] == b'_')
        {
            out.push(bytes[i] as char);
            out.push_str("ip_");
            i += if i + 5 < bytes.len() && bytes[i + 5] == b'_' {
                6
            } else {
                5
            };
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out.trim_matches('_').to_owned()
}

fn strip_pii_affix_prefixes(mut s: &str) -> &str {
    const PREFIXES: &[&str] = &[
        "x_filter_",
        "xfilter_",
        "filter_",
        "_cf_",
        "cf_",
        "request_http_",
        "response_http_",
        "req_http_",
        "resp_http_",
        "bereq_http_",
        "beresp_http_",
        "obj_http_",
        "http_",
        "request_",
        "response_",
        "req_",
        "resp_",
        "headers_",
        "header_",
        "original_",
        "orig_",
        "raw_",
    ];
    loop {
        let mut changed = false;
        for prefix in PREFIXES {
            if let Some(rest) = s.strip_prefix(prefix) {
                s = rest;
                changed = true;
                break;
            }
        }
        if !changed {
            return s.trim_matches('_');
        }
    }
}

fn strip_pii_suffixes(mut s: &str) -> &str {
    loop {
        let before = s;
        if let Some(rest) = s.strip_suffix("_hash") {
            s = rest;
        } else if let Some(rest) = s.strip_suffix("_addr") {
            s = rest;
        } else {
            let trimmed_digits = s.trim_end_matches(|c: char| c.is_ascii_digit());
            if trimmed_digits.len() < s.len() {
                if let Some(rest) = trimmed_digits.strip_suffix("_v") {
                    s = rest;
                } else {
                    s = trimmed_digits.strip_suffix('_').unwrap_or(trimmed_digits);
                }
            }
        }
        if s == before {
            return s.trim_matches('_');
        }
    }
}

fn matches_pii_canon(cand: &str) -> bool {
    if cand.is_empty() {
        return false;
    }
    if PII_FILTER_KEYS.contains(&cand) {
        return true;
    }
    let no_under: String = cand.chars().filter(|&c| c != '_').collect();
    !no_under.is_empty() && PII_NONWORD_KEYS.contains(&no_under.as_str())
}

fn segment_is_pii(seg: &str) -> bool {
    if seg.is_empty() {
        return false;
    }
    if PII_FILTER_KEYS.contains(&seg) {
        return true;
    }
    let mut canon = String::with_capacity(seg.len());
    let mut prev_sep = false;
    let mut nw_under = String::with_capacity(seg.len());
    for ch in seg.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            canon.push(c);
            nw_under.push(c);
            prev_sep = false;
        } else {
            if c == '_' {
                nw_under.push('_');
            }
            if !prev_sep && !canon.is_empty() {
                canon.push('_');
                prev_sep = true;
            }
        }
    }
    let canon = canon.trim_matches('_');
    if canon.is_empty() && nw_under.is_empty() {
        return false;
    }
    let ipv_norm = if canon.is_empty() {
        String::new()
    } else {
        normalize_pii_ipv(canon)
    };
    let affix_stripped = strip_pii_affix_prefixes(&ipv_norm);
    let no_suffix = strip_pii_suffixes(&ipv_norm);
    let stripped = strip_pii_suffixes(affix_stripped);

    let nw_ipv = if nw_under.is_empty() {
        String::new()
    } else {
        normalize_pii_ipv(&nw_under)
    };
    let nw_affix = strip_pii_affix_prefixes(&nw_ipv);
    let nw_stripped = strip_pii_suffixes(nw_affix);

    let matched = [
        canon,
        ipv_norm.as_str(),
        affix_stripped,
        no_suffix,
        stripped,
        nw_ipv.as_str(),
        nw_affix,
        nw_stripped,
    ]
    .into_iter()
    .any(matches_pii_canon);
    matched
}

fn is_pii_split_outer(ch: char) -> bool {
    matches!(
        ch,
        '[' | ']' | '/' | ':' | ';' | '|' | ',' | '&' | '=' | '?' | '{' | '}'
    ) || is_pii_ws_or_ctrl(ch)
}

fn is_pii_split_all(ch: char) -> bool {
    ch == '.' || is_pii_split_outer(ch)
}

fn is_pii_key_exact(candidate: &str) -> bool {
    let raw_pre = normalize_pii_unicode(candidate)
        .trim_matches(is_pii_ws_or_ctrl)
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']'))
        .to_ascii_lowercase();
    if raw_pre.is_empty() {
        return false;
    }
    if segment_is_pii(&raw_pre) {
        return true;
    }
    let decoded = normalize_pii_unicode(&decode_pii_url_twice(candidate));
    let raw_lower = decoded
        .trim_matches(is_pii_ws_or_ctrl)
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']'))
        .to_ascii_lowercase();
    let camel = camel_to_snake_lower(&decoded)
        .trim_matches(is_pii_ws_or_ctrl)
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']'))
        .to_owned();
    let raw_unbrack = strip_pii_brackets(&raw_lower)
        .trim_matches(is_pii_ws_or_ctrl)
        .to_owned();
    let camel_unbrack = strip_pii_brackets(&camel)
        .trim_matches(is_pii_ws_or_ctrl)
        .to_owned();
    [raw_lower, camel, raw_unbrack, camel_unbrack]
        .iter()
        .any(|cand| segment_is_pii(cand))
}

const MAX_JSON_DEPTH: usize = 16;

fn is_sql_unicode_ws(ch: char) -> bool {
    ch.is_whitespace()
        || matches!(
            ch,
            '\u{180e}' | '\u{200b}'..='\u{200d}' | '\u{2060}' | '\u{feff}'
        )
}

fn trim_sql_ws(s: &str) -> &str {
    s.trim_matches(is_sql_unicode_ws)
}

fn is_extra_column_ident(raw: &str) -> bool {
    let trimmed = trim_sql_ws(raw)
        .trim_start_matches(['+', '-', '!', '$'])
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']' | '{' | '}'));
    if trimmed.eq_ignore_ascii_case("_extra") {
        return true;
    }
    trimmed
        .split(['.', ':', '[', ']', '{', '}', ','])
        .any(|seg| {
            let s = trim_sql_ws(seg)
                .trim_start_matches(['+', '-', '!', '$'])
                .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']' | '{' | '}'));
            s.eq_ignore_ascii_case("_extra")
        })
}

fn is_pii_identifier(raw: &str) -> bool {
    let raw_pre = normalize_pii_unicode(raw)
        .trim_matches(is_pii_ws_or_ctrl)
        .trim_start_matches(['+', '-', '!', '$'])
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']' | '{' | '}'))
        .to_ascii_lowercase();
    if raw_pre.is_empty() {
        return false;
    }
    if segment_is_pii(&raw_pre) {
        return true;
    }
    let decoded = normalize_pii_unicode(&decode_pii_url_twice(raw));
    let raw_lower = decoded
        .trim_matches(is_pii_ws_or_ctrl)
        .trim_start_matches(['+', '-', '!', '$'])
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']' | '{' | '}'))
        .to_ascii_lowercase();
    let camel = camel_to_snake_lower(&decoded)
        .trim_matches(is_pii_ws_or_ctrl)
        .trim_start_matches(['+', '-', '!', '$'])
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']' | '{' | '}'))
        .to_owned();
    let raw_unbrack = strip_pii_brackets(&raw_lower)
        .trim_matches(is_pii_ws_or_ctrl)
        .to_owned();
    let camel_unbrack = strip_pii_brackets(&camel)
        .trim_matches(is_pii_ws_or_ctrl)
        .to_owned();

    for base in [&raw_lower, &camel, &raw_unbrack, &camel_unbrack] {
        if base.is_empty() {
            continue;
        }
        if segment_is_pii(base) {
            return true;
        }
        for seg in base.split(is_pii_split_outer) {
            let s = seg
                .trim_start_matches(['+', '-', '!', '$'])
                .trim_matches(|c| matches!(c, '"' | '\'' | '`'));
            if !s.is_empty() && segment_is_pii(s) {
                return true;
            }
        }
        for seg in base.split(is_pii_split_all) {
            let s = seg
                .trim_start_matches(['+', '-', '!', '$'])
                .trim_matches(|c| matches!(c, '"' | '\'' | '`'));
            if !s.is_empty() && segment_is_pii(s) {
                return true;
            }
        }
    }
    false
}

fn decode_rfc8259_escapes(raw: &str) -> Option<String> {
    if raw.contains('\0') {
        return None;
    }
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut idx = 0usize;
    while idx < bytes.len() {
        if bytes[idx] != b'\\' {
            let ch = raw.get(idx..)?.chars().next()?;
            out.push(ch);
            idx += ch.len_utf8();
            continue;
        }
        let esc = *bytes.get(idx + 1)?;
        match esc {
            b'"' => {
                out.push('"');
                idx += 2;
            }
            b'\\' => {
                out.push('\\');
                idx += 2;
            }
            b'/' => {
                out.push('/');
                idx += 2;
            }
            b'b' => {
                out.push('\x08');
                idx += 2;
            }
            b'f' => {
                out.push('\x0c');
                idx += 2;
            }
            b'n' => {
                out.push('\n');
                idx += 2;
            }
            b'r' => {
                out.push('\r');
                idx += 2;
            }
            b't' => {
                out.push('\t');
                idx += 2;
            }
            b'u' => {
                let hex_hi = std::str::from_utf8(bytes.get(idx + 2..idx + 6)?).ok()?;
                let hi = u16::from_str_radix(hex_hi, 16).ok()?;
                if hi == 0 {
                    return None;
                }
                if (0xD800..=0xDBFF).contains(&hi) {
                    if bytes.get(idx + 6) != Some(&b'\\') || bytes.get(idx + 7) != Some(&b'u') {
                        return None;
                    }
                    let hex_lo = std::str::from_utf8(bytes.get(idx + 8..idx + 12)?).ok()?;
                    let lo = u16::from_str_radix(hex_lo, 16).ok()?;
                    if !(0xDC00..=0xDFFF).contains(&lo) {
                        return None;
                    }
                    let cp = 0x10000 + ((u32::from(hi - 0xD800)) << 10) + u32::from(lo - 0xDC00);
                    out.push(char::from_u32(cp)?);
                    idx += 12;
                } else if (0xDC00..=0xDFFF).contains(&hi) {
                    return None;
                } else {
                    out.push(char::from_u32(u32::from(hi))?);
                    idx += 6;
                }
            }
            _ => return None,
        }
    }
    if out.contains('\0') {
        return None;
    }
    Some(out)
}

fn check_single_key_or_json_path(raw: &str) -> bool {
    if raw.contains('\0') {
        return true;
    }
    let trimmed = trim_sql_ws(raw);
    if trimmed.is_empty()
        || matches!(trimmed, "$" | "/" | "{}" | "[]")
        || trimmed.ends_with('.')
        || trimmed.contains("..")
        || !trimmed.bytes().any(|b| b.is_ascii_alphabetic())
    {
        return true;
    }
    if raw.contains('*') || raw.contains('?') || is_pii_identifier(trimmed) {
        return true;
    }
    if trimmed.starts_with('/') {
        return trimmed.split('/').any(|seg| {
            if seg.is_empty() {
                return false;
            }
            let decoded = seg.replace("~1", "/").replace("~0", "~");
            is_pii_identifier(&decoded)
                || (decoded.contains('/')
                    && decoded
                        .split('/')
                        .any(|sub| !sub.is_empty() && is_pii_identifier(sub)))
        });
    }
    false
}

fn is_pii_key_or_json_path(raw: &str) -> bool {
    if check_single_key_or_json_path(raw) {
        return true;
    }
    if raw.contains('\\') {
        let Some(decoded) = decode_rfc8259_escapes(raw) else {
            return true;
        };
        if check_single_key_or_json_path(&decoded) {
            return true;
        }
    }
    false
}

fn columns_arg_references_pii(lit: &str) -> bool {
    if lit.contains('\0') {
        return true;
    }
    let Some(inner) = trim_sql_ws(lit)
        .strip_prefix('^')
        .and_then(|s| s.strip_suffix('$'))
    else {
        return true;
    };
    let body = if let Some(unwrapped) = inner.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        if unwrapped.contains(['(', ')']) {
            return true;
        }
        unwrapped
    } else {
        if inner.contains(['(', ')', '|']) {
            return true;
        }
        inner
    };
    if body.is_empty() {
        return true;
    }
    body.split('|').any(|alt| {
        alt.is_empty()
            || !alt
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            || is_pii_identifier(alt)
            || is_extra_column_ident(alt)
            || (alt.contains('-')
                && alt
                    .split('-')
                    .any(|seg| is_pii_key_exact(seg) || is_extra_column_ident(seg)))
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LastSig {
    Other,
    IdentOrClose,
    JsonArrow,
}

fn is_dolq_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

fn is_dolq_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

fn decode_hex_char(bytes: &[u8]) -> Option<char> {
    if !bytes.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let hex_str = std::str::from_utf8(bytes).ok()?;
    let code = u32::from_str_radix(hex_str, 16).ok()?;
    if code == 0 {
        return None;
    }
    char::from_u32(code)
}

/// Decodes a single backslash escape starting at `bytes[i] == b'\\'` inside a SQL `E'...'`
/// string literal, returning the next byte index or `None` on malformed/unterminated escape.
fn decode_sql_e_escape(s: &str, i: usize, out: &mut Option<String>) -> Option<usize> {
    let bytes = s.as_bytes();
    let esc = *bytes.get(i + 1)?;
    match esc {
        b'b' => {
            if let Some(buf) = out.as_mut() {
                buf.push('\x08');
            }
            Some(i + 2)
        }
        b'f' => {
            if let Some(buf) = out.as_mut() {
                buf.push('\x0c');
            }
            Some(i + 2)
        }
        b'n' => {
            if let Some(buf) = out.as_mut() {
                buf.push('\n');
            }
            Some(i + 2)
        }
        b'r' => {
            if let Some(buf) = out.as_mut() {
                buf.push('\r');
            }
            Some(i + 2)
        }
        b't' => {
            if let Some(buf) = out.as_mut() {
                buf.push('\t');
            }
            Some(i + 2)
        }
        b'x' => {
            let mut count = 0usize;
            while count < 2 && bytes.get(i + 2 + count).is_some_and(u8::is_ascii_hexdigit) {
                count += 1;
            }
            if count == 0 {
                if let Some(buf) = out.as_mut() {
                    buf.push('x');
                }
                return Some(i + 2);
            }
            let hex_str = std::str::from_utf8(&bytes[i + 2..i + 2 + count]).ok()?;
            let val = u8::from_str_radix(hex_str, 16).ok()?;
            if val == 0 || val >= 0x80 {
                return None;
            }
            if let Some(buf) = out.as_mut() {
                buf.push(val as char);
            }
            Some(i + 2 + count)
        }
        b'u' => {
            let ch = decode_hex_char(bytes.get(i + 2..i + 6)?)?;
            if let Some(buf) = out.as_mut() {
                buf.push(ch);
            }
            Some(i + 6)
        }
        b'U' => {
            let ch = decode_hex_char(bytes.get(i + 2..i + 10)?)?;
            if let Some(buf) = out.as_mut() {
                buf.push(ch);
            }
            Some(i + 10)
        }
        b'0'..=b'7' => {
            let mut count = 1usize;
            while count < 3
                && bytes
                    .get(i + 1 + count)
                    .is_some_and(|b| matches!(*b, b'0'..=b'7'))
            {
                count += 1;
            }
            let oct_str = std::str::from_utf8(&bytes[i + 1..i + 1 + count]).ok()?;
            let val = u32::from_str_radix(oct_str, 8).ok()?;
            let byte = (val & 0xff) as u8;
            if byte == 0 || byte >= 0x80 {
                return None;
            }
            if let Some(buf) = out.as_mut() {
                buf.push(byte as char);
            }
            Some(i + 1 + count)
        }
        _ => {
            let next_ch = s.get(i + 1..)?.chars().next()?;
            if next_ch == '\0' {
                return None;
            }
            if let Some(buf) = out.as_mut() {
                buf.push(next_ch);
            }
            Some(i + 1 + next_ch.len_utf8())
        }
    }
}

/// Decodes a single `\` escape inside `U&"..."` or `U&'...'`, returning the next index or `None`.
fn decode_sql_unicode_escape(bytes: &[u8], i: usize, out: &mut String) -> Option<usize> {
    match bytes.get(i + 1)? {
        b'\\' => {
            out.push('\\');
            Some(i + 2)
        }
        b'+' => {
            let ch = decode_hex_char(bytes.get(i + 2..i + 8)?)?;
            out.push(ch);
            Some(i + 8)
        }
        _ => {
            let ch = decode_hex_char(bytes.get(i + 1..i + 5)?)?;
            out.push(ch);
            Some(i + 5)
        }
    }
}

/// Advances past ASCII/UTF-8 whitespace and SQL `--` / `/* ... */` comments, returning `None`
/// if an unclosed block comment is encountered.
fn skip_ws_and_comments(s: &str, mut i: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] >= 0x80 {
            if let Some(ch) = s.get(i..).and_then(|tail| tail.chars().next()) {
                if is_sql_unicode_ws(ch) {
                    i += ch.len_utf8();
                    continue;
                }
            }
        }
        if bytes[i] == b'-' && bytes.get(i + 1) == Some(&b'-') {
            i += 2;
            while i < bytes.len() && !matches!(bytes[i], b'\n' | b'\r') {
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            let mut depth = 1u32;
            while i < bytes.len() && depth > 0 {
                if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth > 0 {
                return None;
            }
            continue;
        }
        break;
    }
    Some(i)
}

/// Peeks past whitespace and SQL comments after a string literal to detect `:` in `{ 'k': v }`.
fn next_non_ws_or_comment_is_colon(s: &str, i: usize) -> bool {
    let bytes = s.as_bytes();
    skip_ws_and_comments(s, i)
        .is_some_and(|j| bytes.get(j) == Some(&b':') && bytes.get(j + 1) != Some(&b':'))
}

fn next_non_ws_or_comment_is_open_paren(s: &str, i: usize) -> bool {
    skip_ws_and_comments(s, i).is_some_and(|j| s.as_bytes().get(j) == Some(&b'('))
}

fn next_non_ws_or_comment_is_concat_pipe(s: &str, i: usize) -> bool {
    let bytes = s.as_bytes();
    skip_ws_and_comments(s, i)
        .is_some_and(|j| bytes.get(j) == Some(&b'|') && bytes.get(j + 1) == Some(&b'|'))
}

fn is_dynamic_key_fn(token: &str) -> bool {
    matches!(
        token.trim().to_ascii_lowercase().as_str(),
        "chr"
            | "char"
            | "concat"
            | "concat_ws"
            | "printf"
            | "format"
            | "string_agg"
            | "array_to_string"
            | "list_aggr"
    )
}

fn peek_simple_bracket_pii(rest: &str) -> bool {
    let Some(rel_end) = rest.find(']') else {
        return false;
    };
    if rel_end == 0 {
        return false;
    }
    let inner = &rest[..rel_end];
    inner
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-' | b' '))
        && is_pii_identifier(inner)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExtractArgMode {
    KeyOrPath,
    JsonContainsNeedle,
    JsonStructSchema,
}

#[derive(Clone, Copy)]
struct ExtractFrame {
    paren_depth: u32,
    bracket_depth: u32,
    brace_depth: u32,
    arg_idx: u32,
    mode: ExtractArgMode,
}

fn extract_fn_mode(token: &str) -> Option<ExtractArgMode> {
    match trim_sql_ws(token).to_ascii_lowercase().as_str() {
        "json_extract"
        | "json_extract_string"
        | "json_extract_path"
        | "json_extract_path_text"
        | "json_value"
        | "json_exists"
        | "struct_extract"
        | "map_extract"
        | "element_at"
        | "map_extract_value"
        | "variant_extract"
        | "union_extract"
        | "struct_has"
        | "map_contains"
        | "map_contains_entry" => Some(ExtractArgMode::KeyOrPath),
        "json_contains" => Some(ExtractArgMode::JsonContainsNeedle),
        "json_transform" | "json_transform_strict" | "from_json" | "from_json_strict" => {
            Some(ExtractArgMode::JsonStructSchema)
        }
        _ => None,
    }
}

fn is_safe_duckdb_scalar_schema_type(raw_ty: &str) -> bool {
    let trimmed = trim_sql_ws(raw_ty);
    if trimmed.is_empty() || trimmed.contains('[') || trimmed.contains(']') {
        return false;
    }
    let base = if let Some((b, rest)) = trimmed.split_once('(') {
        let Some(inner) = rest.strip_suffix(')') else {
            return false;
        };
        let inner_trimmed = trim_sql_ws(inner);
        if inner_trimmed.is_empty()
            || !inner_trimmed
                .bytes()
                .all(|ch| ch.is_ascii_digit() || matches!(ch, b',' | b' '))
        {
            return false;
        }
        trim_sql_ws(b)
    } else {
        trimmed
    };
    matches!(
        base.to_ascii_lowercase().as_str(),
        "varchar"
            | "text"
            | "string"
            | "char"
            | "bpchar"
            | "bigint"
            | "int8"
            | "long"
            | "ubigint"
            | "hugeint"
            | "uhugeint"
            | "integer"
            | "int"
            | "int4"
            | "signed"
            | "uinteger"
            | "smallint"
            | "int2"
            | "short"
            | "usmallint"
            | "tinyint"
            | "int1"
            | "utinyint"
            | "double"
            | "float8"
            | "float"
            | "float4"
            | "real"
            | "decimal"
            | "numeric"
            | "boolean"
            | "bool"
            | "logical"
            | "date"
            | "time"
            | "timetz"
            | "timestamp"
            | "datetime"
            | "timestamptz"
            | "timestamp_s"
            | "timestamp_ms"
            | "timestamp_ns"
            | "interval"
            | "uuid"
            | "blob"
            | "bytea"
    )
}

fn json_contains_object_is_safe(val: &Value, depth: usize) -> bool {
    if depth > MAX_JSON_DEPTH {
        return false;
    }
    let Value::Object(map) = val else {
        return false;
    };
    if map.is_empty() {
        return false;
    }
    map.iter().all(|(k, v)| {
        !k.contains('\0')
            && !is_pii_key_or_json_path(k)
            && !is_extra_column_ident(k)
            && match v {
                Value::String(s) => !s.contains('\0'),
                Value::Number(_) | Value::Bool(_) | Value::Null => true,
                Value::Object(_) => json_contains_object_is_safe(v, depth + 1),
                Value::Array(_) => false,
            }
    })
}

fn json_contains_needle_references_pii(lit: &str) -> bool {
    if lit.contains('\0') || json_has_duplicate_keys(lit) {
        return true;
    }
    let Ok(parsed) = serde_json::from_str::<Value>(lit) else {
        return true;
    };
    !json_contains_object_is_safe(&parsed, 0)
}

fn json_struct_schema_object_is_safe(val: &Value, depth: usize) -> bool {
    if depth > MAX_JSON_DEPTH {
        return false;
    }
    let Value::Object(map) = val else {
        return false;
    };
    if map.is_empty() {
        return false;
    }
    map.iter().all(|(k, v)| {
        !k.contains('\0')
            && !is_pii_key_or_json_path(k)
            && !is_extra_column_ident(k)
            && match v {
                Value::String(ty) => !ty.contains('\0') && is_safe_duckdb_scalar_schema_type(ty),
                Value::Object(_) => json_struct_schema_object_is_safe(v, depth + 1),
                _ => false,
            }
    })
}

fn json_struct_schema_references_pii(lit: &str) -> bool {
    if lit.contains('\0') || json_has_duplicate_keys(lit) {
        return true;
    }
    let Ok(parsed) = serde_json::from_str::<Value>(lit) else {
        return true;
    };
    !json_struct_schema_object_is_safe(&parsed, 0)
}

fn literal_references_pii_in_key_or_extract_ctx(
    lit: &str,
    in_subscript_or_arrow_or_brace_key: bool,
    extract_stack: &[ExtractFrame],
) -> bool {
    if in_subscript_or_arrow_or_brace_key && is_pii_key_or_json_path(lit) {
        return true;
    }
    for frame in extract_stack {
        if frame.arg_idx == 0 {
            if is_pii_key_or_json_path(lit) {
                return true;
            }
        } else {
            match frame.mode {
                ExtractArgMode::KeyOrPath => {
                    if is_pii_key_or_json_path(lit) {
                        return true;
                    }
                }
                ExtractArgMode::JsonContainsNeedle => {
                    if frame.arg_idx != 1 || json_contains_needle_references_pii(lit) {
                        return true;
                    }
                }
                ExtractArgMode::JsonStructSchema => {
                    if frame.arg_idx != 1 || json_struct_schema_references_pii(lit) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn is_blind_pii_fn(token: &str) -> bool {
    let lower = trim_sql_ws(token).to_ascii_lowercase();
    lower.starts_with("read_")
        || lower.starts_with("parquet_")
        || lower.starts_with("pragma_")
        || matches!(
            lower.as_str(),
            "struct_extract_at"
                | "struct_contains"
                | "json_merge_patch"
                | "map_concat"
                | "json_each"
                | "json_tree"
                | "json_keys"
                | "to_json"
                | "json_quote"
                | "array_to_json"
                | "row_to_json"
                | "struct_values"
                | "map_values"
                | "map_entries"
                | "query"
                | "query_table"
                | "duckdb_table_sample"
                | "summary"
                | "json_execute_serialized_sql"
                | "json_deserialize_sql"
                | "json_serialize_sql"
                | "json_serialize_plan"
                | "map_contains_value"
                | "map_keys"
                | "struct_indexof"
                | "struct_position"
                | "struct_keys"
                | "remap_struct"
                | "struct_insert"
                | "struct_update"
                | "struct_concat"
                | "json_pretty"
                | "json_group_array"
                | "json_group_object"
                | "json_group_structure"
                | "json_structure"
        )
}

fn is_blocked_sql_keyword(token: &str) -> bool {
    matches!(
        trim_sql_ws(token).to_ascii_lowercase().as_str(),
        "uescape" | "pivot" | "unpivot" | "pivot_wider" | "pivot_longer" | "summarize"
    )
}

fn is_safe_json_cast_fn(token: &str) -> bool {
    matches!(
        trim_sql_ws(token).to_ascii_lowercase().as_str(),
        "cast" | "try_cast" | "json" | "json_parse"
    )
}

fn is_non_call_sql_keyword(token: &str) -> bool {
    matches!(
        trim_sql_ws(token).to_ascii_lowercase().as_str(),
        "select"
            | "distinct"
            | "all"
            | "where"
            | "and"
            | "or"
            | "not"
            | "xor"
            | "is"
            | "on"
            | "using"
            | "when"
            | "then"
            | "else"
            | "having"
            | "qualify"
            | "group"
            | "order"
            | "by"
            | "partition"
            | "over"
            | "filter"
            | "between"
            | "like"
            | "ilike"
            | "similar"
            | "to"
            | "from"
            | "join"
            | "union"
            | "intersect"
            | "except"
            | "as"
            | "asc"
            | "desc"
            | "nulls"
            | "first"
            | "last"
            | "limit"
            | "offset"
    )
}

fn is_non_subscriptable_sql_keyword(token: &str) -> bool {
    matches!(
        trim_sql_ws(token).to_ascii_lowercase().as_str(),
        "select"
            | "where"
            | "and"
            | "or"
            | "not"
            | "in"
            | "on"
            | "using"
            | "case"
            | "when"
            | "then"
            | "else"
            | "having"
            | "qualify"
            | "by"
            | "like"
            | "ilike"
            | "similar"
            | "to"
            | "from"
            | "join"
            | "union"
            | "intersect"
            | "except"
            | "as"
            | "limit"
            | "offset"
            | "for"
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ParenKind {
    PlainGroup,
    SafeJsonCast,
    ExtractFn,
    OtherFn,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingCallKind {
    None,
    SafeJsonCast,
    ExtractFn,
    OtherFn,
}

fn starts_with_sql_string_key(s: &str, pos: usize) -> bool {
    let bytes = s.as_bytes();
    matches!(bytes.get(pos), Some(b'\'' | b'$'))
        || (matches!(bytes.get(pos), Some(b'E' | b'e')) && bytes.get(pos + 1) == Some(&b'\''))
}

fn read_json_type_ident(s: &str, k: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if matches!(bytes.get(k), Some(b'"' | b'`')) {
        let quote = bytes[k];
        let mut m = k + 1;
        let mut tbuf = String::new();
        let mut closed = false;
        while m < bytes.len() {
            if bytes[m] == quote {
                if bytes.get(m + 1) == Some(&quote) {
                    tbuf.push(quote as char);
                    m += 2;
                } else {
                    m += 1;
                    closed = true;
                    break;
                }
            } else {
                tbuf.push(bytes[m] as char);
                m += 1;
            }
        }
        if !closed || !trim_sql_ws(&tbuf).eq_ignore_ascii_case("json") {
            return None;
        }
        return Some(m);
    }
    let mut m = k;
    while m < bytes.len() && (bytes[m].is_ascii_alphanumeric() || bytes[m] == b'_') {
        m += 1;
    }
    (m > k && s[k..m].eq_ignore_ascii_case("json")).then_some(m)
}

struct ExtraExtractCtx<'a> {
    paren_stack: &'a [(ParenKind, bool)],
    extract_stack: &'a [ExtractFrame],
    bracket_depth: u32,
    brace_depth: u32,
    case_depth: u32,
}

fn extra_is_safely_extracted(s: &str, after_ident: usize, ctx: &ExtraExtractCtx<'_>) -> bool {
    let bytes = s.as_bytes();
    let mut pos = after_ident;
    let mut unconsumed = ctx.paren_stack.len();
    loop {
        let Some(j) = skip_ws_and_comments(s, pos) else {
            return false;
        };
        if bytes.get(j) == Some(&b':') && bytes.get(j + 1) == Some(&b':') {
            let Some(k) = skip_ws_and_comments(s, j + 2) else {
                return false;
            };
            let Some(after_type) = read_json_type_ident(s, k) else {
                return false;
            };
            pos = after_type;
            continue;
        }
        if bytes.get(j).is_some_and(|b| matches!(*b, b'A' | b'a'))
            && bytes.get(j + 1).is_some_and(|b| matches!(*b, b'S' | b's'))
            && !bytes
                .get(j + 2)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
        {
            if unconsumed == 0 {
                return false;
            }
            let (frame_kind, trivial_prefix) = ctx.paren_stack[unconsumed - 1];
            if frame_kind != ParenKind::SafeJsonCast || !trivial_prefix {
                return false;
            }
            let Some(k) = skip_ws_and_comments(s, j + 2) else {
                return false;
            };
            let Some(after_type) = read_json_type_ident(s, k) else {
                return false;
            };
            let Some(close_idx) = skip_ws_and_comments(s, after_type) else {
                return false;
            };
            if bytes.get(close_idx) != Some(&b')') {
                return false;
            }
            unconsumed -= 1;
            pos = close_idx + 1;
            continue;
        }
        if bytes.get(j) == Some(&b')') {
            if unconsumed == 0 {
                return false;
            }
            let (frame_kind, trivial_prefix) = ctx.paren_stack[unconsumed - 1];
            if !trivial_prefix
                || !matches!(frame_kind, ParenKind::PlainGroup | ParenKind::SafeJsonCast)
            {
                return false;
            }
            unconsumed -= 1;
            pos = j + 1;
            continue;
        }
        if bytes.get(j) == Some(&b',') {
            if ctx.case_depth != 0 || unconsumed == 0 {
                return false;
            }
            let (frame_kind, trivial_prefix) = ctx.paren_stack[unconsumed - 1];
            if frame_kind != ParenKind::ExtractFn || !trivial_prefix {
                return false;
            }
            let eff_paren_depth = u32::try_from(unconsumed).unwrap_or(u32::MAX);
            return ctx.extract_stack.last().is_some_and(|f| {
                f.paren_depth == eff_paren_depth
                    && f.bracket_depth == ctx.bracket_depth
                    && f.brace_depth == ctx.brace_depth
                    && f.arg_idx == 0
            });
        }
        if matches!(bytes.get(j), Some(b'-' | b'#')) && bytes.get(j + 1) == Some(&b'>') {
            let after_arrow = if bytes.get(j + 2) == Some(&b'>') {
                j + 3
            } else {
                j + 2
            };
            return skip_ws_and_comments(s, after_arrow)
                .is_some_and(|m| starts_with_sql_string_key(s, m));
        }
        if bytes.get(j) == Some(&b'[') {
            return skip_ws_and_comments(s, j + 1)
                .is_some_and(|m| starts_with_sql_string_key(s, m));
        }
        return false;
    }
}

enum RelTok {
    Ident {
        lower: String,
        quoted: bool,
        followed_by_dot: bool,
        followed_by_open_paren: bool,
    },
    StrLit {
        lower: String,
    },
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Dot,
    Colon,
    DoubleColon,
    Semi,
    Star,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScopeDelim {
    Root,
    Paren,
    Bracket,
    Brace,
}

fn scan_rel_digits(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
        if i + 1 < bytes.len() && bytes[i] == b'_' && bytes[i + 1].is_ascii_digit() {
            i += 1;
        }
    }
    i
}

fn find_matching_rel_rparen(tokens: &[RelTok], open_idx: usize) -> Option<usize> {
    let mut depth = 0u32;
    for (idx, tok) in tokens.iter().enumerate().skip(open_idx) {
        match tok {
            RelTok::LParen => depth = depth.saturating_add(1),
            RelTok::RParen => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}

fn is_rel_distinct_from_op(tokens: &[RelTok], idx: usize) -> bool {
    let Some(RelTok::Ident {
        lower: p1,
        quoted: false,
        ..
    }) = idx.checked_sub(1).and_then(|p| tokens.get(p))
    else {
        return false;
    };
    if p1 != "distinct" {
        return false;
    }
    let Some(RelTok::Ident {
        lower: p2,
        quoted: false,
        ..
    }) = idx.checked_sub(2).and_then(|p| tokens.get(p))
    else {
        return false;
    };
    if p2 == "is" {
        return true;
    }
    if p2 == "not" {
        return matches!(
            idx.checked_sub(3).and_then(|p| tokens.get(p)),
            Some(RelTok::Ident {
                lower: p3,
                quoted: false,
                ..
            }) if p3 == "is"
        );
    }
    false
}

fn is_rel_within_group_clause(tokens: &[RelTok], idx: usize) -> bool {
    idx > 0
        && matches!(
            tokens.get(idx),
            Some(RelTok::Ident {
                lower,
                quoted: false,
                ..
            }) if lower == "group" && !is_non_keyword_rel_pos(tokens, idx)
        )
        && matches!(
            tokens.get(idx - 1),
            Some(RelTok::Ident {
                lower: prev_kw,
                quoted: false,
                ..
            }) if prev_kw == "within" && !is_non_keyword_rel_pos(tokens, idx - 1)
        )
        && matches!(tokens.get(idx + 1), Some(RelTok::LParen))
}

fn is_from_alias_stop_keyword(lower: &str) -> bool {
    matches!(
        lower,
        "where"
            | "group"
            | "having"
            | "window"
            | "qualify"
            | "order"
            | "limit"
            | "offset"
            | "fetch"
            | "union"
            | "intersect"
            | "except"
            | "on"
            | "using"
            | "join"
            | "inner"
            | "left"
            | "right"
            | "full"
            | "outer"
            | "cross"
            | "natural"
            | "semi"
            | "anti"
            | "positional"
            | "asof"
            | "tablesample"
            | "lateral"
            | "pivot"
            | "unpivot"
            | "pivot_wider"
            | "pivot_longer"
            | "select"
            | "into"
            | "returning"
            | "for"
    )
}

fn is_schema_or_catalog_ident(lower: &str) -> bool {
    matches!(
        lower,
        "main" | "memory" | "temp" | "system" | "duckdb" | "pg_catalog" | "information_schema"
    )
}

fn is_preceded_by_non_schema_dot(tokens: &[RelTok], idx: usize) -> bool {
    if idx == 0 || !matches!(tokens.get(idx - 1), Some(RelTok::Dot)) {
        return false;
    }
    if idx >= 2
        && matches!(
            tokens.get(idx - 2),
            Some(RelTok::Ident { lower: sch, .. }) if is_schema_or_catalog_ident(sch)
        )
    {
        return false;
    }
    if idx >= 3 && matches!(tokens.get(idx - 3), Some(RelTok::Dot)) {
        return false;
    }
    true
}

fn is_non_keyword_rel_pos(tokens: &[RelTok], idx: usize) -> bool {
    if idx == 0 {
        return false;
    }
    if matches!(tokens.get(idx - 1), Some(RelTok::Dot | RelTok::DoubleColon)) {
        return true;
    }
    let mut r = 0usize;
    let mut p = idx;
    while p > 0 {
        if let Some(RelTok::Ident {
            lower,
            quoted: false,
            ..
        }) = tokens.get(p - 1)
        {
            if lower == "as" {
                r += 1;
                p -= 1;
                continue;
            }
        }
        break;
    }
    if r == 0 {
        return false;
    }
    if p > 0 && matches!(tokens.get(p - 1), Some(RelTok::Dot | RelTok::DoubleColon)) {
        r -= 1;
    }
    r % 2 == 1
}

fn range_has_top_level_setop(tokens: &[RelTok], start: usize, end: usize) -> bool {
    let mut k = start;
    let mut bb_depth = 0u32;
    while k < end {
        match &tokens[k] {
            RelTok::LParen => {
                let Some(rp) = find_matching_rel_rparen(tokens, k) else {
                    return true;
                };
                k = rp + 1;
            }
            RelTok::LBracket | RelTok::LBrace => {
                bb_depth = bb_depth.saturating_add(1);
                k += 1;
            }
            RelTok::RBracket | RelTok::RBrace => {
                bb_depth = bb_depth.saturating_sub(1);
                k += 1;
            }
            RelTok::Ident {
                lower,
                quoted: false,
                ..
            } if bb_depth == 0
                && matches!(lower.as_str(), "union" | "intersect" | "except")
                && !is_non_keyword_rel_pos(tokens, k) =>
            {
                return true;
            }
            _ => {
                k += 1;
            }
        }
    }
    false
}

fn is_star_columns_unpack(tokens: &[RelTok], star_idx: usize) -> bool {
    !matches!(
        star_idx.checked_sub(1).and_then(|p| tokens.get(p)),
        Some(RelTok::Dot)
    ) && matches!(
        tokens.get(star_idx + 1),
        Some(RelTok::Ident {
            lower,
            quoted: false,
            ..
        }) if lower == "columns"
    ) && matches!(tokens.get(star_idx + 2), Some(RelTok::LParen))
}

fn is_star_preceded_by_colon(tokens: &[RelTok], star_idx: usize) -> bool {
    let mut k = star_idx;
    while k >= 2
        && matches!(tokens.get(k - 1), Some(RelTok::Dot))
        && matches!(
            tokens.get(k - 2),
            Some(RelTok::Ident { .. } | RelTok::StrLit { .. })
        )
    {
        k -= 2;
    }
    while k >= 1 && matches!(tokens.get(k - 1), Some(RelTok::LParen)) {
        k -= 1;
    }
    k >= 1 && matches!(tokens.get(k - 1), Some(RelTok::Colon))
}

fn star_has_unsafe_post_modifier_or_alias(
    tokens: &[RelTok],
    star_idx: usize,
    allow_sort_dir: bool,
) -> bool {
    let mut k = star_idx + 1;
    while let Some(RelTok::Ident {
        lower,
        quoted: false,
        ..
    }) = tokens.get(k)
    {
        if matches!(lower.as_str(), "exclude" | "replace" | "rename") {
            if matches!(tokens.get(k + 1), Some(RelTok::LParen)) {
                let Some(rp) = find_matching_rel_rparen(tokens, k + 1) else {
                    return true;
                };
                k = rp + 1;
                continue;
            }
            if lower == "exclude" {
                let is_single_exclude_col = match tokens.get(k + 1) {
                    Some(RelTok::StrLit { .. }) => true,
                    Some(RelTok::Ident {
                        lower: col_kw,
                        quoted,
                        ..
                    }) => *quoted || !is_from_alias_stop_keyword(col_kw),
                    _ => false,
                };
                if is_single_exclude_col {
                    k += 2;
                    continue;
                }
            }
            return true;
        }
        break;
    }
    match tokens.get(k) {
        None | Some(RelTok::Comma | RelTok::RParen | RelTok::Semi) => false,
        Some(RelTok::Ident {
            lower,
            quoted: false,
            ..
        }) => {
            if allow_sort_dir && matches!(lower.as_str(), "asc" | "desc" | "nulls") {
                return false;
            }
            !matches!(
                lower.as_str(),
                "from"
                    | "where"
                    | "group"
                    | "having"
                    | "window"
                    | "qualify"
                    | "order"
                    | "limit"
                    | "offset"
                    | "fetch"
                    | "union"
                    | "intersect"
                    | "except"
                    | "into"
            )
        }
        _ => true,
    }
}

#[derive(Clone, Copy)]
struct WildcardScope {
    delim: ScopeDelim,
    is_expr_paren: bool,
    is_distinct_on_paren: bool,
    saved_in_from: bool,
    in_setop_branch: bool,
    in_disallowed_wildcard_subquery: bool,
    branch_saw_select: bool,
    branch_saw_from: bool,
    in_select_list: bool,
    in_order_by: bool,
}

fn has_positional_wildcard_subquery_or_setop(
    tokens: &[RelTok],
    cte_body_lparens: &std::collections::BTreeSet<usize>,
) -> bool {
    let mut scopes = vec![WildcardScope {
        delim: ScopeDelim::Root,
        is_expr_paren: false,
        is_distinct_on_paren: false,
        saved_in_from: false,
        in_setop_branch: range_has_top_level_setop(tokens, 0, tokens.len()),
        in_disallowed_wildcard_subquery: false,
        branch_saw_select: false,
        branch_saw_from: false,
        in_select_list: false,
        in_order_by: false,
    }];
    let mut in_from = false;
    let mut expect_rel = false;
    let mut prev_rparen_was_distinct_on = false;

    let mut idx = 0usize;
    while idx < tokens.len() {
        let cur = *scopes.last().unwrap_or(&WildcardScope {
            delim: ScopeDelim::Root,
            is_expr_paren: false,
            is_distinct_on_paren: false,
            saved_in_from: false,
            in_setop_branch: false,
            in_disallowed_wildcard_subquery: false,
            branch_saw_select: false,
            branch_saw_from: false,
            in_select_list: false,
            in_order_by: false,
        });

        match &tokens[idx] {
            RelTok::LParen => {
                let mut look = idx + 1;
                while matches!(tokens.get(look), Some(RelTok::LParen)) {
                    look += 1;
                }
                let is_trim_from = look == idx + 1
                    && matches!(
                        tokens.get(look),
                        Some(RelTok::Ident {
                            lower: kw,
                            quoted: false,
                            ..
                        }) if kw == "from"
                    )
                    && idx > 0
                    && matches!(
                        tokens.get(idx - 1),
                        Some(RelTok::Ident {
                            lower: fn_kw,
                            quoted: false,
                            ..
                        }) if matches!(fn_kw.as_str(), "trim" | "btrim" | "ltrim" | "rtrim")
                    );
                let has_subquery_head = !is_trim_from
                    && matches!(
                        tokens.get(look),
                        Some(RelTok::Ident {
                            lower: kw,
                            quoted: false,
                            ..
                        }) if matches!(kw.as_str(), "select" | "with" | "values" | "from" | "table")
                    );

                if expect_rel && !has_subquery_head {
                    scopes.push(WildcardScope {
                        delim: ScopeDelim::Paren,
                        is_expr_paren: false,
                        is_distinct_on_paren: false,
                        saved_in_from: in_from,
                        in_setop_branch: cur.in_setop_branch,
                        in_disallowed_wildcard_subquery: cur.in_disallowed_wildcard_subquery,
                        branch_saw_select: true,
                        branch_saw_from: false,
                        in_select_list: false,
                        in_order_by: false,
                    });
                    in_from = true;
                    expect_rel = true;
                } else if has_subquery_head {
                    let rp = find_matching_rel_rparen(tokens, idx).unwrap_or(tokens.len());
                    let own_setop = range_has_top_level_setop(tokens, idx + 1, rp);
                    let is_exists = idx > 0
                        && matches!(
                            tokens.get(idx - 1),
                            Some(RelTok::Ident {
                                lower: kw,
                                quoted: false,
                                ..
                            }) if kw == "exists"
                        );
                    let is_outer_stmt_wrap = idx == 0 && rp + 1 == tokens.len();
                    let is_redundant_allowed_wrap = idx > 0
                        && matches!(tokens.get(idx - 1), Some(RelTok::LParen))
                        && !cur.is_expr_paren
                        && !cur.in_disallowed_wildcard_subquery;
                    let is_allowed_subquery = cte_body_lparens.contains(&idx)
                        || expect_rel
                        || is_exists
                        || is_outer_stmt_wrap
                        || is_redundant_allowed_wrap;
                    let parent_setop_propagates = !cur.is_expr_paren
                        && cur.in_setop_branch
                        && (idx == 0
                            || matches!(tokens.get(idx - 1), Some(RelTok::LParen))
                            || matches!(
                                tokens.get(idx - 1),
                                Some(RelTok::Ident {
                                    lower: kw,
                                    quoted: false,
                                    ..
                                }) if matches!(
                                    kw.as_str(),
                                    "union" | "all" | "distinct" | "intersect" | "except" | "name"
                                )
                            ));
                    scopes.push(WildcardScope {
                        delim: ScopeDelim::Paren,
                        is_expr_paren: false,
                        is_distinct_on_paren: false,
                        saved_in_from: in_from,
                        in_setop_branch: own_setop || parent_setop_propagates,
                        in_disallowed_wildcard_subquery: !is_allowed_subquery,
                        branch_saw_select: false,
                        branch_saw_from: false,
                        in_select_list: false,
                        in_order_by: false,
                    });
                    in_from = false;
                    expect_rel = false;
                } else {
                    let is_distinct_on = !cur.is_expr_paren
                        && cur.in_select_list
                        && idx >= 2
                        && matches!(
                            tokens.get(idx - 1),
                            Some(RelTok::Ident {
                                lower: k1,
                                quoted: false,
                                ..
                            }) if k1 == "on"
                        )
                        && matches!(
                            tokens.get(idx - 2),
                            Some(RelTok::Ident {
                                lower: k2,
                                quoted: false,
                                ..
                            }) if k2 == "distinct"
                        );
                    scopes.push(WildcardScope {
                        delim: ScopeDelim::Paren,
                        is_expr_paren: true,
                        is_distinct_on_paren: is_distinct_on,
                        saved_in_from: in_from,
                        in_setop_branch: false,
                        in_disallowed_wildcard_subquery: false,
                        branch_saw_select: false,
                        branch_saw_from: false,
                        in_select_list: false,
                        in_order_by: false,
                    });
                    expect_rel = false;
                }
                prev_rparen_was_distinct_on = false;
                idx += 1;
                continue;
            }
            RelTok::LBracket | RelTok::LBrace => {
                scopes.push(WildcardScope {
                    delim: if matches!(tokens[idx], RelTok::LBracket) {
                        ScopeDelim::Bracket
                    } else {
                        ScopeDelim::Brace
                    },
                    is_expr_paren: true,
                    is_distinct_on_paren: false,
                    saved_in_from: in_from,
                    in_setop_branch: false,
                    in_disallowed_wildcard_subquery: false,
                    branch_saw_select: false,
                    branch_saw_from: false,
                    in_select_list: false,
                    in_order_by: false,
                });
                expect_rel = false;
                prev_rparen_was_distinct_on = false;
                idx += 1;
                continue;
            }
            RelTok::RParen => {
                if scopes.len() <= 1 || scopes.last().is_some_and(|s| s.delim != ScopeDelim::Paren)
                {
                    return true;
                }
                if let Some(popped) = scopes.pop() {
                    if !popped.is_expr_paren
                        && popped.branch_saw_from
                        && !popped.branch_saw_select
                        && (popped.in_setop_branch || popped.in_disallowed_wildcard_subquery)
                    {
                        return true;
                    }
                    in_from = popped.saved_in_from;
                    expect_rel = false;
                    prev_rparen_was_distinct_on = popped.is_distinct_on_paren;
                }
                idx += 1;
                continue;
            }
            RelTok::RBracket | RelTok::RBrace => {
                let expected = if matches!(tokens[idx], RelTok::RBracket) {
                    ScopeDelim::Bracket
                } else {
                    ScopeDelim::Brace
                };
                if scopes.len() <= 1 || scopes.last().is_some_and(|s| s.delim != expected) {
                    return true;
                }
                if let Some(popped) = scopes.pop() {
                    in_from = popped.saved_in_from;
                    expect_rel = false;
                    prev_rparen_was_distinct_on = false;
                }
                idx += 1;
                continue;
            }
            RelTok::Semi => {
                if let Some(top) = scopes.last_mut() {
                    top.in_select_list = false;
                    top.in_order_by = false;
                    top.branch_saw_select = false;
                    top.branch_saw_from = false;
                }
                in_from = false;
                expect_rel = false;
                prev_rparen_was_distinct_on = false;
                idx += 1;
                continue;
            }
            RelTok::Star => {
                let is_star_cols = is_star_columns_unpack(tokens, idx);
                if !is_star_cols && is_star_preceded_by_colon(tokens, idx) {
                    return true;
                }
                let nearest_query_disallows_wildcard = scopes
                    .iter()
                    .rev()
                    .find(|sc| !sc.is_expr_paren)
                    .is_some_and(|sc| sc.in_setop_branch || sc.in_disallowed_wildcard_subquery);
                let is_qual_star = idx >= 2
                    && matches!(tokens.get(idx - 1), Some(RelTok::Dot))
                    && matches!(
                        tokens.get(idx - 2),
                        Some(
                            RelTok::Ident { .. }
                                | RelTok::StrLit { .. }
                                | RelTok::RParen
                                | RelTok::RBracket
                                | RelTok::RBrace
                        )
                    );
                if is_qual_star {
                    if nearest_query_disallows_wildcard
                        || (cur.is_expr_paren && !cur.is_distinct_on_paren)
                        || star_has_unsafe_post_modifier_or_alias(
                            tokens,
                            idx,
                            cur.is_distinct_on_paren || cur.in_order_by,
                        )
                    {
                        return true;
                    }
                } else if cur.is_distinct_on_paren {
                    let is_distinct_on_item_star = idx > 0
                        && match &tokens[idx - 1] {
                            RelTok::LParen | RelTok::Comma => true,
                            RelTok::Ident {
                                lower: kw,
                                quoted: false,
                                ..
                            } if !is_non_keyword_rel_pos(tokens, idx - 1) => {
                                matches!(kw.as_str(), "not" | "distinct" | "all")
                            }
                            _ => false,
                        };
                    if is_distinct_on_item_star
                        && !is_star_cols
                        && (nearest_query_disallows_wildcard
                            || star_has_unsafe_post_modifier_or_alias(tokens, idx, true))
                    {
                        return true;
                    }
                } else if cur.is_expr_paren {
                    let is_paren_or_arg_star = idx > 0
                        && match &tokens[idx - 1] {
                            RelTok::LParen | RelTok::LBracket | RelTok::LBrace | RelTok::Comma => {
                                true
                            }
                            RelTok::Ident {
                                lower: kw,
                                quoted: false,
                                ..
                            } if !is_non_keyword_rel_pos(tokens, idx - 1) => {
                                matches!(kw.as_str(), "not" | "distinct" | "all")
                            }
                            _ => false,
                        };
                    let is_count_star = idx >= 2
                        && matches!(tokens.get(idx - 1), Some(RelTok::LParen))
                        && matches!(
                            tokens.get(idx - 2),
                            Some(RelTok::Ident { lower: fn_kw, .. })
                                if fn_kw == "count" && !is_preceded_by_non_schema_dot(tokens, idx - 2)
                        )
                        && matches!(tokens.get(idx + 1), Some(RelTok::RParen));
                    if is_paren_or_arg_star && !is_count_star && !is_star_cols {
                        return true;
                    }
                } else if !in_from {
                    let prev_starts_bare_star = prev_rparen_was_distinct_on
                        || idx == 0
                        || match &tokens[idx - 1] {
                            RelTok::Comma => cur.in_select_list || cur.in_order_by,
                            RelTok::Ident {
                                lower: kw,
                                quoted: false,
                                ..
                            } if !is_non_keyword_rel_pos(tokens, idx - 1) => matches!(
                                kw.as_str(),
                                "select"
                                    | "distinct"
                                    | "all"
                                    | "not"
                                    | "by"
                                    | "where"
                                    | "having"
                                    | "qualify"
                                    | "returning"
                            ),
                            _ => false,
                        };
                    if prev_starts_bare_star
                        && !is_star_cols
                        && (cur.in_setop_branch
                            || cur.in_disallowed_wildcard_subquery
                            || star_has_unsafe_post_modifier_or_alias(tokens, idx, cur.in_order_by))
                    {
                        return true;
                    }
                }
                prev_rparen_was_distinct_on = false;
                idx += 1;
                continue;
            }
            _ => {}
        }

        prev_rparen_was_distinct_on = false;
        if cur.is_expr_paren {
            idx += 1;
            continue;
        }

        if let RelTok::Ident {
            lower,
            quoted: false,
            ..
        } = &tokens[idx]
        {
            if !is_non_keyword_rel_pos(tokens, idx) {
                if lower == "select" {
                    if let Some(top) = scopes.last_mut() {
                        top.branch_saw_select = true;
                        top.in_select_list = true;
                        top.in_order_by = false;
                    }
                    in_from = false;
                    expect_rel = false;
                    idx += 1;
                    continue;
                }
                if lower == "from" && !is_rel_distinct_from_op(tokens, idx) {
                    if let Some(top) = scopes.last_mut() {
                        top.in_select_list = false;
                        top.in_order_by = false;
                        top.branch_saw_from = true;
                    }
                    in_from = true;
                    expect_rel = true;
                    idx += 1;
                    continue;
                }
                if lower == "join" {
                    if let Some(top) = scopes.last_mut() {
                        top.in_select_list = false;
                        top.in_order_by = false;
                    }
                    in_from = true;
                    expect_rel = true;
                    idx += 1;
                    continue;
                }
                if lower == "table" {
                    if cur.in_setop_branch || cur.in_disallowed_wildcard_subquery {
                        return true;
                    }
                    if let Some(top) = scopes.last_mut() {
                        top.in_select_list = false;
                        top.in_order_by = false;
                    }
                    in_from = true;
                    expect_rel = true;
                    idx += 1;
                    continue;
                }
                if matches!(lower.as_str(), "union" | "intersect" | "except") {
                    if cur.branch_saw_from
                        && !cur.branch_saw_select
                        && (cur.in_setop_branch || cur.in_disallowed_wildcard_subquery)
                    {
                        return true;
                    }
                    if let Some(top) = scopes.last_mut() {
                        top.in_setop_branch = true;
                        top.in_select_list = false;
                        top.in_order_by = false;
                        top.branch_saw_select = false;
                        top.branch_saw_from = false;
                    }
                    in_from = false;
                    expect_rel = false;
                    idx += 1;
                    continue;
                }
                if matches!(
                    lower.as_str(),
                    "where"
                        | "group"
                        | "having"
                        | "window"
                        | "qualify"
                        | "order"
                        | "limit"
                        | "offset"
                        | "fetch"
                        | "into"
                ) && !is_rel_within_group_clause(tokens, idx)
                {
                    if let Some(top) = scopes.last_mut() {
                        top.in_select_list = false;
                        top.in_order_by = lower == "order";
                    }
                    in_from = false;
                    expect_rel = false;
                    idx += 1;
                    continue;
                }
                if in_from && matches!(lower.as_str(), "on" | "using") {
                    expect_rel = false;
                    idx += 1;
                    continue;
                }
            }
        }

        if in_from && matches!(tokens[idx], RelTok::Comma) {
            expect_rel = true;
            idx += 1;
            continue;
        }

        if expect_rel {
            if let RelTok::Ident {
                lower,
                quoted,
                followed_by_dot,
                followed_by_open_paren,
            } = &tokens[idx]
            {
                if !*quoted && matches!(lower.as_str(), "lateral" | "only") {
                    idx += 1;
                    continue;
                }
                if *followed_by_dot {
                    idx += 1;
                    continue;
                }
                if *followed_by_open_paren && matches!(tokens.get(idx + 1), Some(RelTok::LParen)) {
                    scopes.push(WildcardScope {
                        delim: ScopeDelim::Paren,
                        is_expr_paren: true,
                        is_distinct_on_paren: false,
                        saved_in_from: in_from,
                        in_setop_branch: false,
                        in_disallowed_wildcard_subquery: false,
                        branch_saw_select: false,
                        branch_saw_from: false,
                        in_select_list: false,
                        in_order_by: false,
                    });
                    expect_rel = false;
                    idx += 2;
                    continue;
                }
                expect_rel = false;
                idx += 1;
                continue;
            }
            if !matches!(tokens[idx], RelTok::Dot) {
                expect_rel = false;
            }
        }

        idx += 1;
    }

    scopes.first().is_some_and(|root| {
        root.branch_saw_from
            && !root.branch_saw_select
            && (root.in_setop_branch || root.in_disallowed_wildcard_subquery)
    })
}

fn has_pii_sconst_identifier(tokens: &[RelTok]) -> bool {
    for (idx, tok) in tokens.iter().enumerate() {
        match tok {
            RelTok::StrLit { lower } => {
                if idx > 0
                    && matches!(
                        tokens.get(idx - 1),
                        Some(RelTok::Ident {
                            lower: prev_kw,
                            quoted: false,
                            ..
                        }) if prev_kw == "as" && !is_non_keyword_rel_pos(tokens, idx - 1)
                    )
                    && (is_pii_identifier(lower) || is_extra_column_ident(lower))
                {
                    return true;
                }
            }
            RelTok::Ident {
                lower,
                quoted: false,
                ..
            } if !is_non_keyword_rel_pos(tokens, idx) => {
                let lp_opt = if lower == "rename" {
                    matches!(tokens.get(idx + 1), Some(RelTok::LParen)).then_some(idx + 1)
                } else if lower == "using" {
                    if matches!(tokens.get(idx + 1), Some(RelTok::LParen)) {
                        Some(idx + 1)
                    } else if matches!(
                        tokens.get(idx + 1),
                        Some(RelTok::Ident {
                            lower: k_kw,
                            quoted: false,
                            ..
                        }) if k_kw == "key"
                    ) && matches!(tokens.get(idx + 2), Some(RelTok::LParen))
                    {
                        Some(idx + 2)
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(lp_idx) = lp_opt {
                    let Some(rp_idx) = find_matching_rel_rparen(tokens, lp_idx) else {
                        return true;
                    };
                    for inner_tok in &tokens[lp_idx + 1..rp_idx] {
                        if let RelTok::StrLit { lower: inner_lit } = inner_tok {
                            if is_pii_identifier(inner_lit) || is_extra_column_ident(inner_lit) {
                                return true;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    false
}

fn references_whole_row_relation_struct(tokens: &[RelTok]) -> bool {
    if has_pii_sconst_identifier(tokens) {
        return true;
    }
    let mut declared_relations = std::collections::BTreeSet::new();
    let mut decl_indices = std::collections::BTreeSet::new();
    let mut cte_body_lparens = std::collections::BTreeSet::new();
    // (delim, was_from_rel_paren, saved_in_from, is_scalar_expr_paren)
    let mut paren_frames: Vec<(ScopeDelim, bool, bool, bool)> = Vec::new();
    let mut in_from = false;
    let mut expect_rel = false;
    let mut expect_opt_alias = false;

    let mut idx = 0usize;
    while idx < tokens.len() {
        let in_scalar_paren = paren_frames.last().is_some_and(|f| f.3);

        if let RelTok::Ident {
            lower,
            quoted: false,
            ..
        } = &tokens[idx]
        {
            let prev_allows_with = idx == 0
                || matches!(tokens.get(idx - 1), Some(RelTok::LParen))
                || matches!(
                    tokens.get(idx - 1),
                    Some(RelTok::Ident {
                        lower: kw,
                        quoted: false,
                        ..
                    }) if matches!(
                        kw.as_str(),
                        "union" | "all" | "distinct" | "intersect" | "except" | "name"
                    )
                );
            if lower == "with" && prev_allows_with && !is_non_keyword_rel_pos(tokens, idx) {
                let mut k = idx + 1;
                if matches!(
                    tokens.get(k),
                    Some(RelTok::Ident {
                        lower: kw,
                        quoted: false,
                        ..
                    }) if kw == "recursive"
                ) {
                    k += 1;
                }
                while let Some(
                    RelTok::Ident {
                        lower: cte_name, ..
                    }
                    | RelTok::StrLit { lower: cte_name },
                ) = tokens.get(k)
                {
                    let mut after_name = k + 1;
                    let has_col_alias_list = matches!(tokens.get(after_name), Some(RelTok::LParen));
                    if has_col_alias_list {
                        let Some(col_close) = find_matching_rel_rparen(tokens, after_name) else {
                            break;
                        };
                        after_name = col_close + 1;
                    }
                    if !matches!(
                        tokens.get(after_name),
                        Some(RelTok::Ident {
                            lower: kw,
                            quoted: false,
                            ..
                        }) if kw == "as" && !is_non_keyword_rel_pos(tokens, after_name)
                    ) {
                        break;
                    }
                    let mut body_open = after_name + 1;
                    if matches!(
                        tokens.get(body_open),
                        Some(RelTok::Ident {
                            lower: kw,
                            quoted: false,
                            ..
                        }) if kw == "not"
                    ) {
                        body_open += 1;
                    }
                    if matches!(
                        tokens.get(body_open),
                        Some(RelTok::Ident {
                            lower: kw,
                            quoted: false,
                            ..
                        }) if kw == "materialized"
                    ) {
                        body_open += 1;
                    }
                    if !matches!(tokens.get(body_open), Some(RelTok::LParen)) {
                        break;
                    }
                    let Some(body_close) = find_matching_rel_rparen(tokens, body_open) else {
                        break;
                    };
                    if has_col_alias_list {
                        return true;
                    }
                    cte_body_lparens.insert(body_open);
                    declared_relations.insert(cte_name.clone());
                    decl_indices.insert(k);
                    if matches!(tokens.get(body_close + 1), Some(RelTok::Comma)) {
                        k = body_close + 2;
                        continue;
                    }
                    break;
                }
            }
        }

        match &tokens[idx] {
            RelTok::LParen => {
                let mut look = idx + 1;
                while matches!(tokens.get(look), Some(RelTok::LParen)) {
                    look += 1;
                }
                let is_trim_from = look == idx + 1
                    && matches!(
                        tokens.get(look),
                        Some(RelTok::Ident {
                            lower: kw,
                            quoted: false,
                            ..
                        }) if kw == "from"
                    )
                    && idx > 0
                    && matches!(
                        tokens.get(idx - 1),
                        Some(RelTok::Ident {
                            lower: fn_kw,
                            quoted: false,
                            ..
                        }) if matches!(fn_kw.as_str(), "trim" | "btrim" | "ltrim" | "rtrim")
                    );
                let has_subquery_head = !is_trim_from
                    && matches!(
                        tokens.get(look),
                        Some(RelTok::Ident {
                            lower: kw,
                            quoted: false,
                            ..
                        }) if matches!(kw.as_str(), "select" | "with" | "values" | "from" | "table")
                    );
                if expect_rel && !(has_subquery_head && look == idx + 1) {
                    paren_frames.push((ScopeDelim::Paren, true, in_from, false));
                    in_from = true;
                    expect_rel = true;
                    expect_opt_alias = false;
                } else if has_subquery_head {
                    paren_frames.push((ScopeDelim::Paren, expect_rel, in_from, false));
                    in_from = false;
                    expect_rel = false;
                    expect_opt_alias = false;
                } else {
                    paren_frames.push((ScopeDelim::Paren, false, in_from, true));
                    expect_rel = false;
                    expect_opt_alias = false;
                }
                idx += 1;
                continue;
            }
            RelTok::LBracket | RelTok::LBrace => {
                let delim = if matches!(tokens[idx], RelTok::LBracket) {
                    ScopeDelim::Bracket
                } else {
                    ScopeDelim::Brace
                };
                paren_frames.push((delim, false, in_from, true));
                expect_rel = false;
                expect_opt_alias = false;
                idx += 1;
                continue;
            }
            RelTok::RParen => {
                let Some((delim, was_from_rel_paren, saved_in_from, _)) = paren_frames.pop() else {
                    return true;
                };
                if delim != ScopeDelim::Paren {
                    return true;
                }
                in_from = saved_in_from;
                expect_rel = false;
                expect_opt_alias = was_from_rel_paren;
                idx += 1;
                continue;
            }
            RelTok::RBracket | RelTok::RBrace => {
                let expected = if matches!(tokens[idx], RelTok::RBracket) {
                    ScopeDelim::Bracket
                } else {
                    ScopeDelim::Brace
                };
                let Some((delim, _, saved_in_from, _)) = paren_frames.pop() else {
                    return true;
                };
                if delim != expected {
                    return true;
                }
                in_from = saved_in_from;
                expect_rel = false;
                expect_opt_alias = false;
                idx += 1;
                continue;
            }
            RelTok::Semi => {
                in_from = false;
                expect_rel = false;
                expect_opt_alias = false;
                idx += 1;
                continue;
            }
            _ => {}
        }

        if in_scalar_paren {
            idx += 1;
            continue;
        }

        if let RelTok::Ident {
            lower,
            quoted: false,
            ..
        } = &tokens[idx]
        {
            if !is_non_keyword_rel_pos(tokens, idx) {
                if (lower == "from" && !is_rel_distinct_from_op(tokens, idx))
                    || lower == "join"
                    || lower == "table"
                {
                    in_from = true;
                    expect_rel = true;
                    expect_opt_alias = false;
                    idx += 1;
                    continue;
                }
                if in_from
                    && matches!(
                        lower.as_str(),
                        "select"
                            | "where"
                            | "group"
                            | "having"
                            | "window"
                            | "qualify"
                            | "order"
                            | "limit"
                            | "offset"
                            | "fetch"
                            | "union"
                            | "intersect"
                            | "except"
                    )
                    && !is_rel_within_group_clause(tokens, idx)
                {
                    in_from = false;
                    expect_rel = false;
                    expect_opt_alias = false;
                    idx += 1;
                    continue;
                }
                if in_from && matches!(lower.as_str(), "on" | "using") {
                    expect_rel = false;
                    expect_opt_alias = false;
                    idx += 1;
                    continue;
                }
            }
        }

        if in_from && matches!(tokens[idx], RelTok::Comma) {
            expect_rel = true;
            expect_opt_alias = false;
            idx += 1;
            continue;
        }

        if expect_rel {
            if let RelTok::Ident {
                lower,
                quoted,
                followed_by_dot,
                followed_by_open_paren,
            } = &tokens[idx]
            {
                if !*quoted && matches!(lower.as_str(), "lateral" | "only") {
                    idx += 1;
                    continue;
                }
                if *followed_by_dot {
                    decl_indices.insert(idx);
                    idx += 1;
                    continue;
                }
                if *followed_by_open_paren && matches!(tokens.get(idx + 1), Some(RelTok::LParen)) {
                    decl_indices.insert(idx);
                    paren_frames.push((ScopeDelim::Paren, true, in_from, true));
                    expect_rel = false;
                    expect_opt_alias = false;
                    idx += 2;
                    continue;
                }
                declared_relations.insert(lower.clone());
                decl_indices.insert(idx);
                expect_rel = false;
                expect_opt_alias = true;
                idx += 1;
                continue;
            }
            if let RelTok::StrLit { lower } = &tokens[idx] {
                declared_relations.insert(lower.clone());
                decl_indices.insert(idx);
                expect_rel = false;
                expect_opt_alias = true;
                idx += 1;
                continue;
            }
            if !matches!(tokens[idx], RelTok::Dot) {
                expect_rel = false;
            }
            idx += 1;
            continue;
        }

        if expect_opt_alias {
            if matches!(tokens[idx], RelTok::Star) {
                idx += 1;
                continue;
            }
            if let RelTok::Ident { lower, quoted, .. } = &tokens[idx] {
                if !*quoted && lower == "as" && !is_non_keyword_rel_pos(tokens, idx) {
                    if let Some(
                        RelTok::Ident {
                            lower: alias_lower, ..
                        }
                        | RelTok::StrLit { lower: alias_lower },
                    ) = tokens.get(idx + 1)
                    {
                        if matches!(tokens.get(idx + 2), Some(RelTok::LParen)) {
                            return true;
                        }
                        declared_relations.insert(alias_lower.clone());
                        decl_indices.insert(idx + 1);
                        expect_opt_alias = false;
                        idx += 2;
                        continue;
                    }
                } else if *quoted || !is_from_alias_stop_keyword(lower) {
                    if matches!(tokens.get(idx + 1), Some(RelTok::LParen)) {
                        return true;
                    }
                    declared_relations.insert(lower.clone());
                    decl_indices.insert(idx);
                    expect_opt_alias = false;
                    idx += 1;
                    continue;
                }
            } else if let RelTok::StrLit { lower: alias_lower } = &tokens[idx] {
                if matches!(tokens.get(idx + 1), Some(RelTok::LParen)) {
                    return true;
                }
                declared_relations.insert(alias_lower.clone());
                decl_indices.insert(idx);
                expect_opt_alias = false;
                idx += 1;
                continue;
            }
            expect_opt_alias = false;
        }

        idx += 1;
    }

    if !paren_frames.is_empty()
        || has_positional_wildcard_subquery_or_setop(tokens, &cte_body_lparens)
    {
        return true;
    }

    for (idx, tok) in tokens.iter().enumerate() {
        if let RelTok::Ident {
            lower,
            followed_by_dot,
            ..
        } = tok
        {
            let is_implicit_subquery_rel = lower == "unnamed_subquery"
                || lower == "unnamed_query"
                || lower
                    .strip_prefix("unnamed_subquery")
                    .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
                || lower
                    .strip_prefix("unnamed_query")
                    .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()));
            if decl_indices.contains(&idx)
                || !(declared_relations.contains(lower) || is_implicit_subquery_rel)
            {
                continue;
            }
            if idx > 0
                && matches!(
                    tokens.get(idx - 1),
                    Some(RelTok::Ident {
                        lower: prev_kw,
                        quoted: false,
                        ..
                    }) if prev_kw == "as" && !is_non_keyword_rel_pos(tokens, idx - 1)
                )
            {
                continue;
            }
            if !*followed_by_dot {
                return true;
            }
            if matches!(tokens.get(idx + 1), Some(RelTok::Dot))
                && matches!(
                    tokens.get(idx + 2),
                    Some(RelTok::Ident {
                        followed_by_open_paren: true,
                        ..
                    })
                )
            {
                return true;
            }
        }
    }
    false
}

/// Single-pass `O(N)` DuckDB/PostgreSQL SQL & filter-expression lexical scanner.
/// Fails closed (`true`) on unterminated comments, quotes, escapes, `UESCAPE`, or brackets.
fn sql_or_filter_references_pii(text: &str) -> bool {
    if text.contains('\0') {
        return true;
    }
    let s = trim_sql_ws(text);
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut bracket_depth = 0u32;
    let mut brace_depth = 0u32;
    let mut paren_depth = 0u32;
    let mut case_depth = 0u32;
    let mut paren_stack: Vec<(ParenKind, bool)> = Vec::new();
    let mut pending_call = PendingCallKind::None;
    let mut pending_extract_dot_chain = false;
    let mut prev_was_dot = false;
    let mut rel_tokens: Vec<RelTok> = Vec::new();
    let mut subscript_depths: Vec<u32> = Vec::new();
    let mut brace_stack: Vec<(u32, u32, bool)> = Vec::new();
    let mut pending_extract_mode: Option<ExtractArgMode> = None;
    let mut extract_stack: Vec<ExtractFrame> = Vec::new();
    let mut pending_columns_fn = false;
    let mut columns_fn_depth: Option<u32> = None;
    let mut columns_arg_count = 0u32;
    let mut json_arrow_paren_depth: Option<u32> = None;
    let mut json_arrow_list_depth: Option<u32> = None;
    let mut last_sig = LastSig::Other;

    while i < bytes.len() {
        // 1. ASCII / UTF-8 whitespace (preserves `last_sig`, `pending_extract_fn`, `pending_columns_fn`)
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] >= 0x80 {
            if let Some(ch) = s.get(i..).and_then(|tail| tail.chars().next()) {
                if is_sql_unicode_ws(ch) {
                    i += ch.len_utf8();
                    continue;
                }
            }
        }

        // 2. `--` line comments (terminated by `\n` or `\r` per PostgreSQL/DuckDB `libpg_query`)
        if bytes[i] == b'-' && bytes.get(i + 1) == Some(&b'-') {
            i += 2;
            while i < bytes.len() && !matches!(bytes[i], b'\n' | b'\r') {
                i += 1;
            }
            continue;
        }

        // 3. `/* ... */` nested block comments (fail closed if unclosed at EOF)
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            let mut depth = 1u32;
            while i < bytes.len() && depth > 0 {
                if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth > 0 {
                return true;
            }
            continue;
        }

        let in_subscript = !subscript_depths.is_empty();
        let in_brace_key = brace_stack.last().is_some_and(|b| b.2);
        let in_arrow_rhs = last_sig == LastSig::JsonArrow
            || json_arrow_paren_depth.is_some()
            || json_arrow_list_depth.is_some();
        let in_extract_key_arg = extract_stack.iter().any(|f| f.arg_idx >= 1);
        let check_key_ctx = in_subscript || in_arrow_rhs || !extract_stack.is_empty();

        // 4. `'...'`, `E'...'` / `e'...'`, and `U&'...'` / `u&'...'` string literals,
        //    folding ISO/IEC 9075 / DuckDB `libpg_query` adjacent single-quoted continuations
        //    across whitespace and SQL comments (`'a'\n'b'`, `'a' -- c\n'b'`, etc.).
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum QuoteMode {
            Standard,
            Backslash,
            Unicode,
        }
        let initial_quote_mode = if matches!(bytes[i], b'U' | b'u')
            && bytes.get(i + 1) == Some(&b'&')
            && bytes.get(i + 2) == Some(&b'\'')
        {
            i += 3;
            Some(QuoteMode::Unicode)
        } else if matches!(bytes[i], b'E' | b'e') && bytes.get(i + 1) == Some(&b'\'') {
            i += 2;
            Some(QuoteMode::Backslash)
        } else if bytes[i] == b'\'' {
            i += 1;
            Some(QuoteMode::Standard)
        } else {
            None
        };
        if let Some(base_mode) = initial_quote_mode {
            let mut captured = Some(String::new());
            let mut seg_mode = base_mode;
            loop {
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if bytes.get(i + 1) == Some(&b'\'') {
                            if let Some(buf) = captured.as_mut() {
                                buf.push('\'');
                            }
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else if seg_mode == QuoteMode::Unicode && bytes[i] == b'\\' {
                        let mut tmp = String::new();
                        let out = captured.as_mut().unwrap_or(&mut tmp);
                        let Some(next_i) = decode_sql_unicode_escape(bytes, i, out) else {
                            return true;
                        };
                        i = next_i;
                    } else if seg_mode == QuoteMode::Backslash && bytes[i] == b'\\' {
                        let Some(next_i) = decode_sql_e_escape(s, i, &mut captured) else {
                            return true;
                        };
                        i = next_i;
                    } else if let Some(ch) = s.get(i..).and_then(|tail| tail.chars().next()) {
                        if let Some(buf) = captured.as_mut() {
                            buf.push(ch);
                        }
                        i += ch.len_utf8();
                    } else {
                        return true;
                    }
                }
                if !closed {
                    return true;
                }
                let Some(j) = skip_ws_and_comments(s, i) else {
                    return true;
                };
                if bytes.get(j) == Some(&b'\'') {
                    i = j + 1;
                    seg_mode = base_mode;
                } else if matches!(bytes.get(j), Some(b'E' | b'e'))
                    && bytes.get(j + 1) == Some(&b'\'')
                {
                    i = j + 2;
                    seg_mode = QuoteMode::Backslash;
                } else if matches!(bytes.get(j), Some(b'U' | b'u'))
                    && bytes.get(j + 1) == Some(&b'&')
                    && bytes.get(j + 2) == Some(&b'\'')
                {
                    i = j + 3;
                    seg_mode = QuoteMode::Unicode;
                } else {
                    break;
                }
            }
            if let Some(lit) = captured.as_deref() {
                if let Some(d) = columns_fn_depth {
                    if paren_depth != d || columns_arg_count > 0 || columns_arg_references_pii(lit)
                    {
                        return true;
                    }
                    columns_arg_count = 1;
                }
                let in_sub_arrow_brace = in_subscript
                    || in_arrow_rhs
                    || (brace_depth > 0 && next_non_ws_or_comment_is_colon(s, i));
                if literal_references_pii_in_key_or_extract_ctx(
                    lit,
                    in_sub_arrow_brace,
                    &extract_stack,
                ) {
                    return true;
                }
            }
            if (check_key_ctx || columns_fn_depth.is_some() || in_brace_key)
                && skip_ws_and_comments(s, i).is_some_and(|j| bytes.get(j) == Some(&b'['))
            {
                return true;
            }
            if last_sig == LastSig::JsonArrow
                && json_arrow_paren_depth.is_none()
                && json_arrow_list_depth.is_none()
                && next_non_ws_or_comment_is_concat_pipe(s, i)
            {
                return true;
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            rel_tokens.push(RelTok::StrLit {
                lower: trim_sql_ws(captured.as_deref().unwrap_or_default()).to_lowercase(),
            });
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            continue;
        }

        // 5. `$$...$$` and `$tag$...$tag$` DuckDB dollar-quoted string literals
        if bytes[i] == b'$' {
            let tag_end = if bytes.get(i + 1) == Some(&b'$') {
                Some(i + 1)
            } else if bytes.get(i + 1).is_some_and(|b| is_dolq_start(*b)) {
                let mut j = i + 2;
                while j < bytes.len() && is_dolq_cont(bytes[j]) {
                    j += 1;
                }
                (bytes.get(j) == Some(&b'$')).then_some(j)
            } else {
                None
            };
            if let Some(end_dollar) = tag_end {
                let delim = &bytes[i..=end_dollar];
                let body_start = end_dollar + 1;
                let Some(rel) = bytes
                    .get(body_start..)
                    .and_then(|hay| hay.windows(delim.len()).position(|w| w == delim))
                else {
                    return true;
                };
                let body = &s[body_start..body_start + rel];
                let after = body_start + rel + delim.len();
                if let Some(d) = columns_fn_depth {
                    if paren_depth != d || columns_arg_count > 0 || columns_arg_references_pii(body)
                    {
                        return true;
                    }
                    columns_arg_count = 1;
                }
                let in_sub_arrow_brace = in_subscript
                    || in_arrow_rhs
                    || (brace_depth > 0 && next_non_ws_or_comment_is_colon(s, after));
                if literal_references_pii_in_key_or_extract_ctx(
                    body,
                    in_sub_arrow_brace,
                    &extract_stack,
                ) {
                    return true;
                }
                if (check_key_ctx || columns_fn_depth.is_some() || in_brace_key)
                    && skip_ws_and_comments(s, after).is_some_and(|j| bytes.get(j) == Some(&b'['))
                {
                    return true;
                }
                if last_sig == LastSig::JsonArrow
                    && json_arrow_paren_depth.is_none()
                    && json_arrow_list_depth.is_none()
                    && next_non_ws_or_comment_is_concat_pipe(s, after)
                {
                    return true;
                }
                i = after;
                if let Some(top) = paren_stack.last_mut() {
                    top.1 = false;
                }
                rel_tokens.push(RelTok::StrLit {
                    lower: trim_sql_ws(body).to_lowercase(),
                });
                pending_call = PendingCallKind::None;
                pending_extract_mode = None;
                pending_extract_dot_chain = false;
                pending_columns_fn = false;
                prev_was_dot = false;
                last_sig = LastSig::Other;
                continue;
            }
            if columns_fn_depth.is_some() || in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            i += 1;
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            rel_tokens.push(RelTok::Other);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            continue;
        }

        // 6. `"..."`, `` `...` ``, and `U&"..."` / `u&"..."` quoted identifiers
        if (matches!(bytes[i], b'U' | b'u')
            && bytes.get(i + 1) == Some(&b'&')
            && bytes.get(i + 2) == Some(&b'"'))
            || matches!(bytes[i], b'"' | b'`')
        {
            if columns_fn_depth.is_some() || in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            let unicode_escapes = matches!(bytes[i], b'U' | b'u');
            let quote = if unicode_escapes { b'"' } else { bytes[i] };
            i += if unicode_escapes { 3 } else { 1 };
            let mut ident_buf = String::new();
            let mut closed = false;
            while i < bytes.len() {
                if bytes[i] == quote {
                    if bytes.get(i + 1) == Some(&quote) {
                        ident_buf.push(quote as char);
                        i += 2;
                    } else {
                        i += 1;
                        closed = true;
                        break;
                    }
                } else if unicode_escapes && bytes[i] == b'\\' {
                    let Some(next_i) = decode_sql_unicode_escape(bytes, i, &mut ident_buf) else {
                        return true;
                    };
                    i = next_i;
                } else if let Some(ch) = s.get(i..).and_then(|tail| tail.chars().next()) {
                    ident_buf.push(ch);
                    i += ch.len_utf8();
                } else {
                    return true;
                }
            }
            if !closed || is_pii_identifier(&ident_buf) || is_blocked_sql_keyword(&ident_buf) {
                return true;
            }
            let followed_by_dot =
                skip_ws_and_comments(s, i).is_some_and(|j| bytes.get(j) == Some(&b'.'));
            let followed_by_open_paren = next_non_ws_or_comment_is_open_paren(s, i);
            if is_blind_pii_fn(&ident_buf) && followed_by_open_paren {
                return true;
            }
            let ident_trimmed = trim_sql_ws(&ident_buf);
            let extra_ctx = ExtraExtractCtx {
                paren_stack: &paren_stack,
                extract_stack: &extract_stack,
                bracket_depth,
                brace_depth,
                case_depth,
            };
            if ident_trimmed.eq_ignore_ascii_case("_extra")
                && !extra_is_safely_extracted(s, i, &extra_ctx)
            {
                return true;
            }
            if (in_subscript || in_brace_key)
                && (is_dynamic_key_fn(&ident_buf) || followed_by_open_paren)
            {
                return true;
            }
            let was_dot = prev_was_dot;
            prev_was_dot = false;
            let preserves_trivial_prefix = ident_trimmed.eq_ignore_ascii_case("_extra")
                || followed_by_dot
                || (!was_dot && followed_by_open_paren && is_safe_json_cast_fn(ident_trimmed));
            if !preserves_trivial_prefix {
                if let Some(top) = paren_stack.last_mut() {
                    top.1 = false;
                }
            }
            pending_extract_mode = extract_fn_mode(&ident_buf);
            pending_extract_dot_chain = pending_extract_mode.is_some() && was_dot;
            pending_call = if pending_extract_mode.is_some() {
                PendingCallKind::ExtractFn
            } else if !was_dot && is_safe_json_cast_fn(ident_trimmed) {
                PendingCallKind::SafeJsonCast
            } else {
                PendingCallKind::OtherFn
            };
            pending_columns_fn = ident_trimmed.eq_ignore_ascii_case("columns");
            rel_tokens.push(RelTok::Ident {
                lower: ident_trimmed.to_lowercase(),
                quoted: true,
                followed_by_dot,
                followed_by_open_paren,
            });
            last_sig = LastSig::IdentOrClose;
            continue;
        }

        // 7. `[` ... `]` T-SQL single bracketed PII identifier OR DuckDB `LIST` / subscript
        if bytes[i] == b'[' {
            if columns_fn_depth.is_some() || in_subscript || in_extract_key_arg {
                return true;
            }
            if s.get(i + 1..).is_some_and(peek_simple_bracket_pii) {
                return true;
            }
            bracket_depth = bracket_depth.saturating_add(1);
            if (last_sig == LastSig::JsonArrow || json_arrow_paren_depth.is_some())
                && json_arrow_list_depth.is_none()
            {
                json_arrow_list_depth = Some(bracket_depth);
            } else if last_sig == LastSig::JsonArrow {
                return true;
            }
            if last_sig == LastSig::IdentOrClose {
                subscript_depths.push(bracket_depth);
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            rel_tokens.push(RelTok::LBracket);
            last_sig = LastSig::Other;
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            i += 1;
            continue;
        }
        if bytes[i] == b']' {
            if columns_fn_depth.is_some() || bracket_depth == 0 || last_sig == LastSig::JsonArrow {
                return true;
            }
            if json_arrow_list_depth == Some(bracket_depth) {
                json_arrow_list_depth = None;
                if json_arrow_paren_depth.is_none()
                    && next_non_ws_or_comment_is_concat_pipe(s, i + 1)
                {
                    return true;
                }
            }
            if subscript_depths.last() == Some(&bracket_depth) {
                subscript_depths.pop();
            }
            bracket_depth -= 1;
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            rel_tokens.push(RelTok::RBracket);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::IdentOrClose;
            i += 1;
            continue;
        }

        // 8. Unquoted identifiers (ASCII & non-ASCII UTF-8 per `libpg_query` `scan.l`) & hyphenated filter/header tokens
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' || bytes[i] >= 0x80 {
            if columns_fn_depth.is_some() || in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            let start = i;
            let mut end = start
                + s.get(start..)
                    .and_then(|tail| tail.chars().next())
                    .map_or(1, char::len_utf8);
            while end < bytes.len() {
                if bytes[end].is_ascii_alphanumeric() || matches!(bytes[end], b'_' | b'$') {
                    end += 1;
                } else if bytes[end] >= 0x80 {
                    let Some(ch) = s.get(end..).and_then(|tail| tail.chars().next()) else {
                        break;
                    };
                    if is_sql_unicode_ws(ch) {
                        break;
                    }
                    end += ch.len_utf8();
                } else {
                    break;
                }
            }
            let mut hyph_end = end;
            while bytes.get(hyph_end) == Some(&b'-') {
                let seg_start = hyph_end + 1;
                let Some(&next) = bytes.get(seg_start) else {
                    break;
                };
                if !(next.is_ascii_alphanumeric() || next == b'_') {
                    break;
                }
                if (matches!(next, b'E' | b'e') && bytes.get(seg_start + 1) == Some(&b'\''))
                    || (matches!(next, b'U' | b'u')
                        && bytes.get(seg_start + 1) == Some(&b'&')
                        && matches!(bytes.get(seg_start + 2), Some(b'"' | b'\'')))
                {
                    break;
                }
                hyph_end = seg_start + 1;
                while hyph_end < bytes.len()
                    && (bytes[hyph_end].is_ascii_alphanumeric() || bytes[hyph_end] == b'_')
                {
                    hyph_end += 1;
                }
            }
            if hyph_end > end {
                let hyph_span = &s[start..hyph_end];
                if is_pii_identifier(hyph_span)
                    || hyph_span
                        .split('-')
                        .any(|seg| is_pii_key_exact(seg) || is_extra_column_ident(seg))
                {
                    return true;
                }
            }
            let token = &s[start..end];
            if is_pii_identifier(token) || is_blocked_sql_keyword(token) {
                return true;
            }
            let followed_by_dot =
                skip_ws_and_comments(s, end).is_some_and(|j| bytes.get(j) == Some(&b'.'));
            let followed_by_open_paren = next_non_ws_or_comment_is_open_paren(s, end);
            if is_blind_pii_fn(token) && followed_by_open_paren {
                return true;
            }
            if token.eq_ignore_ascii_case("case") {
                case_depth = case_depth.saturating_add(1);
            } else if token.eq_ignore_ascii_case("end") {
                case_depth = case_depth.saturating_sub(1);
            }
            let extra_ctx = ExtraExtractCtx {
                paren_stack: &paren_stack,
                extract_stack: &extract_stack,
                bracket_depth,
                brace_depth,
                case_depth,
            };
            if token.eq_ignore_ascii_case("_extra")
                && !extra_is_safely_extracted(s, end, &extra_ctx)
            {
                return true;
            }
            if (in_subscript || in_brace_key)
                && (is_dynamic_key_fn(token) || followed_by_open_paren)
            {
                return true;
            }
            let was_dot = prev_was_dot;
            prev_was_dot = false;
            let preserves_trivial_prefix = token.eq_ignore_ascii_case("_extra")
                || followed_by_dot
                || (!was_dot && followed_by_open_paren && is_safe_json_cast_fn(token));
            if !preserves_trivial_prefix {
                if let Some(top) = paren_stack.last_mut() {
                    top.1 = false;
                }
            }
            pending_extract_mode = extract_fn_mode(token);
            pending_extract_dot_chain = pending_extract_mode.is_some() && was_dot;
            pending_call = if pending_extract_mode.is_some() {
                PendingCallKind::ExtractFn
            } else if !was_dot && is_safe_json_cast_fn(token) {
                PendingCallKind::SafeJsonCast
            } else if !was_dot && is_non_call_sql_keyword(token) {
                PendingCallKind::None
            } else {
                PendingCallKind::OtherFn
            };
            pending_columns_fn = token.eq_ignore_ascii_case("columns");
            rel_tokens.push(RelTok::Ident {
                lower: token.to_lowercase(),
                quoted: false,
                followed_by_dot,
                followed_by_open_paren,
            });
            last_sig = if !is_non_keyword_rel_pos(&rel_tokens, rel_tokens.len() - 1)
                && is_non_subscriptable_sql_keyword(token)
            {
                LastSig::Other
            } else {
                LastSig::IdentOrClose
            };
            i = end;
            continue;
        }

        // 9. Strict single-literal `COLUMNS(...)` gate for all non-comment/non-literal tokens
        if let Some(d) = columns_fn_depth {
            if bytes[i] == b')' && paren_depth == d && columns_arg_count == 1 {
                columns_fn_depth = None;
                columns_arg_count = 0;
                paren_depth = paren_depth.saturating_sub(1);
                let _ = paren_stack.pop();
                rel_tokens.push(RelTok::RParen);
                pending_call = PendingCallKind::None;
                pending_extract_mode = None;
                pending_extract_dot_chain = false;
                pending_columns_fn = false;
                prev_was_dot = false;
                last_sig = LastSig::IdentOrClose;
                i += 1;
                continue;
            }
            return true;
        }

        // 10. Parens, braces, JSON arrows (`->`, `->>`, `#>`, `#>>`) & other punctuation/digits
        if bytes[i] == b'(' {
            if matches!(
                pending_call,
                PendingCallKind::ExtractFn | PendingCallKind::OtherFn
            ) {
                if let Some(top) = paren_stack.last_mut() {
                    top.1 = false;
                }
            }
            let pkind = match pending_call {
                PendingCallKind::None => ParenKind::PlainGroup,
                PendingCallKind::SafeJsonCast => ParenKind::SafeJsonCast,
                PendingCallKind::ExtractFn => ParenKind::ExtractFn,
                PendingCallKind::OtherFn => ParenKind::OtherFn,
            };
            paren_stack.push((pkind, true));
            paren_depth = paren_depth.saturating_add(1);
            if let Some(mode) = pending_extract_mode {
                extract_stack.push(ExtractFrame {
                    paren_depth,
                    bracket_depth,
                    brace_depth,
                    arg_idx: u32::from(pending_extract_dot_chain),
                    mode,
                });
            }
            if pending_columns_fn && columns_fn_depth.is_none() {
                columns_fn_depth = Some(paren_depth);
                columns_arg_count = 0;
            }
            if last_sig == LastSig::JsonArrow && json_arrow_paren_depth.is_none() {
                json_arrow_paren_depth = Some(paren_depth);
            }
            rel_tokens.push(RelTok::LParen);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            i += 1;
            continue;
        }
        if bytes[i] == b')' {
            if last_sig == LastSig::JsonArrow {
                return true;
            }
            if extract_stack
                .last()
                .is_some_and(|f| f.paren_depth == paren_depth)
            {
                extract_stack.pop();
            }
            if json_arrow_paren_depth == Some(paren_depth) {
                json_arrow_paren_depth = None;
                if next_non_ws_or_comment_is_concat_pipe(s, i + 1) {
                    return true;
                }
            }
            paren_depth = paren_depth.saturating_sub(1);
            let _ = paren_stack.pop();
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            rel_tokens.push(RelTok::RParen);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::IdentOrClose;
            i += 1;
            continue;
        }
        if bytes[i] == b'.' {
            if in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            if last_sig != LastSig::IdentOrClose {
                if let Some(top) = paren_stack.last_mut() {
                    top.1 = false;
                }
            }
            rel_tokens.push(RelTok::Dot);
            prev_was_dot = true;
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            last_sig = LastSig::Other;
            i += 1;
            continue;
        }
        if bytes[i] == b'{' {
            if in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            brace_depth = brace_depth.saturating_add(1);
            brace_stack.push((paren_depth, bracket_depth, true));
            rel_tokens.push(RelTok::LBrace);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            i += 1;
            continue;
        }
        if bytes[i] == b'}' {
            if brace_depth == 0 || in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            brace_depth = brace_depth.saturating_sub(1);
            brace_stack.pop();
            rel_tokens.push(RelTok::RBrace);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::IdentOrClose;
            i += 1;
            continue;
        }
        if matches!(bytes[i], b'-' | b'#') && bytes.get(i + 1) == Some(&b'>') {
            if in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            i += if bytes.get(i + 2) == Some(&b'>') {
                3
            } else {
                2
            };
            rel_tokens.push(RelTok::Other);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::JsonArrow;
            continue;
        }
        if bytes[i] == b'#' {
            return true;
        }
        if bytes[i].is_ascii_digit() {
            if in_extract_key_arg {
                return true;
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            i = scan_rel_digits(bytes, i);
            if i < bytes.len() && bytes[i] == b'.' && bytes.get(i + 1) != Some(&b'.') {
                i += 1;
                if i < bytes.len() && bytes[i].is_ascii_digit() {
                    i = scan_rel_digits(bytes, i);
                }
            }
            if i < bytes.len() && matches!(bytes[i], b'e' | b'E') {
                if i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
                    i = scan_rel_digits(bytes, i + 1);
                } else if i + 2 < bytes.len()
                    && matches!(bytes[i + 1], b'+' | b'-')
                    && bytes[i + 2].is_ascii_digit()
                {
                    i = scan_rel_digits(bytes, i + 2);
                }
            }
            if last_sig == LastSig::JsonArrow
                && json_arrow_paren_depth.is_none()
                && json_arrow_list_depth.is_none()
                && next_non_ws_or_comment_is_concat_pipe(s, i)
            {
                return true;
            }
            rel_tokens.push(RelTok::Other);
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            continue;
        }
        if bytes[i] == b',' {
            if in_subscript
                || last_sig == LastSig::JsonArrow
                || (json_arrow_paren_depth.is_some() && json_arrow_list_depth.is_none())
            {
                return true;
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            if let Some(top) = extract_stack.last_mut() {
                if top.paren_depth == paren_depth
                    && top.bracket_depth == bracket_depth
                    && top.brace_depth == brace_depth
                {
                    top.arg_idx = top.arg_idx.saturating_add(1);
                    if top.arg_idx >= 2
                        && matches!(
                            top.mode,
                            ExtractArgMode::JsonContainsNeedle | ExtractArgMode::JsonStructSchema
                        )
                    {
                        return true;
                    }
                }
            }
            if let Some(top) = brace_stack.last_mut() {
                if top.0 == paren_depth && top.1 == bracket_depth {
                    top.2 = true;
                }
            }
            rel_tokens.push(RelTok::Comma);
            i += 1;
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            continue;
        }
        if bytes[i] == b':' {
            if in_subscript || in_arrow_rhs || in_extract_key_arg {
                return true;
            }
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            if bytes.get(i + 1) == Some(&b':') {
                if bytes.get(i + 2) == Some(&b':') {
                    while i < bytes.len() && bytes[i] == b':' {
                        i += 1;
                    }
                    rel_tokens.push(RelTok::Other);
                } else {
                    i += 2;
                    rel_tokens.push(RelTok::DoubleColon);
                }
            } else {
                if let Some(top) = brace_stack.last_mut() {
                    if top.0 == paren_depth && top.1 == bracket_depth {
                        top.2 = false;
                    }
                }
                i += 1;
                rel_tokens.push(RelTok::Colon);
            }
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            continue;
        }
        if bytes[i] == b'|'
            && bytes.get(i + 1) == Some(&b'|')
            && (in_subscript || in_brace_key || in_arrow_rhs || in_extract_key_arg)
        {
            return true;
        }
        if in_subscript || in_arrow_rhs || in_extract_key_arg {
            return true;
        }
        if bytes[i] == b';' {
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            rel_tokens.push(RelTok::Semi);
            i += 1;
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            continue;
        }
        if bytes[i] == b'*' {
            if let Some(top) = paren_stack.last_mut() {
                top.1 = false;
            }
            if bytes.get(i + 1) == Some(&b'*') {
                rel_tokens.push(RelTok::Other);
                i += 2;
            } else {
                rel_tokens.push(RelTok::Star);
                i += 1;
            }
            pending_call = PendingCallKind::None;
            pending_extract_mode = None;
            pending_extract_dot_chain = false;
            pending_columns_fn = false;
            prev_was_dot = false;
            last_sig = LastSig::Other;
            continue;
        }

        if let Some(top) = paren_stack.last_mut() {
            top.1 = false;
        }
        rel_tokens.push(RelTok::Other);
        i += s
            .get(i..)
            .and_then(|tail| tail.chars().next())
            .map_or(1, char::len_utf8);
        pending_call = PendingCallKind::None;
        pending_extract_mode = None;
        pending_extract_dot_chain = false;
        pending_columns_fn = false;
        prev_was_dot = false;
        last_sig = LastSig::Other;
    }

    bracket_depth > 0
        || brace_depth > 0
        || columns_fn_depth.is_some()
        || references_whole_row_relation_struct(&rel_tokens)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FilterSlot {
    /// Root / filter-container context: permits strict allowlisted literal suppression.
    General,
    /// Sticky context inside `_view_extras` or any query/SQL/expression/meta key:
    /// NEVER permits `NonPiiLiteralValue` suppression anywhere in its subtree.
    QueryOrExtras,
    /// Value of a column or ambiguous selector key: checked via `is_pii_identifier` + SQL scanner.
    FieldSelector,
    /// Verified scalar comparison literal (`value`/`values`/`include`/`exclude`, or safe pill `id`)
    /// paired with an unambiguous non-PII column selector.
    NonPiiLiteralValue,
}

const COLUMN_SELECTOR_KEYS: &[&str] = &["col", "column", "field", "field_name", "dimension"];

const AMBIGUOUS_SELECTOR_KEYS: &[&str] = &["name", "key", "attr", "attribute", "lhs", "left"];

const SCALAR_LITERAL_VALUE_KEYS: &[&str] = &["value", "values", "include", "exclude"];

const ALLOWED_PILL_OBJECT_KEYS: &[&str] = &[
    "id",
    "col",
    "column",
    "field",
    "field_name",
    "dimension",
    "mode",
    "op",
    "operator",
    "value",
    "values",
    "include",
    "exclude",
    "case_sensitive",
    "regex",
];

const ALLOWED_VALUE_FILTER_MAP_KEYS: &[&str] = &[
    "mode",
    "value",
    "values",
    "op",
    "operator",
    "include",
    "exclude",
    "case_sensitive",
    "regex",
];

const QUERY_OR_EXTRAS_SUBTREE_KEYS: &[&str] = &[
    "_view_extras",
    "extras",
    "sql",
    "raw_sql",
    "where",
    "query",
    "query_filters",
    "expr",
    "expression",
    "having",
    "select",
    "from",
    "search",
    "raw",
    "statement",
    "sort",
    "order",
    "order_by",
    "group",
    "group_by",
    "custom",
    "advanced",
    "meta",
    "metadata",
    "params",
    "args",
    "options",
    "body",
    "payload",
    "data",
    "ast",
    "criteria",
];

const EXTRA_RESERVED_CONTAINER_KEYS: &[&str] = &[
    "filter",
    "filters",
    "filters_json",
    "clause",
    "clauses",
    "condition",
    "conditions",
    "and",
    "or",
    "not",
    "nor",
    "xor",
    "view",
    "views",
    "rule",
    "rules",
    "predicate",
    "predicates",
    "lhs",
    "left",
    "rhs",
    "right",
    "value",
    "values",
];

fn normalize_filter_key(raw: &str) -> String {
    raw.trim()
        .chars()
        .map(|c| {
            if c == '-' {
                '_'
            } else {
                c.to_ascii_lowercase()
            }
        })
        .collect()
}

fn key_in_set_or_dedup(norm: &str, set: &[&str]) -> bool {
    if set.contains(&norm) {
        return true;
    }
    if let Some((base, suffix)) = norm.rsplit_once('_') {
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) && set.contains(&base) {
            return true;
        }
    }
    false
}

fn is_query_or_extras_key(raw: &str) -> bool {
    let norm = normalize_filter_key(raw);
    key_in_set_or_dedup(&norm, QUERY_OR_EXTRAS_SUBTREE_KEYS)
}

fn is_reserved_filter_key(raw: &str) -> bool {
    let norm = normalize_filter_key(raw);
    key_in_set_or_dedup(&norm, QUERY_OR_EXTRAS_SUBTREE_KEYS)
        || key_in_set_or_dedup(&norm, EXTRA_RESERVED_CONTAINER_KEYS)
}

fn is_valid_column_selector_ident(raw: &str) -> bool {
    let st = raw.trim();
    if st.is_empty() || st.len() > MAX_FIELD_CHARS {
        return false;
    }
    let bytes = st.as_bytes();
    if !(bytes[0].is_ascii_alphanumeric() || bytes[0] == b'_') {
        return false;
    }
    if st.ends_with(['.', ':', '-']) || st.contains("..") || st.contains("::") {
        return false;
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || matches!(*b, b'_' | b'.' | b':' | b'-'))
    {
        return false;
    }
    !is_pii_identifier(st) && !is_extra_column_ident(st) && !is_reserved_filter_key(st)
}

fn is_valid_column_map_key(raw_key: &str) -> bool {
    if raw_key.is_empty() || raw_key.len() > MAX_FIELD_CHARS {
        return false;
    }
    let bytes = raw_key.as_bytes();
    if !(bytes[0].is_ascii_alphanumeric() || bytes[0] == b'_') {
        return false;
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return false;
    }
    !is_pii_identifier(raw_key)
        && !is_extra_column_ident(raw_key)
        && !is_reserved_filter_key(raw_key)
}

fn is_json_scalar(v: &Value) -> bool {
    matches!(
        v,
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null
    )
}

fn is_scalar_or_flat_scalar_array(v: &Value) -> bool {
    is_json_scalar(v) || matches!(v, Value::Array(items) if items.iter().all(is_json_scalar))
}

fn is_safe_pill_id(v: &Value) -> bool {
    v.as_str().is_some_and(|id_s| {
        id_s.len() <= 128
            && !id_s.bytes().any(|b| {
                b.is_ascii_whitespace()
                    || matches!(
                        b,
                        b'=' | b'\''
                            | b'"'
                            | b'`'
                            | b'('
                            | b')'
                            | b';'
                            | b'['
                            | b']'
                            | b'{'
                            | b'}'
                            | b':'
                            | b'/'
                    )
            })
            && !is_pii_identifier(id_s)
    })
}

fn is_value_filter_map(map: &serde_json::Map<String, Value>) -> bool {
    if map.is_empty() {
        return false;
    }
    let mut saw_literal_key = false;
    let mut saw_filter_structure_key = false;
    for (k, v) in map {
        let kl = k.trim().to_ascii_lowercase();
        if !ALLOWED_VALUE_FILTER_MAP_KEYS.contains(&kl.as_str()) {
            return false;
        }
        if matches!(
            kl.as_str(),
            "mode" | "op" | "operator" | "include" | "exclude"
        ) {
            saw_filter_structure_key = true;
        }
        if SCALAR_LITERAL_VALUE_KEYS.contains(&kl.as_str()) {
            if !is_scalar_or_flat_scalar_array(v) {
                return false;
            }
            saw_literal_key = true;
        } else if matches!(kl.as_str(), "mode" | "op" | "operator") {
            if !v.is_string() {
                return false;
            }
        } else if matches!(kl.as_str(), "case_sensitive" | "regex")
            && !(v.is_boolean() || v.is_null())
        {
            return false;
        }
    }
    saw_literal_key && saw_filter_structure_key
}

fn object_allows_literal_value_suppression(map: &serde_json::Map<String, Value>) -> bool {
    let mut saw_column_selector = false;
    for (k, v) in map {
        let kl = k.trim().to_ascii_lowercase();
        if !ALLOWED_PILL_OBJECT_KEYS.contains(&kl.as_str()) {
            return false;
        }
        if COLUMN_SELECTOR_KEYS.contains(&kl.as_str()) {
            let Some(s) = v.as_str() else {
                return false;
            };
            if !is_valid_column_selector_ident(s) {
                return false;
            }
            saw_column_selector = true;
        } else if SCALAR_LITERAL_VALUE_KEYS.contains(&kl.as_str()) {
            if !is_scalar_or_flat_scalar_array(v) {
                return false;
            }
        } else if matches!(kl.as_str(), "mode" | "op" | "operator") {
            if !v.is_string() {
                return false;
            }
        } else if matches!(kl.as_str(), "case_sensitive" | "regex")
            && !(v.is_boolean() || v.is_null())
        {
            return false;
        }
    }
    saw_column_selector
}

struct NoDupJson;

impl<'de> serde::de::Deserialize<'de> for NoDupJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct NoDupVisitor;

        impl<'de> serde::de::Visitor<'de> for NoDupVisitor {
            type Value = NoDupJson;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("valid JSON without duplicate or colliding object keys")
            }

            fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_i128<E>(self, _: i128) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_u128<E>(self, _: u128) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_string<E>(self, _: String) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
            where
                D2: serde::de::Deserializer<'de>,
            {
                deserializer.deserialize_any(self)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(NoDupJson)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                while seq.next_element::<NoDupJson>()?.is_some() {}
                Ok(NoDupJson)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut seen_exact = std::collections::BTreeSet::new();
                let mut seen_norm = std::collections::BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    let norm = key.trim().to_ascii_lowercase();
                    if !seen_exact.insert(key) || !seen_norm.insert(norm) {
                        return Err(serde::de::Error::custom("duplicate or colliding JSON key"));
                    }
                    map.next_value::<NoDupJson>()?;
                }
                Ok(NoDupJson)
            }
        }

        deserializer.deserialize_any(NoDupVisitor)
    }
}

pub(crate) fn json_has_duplicate_keys(raw: &str) -> bool {
    serde_json::from_str::<NoDupJson>(raw).is_err()
}

fn column_filter_map_references_pii(
    child_map: &serde_json::Map<String, Value>,
    depth: usize,
) -> bool {
    if depth > MAX_JSON_DEPTH {
        return true;
    }
    child_map.iter().any(|(ck, cv)| {
        if is_pii_identifier(ck) || is_extra_column_ident(ck) {
            return true;
        }
        let ck_lower = ck.trim().to_ascii_lowercase();
        if SCALAR_LITERAL_VALUE_KEYS.contains(&ck_lower.as_str())
            && is_scalar_or_flat_scalar_array(cv)
        {
            value_references_pii(cv, FilterSlot::NonPiiLiteralValue, false, depth + 1)
        } else {
            value_references_pii(cv, FilterSlot::General, false, depth + 1)
        }
    })
}

fn value_references_pii(
    value: &Value,
    slot: FilterSlot,
    in_query_or_extras: bool,
    depth: usize,
) -> bool {
    if depth > MAX_JSON_DEPTH {
        return true;
    }
    match value {
        Value::Object(map) => {
            // Sticky monotone `in_query_or_extras`: once inside `_view_extras` or a query/SQL key,
            // no descendant can ever pass `false`, suppress literal scanning, or revert to General.
            let obj_in_query = in_query_or_extras || slot == FilterSlot::QueryOrExtras;
            let container_slot = if obj_in_query {
                FilterSlot::QueryOrExtras
            } else {
                FilterSlot::General
            };
            let suppress_literal_values = !obj_in_query
                && slot == FilterSlot::General
                && object_allows_literal_value_suppression(map);

            map.iter().any(|(k, v)| {
                if k.contains('\0') || is_pii_identifier(k) || is_extra_column_ident(k) {
                    return true;
                }
                let key_norm = trim_sql_ws(k).to_ascii_lowercase();
                let child_in_query = obj_in_query || is_query_or_extras_key(k);
                if child_in_query && is_query_or_extras_key(k) {
                    return value_references_pii(v, FilterSlot::QueryOrExtras, true, depth + 1);
                }
                if COLUMN_SELECTOR_KEYS.contains(&key_norm.as_str())
                    || AMBIGUOUS_SELECTOR_KEYS.contains(&key_norm.as_str())
                {
                    if v.is_string() {
                        return value_references_pii(
                            v,
                            FilterSlot::FieldSelector,
                            child_in_query,
                            depth + 1,
                        );
                    }
                    return value_references_pii(v, FilterSlot::QueryOrExtras, true, depth + 1);
                }
                if suppress_literal_values {
                    if SCALAR_LITERAL_VALUE_KEYS.contains(&key_norm.as_str())
                        && is_scalar_or_flat_scalar_array(v)
                    {
                        return value_references_pii(
                            v,
                            FilterSlot::NonPiiLiteralValue,
                            false,
                            depth + 1,
                        );
                    }
                    if key_norm == "id" && is_safe_pill_id(v) {
                        return value_references_pii(
                            v,
                            FilterSlot::NonPiiLiteralValue,
                            false,
                            depth + 1,
                        );
                    }
                }
                if !child_in_query && slot == FilterSlot::General && is_valid_column_map_key(k) {
                    if let Value::Object(child_map) = v {
                        if is_value_filter_map(child_map) {
                            return column_filter_map_references_pii(child_map, depth + 1);
                        }
                    }
                }
                value_references_pii(v, container_slot, child_in_query, depth + 1)
            })
        }
        Value::Array(items) => {
            let arr_in_query = in_query_or_extras || slot == FilterSlot::QueryOrExtras;
            items.iter().any(|item| {
                // Never allow nested Objects/Arrays inside a NonPiiLiteralValue array to inherit suppression,
                // and never drop sticky `arr_in_query`.
                let elem_slot = if arr_in_query {
                    FilterSlot::QueryOrExtras
                } else if slot == FilterSlot::NonPiiLiteralValue && !is_json_scalar(item) {
                    FilterSlot::General
                } else {
                    slot
                };
                value_references_pii(item, elem_slot, arr_in_query, depth + 1)
            })
        }
        Value::String(s) => {
            if s.contains('\0') {
                return true;
            }
            if slot == FilterSlot::FieldSelector
                && (is_pii_identifier(s) || is_extra_column_ident(s))
            {
                return true;
            }
            let t = trim_sql_ws(s);
            if t.len() > MAX_FILTERS_BYTES {
                return true;
            }
            let allow_literal_skip = !in_query_or_extras && slot == FilterSlot::NonPiiLiteralValue;
            if t.starts_with('{') || t.starts_with('[') {
                let nested_in_query = in_query_or_extras
                    || matches!(slot, FilterSlot::QueryOrExtras | FilterSlot::FieldSelector);
                let child_slot = if nested_in_query {
                    FilterSlot::QueryOrExtras
                } else {
                    FilterSlot::General
                };
                match serde_json::from_str::<Value>(t) {
                    Ok(nested) => {
                        if json_has_duplicate_keys(t) {
                            return true;
                        }
                        return value_references_pii(
                            &nested,
                            child_slot,
                            nested_in_query,
                            depth + 1,
                        );
                    }
                    Err(_) if !allow_literal_skip => {
                        let deep_brackets = t
                            .bytes()
                            .take_while(|b| {
                                matches!(*b, b'[' | b'{' | b' ' | b'\t' | b'\r' | b'\n')
                            })
                            .filter(|b| matches!(*b, b'[' | b'{'))
                            .count();
                        if t.starts_with('{')
                            || deep_brackets > MAX_JSON_DEPTH
                            || t.contains("\\u")
                            || t.contains("\\U")
                        {
                            return true;
                        }
                    }
                    Err(_) => {}
                }
            }
            if !allow_literal_skip && t.starts_with('"') && t.ends_with('"') && t.len() >= 2 {
                let (next_slot, next_in_query) = if slot == FilterSlot::FieldSelector {
                    (FilterSlot::FieldSelector, in_query_or_extras)
                } else {
                    (FilterSlot::QueryOrExtras, true)
                };
                if let Ok(inner) = serde_json::from_str::<String>(t) {
                    if value_references_pii(
                        &Value::String(inner),
                        next_slot,
                        next_in_query,
                        depth + 1,
                    ) {
                        return true;
                    }
                } else if !t[1..t.len() - 1].contains('"')
                    && (t.contains("\\u")
                        || t.contains("\\U")
                        || t.contains("\\x")
                        || value_references_pii(
                            &Value::String(t[1..t.len() - 1].to_owned()),
                            next_slot,
                            next_in_query,
                            depth + 1,
                        ))
                {
                    return true;
                }
            }
            if !in_query_or_extras && slot == FilterSlot::NonPiiLiteralValue {
                return false;
            }
            sql_or_filter_references_pii(t)
        }
        _ => false,
    }
}

pub fn filters_reference_pii(filters_json: &str) -> bool {
    if filters_json.len() > MAX_FILTERS_BYTES
        || filters_json.contains('\0')
        || json_has_duplicate_keys(filters_json)
    {
        return true;
    }
    let Ok(parsed) = serde_json::from_str::<Value>(filters_json) else {
        return true;
    };
    value_references_pii(&parsed, FilterSlot::General, false, 0)
}

pub fn list_for_role<S: ViewStore>(
    store: &S,
    service_id: &str,
    role: Role,
    limit: usize,
) -> Result<Vec<SavedView>, Error> {
    let (_, mut views) = load(store, service_id)?;
    if role == Role::AnalystNoPii {
        views.retain(|view| !filters_reference_pii(&view.filters_json));
    }
    views.truncate(limit);
    Ok(views)
}

pub fn list<S: ViewStore>(
    store: &S,
    service_id: &str,
    limit: usize,
) -> Result<Vec<SavedView>, Error> {
    list_for_role(store, service_id, Role::Admin, limit)
}

fn update<S: ViewStore, T>(
    store: &S,
    service_id: &str,
    mut operation: impl FnMut(&mut Vec<SavedView>) -> Result<T, Error>,
) -> Result<T, Error> {
    for _ in 0..3 {
        let (generation, mut views) = load(store, service_id)?;
        let before = views.clone();
        let result = operation(&mut views)?;
        if views == before {
            return Ok(result);
        }
        let value = json!({"views": views, "updated_at": store.now()});
        if store
            .cas(&key(service_id), generation.as_deref(), &value)
            .map_err(|_| Error::Storage)?
        {
            return Ok(result);
        }
    }
    Err(Error::Conflict)
}

fn bounded(value: Option<String>) -> Result<Option<String>, Error> {
    match value {
        Some(text)
            if text.chars().count() > MAX_FIELD_CHARS || text.chars().any(char::is_control) =>
        {
            Err(Error::InvalidInput)
        }
        other => Ok(other),
    }
}

fn new_id(store: &impl ViewStore) -> Result<String, Error> {
    let bytes = store.random().map_err(|_| Error::Storage)?;
    Ok(hex::encode(&bytes[..16]))
}

pub(crate) fn valid_view_id(id: &str) -> bool {
    (1..=MAX_FIELD_CHARS).contains(&id.chars().count())
        && !matches!(id, "." | "..")
        && !id
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '?' | '#' | '\\'))
}

/// Creates a view, or replaces the one whose `id` the body names, like the self-hosted
/// `INSERT OR REPLACE`.
pub fn save<S: ViewStore>(store: &S, service_id: &str, body: &[u8]) -> Result<String, Error> {
    let input: ViewInput = serde_json::from_slice(body).map_err(|_| Error::InvalidInput)?;
    if input.service_id != service_id {
        return Err(Error::NotFound);
    }
    let name = input.name.trim().to_owned();
    if name.is_empty()
        || name.chars().count() > MAX_NAME_CHARS
        || name.chars().any(char::is_control)
        || input.filters_json.len() > MAX_FILTERS_BYTES
        || input.filters_json.contains('\0')
        || serde_json::from_str::<Value>(&input.filters_json).is_err()
        || json_has_duplicate_keys(&input.filters_json)
    {
        return Err(Error::InvalidInput);
    }
    let id = match input.id.filter(|id| !id.is_empty()) {
        Some(id) if valid_view_id(&id) => id,
        Some(_) => return Err(Error::InvalidInput),
        None => new_id(store)?,
    };
    let view = SavedView {
        id: id.clone(),
        service_id: service_id.to_owned(),
        name,
        filters_json: input.filters_json,
        time_range_type: bounded(input.time_range_type)?,
        start_time: bounded(input.start_time)?,
        end_time: bounded(input.end_time)?,
        page: bounded(input.page)?,
        created_at: Some(
            bounded(input.created_at)?
                .unwrap_or_else(|| crate::admin::rfc3339_from_secs(store.now())),
        ),
    };
    update(store, service_id, |views| {
        if let Some(existing) = views.iter_mut().find(|v| v.id == view.id) {
            *existing = view.clone();
        } else if views.len() >= MAX_VIEWS {
            return Err(Error::InvalidInput);
        } else {
            views.push(view.clone());
        }
        Ok(())
    })?;
    Ok(id)
}

pub fn delete<S: ViewStore>(store: &S, service_id: &str, view_id: &str) -> Result<(), Error> {
    update(store, service_id, |views| {
        let before = views.len();
        views.retain(|v| v.id != view_id);
        if views.len() == before {
            return Err(Error::NotFound);
        }
        Ok(())
    })
}

fn json_response(status: u16, value: Value) -> Response {
    Response::json(status, &value)
}

/// Serves `/api/views…` for the install's active service `service_id`. Every signed-in role may
/// read views (with PII-referencing views omitted for `AnalystNoPii`); only `Admin` may write or
/// delete views.
pub fn handle<S: ViewStore>(
    store: &S,
    service_id: &str,
    role: Role,
    request: &Request,
) -> Result<Response, Error> {
    let path = request.path.split('?').next().unwrap_or_default();
    let tail = path
        .strip_prefix("/api/views")
        .ok_or(Error::NotFound)?
        .trim_matches('/');
    let segment = (!tail.is_empty())
        .then(|| {
            url::form_urlencoded::parse(format!("v={tail}").as_bytes())
                .next()
                .map(|(_, value)| value.into_owned())
                .unwrap_or_default()
        })
        .filter(|segment| !segment.contains('/'));
    if !tail.is_empty() && segment.is_none() {
        return Err(Error::NotFound);
    }
    match (request.method.as_str(), segment) {
        ("GET", Some(wanted)) => {
            if wanted != service_id {
                return Err(Error::NotFound);
            }
            let limit = match request.query_param("limit") {
                Some(value) => value
                    .parse::<usize>()
                    .ok()
                    .filter(|limit| (1..=MAX_LIMIT).contains(limit))
                    .ok_or(Error::InvalidInput)?,
                None => DEFAULT_LIMIT,
            };
            let views = list_for_role(store, service_id, role, limit)?;
            Ok(json_response(
                200,
                serde_json::to_value(views).map_err(|_| Error::Storage)?,
            ))
        }
        ("POST", None) => {
            if role != Role::Admin {
                return Err(Error::Forbidden);
            }
            if request.body.len() > crate::http::body_limit(request) {
                return Err(Error::InvalidInput);
            }
            let id = save(store, service_id, &request.body)?;
            Ok(json_response(201, json!({"id": id, "status": "success"})))
        }
        ("DELETE", Some(view_id)) => {
            if role != Role::Admin {
                return Err(Error::Forbidden);
            }
            let scoped = request
                .query_param("service_id")
                .or_else(|| request.header("x-service-id").map(str::to_owned));
            if scoped.is_some_and(|scoped| scoped != service_id) {
                return Err(Error::NotFound);
            }
            delete(store, service_id, &view_id)?;
            Ok(Response::new(204, Vec::new()).private())
        }
        _ => Err(Error::NotFound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct Memory {
        records: RefCell<BTreeMap<String, (u64, Value)>>,
        fail_cas: RefCell<u32>,
        randoms: RefCell<u8>,
    }

    impl ViewStore for Memory {
        fn get(&self, key: &str) -> Result<Option<Record>, String> {
            Ok(self.records.borrow().get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            }))
        }
        fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String> {
            if *self.fail_cas.borrow() > 0 {
                *self.fail_cas.borrow_mut() -= 1;
                return Ok(false);
            }
            let mut records = self.records.borrow_mut();
            let current = records.get(key).map(|(g, _)| g.to_string());
            if current.as_deref() != generation {
                return Ok(false);
            }
            let next = current.map_or(1, |g| g.parse::<u64>().unwrap() + 1);
            records.insert(key.to_owned(), (next, value.clone()));
            Ok(true)
        }
        fn random(&self) -> Result<[u8; 32], String> {
            *self.randoms.borrow_mut() += 1;
            Ok([*self.randoms.borrow(); 32])
        }
        fn now(&self) -> u64 {
            1_700_000_000
        }
    }

    fn request(method: &str, path: &str, body: Value) -> Request {
        Request {
            method: method.into(),
            path: path.into(),
            headers: BTreeMap::new(),
            body: if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            },
        }
    }

    fn body(response: &Response) -> Value {
        serde_json::from_slice(&response.body).unwrap()
    }

    fn call(store: &Memory, method: &str, path: &str, body: Value) -> Result<Response, Error> {
        handle(store, "svc1", Role::Admin, &request(method, path, body))
    }

    fn call_as(
        store: &Memory,
        role: Role,
        method: &str,
        path: &str,
        body: Value,
    ) -> Result<Response, Error> {
        handle(store, "svc1", role, &request(method, path, body))
    }

    fn failure(result: Result<Response, Error>) -> Error {
        match result {
            Ok(response) => panic!("expected an error, got status {}", response.status),
            Err(error) => error,
        }
    }

    fn view(name: &str) -> Value {
        json!({"service_id":"svc1","name":name,"filters_json":"{}"})
    }

    fn post(store: &Memory, view: Value) -> String {
        body(&call(store, "POST", "/api/views/", view).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn views_round_trip_through_the_service_record_for_every_user() {
        let store = Memory::default();
        let created = call(
            &store,
            "POST",
            "/api/views/",
            json!({"service_id":"svc1","name":" Errors ","filters_json":"{\"filters\":[]}","page":"/dashboard"}),
        )
        .unwrap();
        assert_eq!(created.status, 201);
        let id = body(&created)["id"].as_str().unwrap().to_owned();
        assert_eq!(body(&created)["status"], "success");
        let rows = body(&call(&store, "GET", "/api/views/svc1?limit=10", Value::Null).unwrap());
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["id"], id);
        assert_eq!(rows[0]["name"], "Errors");
        assert_eq!(rows[0]["service_id"], "svc1");
        assert_eq!(rows[0]["page"], "/dashboard");
        assert!(rows[0]["created_at"].is_string());
        assert!(store.records.borrow().contains_key("views:svc1"));
        let path = format!("/api/views/{id}");
        assert_eq!(
            call(&store, "DELETE", &path, Value::Null).unwrap().status,
            204
        );
        assert_eq!(
            failure(call(&store, "DELETE", &path, Value::Null)),
            Error::NotFound
        );
        let rows = body(&call(&store, "GET", "/api/views/svc1", Value::Null).unwrap());
        assert_eq!(rows.as_array().unwrap().len(), 0);
    }

    #[test]
    fn saving_an_existing_id_replaces_it_and_a_lost_cas_race_is_retried() {
        let store = Memory::default();
        let first = post(&store, view("one"));
        *store.fail_cas.borrow_mut() = 1;
        let second = post(&store, view("two"));
        assert_ne!(first, second);
        let mut renamed = view("renamed");
        renamed["id"] = first.clone().into();
        assert_eq!(post(&store, renamed), first);
        let names: Vec<String> = list(&store, "svc1", 50)
            .unwrap()
            .into_iter()
            .map(|v| v.name)
            .collect();
        assert_eq!(names, vec!["renamed", "two"]);
        *store.fail_cas.borrow_mut() = 3;
        assert_eq!(
            failure(call(&store, "POST", "/api/views", view("x"))),
            Error::Conflict
        );
    }

    #[test]
    fn views_are_scoped_to_the_active_service_and_validated() {
        let store = Memory::default();
        let other = json!({"service_id":"svc2","name":"n","filters_json":"{}"});
        assert_eq!(
            failure(call(&store, "POST", "/api/views/", other)),
            Error::NotFound
        );
        assert_eq!(
            failure(call(&store, "GET", "/api/views/svc2", Value::Null)),
            Error::NotFound
        );
        for bad in [
            json!({"service_id":"svc1","name":"   ","filters_json":"{}"}),
            json!({"service_id":"svc1","name":"n","filters_json":"not json"}),
            json!({"service_id":"svc1","name":"n","filters_json":"{}","extra":1}),
            json!({"service_id":"svc1","name":"n","filters_json":"{}","page":"x".repeat(201)}),
            json!({"service_id":"svc1","name":"n"}),
        ] {
            assert_eq!(
                failure(call(&store, "POST", "/api/views/", bad)),
                Error::InvalidInput
            );
        }
        assert!(store.records.borrow().is_empty());
        assert_eq!(
            failure(call(&store, "GET", "/api/views/svc1?limit=0", Value::Null)),
            Error::InvalidInput
        );
        assert_eq!(
            failure(call(&store, "GET", "/api/views/svc1/extra", Value::Null)),
            Error::NotFound
        );
        let id = post(&store, view("n"));
        assert_eq!(
            failure(call(
                &store,
                "DELETE",
                &format!("/api/views/{id}?service_id=svc2"),
                Value::Null
            )),
            Error::NotFound
        );
        for _ in 1..MAX_VIEWS {
            post(&store, view("n"));
        }
        assert_eq!(
            failure(call(&store, "POST", "/api/views/", view("n"))),
            Error::InvalidInput
        );
        assert_eq!(list(&store, "svc1", MAX_LIMIT).unwrap().len(), MAX_VIEWS);
    }

    #[test]
    fn analyst_and_no_pii_cannot_create_or_delete_views() {
        let store = Memory::default();
        let id = post(&store, view("Admin View"));
        for role in [Role::Analyst, Role::AnalystNoPii] {
            assert_eq!(
                failure(call_as(&store, role, "POST", "/api/views", view("Denied"))),
                Error::Forbidden
            );
            assert_eq!(
                failure(call_as(&store, role, "POST", "/api/views/", view("Denied"))),
                Error::Forbidden
            );
            assert_eq!(
                failure(call_as(
                    &store,
                    role,
                    "DELETE",
                    &format!("/api/views/{id}"),
                    Value::Null
                )),
                Error::Forbidden
            );
        }
        assert_eq!(list(&store, "svc1", 10).unwrap().len(), 1);
    }

    #[test]
    fn analyst_no_pii_omits_all_18_pii_keys_across_json_and_sql_shapes_before_limit() {
        let store = Memory::default();

        for (idx, pii_key) in PII_FILTER_KEYS.iter().enumerate() {
            let shaped = match idx % 6 {
                0 => {
                    json!({"filters":[{"column": pii_key.to_uppercase(), "op": "=", "value": "198.51.100.7"}]})
                }
                1 => json!([{"field": *pii_key, "value": "abc"}]),
                2 => json!({format!("{pii_key}_1"): "198.51.100.7"}),
                3 => {
                    json!({"_view_extras": {"sql": format!("SELECT * FROM logs WHERE {pii_key} = 'secret'")}})
                }
                4 => json!({"search": format!("+{pii_key}:198.51.100.7 status:200")}),
                _ => {
                    json!({"filters":[{"field_name": format!("req.http.{pii_key}"), "value": "x"}]})
                }
            };
            post(
                &store,
                json!({
                    "service_id": "svc1",
                    "name": format!("pii-{pii_key}"),
                    "filters_json": shaped.to_string(),
                }),
            );
        }
        assert!(filters_reference_pii("not-valid-json"));

        let benign_id = post(
            &store,
            json!({
                "service_id": "svc1",
                "name": "Safe Health & Gzip",
                "filters_json": json!({
                    "filters": [
                        {"column": "status", "op": "=", "value": "200"},
                        {"column": "content_encoding", "op": "=", "value": "gzip"},
                        {"column": "url", "op": "=", "value": "/sid/health"}
                    ],
                    "_view_extras": {"sql": "SELECT count(*) FROM logs WHERE url = '/sid/health' AND content_encoding = 'gzip'"}
                }).to_string(),
            }),
        );

        let admin_rows =
            body(&call_as(&store, Role::Admin, "GET", "/api/views/svc1", Value::Null).unwrap());
        let analyst_rows =
            body(&call_as(&store, Role::Analyst, "GET", "/api/views/svc1", Value::Null).unwrap());
        assert_eq!(
            admin_rows.as_array().unwrap().len(),
            PII_FILTER_KEYS.len() + 1
        );
        assert_eq!(
            analyst_rows.as_array().unwrap().len(),
            PII_FILTER_KEYS.len() + 1
        );

        // Put a PII view MORE RECENT than benign_id so ?limit=1 proves filtering happens BEFORE truncate(limit)!
        post(
            &store,
            json!({
                "service_id": "svc1",
                "name": "Newest PII View",
                "filters_json": json!({"client_ip": "203.0.113.9"}).to_string(),
            }),
        );

        let nopii_limit_one = body(
            &call_as(
                &store,
                Role::AnalystNoPii,
                "GET",
                "/api/views/svc1?limit=1",
                Value::Null,
            )
            .unwrap(),
        );
        let arr = nopii_limit_one.as_array().unwrap();
        assert_eq!(
            arr.len(),
            1,
            "PII filtering must happen before truncate(limit)"
        );
        assert_eq!(arr[0]["id"], benign_id);
        assert_eq!(arr[0]["name"], "Safe Health & Gzip");
    }

    #[test]
    fn sec19_blocks_all_nine_bypass_shapes_while_preserving_non_pii_filter_pills() {
        assert_eq!(PII_FILTER_KEYS.len(), 18);

        let pii_bypasses = [
            r#"{"clauses":[{"col":"c_ip","op":"=","value":"198.51.100.7"}]}"#,
            r#"{"filters":[{"name":"client_ip","op":"=","value":"198.51.100.7"}]}"#,
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (c_ip) = '198.51.100.7'"}}"#,
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE '198.51.100.7' = client_ip"}}"#,
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE lower(x_forwarded_for) = '198.51.100.7'"}}"#,
            "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE \\\"c_ip\\\" IN\\n('198.51.100.7')\"}}",
            r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE [oip] = '198.51.100.7'"}}"#,
            r#"{"c-ip":"198.51.100.7"}"#,
            r#"{"session-id":"sess_secret_99"}"#,
        ];
        for payload in pii_bypasses {
            assert!(
                filters_reference_pii(payload),
                "expected PII bypass payload to be blocked: {payload}"
            );
        }

        let non_pii_views = [
            r#"{"url":{"mode":"include","values":["/api/ip:status"]}}"#,
            r#"{"clauses":[{"col":"path","op":"=","value":"/ip=1"}]}"#,
            r#"{"_view_extras":{"sql":"SELECT status FROM logs WHERE path = '/api/ip:status'"}}"#,
        ];
        for payload in non_pii_views {
            assert!(
                !filters_reference_pii(payload),
                "expected non-PII FilterPill/SQL view to be allowed: {payload}"
            );
        }

        let store = Memory::default();
        for (idx, payload) in pii_bypasses.iter().enumerate() {
            post(
                &store,
                json!({
                    "service_id": "svc1",
                    "name": format!("bypass-{idx}"),
                    "filters_json": *payload,
                }),
            );
        }
        for (idx, payload) in non_pii_views.iter().enumerate() {
            post(
                &store,
                json!({
                    "service_id": "svc1",
                    "name": format!("benign-{idx}"),
                    "filters_json": *payload,
                }),
            );
        }

        let nopii_rows = body(
            &call_as(
                &store,
                Role::AnalystNoPii,
                "GET",
                "/api/views/svc1",
                Value::Null,
            )
            .unwrap(),
        );
        let names: Vec<_> = nopii_rows
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["benign-0", "benign-1", "benign-2"]);
    }

    #[test]
    fn sec19_blocks_wave2_redteam_lexer_edge_cases_and_structural_bypasses() {
        let blocked_vectors = [
            // Wave-2 redteam-1 .. redteam-11
            (
                "redteam-1",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE /*x*/client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "redteam-2",
                "{\"_view_extras\":{\"sql\":\"-- Admin's investigation\\nSELECT * FROM logs WHERE client_ip = '198.51.100.7'\"}}",
            ),
            (
                "redteam-3",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE /* ' */ client_ip = '198.51.100.7' -- '"}}"#,
            ),
            (
                "redteam-4",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE list_contains([client_ip, oip], '198.51.100.7')"}}"#,
            ),
            (
                "redteam-5",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE [c_ip = '198.51.100.7'][1]"}}"#,
            ),
            (
                "redteam-6",
                r#"{"where":{"value":"client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "redteam-7",
                r#"{"_view_extras":{"sql":{"value":"SELECT * FROM logs WHERE sid = 'sess_secret_99'"}}}"#,
            ),
            (
                "redteam-8",
                r#"{"_view_extras":{"sql":"SELECT 1/sid FROM logs WHERE 1=1"}}"#,
            ),
            (
                "redteam-9",
                r#"{"_view_extras":{"sql":"SELECT status AS \"Admin's status\" FROM logs WHERE client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "redteam-10",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE path = $$Admin's path$$ AND client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "redteam-11",
                r#"{"filters":[{"name":"suspicious_traffic","value":"SELECT * FROM logs WHERE client_ip = '198.51.100.7'"}]}"#,
            ),
            // Lexer adversarial edge cases 21..34
            (
                "adv-21-escaped-e-str",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE msg = E'admin\\'s' AND client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "adv-22-win-path-std-str",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE path = 'C:\\dir\\' AND client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "adv-23-where-adj-str",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE'foo\\' AND client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "adv-24-nested-comment",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE /* a /* ' */ b */ client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "adv-25-tagged-dollar",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE path = $q$Admin's$q$ AND sid = 'sess_1'"}}"#,
            ),
            (
                "adv-26-unterm-comment",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE /* client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "adv-27-unterm-squote",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE path = 'unclosed"}}"#,
            ),
            (
                "adv-28a-unterm-dquote",
                r#"{"_view_extras":{"sql":"SELECT \"unclosed"}}"#,
            ),
            (
                "adv-28b-unterm-dollar",
                r#"{"_view_extras":{"sql":"SELECT $q$unclosed"}}"#,
            ),
            (
                "adv-28c-unterm-bracket",
                r#"{"_view_extras":{"sql":"SELECT [1, 2"}}"#,
            ),
            (
                "adv-28d-unmatched-close-bracket",
                r#"{"_view_extras":{"sql":"SELECT 1]"}}"#,
            ),
            (
                "adv-29a-qualified-ident",
                r#"{"_view_extras":{"sql":"SELECT t.client_ip FROM logs t"}}"#,
            ),
            (
                "adv-29b-qualified-quoted-ident",
                r#"{"_view_extras":{"sql":"SELECT t.\"x-forwarded-for\" FROM logs t"}}"#,
            ),
            (
                "adv-30-bracket-backtick",
                r#"{"_view_extras":{"sql":"SELECT [x-forwarded-for], `c-ip` FROM logs"}}"#,
            ),
            (
                "adv-31a-unicode-ident",
                r#"{"_view_extras":{"sql":"SELECT U&\"client\\005fip\" FROM logs"}}"#,
            ),
            (
                "adv-31b-unicode-plus6-ident",
                r#"{"_view_extras":{"sql":"SELECT u&\"client\\+00005fip\" FROM logs"}}"#,
            ),
            (
                "adv-31c-uescape-keyword",
                r#"{"_view_extras":{"sql":"SELECT U&\"client!005fip\" UESCAPE '!' FROM logs"}}"#,
            ),
            (
                "adv-32-cr-line-comment",
                "{\"_view_extras\":{\"sql\":\"-- Admin's\\rSELECT client_ip FROM logs\"}}",
            ),
            (
                "adv-33a-minus-nospace",
                r#"{"_view_extras":{"sql":"SELECT status-sid FROM logs"}}"#,
            ),
            (
                "adv-33b-ident-adj-line-comment",
                r#"{"_view_extras":{"sql":"SELECT client_ip--note"}}"#,
            ),
            (
                "adv-34a-extra-subscript-and-arrow",
                r#"{"_view_extras":{"sql":"SELECT _extra['client_ip'], _extra->>'x-forwarded-for' FROM logs"}}"#,
            ),
            (
                "adv-34b-extra-path-arrow",
                r#"{"_view_extras":{"sql":"SELECT _extra#>>'{meta,client_ip}' FROM logs"}}"#,
            ),
            (
                "adv-34c-json-extract-fn",
                r#"{"_view_extras":{"sql":"SELECT json_extract_string(coalesce(_extra, '{}'), '$.sid') FROM logs"}}"#,
            ),
            (
                "adv-34d-struct-extract-fn",
                r#"{"_view_extras":{"sql":"SELECT struct_extract(_extra, 'oip') FROM logs"}}"#,
            ),
            (
                "adv-34e-struct-literal-key",
                r#"{"_view_extras":{"sql":"SELECT {'client_ip': 1} FROM logs"}}"#,
            ),
            (
                "adv-34f-e-hex-subscript",
                r#"{"_view_extras":{"sql":"SELECT _extra[E'\\x63lient_ip'] FROM logs"}}"#,
            ),
            // Structural bypasses struct-s1 .. struct-s8 + bare value map + unsafe pill id
            (
                "struct-s1-op-smuggling",
                r#"{"filters":[{"column":"status","op":"= 200 OR client_ip = '198.51.100.7'","value":"200"}]}"#,
            ),
            (
                "struct-s1b-label-smuggling",
                r#"{"filters":[{"column":"status","label":"client_ip = '198.51.100.7'","value":"200"}]}"#,
            ),
            (
                "struct-s2-mixed-sql-sibling",
                r#"{"filters":[{"column":"status","sql":"SELECT 1","value":"client_ip = '198.51.100.7'"}]}"#,
            ),
            (
                "struct-s3-reserved-column-where",
                r#"{"filters":[{"column":"where","value":"client_ip = '198.51.100.7'"}]}"#,
            ),
            (
                "struct-s4-nested-object-in-values",
                r#"{"column":"status","values":[{"value":"SELECT * FROM logs WHERE client_ip = '198.51.100.7'"}]}"#,
            ),
            (
                "struct-s5-bracket-subscript-selector",
                r#"{"filters":[{"field":"custom_fields['oip']","value":"151.101.1.9"}]}"#,
            ),
            (
                "struct-s6-double-encoded-json-sql",
                r#"{"_view_extras":{"sql":"\"SELECT * FROM logs WHERE \\u0063lient_ip = '198.51.100.7'\""}}"#,
            ),
            (
                "struct-s7-dedup-reserved-where-1",
                r#"{"where_1":{"value":"client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "struct-s8-depth-17-fail-closed",
                r#"[[[[[[[[[[[[[[[[[{"status":"200"}]]]]]]]]]]]]]]]]]"#,
            ),
            (
                "reconcil-bare-value-map",
                r#"{"foo":{"value":"SELECT * FROM logs WHERE client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "reconcil-unsafe-pill-id-sql",
                r#"{"filters":[{"id":"SELECT client_ip FROM logs","column":"url","mode":"include","value":"/ok"}]}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-35-sql-paths",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE url = '/api/ip/sid/health' AND status = 200"}}"#,
            ),
            (
                "benign-36-sql-complex",
                "{\"_view_extras\":{\"sql\":\"-- Today's run\\nSELECT arr[1], status FROM logs WHERE path = 'C:\\\\ip\\\\sid\\\\' AND pop = $$user's$$\"}}",
            ),
            (
                "benign-37-filter-spec",
                r#"{"url":{"mode":"include","values":["/api/ip:status"]},"country_1":{"mode":"exclude","values":["CA"]}}"#,
            ),
            (
                "benign-38-clause-col",
                r#"{"clauses":[{"col":"path","op":"=","value":"/ip=1"}]}"#,
            ),
            (
                "benign-39-ui-pill",
                r#"{"filters":[{"id":"url-ip-0","column":"url","mode":"include","value":"Don't /ip"}],"_view_extras":{"edgeOnly":true,"compareMode":false,"compareStartTime":null,"compareEndTime":null,"relativeRange":"24h"}}"#,
            ),
            (
                "benign-legacy-pill-array",
                r#"[{"column":"status","value":"404","mode":"include"}]"#,
            ),
            (
                "benign-req-http-header-selector",
                r#"{"filters":[{"field_name":"req.http.accept","mode":"include","value":"text/html"}]}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_catalog_non_pii_fields_including_4xx_and_5xx_never_false_positive() {
        let catalog = crate::catalog::data();
        let mut seen = std::collections::BTreeSet::new();
        for section in ["fields", "api_fields"] {
            for entry in catalog[section].as_array().into_iter().flatten() {
                if let Some(id) = entry["id"].as_str() {
                    seen.insert(id.to_owned());
                }
            }
        }
        assert!(seen.contains("4xx"), "catalog must include 4xx");
        assert!(seen.contains("5xx"), "catalog must include 5xx");

        for col in &seen {
            let pill = json!({
                "filters": [{
                    "id": "url-ip-0",
                    "column": col,
                    "mode": "include",
                    "value": "Don't /api/ip:status"
                }]
            })
            .to_string();
            let map_spec = json!({
                col: {
                    "mode": "include",
                    "values": ["/api/ip:status"]
                }
            })
            .to_string();

            if PII_FILTER_KEYS.contains(&col.as_str()) {
                assert!(
                    filters_reference_pii(&pill),
                    "PII catalog column {col} must be blocked in FilterPill"
                );
                assert!(
                    filters_reference_pii(&map_spec),
                    "PII catalog column {col} must be blocked in FiltersPayload"
                );
            } else {
                assert!(
                    !filters_reference_pii(&pill),
                    "non-PII catalog column {col} must be allowed in FilterPill: {pill}"
                );
                assert!(
                    !filters_reference_pii(&map_spec),
                    "non-PII catalog column {col} must be allowed in FiltersPayload: {map_spec}"
                );
            }
        }

        for pii_key in PII_FILTER_KEYS {
            let pill = json!({
                "filters": [{
                    "id": "url-ip-0",
                    "column": *pii_key,
                    "mode": "include",
                    "value": "sentinel-value"
                }]
            })
            .to_string();
            let map_spec = json!({
                *pii_key: {
                    "mode": "include",
                    "values": ["sentinel-value"]
                }
            })
            .to_string();
            assert!(
                filters_reference_pii(&pill),
                "PII key {pii_key} must be blocked in FilterPill"
            );
            assert!(
                filters_reference_pii(&map_spec),
                "PII key {pii_key} must be blocked in FiltersPayload"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave3_dupkeys_sticky_slots_columns_and_json_paths() {
        let blocked_vectors = [
            // Challenge 1: duplicate & case-colliding JSON object keys (top-level & nested JSON strings)
            (
                "rt3-dupkey-view-extras",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE client_ip = '198.51.100.7'"},"_view_extras":{"edgeOnly":true}}"#,
            ),
            (
                "rt3-dupkey-pill-column",
                r#"{"filters":[{"column":"client_ip","mode":"include","value":"198.51.100.7","column":"url"}]}"#,
            ),
            (
                "rt3-dupkey-case-colliding-column",
                r#"{"filters":[{"column":"client_ip","mode":"include","value":"198.51.100.7","Column":"url"}]}"#,
            ),
            (
                "rt3-dupkey-nested-json-str",
                r#"{"filters":"[{\"column\":\"client_ip\",\"mode\":\"include\",\"value\":\"198.51.100.7\",\"column\":\"url\"}]"}"#,
            ),
            // Challenge 2: sticky `in_query_or_extras` & non-scalar selector keys
            (
                "rt3-sticky-escape-name-map",
                r#"{"_view_extras":{"name":{"url":{"mode":"include","values":["SELECT * FROM logs WHERE client_ip = '198.51.100.7'"]}}}}"#,
            ),
            (
                "rt3-sticky-escape-key-pill",
                r#"{"_view_extras":{"key":{"column":"url","mode":"include","value":"SELECT * FROM logs WHERE sid = 'sess_secret_99'"}}}"#,
            ),
            (
                "rt3-sticky-escape-where-lhs",
                r#"{"where":{"lhs":{"status":{"mode":"include","values":["client_ip = '198.51.100.7'"]}}}}"#,
            ),
            (
                "rt3-sticky-escape-left-json-str",
                r#"{"_view_extras":{"left":"[{\"column\":\"status\",\"mode\":\"include\",\"value\":\"SELECT * FROM logs WHERE client_ip = '198.51.100.7'\"}]"}}"#,
            ),
            (
                "rt3-selector-nonscalar-root",
                r#"{"lhs":{"url":{"mode":"include","values":["SELECT * FROM logs WHERE client_ip = '198.51.100.7'"]}}}"#,
            ),
            // Challenge 3: DuckDB `COLUMNS(...)` exact, regex, wildcard, lambda, and quoted `"COLUMNS"`
            (
                "rt3-duckdb-columns-exact",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE COLUMNS('client_ip') = '198.51.100.7'"}}"#,
            ),
            (
                "rt3-duckdb-columns-regex",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('^(c_ip|oip|sid)$') FROM logs"}}"#,
            ),
            (
                "rt3-duckdb-columns-wildcard",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('.*') FROM logs"}}"#,
            ),
            (
                "rt3-duckdb-columns-lambda",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS(c -> c = 'client_ip') FROM logs"}}"#,
            ),
            (
                "rt3-duckdb-columns-quoted-fn",
                r#"{"_view_extras":{"sql":"SELECT \"COLUMNS\"('^(status|c-ip)$') FROM logs"}}"#,
            ),
            (
                "rt3-duckdb-columns-group-split",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('^cl(ie)nt_ip$') FROM logs"}}"#,
            ),
            // Challenge 4 + Reviewer Minor 1: RFC 6901 JSON Pointer, `VARCHAR[]` path list, and parenthesized keys
            (
                "rt3-duckdb-json-pointer-arrow",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>'/client_ip' = '198.51.100.7'"}}"#,
            ),
            (
                "rt3-duckdb-json-pointer-fn",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, '/headers/x-forwarded-for') = '198.51.100.7'"}}"#,
            ),
            (
                "rt3-duckdb-json-pointer-rfc6901-tilde1",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>'/headers~1client_ip' = '198.51.100.7'"}}"#,
            ),
            (
                "rt3-duckdb-json-arrow-path-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>['headers', 'client_ip'] = '198.51.100.7'"}}"#,
            ),
            (
                "rt3-duckdb-json-single-arrow-path-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->['headers', 'sid'] IS NOT NULL"}}"#,
            ),
            (
                "rt3-duckdb-paren-subscript-key",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra[('client_ip')] = '198.51.100.7'"}}"#,
            ),
            (
                "rt3-duckdb-paren-arrow-key",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>('sid') = 'sess_secret_99'"}}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-sql-columns-nonpii",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('^(status|url|pop)$') FROM logs"}}"#,
            ),
            (
                "benign-sql-json-pointer-nonpii",
                r#"{"_view_extras":{"sql":"SELECT _extra->>'/request/url' FROM logs WHERE json_extract_string(_extra, '/meta/pop') = '/ip/sid'"}}"#,
            ),
            (
                "benign-sql-in-list-literals",
                r#"{"_view_extras":{"sql":"SELECT status FROM logs WHERE status IN ('ip', 'sid')"}}"#,
            ),
            (
                "benign-pill-bracket-prefix-literal",
                r#"{"filters":[{"column":"url","mode":"include","value":"[ERROR] /api/ip/sid"}]}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_columns_lambdas_and_dynamic_key_concat_while_preserving_benign_views() {
        let blocked_vectors = [
            (
                "bug-cloud-1-columns-negated-lambda",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE COLUMNS(c -> c != 'url') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-columns-starts-with-lambda",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS(c -> starts_with(c, 'clien')) FROM logs"}}"#,
            ),
            (
                "bug-cloud-1-columns-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE COLUMNS('clien' || 't_ip') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-subscript-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra['clien' || 't_ip'] = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-json-extract-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, 'clien' || 't_ip') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-json-extract-chr-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, chr(99) || 'id') = 'sess1'"}}"#,
            ),
            (
                "bug-cloud-1-struct-key-concat",
                r#"{"_view_extras":{"sql":"SELECT {'clien' || 't_ip': 1}"}}"#,
            ),
            (
                "bug-cloud-1-columns-empty",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS() FROM logs"}}"#,
            ),
            (
                "bug-cloud-1-columns-two-literals",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('url', 'pop') FROM logs"}}"#,
            ),
            (
                "bug-cloud-1-json-arrow-paren-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>('clien' || 't_ip') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-json-arrow-fn-rhs",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>concat('clien', 't_ip') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-json-arrow-list-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->['headers', 'clien' || 't_ip'] IS NOT NULL"}}"#,
            ),
            (
                "bug-cloud-1-subscript-fn-key",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra[concat('clien', 't_ip')] = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-struct-fn-key",
                r#"{"_view_extras":{"sql":"SELECT {format('{}', 'cid'): 1}"}}"#,
            ),
            (
                "bug-cloud-1-r2-unobf-multiline-arrow",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE _extra ->> '$.request.headers.'\\n  'x-forwarded-for' = '198.51.100.7'\"}}",
            ),
            (
                "bug-cloud-1-r2-newline-arrow",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE _extra->>'clien'\\n't_ip' = '198.51.100.7'\"}}",
            ),
            (
                "bug-cloud-1-r2-comment-newline-arrow",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE _extra->>'clien' -- split\\n't_ip' = '198.51.100.7'\"}}",
            ),
            (
                "bug-cloud-1-r2-newline-extract",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE json_extract_string(_extra, 'clien'\\n't_ip') = '198.51.100.7'\"}}",
            ),
            (
                "bug-cloud-1-r2-newline-subscript",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE _extra['clien'\\n't_ip'] = '\\\"198.51.100.7\\\"'\"}}",
            ),
            (
                "bug-cloud-1-r2-newline-struct",
                "{\"_view_extras\":{\"sql\":\"SELECT {'clien'\\n't_ip': '198.51.100.7'} FROM logs\"}}",
            ),
            (
                "bug-cloud-1-r2-cte-subscript-ident",
                "{\"_view_extras\":{\"sql\":\"WITH k(c) AS (VALUES (chr(99)||chr(108)||chr(105)||chr(101)||chr(110)||chr(116)||chr(95)||chr(105)||chr(112))) SELECT * FROM logs, k WHERE _extra[c] = '\\\"198.51.100.7\\\"'\"}}",
            ),
            (
                "bug-cloud-1-r3-slice-subscript-json",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra::JSON)['xclient_ipx'[2:10]] = '\"198.51.100.7\"'"}}"#,
            ),
            (
                "bug-cloud-1-r3-fn-slice-subscript",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json(_extra)['xipx'[2:3]] = '\"198.51.100.7\"'"}}"#,
            ),
            (
                "bug-cloud-1-r3-chained-slice-subscript",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra::JSON)['request']['headers']['xx-forwarded-forx'[2:16]] = '\"198.51.100.7\"'"}}"#,
            ),
            (
                "bug-cloud-1-r3-arrow-slice",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>'xclient_ipx'[2:10] = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r3-extract-slice",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, 'xclient_ipx'[2:10]) = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r3-dollar-slice",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra[$$xclient_ipx$$[2:10]] = '\"198.51.100.7\"'"}}"#,
            ),
            (
                "bug-cloud-1-r3-list-nested-subscript",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra::JSON)[['a', 'xipx'[2:3]][2]] = '\"198.51.100.7\"'"}}"#,
            ),
            (
                "bug-cloud-1-r3-operator-subscript",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra::JSON)[1+1] IS NOT NULL"}}"#,
            ),
            (
                "bug-cloud-1-r3-wildcard-arrow",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->>'$.*')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r3-filter-jsonpath",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra->>'$[?(@.a==1)]' IS NOT NULL"}}"#,
            ),
            (
                "bug-cloud-1-r3-json-contains",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{\"client_ip\":\"198.51.100.7\"}')"}}"#,
            ),
            (
                "bug-cloud-1-r3-json-each",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs, json_each(_extra) WHERE value = '\"198.51.100.7\"'"}}"#,
            ),
            (
                "bug-cloud-1-r3-json-tree",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs, json_tree(_extra) WHERE value = '\"198.51.100.7\"'"}}"#,
            ),
            (
                "bug-cloud-1-r3-json-transform",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_transform(_extra, '{\"client_ip\":\"VARCHAR\"}') IS NOT NULL"}}"#,
            ),
            (
                "bug-cloud-1-r3-from-json",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '{\"client_ip\":\"VARCHAR\"}') IS NOT NULL"}}"#,
            ),
            (
                "bug-cloud-1-r3-map-extract-value",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE map_extract_value(kv, 'client_ip') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r3-struct-extract-at",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE struct_extract_at(logs, 4) = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r3-row-to-json",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE row_to_json(logs)::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r3-extra-raw-like",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE _extra LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r3-extra-cast-varchar",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE CAST(_extra AS VARCHAR) LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r3-extra-contains",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE contains(_extra, '198.51.100.7')"}}"#,
            ),
            (
                "bug-cloud-1-r3-select-raw-extra",
                r#"{"_view_extras":{"sql":"SELECT _extra FROM logs"}}"#,
            ),
            // BUG-CLOUD-1-R4: Unicode spaces, root/escaped JSON paths, _extra paren wrappers, dot-chain extractors, blind/dynamic fns, #N/PIVOT/UNPIVOT, whole-row structs, and JSON _extra keys
            (
                "bug-cloud-1-r4-zwsp-quote-cont",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE _extra->>'clien'\\u200b\\n't_ip' = '198.51.100.7'\"}}",
            ),
            (
                "bug-cloud-1-r4-wj-quote-cont",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE _extra->>'clien'\\u2060\\n't_ip' = '198.51.100.7'\"}}",
            ),
            (
                "bug-cloud-1-r4-bom-blind-fn",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs WHERE row_to_json\\ufeff(logs)::VARCHAR LIKE '%198.51.100.7%'\"}}",
            ),
            (
                "bug-cloud-1-r4-root-dollar-arrow",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->>'$') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-root-sarrow",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->'$')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-root-dollar-fn",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, '$') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-root-empty-arrow",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->>'') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-root-slash-ptr",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->>'/') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-pos-jsonpath",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->>'$[0]') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-contains-u-esc",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{\"\\u0063lient_ip\":\"198.51.100.7\"}')"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-transform-u-esc",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_transform(_extra, '{\"\\u0063lient_ip\":\"VARCHAR\"}') IS NOT NULL"}}"#,
            ),
            (
                "bug-cloud-1-r4-from-json-u-esc",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '{\"\\u0063lient_ip\":\"VARCHAR\"}') IS NOT NULL"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-object-wrap",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_object('safe', _extra)->>'safe' LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-struct-pack-wrap",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE struct_pack(safe := _extra)['safe'] LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-array-int-arrow",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (json_array(_extra)->>0) LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-position-in-extra-wrap",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE struct_pack(ok := POSITION('198.51.100.7' IN _extra))['ok'] > 0"}}"#,
            ),
            (
                "bug-cloud-1-r4-case-in-extract-arg0",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(CASE WHEN contains(_extra, '198.51.100.7') THEN '{\"safe\":\"1\"}' ELSE '{}' END, '$.safe') = '1'"}}"#,
            ),
            (
                "bug-cloud-1-r4-if-in-extract-arg0",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(if(contains(_extra, '198.51.100.7'), '{\"a\":\"1\"}', '{}'), '$.a') = '1'"}}"#,
            ),
            (
                "bug-cloud-1-r4-dot-map-extract-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE kv.map_extract_value('clien' || 't_ip') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r4-dot-json-extract-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->'request'->'headers').json_extract_string('x-forwar' || 'ded-for') = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r4-dot-cte-ident",
                r#"{"_view_extras":{"sql":"WITH k(c) AS (VALUES ('ip')) SELECT * FROM logs, k WHERE kv.map_extract_value(c) = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r4-query-dynamic-sql",
                r#"{"_view_extras":{"sql":"SELECT * FROM query('SELECT * FROM logs WHERE ' || chr(99) || 'lient_ip = ''198.51.100.7''')"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-exec-serialized",
                r#"{"_view_extras":{"sql":"SELECT * FROM json_execute_serialized_sql('{\"statements\":[]}')"}}"#,
            ),
            (
                "bug-cloud-1-r4-map-contains-value",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE map_contains_value(kv, '198.51.100.7')"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-pretty-extra",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_pretty(_extra) LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-positional-hash-col",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE #2 = '198.51.100.7'"}}"#,
            ),
            (
                "bug-cloud-1-r4-unpivot-logs",
                r#"{"_view_extras":{"sql":"UNPIVOT logs ON (* EXCLUDE (timestamp)) INTO NAME col VALUE val"}}"#,
            ),
            (
                "bug-cloud-1-r4-pivot-logs",
                r#"{"_view_extras":{"sql":"PIVOT logs ON status USING first(url)"}}"#,
            ),
            (
                "bug-cloud-1-r4-pivot-longer",
                r#"{"_view_extras":{"sql":"WITH p AS (PIVOT_LONGER logs ON url) SELECT * FROM p"}}"#,
            ),
            (
                "bug-cloud-1-r4-logs-cast-varchar",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-alias-cast-varchar",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs AS t WHERE CAST(t AS VARCHAR) LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-format-logs-struct",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE format('{}', logs) LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-logs-struct",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json(logs)::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-list-logs-struct",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE [logs]::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-select-logs-struct",
                r#"{"_view_extras":{"sql":"SELECT logs FROM logs"}}"#,
            ),
            (
                "bug-cloud-1-r4-cte-row-struct",
                r#"{"_view_extras":{"sql":"WITH x AS (SELECT * FROM logs) SELECT * FROM x WHERE x::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-subquery-alias-struct",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT * FROM logs) sub WHERE sub::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "bug-cloud-1-r4-json-map-extra",
                r#"{"_extra":{"mode":"include","values":["198.51.100.7"]}}"#,
            ),
            (
                "bug-cloud-1-r4-json-kv-extra",
                r#"{"_extra":"198.51.100.7"}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-columns-regex-nonpii",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('^(status|url|pop)$') FROM logs"}}"#,
            ),
            (
                "benign-array-index",
                r#"{"_view_extras":{"sql":"SELECT arr[1] FROM logs"}}"#,
            ),
            (
                "benign-array-zero-index",
                r#"{"_view_extras":{"sql":"SELECT arr[0] FROM logs"}}"#,
            ),
            (
                "benign-extra-subscript-safe",
                r#"{"_view_extras":{"sql":"SELECT _extra['safe'] FROM logs"}}"#,
            ),
            (
                "benign-chained-subscripts-safe",
                r#"{"_view_extras":{"sql":"SELECT (_extra::JSON)['request']['headers']['safe_header'] FROM logs"}}"#,
            ),
            (
                "benign-json-fn-subscript-safe",
                r#"{"_view_extras":{"sql":"SELECT json(_extra)['safe_header'] FROM logs"}}"#,
            ),
            (
                "benign-json-contains-safe",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{\"safe_header\":\"ok\"}')"}}"#,
            ),
            (
                "benign-json-extract-eq-pii-lit",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, '/meta/pop') = '/ip/sid'"}}"#,
            ),
            (
                "benign-like-wildcard-nonkey",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE url LIKE '%*%'"}}"#,
            ),
            (
                "benign-struct-safe-key",
                r#"{"_view_extras":{"sql":"SELECT {'safe': 1}"}}"#,
            ),
            (
                "benign-multiline-safe-json-path",
                "{\"_view_extras\":{\"sql\":\"SELECT _extra ->> '$.request.'\\n  'url' FROM logs\"}}",
            ),
            (
                "benign-r4-alias-qualified-cols",
                r#"{"_view_extras":{"sql":"SELECT t.status, logs.url FROM logs AS t WHERE t.status = 200"}}"#,
            ),
            (
                "benign-r4-cte-safe-cols",
                r#"{"_view_extras":{"sql":"WITH safe_cte AS (SELECT status, url FROM logs) SELECT safe_cte.status, url FROM safe_cte WHERE status = 200"}}"#,
            ),
            (
                "benign-r4-dot-chain-safe",
                r#"{"_view_extras":{"sql":"SELECT (_extra->'headers').json_extract_string('safe_header') FROM logs"}}"#,
            ),
            (
                "benign-r4-cast-extra-json-safe",
                r#"{"_view_extras":{"sql":"SELECT CAST(_extra AS JSON)['safe_header'] FROM logs"}}"#,
            ),
            (
                "benign-r4-bare-logs-pill",
                r#"{"filters":[{"column":"url","mode":"include","value":"logs"}]}"#,
            ),
            (
                "benign-r4-trim-from-url",
                r#"{"_view_extras":{"sql":"SELECT TRIM(FROM url) FROM logs WHERE url = '/ok'"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave5_r5_json_object_specs_nonascii_tvf_summarize_and_positional_wildcards() {
        let blocked_vectors = [
            // 1. 2-arg JSON/struct/map functions & file readers
            (
                "r5-json-merge-patch",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_merge_patch(_extra, '{\"safe\":\"ok\"}')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-dot-json-merge-patch",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (_extra->'request').json_merge_patch('{\"safe\":\"1\"}')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-struct-contains",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE struct_contains((status, url), '198.51.100.7')"}}"#,
            ),
            (
                "r5-map-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE map_concat(kv, map {'a': 'b'})::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-read-parquet-fn",
                r#"{"_view_extras":{"sql":"SELECT * FROM read_parquet('x.parquet')"}}"#,
            ),
            (
                "r5-parquet-scan-fn",
                r#"{"_view_extras":{"sql":"SELECT * FROM parquet_scan('x.parquet')"}}"#,
            ),
            (
                "r5-json-contains-scalar-sid",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '\"sess_secret_99\"')"}}"#,
            ),
            (
                "r5-json-contains-scalar-ipv6",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '\"2001:db8::dead:beef\"')"}}"#,
            ),
            (
                "r5-json-contains-array",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '[\"198.51.100.7\"]')"}}"#,
            ),
            (
                "r5-json-contains-empty-obj",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{}')"}}"#,
            ),
            (
                "r5-json-contains-nested-empty-obj",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{\"request\":{}}')"}}"#,
            ),
            (
                "r5-json-contains-dup-key",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{\"client_ip\":\"198.51.100.7\",\"client_ip\":\"0.0.0.0\"}')"}}"#,
            ),
            (
                "r5-from-json-map-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '\"MAP(VARCHAR, VARCHAR)\"')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-from-json-strict-map-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json_strict(_extra::JSON, '\"MAP(VARCHAR, VARCHAR)\"')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-from-json-json-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra, '\"JSON\"')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-from-json-strict-json-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json_strict(_extra, '\"JSON\"')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-json-transform-json-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_transform(_extra, '\"JSON\"')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-json-transform-varchar-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_transform(_extra, '\"VARCHAR\"') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-json-transform-strict-varchar",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_transform_strict(_extra, '\"VARCHAR\"') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-from-json-nested-json-leaf",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '{\"request\":\"JSON\"}')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-from-json-nested-map-leaf",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '{\"request\":\"MAP(VARCHAR, VARCHAR)\"}')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-from-json-array-type-leaf",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '{\"request\":\"VARCHAR[]\"}')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-extract-list-key-arg",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, ['safe']) IS NOT NULL"}}"#,
            ),
            (
                "r5-extract-bare-digit-key-arg",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_extract_string(_extra, 0) IS NOT NULL"}}"#,
            ),
            // 2. Non-ASCII UTF-8 unquoted identifiers & dollar-in-ident continuations
            (
                "r5-nonascii-alias-cast",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM logs AS ä WHERE ä::VARCHAR LIKE '%198.51.100.7%'\"}}",
            ),
            (
                "r5-nonascii-cte-struct",
                "{\"_view_extras\":{\"sql\":\"WITH ä AS (SELECT * FROM logs) SELECT * FROM ä WHERE ä::VARCHAR LIKE '%198.51.100.7%'\"}}",
            ),
            (
                "r5-nonascii-subscript-concat",
                "{\"_view_extras\":{\"sql\":\"SELECT * FROM (SELECT (_extra->'request'->'headers') AS ä FROM logs) WHERE ä['x-forwar' || 'ded-for'] = '\\\"198.51.100.7\\\"'\"}}",
            ),
            // 3. Schema/catalog-qualified relation structs, UFCS dot-chained rel.fn(...), and TVF subquery walking
            (
                "r5-main-dot-logs-struct",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE main.logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-quoted-main-dot-logs-struct",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE \"main\".\"logs\"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-memory-main-logs-struct",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE memory.main.logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-dot-t-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs AS t WHERE t.concat('') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-dot-logs-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE logs.concat('') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-dot-main-logs-concat",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE main.logs.concat('') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r5-tvf-range-subquery",
                r#"{"_view_extras":{"sql":"SELECT * FROM range((SELECT count(*) FROM logs AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'))"}}"#,
            ),
            (
                "r5-tvf-correlated-main-logs",
                r#"{"_view_extras":{"sql":"SELECT logs.status, logs.url FROM logs WHERE EXISTS (SELECT 1 FROM range((SELECT count(*) FROM main.logs AS t WHERE t.timestamp = logs.timestamp AND t::VARCHAR LIKE '%198.51.100.7%')))"}}"#,
            ),
            // 4. DuckDB SUMMARIZE keyword
            (
                "r5-summarize-logs",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SUMMARIZE logs) AS s WHERE s.max = '198.51.100.7'"}}"#,
            ),
            (
                "r5-summarize-select-logs",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SUMMARIZE SELECT * FROM logs) AS s WHERE s.max = '198.51.100.7'"}}"#,
            ),
            // 5. Positional column-alias lists, wildcard set-ops, and wildcard scalar/IN subqueries
            (
                "r5-from-alias-col-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs AS t(c1, c2) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r5-from-implicit-alias-col-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT * FROM logs) t(c1, c2, c3, c4) WHERE t.c4 = 'sess_secret_99'"}}"#,
            ),
            (
                "r5-cte-col-list-star",
                r#"{"_view_extras":{"sql":"WITH c(c1, c2, c3, c4, c5, c6, c7) AS (SELECT * FROM logs) SELECT * FROM c WHERE c.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r5-cte-col-list-table-logs",
                r#"{"_view_extras":{"sql":"WITH c(c1, c2, c3, c4, c5, c6, c7) AS (TABLE logs) SELECT * FROM c WHERE c.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r5-cte-col-list-from-first",
                r#"{"_view_extras":{"sql":"WITH c(c1, c2, c3, c4, c5, c6, c7) AS (FROM logs) SELECT * FROM c WHERE c.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r5-union-all-star-rename",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT now()::TIMESTAMPTZ AS a, '' AS b, '' AS c, '' AS d, 0 AS e, '' AS f, '' AS g WHERE 1=0 UNION ALL SELECT * FROM logs) sub WHERE sub.b = '198.51.100.7'"}}"#,
            ),
            (
                "r5-union-all-table-logs-rename",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT now()::TIMESTAMPTZ AS a, '' AS b, '' AS c, '' AS d, 0 AS e, '' AS f, '' AS g WHERE 1=0 UNION ALL TABLE logs) sub WHERE sub.b = '198.51.100.7'"}}"#,
            ),
            (
                "r5-union-all-from-first-rename",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT now()::TIMESTAMPTZ AS a, '' AS b, '' AS c, '' AS d, 0 AS e, '' AS f, '' AS g WHERE 1=0 UNION ALL FROM logs) sub WHERE sub.b = '198.51.100.7'"}}"#,
            ),
            (
                "r5-intersect-star-values",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs INTERSECT VALUES ('2026-01-01'::TIMESTAMPTZ, '198.51.100.7', '', '', 200, '/', '{}')"}}"#,
            ),
            (
                "r5-row-in-select-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (logs.timestamp, '198.51.100.7', '', '', logs.status, logs.url, '') IN (SELECT * FROM logs)"}}"#,
            ),
            (
                "r5-scalar-subquery-star-cast",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (SELECT * FROM logs AS t WHERE t.timestamp = logs.timestamp LIMIT 1)::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-r5-json-contains-obj",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{\"safe_header\":\"ok\"}')"}}"#,
            ),
            (
                "benign-r5-json-contains-nested-obj",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_contains(_extra, '{\"request\":{\"safe_header\":\"ok\"}}')"}}"#,
            ),
            (
                "benign-r5-from-json-safe-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '{\"safe_header\":\"VARCHAR\"}') IS NOT NULL"}}"#,
            ),
            (
                "benign-r5-json-transform-safe-schema",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_transform(_extra, '{\"safe_header\":\"VARCHAR(64)\",\"nested\":{\"ok\":\"BOOLEAN\"}}') IS NOT NULL"}}"#,
            ),
            (
                "benign-r5-main-logs-qualified-cols",
                r#"{"_view_extras":{"sql":"SELECT t.status, logs.url, main.logs.url FROM main.logs AS t WHERE t.status = 200"}}"#,
            ),
            (
                "benign-r5-dot-chain-extra-safe",
                r#"{"_view_extras":{"sql":"SELECT (_extra->'request'->'headers').json_extract_string('safe_header') FROM logs"}}"#,
            ),
            (
                "benign-r5-count-star-and-mult",
                r#"{"_view_extras":{"sql":"SELECT count(*), status * 2 FROM logs WHERE status IN (SELECT status FROM logs)"}}"#,
            ),
            (
                "benign-r5-derived-table-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT status, url FROM logs) sub WHERE sub.status = 200"}}"#,
            ),
            (
                "benign-r5-cte-select-star",
                r#"{"_view_extras":{"sql":"WITH c AS (SELECT * FROM logs) SELECT c.status FROM c WHERE c.status = 200"}}"#,
            ),
            (
                "benign-r5-cte-explicit-union-outer-star",
                r#"{"_view_extras":{"sql":"WITH p AS (SELECT url FROM logs UNION ALL SELECT url FROM logs) SELECT * FROM p"}}"#,
            ),
            (
                "benign-r5-exists-select-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE EXISTS (SELECT * FROM logs WHERE status = 500)"}}"#,
            ),
            (
                "benign-r5-top-level-select-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r5-tvf-range-count-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM range((SELECT count(*) FROM logs WHERE status = 200))"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave6_r6_columns_only_stop_kw_hyphen_nul_paren_star_and_bom() {
        let blocked_vectors = [
            // 1. COLUMNS(...) unanchored RE2 substrings, bare anchors, empty branches, unparenthesized '|', and '^_extra$'
            (
                "r6-columns-anchored-extra",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE COLUMNS('^_extra$') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-columns-prefix-oi",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE COLUMNS('^oi') = '203.0.113.9'"}}"#,
            ),
            (
                "r6-columns-substr-xtra",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE COLUMNS('xtra') LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-columns-substr-ookie",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE COLUMNS('ookie') = 'sess_secret_99'"}}"#,
            ),
            (
                "r6-columns-substr-dge-si",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE COLUMNS('dge_si') = 'esid_secret_77'"}}"#,
            ),
            (
                "r6-columns-prefix-ci",
                r#"{"_view_extras":{"sql":"SELECT * FROM client_vitals WHERE concat_ws('|', *COLUMNS('^ci')) LIKE '%cid_secret_99%'"}}"#,
            ),
            (
                "r6-columns-unpack-caret",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE concat_ws('|', *COLUMNS('^')) LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-columns-unpack-empty-alt",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE concat_ws('|', *COLUMNS('status|')) LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-columns-unparen-pipe-anchor-split",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('^status|oip$') FROM logs"}}"#,
            ),
            // 2. FROM ONLY logs & unreserved stop-keywords sample / set / values
            (
                "r6-from-only-logs-alias-cast",
                r#"{"_view_extras":{"sql":"SELECT * FROM ONLY logs AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-from-only-logs-col-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM ONLY logs AS t(c1, c2) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r6-from-only-implicit-alias-cast",
                r#"{"_view_extras":{"sql":"SELECT * FROM ONLY logs t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-from-only-implicit-col-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM ONLY logs t(c1, c2) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r6-stop-kw-sample-cast",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs sample WHERE sample::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-stop-kw-set-cast",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs set WHERE set::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-stop-kw-values-cast",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs values WHERE values::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-stop-kw-sample-col-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs sample(c1, c2) WHERE sample.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r6-stop-kw-set-col-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs set(c1, c2) WHERE set.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r6-stop-kw-values-col-list",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs values(c1, c2) WHERE values.c2 = '198.51.100.7'"}}"#,
            ),
            // 3. Unquoted hyphen-merge token smuggling (status-_extra.contains(...), status-map_contains_value(...))
            (
                "r6-hyphen-extra-contains",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE status-_extra.contains('198.51.100.7')::INT = 199"}}"#,
            ),
            (
                "r6-hyphen-extra-instr",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE status-_extra.instr('198.51.100.7') < 200"}}"#,
            ),
            (
                "r6-hyphen-extra-regexp",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE status-_extra.regexp_matches('198\\.51\\.100\\.7')::INT = 199"}}"#,
            ),
            (
                "r6-hyphen-blind-fn",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE status-map_contains_value(kv, '198.51.100.7')::INT = 199"}}"#,
            ),
            // 4. Fail-closed NUL-byte ('\0' / \u0000) rejection
            (
                "r6-from-json-u0000-suffix",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json(_extra::JSON, '{\"client_ip\\u0000safe\":\"VARCHAR\"}').client_ip = '198.51.100.7'"}}"#,
            ),
            (
                "r6-from-json-strict-sid-u0000",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE from_json_strict(_extra::JSON, '{\"sid\\u0000safe\":\"VARCHAR\"}').sid = 'sess_secret_99'"}}"#,
            ),
            (
                "r6-json-transform-nested-u0000",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE json_transform(_extra, '{\"request\":{\"headers\":{\"x-forwarded-for\\u0000safe\":\"VARCHAR\"}}}')::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r6-outer-json-u0000",
                r#"{"url":{"mode":"include","values":["safe\u0000val"]}}"#,
            ),
            // 5. Parenthesized wildcard SELECT (*) / SELECT ((*)) in set-ops & subqueries
            (
                "r6-paren-star-union-rename",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT now()::TIMESTAMPTZ AS a, '' AS b, '' AS c, '' AS d, 0 AS e, '' AS f, '' AS g WHERE 1=0 UNION ALL SELECT (*) FROM logs) sub WHERE sub.b = '198.51.100.7'"}}"#,
            ),
            (
                "r6-double-paren-star-union",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT now()::TIMESTAMPTZ AS a, '' AS b, '' AS c, '' AS d, 0 AS e, '' AS f, '' AS g WHERE 1=0 UNION ALL SELECT ((*)) FROM logs) sub WHERE sub.b = '198.51.100.7'"}}"#,
            ),
            (
                "r6-paren-star-intersect-values",
                r#"{"_view_extras":{"sql":"SELECT (*) FROM logs INTERSECT VALUES ('2026-01-01'::TIMESTAMPTZ, '198.51.100.7', '', '', 200, '/', '{}')"}}"#,
            ),
            (
                "r6-paren-star-row-in-subquery",
                r#"{"_view_extras":{"sql":"SELECT * FROM logs WHERE (logs.timestamp, '198.51.100.7', '', '', logs.status, logs.url, '') IN (SELECT (*) FROM logs)"}}"#,
            ),
            // 6. BOM / ZWSP / WJ before nested JSON string
            (
                "r6-bom-nested-json-u0063",
                r#"{"custom_blob":"\ufeff{\"\\u0063lient_ip\":\"198.51.100.7\"}"}"#,
            ),
            (
                "r6-zwsp-nested-json-u0063",
                r#"{"custom_blob":"\u200b{\"\\u0063lient_ip\":\"198.51.100.7\"}"}"#,
            ),
            (
                "r6-wj-nested-json-u0063",
                r#"{"custom_blob":"\u2060{\"\\u0063lient_ip\":\"198.51.100.7\"}"}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-r6-columns-single-anchored",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('^status$') FROM logs"}}"#,
            ),
            (
                "benign-r6-columns-multi-anchored",
                r#"{"_view_extras":{"sql":"SELECT COLUMNS('^(status|url|pop)$') FROM logs"}}"#,
            ),
            (
                "benign-r6-subtraction-and-paren-mult",
                r#"{"_view_extras":{"sql":"SELECT status - 200, status-200, (status + 1) * 2 FROM logs WHERE (SELECT count(*) FROM logs) > 0"}}"#,
            ),
            (
                "benign-r6-tablesample-and-using-sample",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs TABLESAMPLE 10 PERCENT WHERE status = 200"}}"#,
            ),
            (
                "benign-r6-using-sample",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs USING SAMPLE 10 ROWS"}}"#,
            ),
            (
                "benign-r6-from-only-logs-safe-cols",
                r#"{"_view_extras":{"sql":"SELECT t.status, t.url FROM ONLY logs AS t WHERE t.status = 200"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave7_r7_paren_join_inheritance_star_alias_pattern_and_catalog_dot() {
        let blocked_vectors = [
            // 1. (RS-A) Parenthesized joined_table outer '('
            (
                "r7-paren-join-as-t-cast",
                r#"{"_view_extras":{"sql":"SELECT t.status, t.url FROM ((SELECT * FROM logs) AS t CROSS JOIN (SELECT 1 AS x) AS u) WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-paren-join-as-t-col-list",
                r#"{"_view_extras":{"sql":"SELECT t.c5, t.c6 FROM ((SELECT * FROM logs) AS t(c1, c2, c3, c4, c5, c6, c7) CROSS JOIN (SELECT 1 AS x) AS u) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-paren-join-bare-t-cast",
                r#"{"_view_extras":{"sql":"SELECT t.status, t.url FROM ((SELECT * FROM logs) t CROSS JOIN (SELECT 1 AS x) u) WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-paren-join-bare-t-col-list",
                r#"{"_view_extras":{"sql":"SELECT t.c5, t.c6 FROM ((SELECT * FROM logs) t(c1, c2, c3, c4, c5, c6, c7) CROSS JOIN (SELECT 1 AS x) u) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            // 2. (RS-B) PostgreSQL/DuckDB FROM [main.]logs * [AS] t[(c1, c2)] table-inheritance suffix
            (
                "r7-from-logs-star-as-t-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM logs * AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-from-logs-star-bare-t-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM logs * t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-from-logs-star-as-t-cols",
                r#"{"_view_extras":{"sql":"SELECT c1 FROM logs * AS t(c1, c2) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-from-logs-star-bare-t-cols",
                r#"{"_view_extras":{"sql":"SELECT c1 FROM logs * t(c1, c2) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-from-main-logs-star-as-t-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM main.logs * AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            // 3. (RS-C) StarExpression column-renaming, pattern-filtered wildcards, and <catalog>.logs::VARCHAR
            (
                "r7-star-as-s",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT * AS s FROM logs) WHERE s_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-star-bare-s",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT * s FROM logs) WHERE s_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-paren-star-as-s",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT (*) AS s FROM logs) WHERE s_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-paren-qual-star-as-s",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT (logs.*) AS s FROM logs) WHERE s_90 LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-qual-star-as-s",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT logs.* AS s FROM logs) WHERE s_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-star-exclude-as-s",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT * EXCLUDE (timestamp) AS s FROM logs) WHERE s_1 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-star-replace-as-s",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT * REPLACE (status AS status) AS s FROM logs) WHERE s_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-prefix-colon-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT p: * FROM logs) WHERE p_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-quoted-prefix-colon-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT \"p\": * FROM logs) WHERE p_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-prefix-colon-qual-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT p: logs.* FROM logs) WHERE p_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-prefix-colon-paren-star",
                r#"{"_view_extras":{"sql":"SELECT * FROM (SELECT p: (*) FROM logs) WHERE p_2 = '198.51.100.7'"}}"#,
            ),
            (
                "r7-star-like-oi",
                r#"{"_view_extras":{"sql":"SELECT url, * LIKE '%oi%' AS x FROM logs WHERE x = '203.0.113.9'"}}"#,
            ),
            (
                "r7-star-ilike-extra",
                r#"{"_view_extras":{"sql":"SELECT url, * ILIKE '%EXTRA%' AS x FROM logs WHERE x LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-star-glob-ookie",
                r#"{"_view_extras":{"sql":"SELECT url, * GLOB '*ookie*' AS x FROM logs WHERE x = 'sess_secret_99'"}}"#,
            ),
            (
                "r7-star-similar-to-dge-si",
                r#"{"_view_extras":{"sql":"SELECT url, * SIMILAR TO '.*dge_si.*' AS x FROM logs WHERE x = 'esid_secret_77'"}}"#,
            ),
            (
                "r7-star-regex-tilde-oi",
                r#"{"_view_extras":{"sql":"SELECT url, * ~ '^oi' AS x FROM logs WHERE x = '203.0.113.9'"}}"#,
            ),
            (
                "r7-star-regex-bang-tilde",
                r#"{"_view_extras":{"sql":"SELECT url, * !~ '^timestamp$' AS x FROM logs WHERE x = '198.51.100.7'"}}"#,
            ),
            (
                "r7-star-op-like-extra",
                r#"{"_view_extras":{"sql":"SELECT url, * ~~ '%extra%' AS x FROM logs WHERE x LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-star-op-ilike-extra",
                r#"{"_view_extras":{"sql":"SELECT url, * ~~* '%EXTRA%' AS x FROM logs WHERE x LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-qual-star-like-oi",
                r#"{"_view_extras":{"sql":"SELECT url, logs.* LIKE '%oi%' AS x FROM logs WHERE x = '203.0.113.9'"}}"#,
            ),
            (
                "r7-catalog-dot-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT url FROM catalog91.logs WHERE catalog91.logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r7-cte-shadow-catalog-dot-logs-cast",
                r#"{"_view_extras":{"sql":"WITH catalog91 AS (SELECT 1 AS x) SELECT url FROM catalog91, logs WHERE catalog91.logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-r7-paren-join-safe",
                r#"{"_view_extras":{"sql":"SELECT t.status, t.url, u.x FROM ((SELECT * FROM logs) AS t CROSS JOIN (SELECT 1 AS x) AS u) WHERE t.status = 200"}}"#,
            ),
            (
                "benign-r7-from-logs-star-safe",
                r#"{"_view_extras":{"sql":"SELECT t.status, t.url FROM logs * AS t WHERE t.status = 200"}}"#,
            ),
            (
                "benign-r7-star-exclude",
                r#"{"_view_extras":{"sql":"SELECT * EXCLUDE (timestamp) FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r7-star-columns-unpack",
                r#"{"_view_extras":{"sql":"SELECT concat_ws('|', *COLUMNS('^(status|url)$')) AS s FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r7-count-star",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r7-prefix-colon-col",
                r#"{"_view_extras":{"sql":"SELECT p: status, url FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r7-from-first-star",
                r#"{"_view_extras":{"sql":"FROM logs SELECT * WHERE status = 200"}}"#,
            ),
            (
                "benign-r7-paren-expr-mult",
                r#"{"_view_extras":{"sql":"SELECT (status + 1) * 2 AS x FROM logs WHERE status = 200"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave8_r8_non_kw_contexts_star_rename_not_order_by_and_sconst_identifiers() {
        let blocked_vectors = [
            // 1. (RS-8A) Non-keyword ., ::, and AS resetting in_select_list or in_from
            (
                "r8a-select-as-offset-star-like",
                r#"{"_view_extras":{"sql":"SELECT 1 AS offset, url, * LIKE '%oi%' AS x FROM logs WHERE x = '203.0.113.9'"}}"#,
            ),
            (
                "r8a-select-as-qualify-star-as-s",
                r#"{"_view_extras":{"sql":"SELECT s_6 FROM (SELECT 1 AS qualify, * AS s FROM logs) sub WHERE s_1 = '198.51.100.7'"}}"#,
            ),
            (
                "r8a-select-dot-where-star-like",
                r#"{"_view_extras":{"sql":"SELECT a.where, logs.url, * LIKE '%oi%' AS x FROM (SELECT 1 AS \"where\") a, logs WHERE x = '203.0.113.9'"}}"#,
            ),
            (
                "r8a-on-dot-where-comma-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT t.url FROM (SELECT 1 AS \"where\") a JOIN (SELECT 1 AS y) b ON a.where = 1, logs AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8a-on-dot-union-comma-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT t.url FROM (SELECT 1 AS \"union\") a JOIN (SELECT 1 AS y) b ON a.union = 1, logs AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8a-on-cast-union-comma-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT t.url FROM (SELECT 1 AS x) a JOIN (SELECT 1 AS y) b ON 1::union(z INT) IS NOT NULL, logs AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8a-on-dot-where-comma-logs-cols",
                r#"{"_view_extras":{"sql":"SELECT t.c7 FROM (SELECT 1 AS \"where\") a JOIN (SELECT 1 AS y) b ON a.where = 1, logs AS t(c1, c2, c3, c4, c5, c6, c7, c8) WHERE t.c2 = '198.51.100.7'"}}"#,
            ),
            // 2. (RS-8B) * RENAME (<Sconst>), prefix-NOT * LIKE ..., and ORDER BY * LIKE ...
            (
                "r8b-star-rename-squote-ip",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME ('ip' AS safe_col) FROM logs) WHERE safe_col = '198.51.100.7'"}}"#,
            ),
            (
                "r8b-star-rename-squote-oip",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME ('oip' AS safe_col) FROM logs) WHERE safe_col = '203.0.113.9'"}}"#,
            ),
            (
                "r8b-star-rename-squote-cookie",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME ('cookie_session' AS safe_col) FROM logs) WHERE safe_col = 'sess_secret_99'"}}"#,
            ),
            (
                "r8b-star-rename-squote-esid",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME ('edge_sid' AS safe_col) FROM logs) WHERE safe_col = 'esid_secret_77'"}}"#,
            ),
            (
                "r8b-star-rename-squote-extra",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME ('_extra' AS safe_col) FROM logs) WHERE safe_col LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8b-qual-star-rename-squote-ip",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT logs.* RENAME ('ip' AS safe_col) FROM logs) WHERE safe_col = '198.51.100.7'"}}"#,
            ),
            (
                "r8b-star-rename-equote-ip",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME (E'\\x69p' AS safe_col) FROM logs) WHERE safe_col = '198.51.100.7'"}}"#,
            ),
            (
                "r8b-star-rename-dolq-ip",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME ($$ip$$ AS safe_col) FROM logs) WHERE safe_col = '198.51.100.7'"}}"#,
            ),
            (
                "r8b-star-rename-multiline-ip",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME ('i'\n'p' AS safe_col) FROM logs) WHERE safe_col = '198.51.100.7'"}}"#,
            ),
            (
                "r8b-star-rename-logs-dot-squote-ip",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT * RENAME (logs.'ip' AS safe_col) FROM logs) WHERE safe_col = '198.51.100.7'"}}"#,
            ),
            (
                "r8b-distinct-on-star-rename-ip",
                r#"{"_view_extras":{"sql":"SELECT DISTINCT ON (* RENAME ('ip' AS safe_col)) url FROM logs"}}"#,
            ),
            (
                "r8b-not-star-like-as-s",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT NOT * LIKE '%timestamp%' AS s FROM logs) WHERE s_1 = '198.51.100.7'"}}"#,
            ),
            (
                "r8b-order-by-star-like-oi",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs WHERE url IN ('/a', '/b') ORDER BY * LIKE '%oi%' DESC LIMIT 1"}}"#,
            ),
            (
                "r8b-order-by-star-ilike-extra",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs ORDER BY * ILIKE '%EXTRA%' DESC LIMIT 1"}}"#,
            ),
            (
                "r8b-order-by-star-glob-ookie",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs ORDER BY * GLOB '*ookie*' DESC LIMIT 1"}}"#,
            ),
            (
                "r8b-order-by-star-tilde-ip",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs ORDER BY * ~ '^ip$' DESC LIMIT 1"}}"#,
            ),
            (
                "r8b-order-by-comma-star-exclude-like",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs ORDER BY status, * EXCLUDE (timestamp) LIKE '%oi%' DESC LIMIT 1"}}"#,
            ),
            // 3. (RS-8C) DuckDB Sconst identifiers in AS <Sconst>, USING (<Sconst>), and FROM/ONLY relation & alias clauses
            (
                "r8c-natural-join-as-squote-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs NATURAL JOIN (SELECT '198.51.100.7' AS 'ip') u"}}"#,
            ),
            (
                "r8c-natural-join-as-dolq-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs NATURAL JOIN (SELECT '198.51.100.7' AS $$ip$$) u"}}"#,
            ),
            (
                "r8c-natural-join-as-equote-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs NATURAL JOIN (SELECT '198.51.100.7' AS E'\\x69p') u"}}"#,
            ),
            (
                "r8c-natural-join-as-multiline-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs NATURAL JOIN (SELECT '198.51.100.7' AS 'i'\n'p') u"}}"#,
            ),
            (
                "r8c-natural-join-vitals-as-squote-cid",
                r#"{"_view_extras":{"sql":"SELECT url FROM client_vitals NATURAL JOIN (SELECT 'cid_secret_99' AS 'cid') u"}}"#,
            ),
            (
                "r8c-join-using-squote-ip",
                r#"{"_view_extras":{"sql":"SELECT logs.status FROM logs JOIN (SELECT '198.51.100.7' AS 'ip') u USING ('ip')"}}"#,
            ),
            (
                "r8c-asof-join-using-squote-ip",
                r#"{"_view_extras":{"sql":"SELECT logs.status FROM logs ASOF JOIN (SELECT now() AS timestamp, '198.51.100.7' AS 'ip') u USING (timestamp, 'ip')"}}"#,
            ),
            (
                "r8c-from-squote-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM 'logs' WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8c-from-only-squote-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM ONLY 'logs' WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8c-from-only-paren-squote-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM ONLY ('logs') WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8c-from-logs-as-squote-t-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM logs AS 't' WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8c-from-logs-as-dolq-t-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM logs AS $$t$$ WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8c-from-subquery-as-squote-t-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM (SELECT * FROM logs) AS 't' WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8c-from-logs-as-squote-t-cols",
                r#"{"_view_extras":{"sql":"SELECT c1 FROM logs AS 't'('c1', 'c2', 'c3') WHERE c3 = '198.51.100.7'"}}"#,
            ),
            (
                "r8c-from-squote-logs-bare-t-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM 'logs' t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r8c-from-squote-logs-bare-t-cols",
                r#"{"_view_extras":{"sql":"SELECT t.c1 FROM 'logs' t(c1, c2, c3) WHERE t.c3 = '198.51.100.7'"}}"#,
            ),
            (
                "r8c-from-squote-logs-star-as-squote-t",
                r#"{"_view_extras":{"sql":"SELECT status FROM 'logs' * AS 't' WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-r8-where-url-eq-ip-literal",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs WHERE url = 'ip'"}}"#,
            ),
            (
                "benign-r8-select-as-safe-sconst-alias",
                r#"{"_view_extras":{"sql":"SELECT 'hello' AS 'safe_alias', status FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r8-select-as-union-star",
                r#"{"_view_extras":{"sql":"SELECT 1 AS union, * FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r8-star-exclude-paren-sconst",
                r#"{"_view_extras":{"sql":"SELECT * EXCLUDE ('timestamp') FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r8-star-exclude-bare-sconst",
                r#"{"_view_extras":{"sql":"SELECT * EXCLUDE 'timestamp' FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r8-star-rename-safe-sconst",
                r#"{"_view_extras":{"sql":"SELECT * RENAME ('url' AS req_url) FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r8-from-squote-logs-as-squote-t",
                r#"{"_view_extras":{"sql":"SELECT t.status, t.url FROM 'logs' AS 't' WHERE t.status = 200"}}"#,
            ),
            (
                "benign-r8-order-by-star-asc",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs WHERE status = 200 ORDER BY * ASC"}}"#,
            ),
            (
                "benign-r8-order-by-mult-desc",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs WHERE status = 200 ORDER BY status * 2 DESC"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave9_r9_as_parity_float_dot_paren_not_star_unnamed_subquery_pragma_and_delims()
    {
        let blocked_vectors = [
            // 1. (R9-0a) Iterative AS-run parity (AS as, .as, .as AS as, ::as) before FROM / JOIN
            (
                "r9-0a-as-as-from-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT status AS as FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9-0a-dot-as-from-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT {'as': status}.as FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9-0a-dot-as-as-as-from-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT {'as': status}.as AS as FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9-0a-typecast-as-from-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT status, 1::as FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9-0a-on-dot-as-join-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT t.url FROM (SELECT 200 AS \"as\") a JOIN (SELECT 200 AS y) b ON b.y = a.as JOIN logs AS t ON true WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            // 2. (R9-0b) Trailing-dot float 1. / 1._as before FROM
            (
                "r9-0b-float-dot-from-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT 1. FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9-0b-float-dot-ident-from-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT 1._as FROM logs WHERE logs::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            // 3. (R9-A) Parenthesized & DISTINCT ON / ORDER BY (NOT * ...)
            (
                "r9a-paren-not-star-like-as-s",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT (NOT * LIKE '%timestamp%') AS s FROM logs) WHERE s_1 = '198.51.100.7'"}}"#,
            ),
            (
                "r9a-double-paren-not-star-like-as-s",
                r#"{"_view_extras":{"sql":"SELECT count(*) AS cnt FROM (SELECT ((NOT * LIKE '%timestamp%')) AS s FROM logs) WHERE s_1 = '198.51.100.7'"}}"#,
            ),
            (
                "r9a-paren-not-star-not-like-oip",
                r#"{"_view_extras":{"sql":"SELECT url, (NOT * NOT LIKE '%oi%') AS x FROM logs WHERE x = '203.0.113.9'"}}"#,
            ),
            (
                "r9a-order-by-paren-not-star-not-like",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs ORDER BY (NOT * NOT LIKE '%oi%') DESC LIMIT 1"}}"#,
            ),
            (
                "r9a-order-by-paren-not-star-similar",
                r#"{"_view_extras":{"sql":"SELECT url FROM logs ORDER BY (NOT * SIMILAR TO '(timestamp|ts_ms)') DESC LIMIT 1"}}"#,
            ),
            (
                "r9a-distinct-on-not-star-like",
                r#"{"_view_extras":{"sql":"SELECT DISTINCT ON (NOT * LIKE '%oi%') url FROM logs"}}"#,
            ),
            (
                "r9a-distinct-on-not-star-not-like",
                r#"{"_view_extras":{"sql":"SELECT DISTINCT ON (NOT * NOT LIKE '%oi%') url FROM logs"}}"#,
            ),
            // 4. (R9-B) DuckDB implicit unaliased subquery relation bindings (unnamed_subquery / unnamed_query)
            (
                "r9b-unnamed-subquery-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM (SELECT * FROM logs) WHERE unnamed_subquery::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9b-quoted-unnamed-subquery-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM (SELECT * FROM logs) WHERE \"unnamed_subquery\"::VARCHAR LIKE '%203.0.113.9%'"}}"#,
            ),
            (
                "r9b-format-unnamed-subquery",
                r#"{"_view_extras":{"sql":"SELECT status FROM (SELECT * FROM logs) WHERE format('{}', unnamed_subquery) LIKE '%sess_secret_99%'"}}"#,
            ),
            (
                "r9b-unnamed-subquery2-cast",
                r#"{"_view_extras":{"sql":"SELECT a FROM (SELECT 1 AS a), (SELECT * FROM logs) WHERE unnamed_subquery2::VARCHAR LIKE '%esid_secret_77%'"}}"#,
            ),
            (
                "r9b-from-logs-unnamed-subquery",
                r#"{"_view_extras":{"sql":"SELECT status FROM (FROM logs) WHERE unnamed_subquery::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9b-table-logs-unnamed-subquery",
                r#"{"_view_extras":{"sql":"SELECT status FROM (TABLE logs) WHERE unnamed_subquery::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9b-lateral-unnamed-subquery",
                r#"{"_view_extras":{"sql":"SELECT x.a FROM (SELECT 1 AS a) x, LATERAL (SELECT * FROM logs) WHERE unnamed_subquery::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9b-unnamed-query-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM (SELECT * FROM logs) WHERE unnamed_query::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            // 5. (R9-C) pragma_* / duckdb_table_sample / summary & []/{} expression depth in ON before comma-joins
            (
                "r9c-pragma-storage-info",
                r#"{"_view_extras":{"sql":"SELECT stats FROM pragma_storage_info('logs') WHERE stats LIKE '%198.51.1%'"}}"#,
            ),
            (
                "r9c-duckdb-table-sample",
                r#"{"_view_extras":{"sql":"SELECT * FROM duckdb_table_sample('logs')"}}"#,
            ),
            (
                "r9c-summary-fn",
                r#"{"_view_extras":{"sql":"SELECT summary(status) FROM logs"}}"#,
            ),
            (
                "r9c-on-brace-where-comma-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT t.url FROM (SELECT 1 AS a) x JOIN (SELECT 1 AS b) y ON {where: 1} IS NOT NULL, logs AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r9c-on-listcomp-for-comma-logs-cast",
                r#"{"_view_extras":{"sql":"SELECT t.url FROM (SELECT 1 AS a) x JOIN (SELECT 1 AS b) y ON [x FOR x IN [1]] IS NOT NULL, logs AS t WHERE t::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-r9-as-as-from-logs",
                r#"{"_view_extras":{"sql":"SELECT status AS as, url FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r9-dot-as-as-where-from-logs",
                r#"{"_view_extras":{"sql":"SELECT {'as': status}.as AS where, url FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r9-float-dot-from-logs",
                r#"{"_view_extras":{"sql":"SELECT status, 1. FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r9-unnamed-subquery-safe-cols",
                r#"{"_view_extras":{"sql":"SELECT unnamed_subquery.status, unnamed_subquery.url FROM (SELECT * FROM logs) WHERE unnamed_subquery.status = 200"}}"#,
            ),
            (
                "benign-r9-paren-not-expr-mult",
                r#"{"_view_extras":{"sql":"SELECT (NOT (status = 404))::INT * 2 AS x FROM logs WHERE status = 200"}}"#,
            ),
            (
                "benign-r9-on-bracket-list-eq",
                r#"{"_view_extras":{"sql":"SELECT a.url FROM logs AS a JOIN logs AS b ON [a.status, 1] = [b.status, 1] WHERE a.status = 200"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave10_r10_whitespace_quoted_and_sconst_relation_aliases() {
        let blocked_vectors = [
            (
                "r10-from-logs-as-dquote-space-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS \" \" WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-from-logs-as-dquote-tab-oip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS \"\t\" WHERE \"\t\"::VARCHAR LIKE '%203.0.113.9%'"}}"#,
            ),
            (
                "r10-from-logs-as-dquote-zwsp-cookie",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS \"\u200b\" WHERE \"\u200b\"::VARCHAR LIKE '%sess_secret_99%'"}}"#,
            ),
            (
                "r10-from-logs-as-dquote-bom-esid",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS \"\ufeff\" WHERE \"\ufeff\"::VARCHAR LIKE '%esid_secret_77%'"}}"#,
            ),
            (
                "r10-from-logs-as-dquote-wj-extra",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS \"\u2060\" WHERE format('{}', \"\u2060\") LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-from-logs-as-squote-space-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS ' ' WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-from-logs-as-equote-space-oip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS E'\\x20' WHERE \" \"::VARCHAR LIKE '%203.0.113.9%'"}}"#,
            ),
            (
                "r10-from-logs-as-dolq-space-esid",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs AS $$ $$ WHERE \" \"::VARCHAR LIKE '%esid_secret_77%'"}}"#,
            ),
            (
                "r10-from-logs-bare-squote-space-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM logs ' ' WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-from-squote-space-rel-cast",
                r#"{"_view_extras":{"sql":"SELECT status FROM ' ' WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-from-subq-as-dquote-space-ip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM (SELECT * FROM logs) AS \" \" WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-from-subq-as-squote-space-oip",
                r#"{"_view_extras":{"sql":"SELECT status, url FROM (SELECT * FROM logs) AS ' ' WHERE \" \"::VARCHAR LIKE '%203.0.113.9%'"}}"#,
            ),
            (
                "r10-join-logs-as-dquote-space-ip",
                r#"{"_view_extras":{"sql":"SELECT x.a FROM (SELECT 1 AS a) x JOIN logs AS \" \" ON true WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-only-logs-as-dquote-space-ip",
                r#"{"_view_extras":{"sql":"SELECT status FROM ONLY logs AS \" \" WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-logs-star-as-dquote-space-ip",
                r#"{"_view_extras":{"sql":"SELECT status FROM logs * AS \" \" WHERE \" \"::VARCHAR LIKE '%198.51.100.7%'"}}"#,
            ),
            (
                "r10-vitals-as-dquote-space-cid",
                r#"{"_view_extras":{"sql":"SELECT url FROM client_vitals AS \" \" WHERE \" \"::VARCHAR LIKE '%cid_secret_99%'"}}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-r10-dquote-space-dot-safe-cols",
                r#"{"_view_extras":{"sql":"SELECT \" \".status, \" \".url FROM logs AS \" \" WHERE \" \".status = 200"}}"#,
            ),
            (
                "benign-r10-empty-sconst-projection-and-where",
                r#"{"_view_extras":{"sql":"SELECT '' AS empty_s, status FROM logs WHERE url != ''"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn sec19_blocks_wave11_r11_within_group_ordered_set_aggregate_comma_join_relations() {
        let blocked_vectors = [
            (
                "r11-within-group-mode-comma-logs-cast-ip",
                r#"{"_view_extras":{"sql":"SELECT min(outer_v.url) AS url FROM client_vitals AS outer_v HAVING (SELECT c::VARCHAR LIKE '%198.51.100.7%' FROM (SELECT 1 AS a) AS x JOIN (SELECT 1 AS b) AS y ON mode() WITHIN GROUP (ORDER BY outer_v.url) = '/ok', logs AS c LIMIT 1)"}}"#,
            ),
            (
                "r11-within-group-percentile-comma-logs-format-oip",
                r#"{"_view_extras":{"sql":"SELECT min(outer_v.url) AS url FROM client_vitals AS outer_v HAVING (SELECT format('{}', c) LIKE '%203.0.113.9%' FROM (SELECT 1 AS a) AS x JOIN (SELECT 1 AS b) AS y ON percentile_disc(0.5) WITHIN GROUP (ORDER BY outer_v.url) = '/ok', logs AS c LIMIT 1)"}}"#,
            ),
        ];

        for (case_id, payload) in blocked_vectors {
            assert!(
                filters_reference_pii(payload),
                "expected {case_id} to be blocked as PII-referencing: {payload}"
            );
        }

        let allowed_vectors = [
            (
                "benign-r11-within-col-group-by",
                r#"{"_view_extras":{"sql":"SELECT a.within, count(*) FROM (SELECT status AS within FROM logs) AS a GROUP BY within"}}"#,
            ),
            (
                "benign-r11-on-within-col-then-group-by",
                r#"{"_view_extras":{"sql":"SELECT a.within, count(*) FROM (SELECT status AS within FROM logs) AS a JOIN logs AS b ON a.within = b.status GROUP BY a.within"}}"#,
            ),
        ];

        for (case_id, payload) in allowed_vectors {
            assert!(
                !filters_reference_pii(payload),
                "expected {case_id} to be allowed for AnalystNoPii: {payload}"
            );
        }
    }

    #[test]
    fn save_rejects_duplicate_json_keys_and_control_chars_in_name_filters_and_metadata() {
        let store = Memory::default();
        for bad_body in [
            json!({"service_id":"svc1","name":"Ops\tView","filters_json":"{}"}),
            json!({"service_id":"svc1","name":"Ops\nView","filters_json":"{}"}),
            json!({"service_id":"svc1","name":"Ops\u{0000}View","filters_json":"{}"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{\"a\":1,\"a\":2}"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{\"nested\":{\"k\":1,\"k\":2}}"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{\"a\":\"x\u{0000}y\"}"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{}","page":"/logs\n"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{}","time_range_type":"1h\t"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{}","start_time":"2026-01-01T00:00:00Z\r"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{}","end_time":"2026-01-01T01:00:00Z\u{0007}"}),
            json!({"service_id":"svc1","name":"Ops","filters_json":"{}","created_at":"2026-01-01T00:00:00Z\n"}),
        ] {
            assert_eq!(
                failure(call(&store, "POST", "/api/views/", bad_body.clone())),
                Error::InvalidInput,
                "expected InvalidInput for {bad_body}"
            );
        }
    }
}
