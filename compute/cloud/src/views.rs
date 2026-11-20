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

fn is_pii_key_exact(candidate: &str) -> bool {
    let trimmed = candidate
        .trim()
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']'));
    if trimmed.is_empty() {
        return false;
    }
    let canon: String = trimmed
        .chars()
        .map(|c| {
            if c == '-' {
                '_'
            } else {
                c.to_ascii_lowercase()
            }
        })
        .collect();
    if PII_FILTER_KEYS.contains(&canon.as_str()) {
        return true;
    }
    if let Some((base, suffix)) = canon.rsplit_once('_') {
        if !suffix.is_empty()
            && suffix.bytes().all(|b| b.is_ascii_digit())
            && PII_FILTER_KEYS.contains(&base)
        {
            return true;
        }
    }
    false
}

const MAX_JSON_DEPTH: usize = 16;

fn is_pii_identifier(raw: &str) -> bool {
    let trimmed = raw
        .trim()
        .trim_start_matches(['+', '-', '!', '$'])
        .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']' | '{' | '}'));
    if is_pii_key_exact(trimmed) {
        return true;
    }
    trimmed
        .split(['.', ':', '[', ']', '{', '}', ','])
        .any(|segment| {
            let seg = segment
                .trim()
                .trim_start_matches(['+', '-', '!', '$'])
                .trim_matches(|c| matches!(c, '"' | '\'' | '`' | '[' | ']' | '{' | '}'));
            !seg.is_empty() && is_pii_key_exact(seg)
        })
}

fn is_pii_key_or_json_path(raw: &str) -> bool {
    if is_pii_identifier(raw) {
        return true;
    }
    let trimmed = raw.trim();
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

fn columns_arg_references_pii(lit: &str) -> bool {
    if lit.trim().is_empty() || is_pii_identifier(lit) {
        return true;
    }
    if lit.chars().any(|c| {
        !(c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '-' | '^' | '$' | '(' | ')' | '|'))
    }) {
        return true;
    }
    let bytes = lit.as_bytes();
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'('
            && idx > 0
            && (bytes[idx - 1].is_ascii_alphanumeric() || matches!(bytes[idx - 1], b'_' | b'-'))
        {
            return true;
        }
        if b == b')'
            && idx + 1 < bytes.len()
            && (bytes[idx + 1].is_ascii_alphanumeric() || matches!(bytes[idx + 1], b'_' | b'-'))
        {
            return true;
        }
    }
    lit.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .any(|tok| {
            !tok.is_empty()
                && (is_pii_identifier(tok)
                    || (tok.contains('-') && tok.split('-').any(is_pii_key_exact)))
        })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LastSig {
    Other,
    IdentOrClose,
    SubscriptOpen,
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
            if val >= 0x80 {
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
            if byte >= 0x80 {
                return None;
            }
            if let Some(buf) = out.as_mut() {
                buf.push(byte as char);
            }
            Some(i + 1 + count)
        }
        _ => {
            let next_ch = s.get(i + 1..)?.chars().next()?;
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

/// Peeks past whitespace and SQL comments after a string literal to detect `:` in `{ 'k': v }`.
fn next_non_ws_or_comment_is_colon(s: &str, mut i: usize) -> bool {
    let bytes = s.as_bytes();
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] >= 0x80 {
            if let Some(ch) = s.get(i..).and_then(|tail| tail.chars().next()) {
                if ch.is_whitespace() {
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
            continue;
        }
        return bytes[i] == b':';
    }
    false
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

fn is_extract_fn_name(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "json_extract"
            | "json_extract_string"
            | "json_extract_path"
            | "json_extract_path_text"
            | "json_value"
            | "json_exists"
            | "struct_extract"
            | "map_extract"
            | "element_at"
    )
}

/// Single-pass `O(N)` DuckDB/PostgreSQL SQL & filter-expression lexical scanner.
/// Fails closed (`true`) on unterminated comments, quotes, escapes, `UESCAPE`, or brackets.
fn sql_or_filter_references_pii(text: &str) -> bool {
    let s = text.trim();
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut bracket_depth = 0u32;
    let mut brace_depth = 0u32;
    let mut paren_depth = 0u32;
    let mut pending_extract_fn = false;
    let mut extract_fn_depth: Option<u32> = None;
    let mut pending_columns_fn = false;
    let mut columns_fn_depth: Option<u32> = None;
    let mut json_arrow_list_depth: Option<u32> = None;
    let mut last_sig = LastSig::Other;
    let in_key_ctx = |ls: LastSig, ext: Option<u32>, arr_list: Option<u32>| {
        matches!(ls, LastSig::SubscriptOpen | LastSig::JsonArrow)
            || ext.is_some()
            || arr_list.is_some()
    };

    while i < bytes.len() {
        // 1. ASCII / UTF-8 whitespace (preserves `last_sig`, `pending_extract_fn`, `pending_columns_fn`)
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] >= 0x80 {
            if let Some(ch) = s.get(i..).and_then(|tail| tail.chars().next()) {
                if ch.is_whitespace() {
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

        // 4a. `U&'...'` / `u&'...'` Unicode-escaped string literal
        if matches!(bytes[i], b'U' | b'u')
            && bytes.get(i + 1) == Some(&b'&')
            && bytes.get(i + 2) == Some(&b'\'')
        {
            i += 3;
            let check_key_ctx = in_key_ctx(last_sig, extract_fn_depth, json_arrow_list_depth);
            let mut captured = String::new();
            let mut closed = false;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if bytes.get(i + 1) == Some(&b'\'') {
                        captured.push('\'');
                        i += 2;
                    } else {
                        i += 1;
                        closed = true;
                        break;
                    }
                } else if bytes[i] == b'\\' {
                    let Some(next_i) = decode_sql_unicode_escape(bytes, i, &mut captured) else {
                        return true;
                    };
                    i = next_i;
                } else if let Some(ch) = s.get(i..).and_then(|tail| tail.chars().next()) {
                    captured.push(ch);
                    i += ch.len_utf8();
                } else {
                    return true;
                }
            }
            if !closed {
                return true;
            }
            if columns_fn_depth.is_some() && columns_arg_references_pii(&captured) {
                return true;
            }
            if (check_key_ctx || (brace_depth > 0 && next_non_ws_or_comment_is_colon(s, i)))
                && is_pii_key_or_json_path(&captured)
            {
                return true;
            }
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::Other;
            continue;
        }

        // 4b. `E'...'` / `e'...'` escaped string literal vs standard `'...'` string literal
        if (matches!(bytes[i], b'E' | b'e') && bytes.get(i + 1) == Some(&b'\''))
            || bytes[i] == b'\''
        {
            let backslash_escapes = bytes[i] != b'\'';
            i += if backslash_escapes { 2 } else { 1 };
            let check_key_ctx = in_key_ctx(last_sig, extract_fn_depth, json_arrow_list_depth);
            let should_capture = check_key_ctx || columns_fn_depth.is_some() || brace_depth > 0;
            let mut captured = should_capture.then(String::new);
            let mut closed = false;
            while i < bytes.len() {
                if backslash_escapes && bytes[i] == b'\\' {
                    let Some(next_i) = decode_sql_e_escape(s, i, &mut captured) else {
                        return true;
                    };
                    i = next_i;
                } else if bytes[i] == b'\'' {
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
            if let Some(lit) = captured.as_deref() {
                if columns_fn_depth.is_some() && columns_arg_references_pii(lit) {
                    return true;
                }
                if (check_key_ctx || (brace_depth > 0 && next_non_ws_or_comment_is_colon(s, i)))
                    && is_pii_key_or_json_path(lit)
                {
                    return true;
                }
            }
            pending_extract_fn = false;
            pending_columns_fn = false;
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
                let check_key_ctx = in_key_ctx(last_sig, extract_fn_depth, json_arrow_list_depth);
                if columns_fn_depth.is_some() && columns_arg_references_pii(body) {
                    return true;
                }
                if (check_key_ctx || (brace_depth > 0 && next_non_ws_or_comment_is_colon(s, after)))
                    && is_pii_key_or_json_path(body)
                {
                    return true;
                }
                i = after;
                pending_extract_fn = false;
                pending_columns_fn = false;
                last_sig = LastSig::Other;
                continue;
            }
            i += 1;
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::Other;
            continue;
        }

        // 6. `"..."`, `` `...` ``, and `U&"..."` / `u&"..."` quoted identifiers
        if (matches!(bytes[i], b'U' | b'u')
            && bytes.get(i + 1) == Some(&b'&')
            && bytes.get(i + 2) == Some(&b'"'))
            || matches!(bytes[i], b'"' | b'`')
        {
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
            if !closed || is_pii_identifier(&ident_buf) {
                return true;
            }
            pending_extract_fn = is_extract_fn_name(&ident_buf);
            pending_columns_fn = ident_buf.trim().eq_ignore_ascii_case("columns");
            last_sig = LastSig::IdentOrClose;
            continue;
        }

        // 7. `[` ... `]` T-SQL single bracketed PII identifier OR DuckDB `LIST` / subscript
        if bytes[i] == b'[' {
            if s.get(i + 1..).is_some_and(peek_simple_bracket_pii) {
                return true;
            }
            bracket_depth = bracket_depth.saturating_add(1);
            if last_sig == LastSig::JsonArrow && json_arrow_list_depth.is_none() {
                json_arrow_list_depth = Some(bracket_depth);
            }
            last_sig = if last_sig == LastSig::IdentOrClose {
                LastSig::SubscriptOpen
            } else {
                LastSig::Other
            };
            pending_extract_fn = false;
            pending_columns_fn = false;
            i += 1;
            continue;
        }
        if bytes[i] == b']' {
            if bracket_depth == 0 {
                return true;
            }
            if json_arrow_list_depth == Some(bracket_depth) {
                json_arrow_list_depth = None;
            }
            bracket_depth -= 1;
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::IdentOrClose;
            i += 1;
            continue;
        }

        // 8. Unquoted identifiers & hyphenated filter/header tokens
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            let mut end = start + 1;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            while bytes.get(end) == Some(&b'-') {
                let seg_start = end + 1;
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
                end = seg_start + 1;
                while end < bytes.len()
                    && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
                {
                    end += 1;
                }
            }
            let token = &s[start..end];
            if is_pii_identifier(token) {
                return true;
            }
            if token.contains('-') && token.split('-').any(is_pii_key_exact) {
                return true;
            }
            if token.eq_ignore_ascii_case("uescape") {
                return true;
            }
            pending_extract_fn = is_extract_fn_name(token);
            pending_columns_fn = token.eq_ignore_ascii_case("columns");
            last_sig = LastSig::IdentOrClose;
            i = end;
            continue;
        }

        // 9. Parens, braces, JSON arrows (`->`, `->>`, `#>`, `#>>`) & other punctuation/digits
        if bytes[i] == b'(' {
            paren_depth = paren_depth.saturating_add(1);
            if pending_extract_fn && extract_fn_depth.is_none() {
                extract_fn_depth = Some(paren_depth);
            }
            if pending_columns_fn && columns_fn_depth.is_none() {
                columns_fn_depth = Some(paren_depth);
            }
            pending_extract_fn = false;
            pending_columns_fn = false;
            if !matches!(last_sig, LastSig::SubscriptOpen | LastSig::JsonArrow) {
                last_sig = LastSig::Other;
            }
            i += 1;
            continue;
        }
        if bytes[i] == b')' {
            if extract_fn_depth == Some(paren_depth) {
                extract_fn_depth = None;
            }
            if columns_fn_depth == Some(paren_depth) {
                columns_fn_depth = None;
            }
            paren_depth = paren_depth.saturating_sub(1);
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::IdentOrClose;
            i += 1;
            continue;
        }
        if bytes[i] == b'{' {
            brace_depth = brace_depth.saturating_add(1);
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::Other;
            i += 1;
            continue;
        }
        if bytes[i] == b'}' {
            brace_depth = brace_depth.saturating_sub(1);
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::IdentOrClose;
            i += 1;
            continue;
        }
        if matches!(bytes[i], b'-' | b'#') && bytes.get(i + 1) == Some(&b'>') {
            i += if bytes.get(i + 2) == Some(&b'>') {
                3
            } else {
                2
            };
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::JsonArrow;
            continue;
        }
        if bytes[i].is_ascii_digit() {
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            pending_extract_fn = false;
            pending_columns_fn = false;
            last_sig = LastSig::Other;
            continue;
        }
        if columns_fn_depth.is_some() && bytes[i] == b'*' {
            return true;
        }

        i += s
            .get(i..)
            .and_then(|tail| tail.chars().next())
            .map_or(1, char::len_utf8);
        pending_extract_fn = false;
        pending_columns_fn = false;
        last_sig = LastSig::Other;
    }

    bracket_depth > 0
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
    !is_pii_identifier(st) && !is_reserved_filter_key(st)
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
    !is_pii_identifier(raw_key) && !is_reserved_filter_key(raw_key)
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

fn json_has_duplicate_keys(raw: &str) -> bool {
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
        if is_pii_identifier(ck) {
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
                if is_pii_identifier(k) {
                    return true;
                }
                let key_norm = k.trim().to_ascii_lowercase();
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
            if slot == FilterSlot::FieldSelector && is_pii_identifier(s) {
                return true;
            }
            let t = s.trim();
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
    if filters_json.len() > MAX_FILTERS_BYTES || json_has_duplicate_keys(filters_json) {
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
        Some(text) if text.chars().count() > MAX_FIELD_CHARS => Err(Error::InvalidInput),
        other => Ok(other),
    }
}

fn new_id(store: &impl ViewStore) -> Result<String, Error> {
    let bytes = store.random().map_err(|_| Error::Storage)?;
    Ok(hex::encode(&bytes[..16]))
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
        || input.filters_json.len() > MAX_FILTERS_BYTES
        || serde_json::from_str::<Value>(&input.filters_json).is_err()
    {
        return Err(Error::InvalidInput);
    }
    let id = match input.id.filter(|id| !id.is_empty()) {
        Some(id) if id.chars().count() <= MAX_FIELD_CHARS => id,
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
}
