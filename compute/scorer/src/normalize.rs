//! URL → canonical (route, category) pair.
//!
//! Mirrors `backend/scoring/normalize.py`. Same patterns, same category map,
//! same lowercase-then-collapse order. Anything that normalizes differently
//! between Python and Rust would corrupt matrix lookups at runtime (the matrix
//! is TRAINED with the Python normalizer and SCORED with this one). The
//! cross-language parity contract is pinned by the `cross_lang_parity_*` tests
//! below ↔ `tests/scoring/test_normalize_parity.py`; known intentional
//! divergences (encoded `%2F`) are pinned by the parity test
//! (`tests/scoring/test_normalize_parity.py`).
//!
//! **ASCII-only normalization contract (EC-02).** Path keys are normalized as
//! ASCII: lowercasing is `to_ascii_lowercase` and the numeric-id collapse uses
//! ASCII digits only. Python's `str.lower()` / `\d` (full Unicode) therefore
//! diverge from this side on NON-ASCII input — a deliberate, documented limit,
//! not a bug. A non-ASCII route is keyed by its raw bytes here, so the worst
//! case is the edge treating an i18n path as a novel route (an accuracy loss in
//! L2's coarse signal, never a category-evasion bypass). These divergence
//! classes are pinned by `tests/scoring/test_normalize_runtime_parity.py`; if
//! the site needs true Unicode parity, port Unicode case/digit folding into
//! BOTH normalizers together (it would pull unicode crates into the Wasm).
//! Raw C0 control chars (`\t \n \r`) are NOT a divergence: both sides strip
//! them (Python via `urlsplit`, this side in `normalize` below).

/// Coarse first-segment → category map. Mirrors the Python `_CATEGORY_MAP`
/// dict exactly; both must be edited in lockstep when adding new buckets.
const CATEGORY_MAP: &[(&str, &str)] = &[
    ("", "home"),
    ("api", "api"),
    ("graphql", "api"),
    ("products", "product"),
    ("product", "product"),
    ("items", "product"),
    ("p", "product"),
    ("categories", "browse"),
    ("category", "browse"),
    ("search", "browse"),
    ("browse", "browse"),
    ("cart", "cart"),
    ("basket", "cart"),
    ("checkout", "checkout"),
    ("pay", "checkout"),
    ("order", "checkout"),
    ("orders", "checkout"),
    ("account", "account"),
    ("user", "account"),
    ("users", "account"),
    ("profile", "account"),
    ("settings", "account"),
    ("auth", "auth"),
    ("login", "auth"),
    ("signin", "auth"),
    ("signup", "auth"),
    ("register", "auth"),
    ("logout", "auth"),
    ("admin", "admin"),
    ("static", "asset"),
    ("assets", "asset"),
    ("blog", "content"),
    ("news", "content"),
    ("about", "content"),
    ("help", "content"),
    ("support", "content"),
    ("privacy", "content"),
    ("terms", "content"),
    ("faq", "content"),
];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Route {
    pub path: String,
    pub category: String,
}

fn strip_query(url: &str) -> &str {
    // Find a '?' or '#' to delimit. We don't bother with scheme/host parsing
    // because Fastly Compute hands us a relative path already, and the
    // Python side calls urlsplit which also discards everything after '?'.
    let path = url.split('?').next().unwrap_or(url);
    let path = path.split('#').next().unwrap_or(path);

    // Drop scheme://host if present (urlsplit-equivalent: only keep the path
    // component).
    if let Some(idx) = path.find("://") {
        let scheme = &path[..idx];
        let is_valid_scheme = scheme
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic())
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.');
        if is_valid_scheme {
            // Look for the FIRST '/' after the scheme separator.
            let rest = &path[idx + 3..];
            if let Some(slash) = rest.find('/') {
                return &rest[slash..];
            }
            return "/";
        }
    }
    path
}

fn looks_like_id(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }

    // Numeric only.
    if segment.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // UUID v4 (8-4-4-4-12 hex chars).
    if segment.len() == 36
        && segment.chars().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
    {
        return true;
    }
    // 24+ hex chars (content hash / Mongo ObjectId).
    if segment.len() >= 24 && segment.chars().all(|c| c.is_ascii_hexdigit()) {
        return true;
    }
    // Prefixed id: 2-5 uppercase letters, separator (- or _), then alphanumeric.
    if let Some(idx) = segment.find(['-', '_']) {
        let prefix = &segment[..idx];
        if (2..=5).contains(&prefix.len()) && prefix.chars().all(|c| c.is_ascii_uppercase()) {
            let suffix = &segment[idx + 1..];
            if !suffix.is_empty()
                && suffix
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                return true;
            }
        }
    }
    // Long opaque alphanumeric (>= 20 chars).
    if segment.len() >= 20
        && segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return true;
    }
    false
}

fn category_for(first_segment: &str) -> &'static str {
    let lower = first_segment.to_ascii_lowercase();
    for (k, v) in CATEGORY_MAP {
        if *k == lower {
            return v;
        }
    }
    "other"
}

/// Decode percent-encoded sequences (%XX → byte) into a UTF-8 string.
/// Mirrors Python's `urllib.parse.unquote` behaviour for the URL-encoded
/// characters that appear in real-world paths. Required so the Rust
/// scorer's category matching keeps parity with the Python normalizer —
/// without this, an attacker can submit `/%61dmin` and bypass the
/// `admin` category match downstream. See audit finding 013.
fn percent_decode(s: &str) -> String {
    let mut bytes = Vec::with_capacity(s.len());
    let mut i = s.bytes();
    while let Some(b) = i.next() {
        if b == b'%' {
            let mut clone = i.clone();
            if let (Some(h1), Some(h2)) = (clone.next(), clone.next()) {
                if let (Some(n1), Some(n2)) = ((h1 as char).to_digit(16), (h2 as char).to_digit(16))
                {
                    bytes.push(((n1 << 4) | n2) as u8);
                    i = clone;
                    continue;
                }
            }
        }
        bytes.push(b);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Decode percent-encoded sequences EXCEPT encoded slashes (`%2f`/`%2F`),
/// which are preserved verbatim. Mirrors Python's `unquote_except_slash`:
/// encoded traversal dots (`%2e%2e`) decode so they can be resolved by the
/// structural collapse, but an encoded slash never becomes a path separator
/// (audit finding 014 — anti category-evasion).
fn unquote_except_slash(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 2 < bytes.len() {
            if let (Some(n1), Some(n2)) = (
                (bytes[i + 1] as char).to_digit(16),
                (bytes[i + 2] as char).to_digit(16),
            ) {
                let decoded = ((n1 << 4) | n2) as u8;
                if decoded == b'/' {
                    // Preserve the encoded slash as data, not a separator.
                    out.push(b'%');
                    out.push(bytes[i + 1]);
                    out.push(bytes[i + 2]);
                } else {
                    out.push(decoded);
                }
                i += 3;
                continue;
            }
        }
        out.push(b);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Iteratively run [`unquote_except_slash`] until the string stops changing.
///
/// Finding 011 (2026-06-15): a single decode pass only peels one
/// encoding layer, so a doubly-encoded traversal like
/// `/admin/%252e%252e/items` would decode to `/admin/%2e%2e/items` and
/// the segment-split + traversal-resolution loop wouldn't see `..`
/// segments. Iterating until fixed-point unwinds multi-level encoding
/// so attacker payloads converge to the real characters before
/// segments are evaluated. Encoded slashes are preserved on every pass.
/// `max_iter` is a paranoid cap on pathological inputs (well-formed input
/// always converges in a handful of passes).
fn unquote_except_slash_until_stable(s: &str, max_iter: usize) -> String {
    let mut current = s.to_string();
    for _ in 0..max_iter {
        let next = unquote_except_slash(&current);
        if next == current {
            return next;
        }
        current = next;
    }
    current
}

/// Normalize a URL to its (canonical route, category) pair.
pub fn normalize(url: &str) -> Route {
    // EC-02: mirror Python's `urlsplit`, which strips ASCII tab/newline/CR from
    // the URL before parsing (CPython bpo-43882 / WHATWG URL). Without this the
    // raw byte survives into a path segment, misses CATEGORY_MAP, and flips the
    // category — e.g. `/adm\tin` keyed as `other` here while the trainer keyed
    // `admin`, a silent train/score key mismatch. (The percent-encoded form
    // `%09` is ordinary data on both sides and is unaffected.) Allocate only
    // when a control char is actually present — never on the hot path of real
    // requests.
    let stripped: String;
    let url = if url.bytes().any(|b| matches!(b, b'\t' | b'\n' | b'\r')) {
        stripped = url.chars().filter(|c| !matches!(c, '\t' | '\n' | '\r')).collect();
        stripped.as_str()
    } else {
        url
    };
    let raw_path = strip_query(url);
    // 011/013/014: iteratively decode everything EXCEPT encoded slashes so
    //   - encoded traversals (`%2e%2e`) and multi-level variants (`%252e…`)
    //     resolve to `..` before the structural collapse below, and
    //   - encoded slashes (`%2F`) survive as data and cannot act as path
    //     separators (finding 014, anti category-evasion). Porting this
    //     "%2F-as-data" model here makes the edge scorer enforce 014 too,
    //     instead of leaving it to the Python trainer alone — and keeps the
    //     train key == score key for encoded-slash URLs.
    let decoded = unquote_except_slash_until_stable(raw_path, 4);
    if decoded.is_empty() || decoded == "/" {
        return Route {
            path: "/".to_string(),
            category: "home".to_string(),
        };
    }
    // posixpath.normpath equivalent: collapse `.`, `..`, and empty segments on
    // REAL slashes only. Because `%2F` is still encoded here, a `%2F`-hidden
    // `..` is part of one segment and never pops a parent.
    let mut norm_segments: Vec<&str> = Vec::new();
    for s in decoded.split('/').filter(|s| !s.is_empty()) {
        if s == "." {
            continue;
        } else if s == ".." {
            norm_segments.pop();
        } else {
            norm_segments.push(s);
        }
    }
    if norm_segments.is_empty() {
        return Route {
            path: "/".to_string(),
            category: "home".to_string(),
        };
    }

    // 014: fully decode each segment AFTER the structural split, so an encoded
    // slash becomes a literal `/` *inside* a segment (data) without ever having
    // acted as a separator. Mirrors Python's per-segment `unquote`.
    let raw_segments: Vec<String> = norm_segments.iter().map(|s| percent_decode(s)).collect();
    let normalized: Vec<String> = raw_segments
        .iter()
        .map(|s| {
            if looks_like_id(s) {
                "*".to_string()
            } else {
                s.to_ascii_lowercase()
            }
        })
        .collect();
    let canonical = format!("/{}", normalized.join("/"));
    let category = category_for(&raw_segments[0]).to_string();
    Route {
        path: canonical,
        category,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Cross-language parity contract ──────────────────────────────────────
    //
    // Pins THIS (Rust) normalizer's output for a golden URL set shared
    // byte-for-byte with `tests/scoring/test_normalize_parity.py`. The matrix is
    // trained with the Python normalizer and scored with this one, so a flip on
    // either side is a silent train/score key mismatch.
    //
    // Every row produces the SAME (path, category) in both languages. The
    // encoded-`%2F` rows used to diverge (this side decoded `%2F` and popped
    // `..`); porting Python's "%2F-as-data" model here closed that gap, so
    // finding 014 (anti category-evasion) is now enforced at the edge too. If
    // you change either normalizer and a row flips, update BOTH this test and
    // the Python counterpart AND the doc — never re-baseline one side alone.

    // (url, rust_path, rust_category) — the Rust side of the GOLDEN table.
    const PARITY_GOLDEN: &[(&str, &str, &str)] = &[
        ("/", "/", "home"),
        ("/items/10243", "/items/*", "product"),
        (
            "/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000",
            "/api/v2/orders/*",
            "api",
        ),
        ("/%61dmin", "/admin", "admin"),
        ("/static/../admin", "/admin", "admin"),
        ("/admin/%2e%2e/items/foo", "/items/foo", "product"),
        ("/search?q=red+shoes&page=2", "/search", "browse"),
        // Encoded `%2F` stays data (finding 014): the slash never separates, so
        // the literal `..` survives inside the segment and the FIRST real
        // segment drives the category — no auth→product evasion.
        ("/auth/login%2F..%2F..%2Fproduct", "/auth/login/../../product", "auth"),
        ("/items/%2e%2e%2fsecret", "/items/../secret", "product"),
        ("/a/b/%2e%2e%2f%2e%2e%2fc", "/a/b/../../c", "other"),
    ];

    #[test]
    fn cross_lang_parity_rust_side() {
        for (url, want_path, want_cat) in PARITY_GOLDEN {
            let r = normalize(url);
            assert_eq!(&r.path, want_path, "{url:?}: path drifted");
            assert_eq!(&r.category, want_cat, "{url:?}: category drifted");
        }
    }

    // One representative URL per category bucket — pins that EVERY value in
    // CATEGORY_MAP (including the otherwise-uncovered "asset") stays reachable
    // and is mirrored identically by the Python _CATEGORY_MAP. Without this a
    // one-sided edit to a less-tested bucket would ship silently. Keep
    // byte-identical with CATEGORY_GOLDEN in
    // tests/scoring/test_normalize_parity.py. Asserts category only.
    const CATEGORY_GOLDEN: &[(&str, &str)] = &[
        ("/", "home"),
        ("/api/status", "api"),
        ("/products/widget", "product"),
        ("/search", "browse"),
        ("/cart", "cart"),
        ("/checkout/step-1", "checkout"),
        ("/account/settings", "account"),
        ("/login", "auth"),
        ("/admin/users", "admin"),
        ("/static/app.js", "asset"),
        ("/blog/post", "content"),
        ("/zzz-unknown", "other"),
    ];

    #[test]
    fn cross_lang_category_golden_rust_side() {
        for (url, want_cat) in CATEGORY_GOLDEN {
            assert_eq!(&normalize(url).category, want_cat, "{url:?}: category drifted");
        }
    }

    #[test]
    fn doc_explicit_examples() {
        // /items/10243 → /items/*
        assert_eq!(normalize("/items/10243").path, "/items/*");
        // /api/v2/orders/<uuid> → /api/v2/orders/*
        assert_eq!(
            normalize("/api/v2/orders/00000abc-1234-5678-9abc-deadbeef0000").path,
            "/api/v2/orders/*"
        );
    }

    #[test]
    fn trivial_paths() {
        assert_eq!(normalize("/").path, "/");
        assert_eq!(normalize("").path, "/");
        assert_eq!(normalize("/home").path, "/home");
    }

    #[test]
    fn dot_segment_collapse() {
        assert_eq!(normalize("/static/../admin").path, "/admin");
        assert_eq!(normalize("/static/../admin").category, "admin");
        assert_eq!(normalize("/a/./b/../c").path, "/a/c");
    }

    #[test]
    fn query_string_stripped() {
        assert_eq!(normalize("/search?q=red+shoes").path, "/search");
        assert_eq!(normalize("/items/42?ref=email").path, "/items/*");
    }

    #[test]
    fn numeric_id_collapse() {
        assert_eq!(normalize("/blog/12345").path, "/blog/*");
        assert_eq!(normalize("/orders/789/items/42").path, "/orders/*/items/*");
    }

    #[test]
    fn uuid_collapse() {
        assert_eq!(
            normalize("/sessions/123e4567-e89b-12d3-a456-426614174000").path,
            "/sessions/*"
        );
    }

    #[test]
    fn hex_hash_collapse() {
        assert_eq!(
            normalize("/jobs/64bc89ff1a2b3c4d5e6f7081").path,
            "/jobs/*"
        );
    }

    #[test]
    fn prefixed_id_collapse() {
        assert_eq!(normalize("/inventory/SKU-12345").path, "/inventory/*");
        assert_eq!(normalize("/orders/ORD-789-ABC").path, "/orders/*");
    }

    #[test]
    fn long_opaque_collapse() {
        assert_eq!(
            normalize("/oauth/callback/abcdef0123456789xyzwAA").path,
            "/oauth/callback/*"
        );
    }

    #[test]
    fn absolute_url_strips_scheme_host() {
        assert_eq!(
            normalize("https://www.example.com/api/v1/users/777?token=abc").path,
            "/api/v1/users/*"
        );
    }

    #[test]
    fn double_slashes_collapse() {
        assert_eq!(normalize("/foo//bar").path, "/foo/bar");
    }

    #[test]
    fn does_not_collapse_short_alphanumeric() {
        // "v2" — too short for LONG_OPAQUE
        assert_eq!(normalize("/api/v2").path, "/api/v2");
        assert_eq!(normalize("/faq").path, "/faq");
        assert_eq!(normalize("/cart").path, "/cart");
    }

    #[test]
    fn does_not_collapse_hyphenated_slug() {
        assert_eq!(normalize("/about-us").path, "/about-us");
        assert_eq!(normalize("/privacy-policy").path, "/privacy-policy");
    }

    #[test]
    fn lowercased() {
        assert_eq!(normalize("/Products/Foo").path, "/products/foo");
    }

    #[test]
    fn category_mapping() {
        assert_eq!(normalize("/").category, "home");
        assert_eq!(normalize("/products/42").category, "product");
        assert_eq!(normalize("/cart").category, "cart");
        assert_eq!(normalize("/checkout/step-1").category, "checkout");
        assert_eq!(normalize("/account/settings").category, "account");
        assert_eq!(normalize("/api/v2/orders/123").category, "api");
        assert_eq!(normalize("/graphql").category, "api");
        assert_eq!(normalize("/login").category, "auth");
        assert_eq!(normalize("/admin/dashboard").category, "admin");
        assert_eq!(normalize("/blog/post").category, "content");
        assert_eq!(normalize("/about-us").category, "other");
    }

    #[test]
    fn percent_decoding_matches_python_normalizer() {
        // Audit finding 013: ensure encoded characters in the path are
        // decoded before category matching + segment collapse so the
        // scorer can't be evaded with `/%61dmin` / `/%2e%2e/`.
        assert_eq!(normalize("/%61dmin").category, "admin");
        assert_eq!(normalize("/a/%2e%2e/b").path, "/b");
    }

    #[test]
    fn embedded_scheme_separator_does_not_truncate_path() {
        // Regression for audit finding 023: an unanchored "://" search
        // in strip_query treated ANY occurrence of "://" as a scheme/host
        // separator, letting an attacker bypass route-specific rules by
        // crafting paths like /admin/delete/http://x/. Now we only strip
        // the prefix when what precedes "://" looks like a valid RFC 3986
        // scheme (starts ascii-alpha, then ascii-alnum/+/-/.).
        assert_eq!(normalize("/admin/delete/http://x/").path, "/admin/delete/http:/x");
        assert_eq!(normalize("/api/v2/orders/file://x/").path, "/api/v2/orders/file:/x");
        // Real absolute URLs still strip correctly.
        assert_eq!(
            normalize("https://www.example.com/api/v1/users/777").path,
            "/api/v1/users/*"
        );
        assert_eq!(normalize("ftp://h/a/b").path, "/a/b");
    }

    #[test]
    fn known_limitation_word_like_user_id() {
        // Documents the deliberate v1 limitation that /users/drew/profile
        // doesn't auto-collapse without per-site route-template config.
        let r = normalize("/users/drew/profile");
        assert_eq!(r.path, "/users/drew/profile");
        assert_eq!(r.category, "account");
    }

    #[test]
    fn finding_011_double_encoded_traversal_resolves() {
        // Single-pass decode would leave %252e%252e as %2e%2e and the
        // segment loop wouldn't see `..` segments to pop. Iterating to
        // fixed-point unwinds the encoding so traversal resolves before
        // segments are evaluated and the category reflects the resolved
        // target rather than the pre-traversal admin path.
        let r = normalize("/admin/%252e%252e/items");
        assert_eq!(r.path, "/items");
        assert_eq!(r.category, "product");

        // Triple-encoded variant — paranoid coverage that the loop
        // converges past two layers.
        let r3 = normalize("/admin/%25252e%25252e/items");
        assert_eq!(r3.path, "/items");
        assert_eq!(r3.category, "product");
    }

    #[test]
    fn finding_014_encoded_slash_is_not_a_separator() {
        // An encoded slash (`%2F`) must NOT act as a path separator, so a
        // `%2F`-hidden traversal can't pop a parent and relabel the route's
        // category (auth → product evasion). The literal `..` survives inside
        // the segment and the FIRST real segment keeps driving the category.
        // This mirrors Python's `test_normalize_finding_014_*` — the edge now
        // enforces the same contract the trainer does.
        let r = normalize("/auth/login%2F..%2F..%2Fproduct");
        assert_eq!(r.path, "/auth/login/../../product");
        assert_eq!(r.category, "auth");

        let r2 = normalize("/items/%2e%2e%2fsecret");
        assert_eq!(r2.path, "/items/../secret");
        assert_eq!(r2.category, "product");

        // A bare encoded slash is data, not a separator.
        let r3 = normalize("/a%2Fb");
        assert_eq!(r3.path, "/a/b");
        assert_eq!(r3.category, "other");
    }
}
