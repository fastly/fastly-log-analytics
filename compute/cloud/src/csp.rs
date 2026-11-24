//! Content-Security-Policy for the static export. Also compiled into build.rs, which hashes
//! every inline script of the exported HTML, so this module must not depend on the crate.

use base64::Engine;
use sha2::{Digest, Sha256};

/// Bodies of `<script>` elements without `src`, as the exact bytes between the start tag and
/// `</script`. Script is a raw-text element, so the browser hashes these bytes undecoded.
pub fn inline_scripts(html: &[u8]) -> Vec<&[u8]> {
    let mut scripts = Vec::new();
    let mut at = 0;
    while let Some(start) = find_ci(html, at, b"<script") {
        at = start + b"<script".len();
        if !matches!(
            html.get(at),
            Some(b'>' | b'/') | Some(b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
        ) {
            continue;
        }
        let Some((body_start, has_src)) = start_tag_end(html, at) else {
            break;
        };
        let Some(end) = find_ci(html, body_start, b"</script") else {
            break;
        };
        if !has_src {
            scripts.push(&html[body_start..end]);
        }
        at = end;
    }
    scripts
}

fn find_ci(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
        .map(|offset| from + offset)
}

/// Walks the start tag's attributes, honoring quoted values that may contain `>`.
fn start_tag_end(html: &[u8], mut at: usize) -> Option<(usize, bool)> {
    let mut has_src = false;
    loop {
        while html.get(at)?.is_ascii_whitespace() || html[at] == b'/' {
            at += 1;
        }
        if html[at] == b'>' {
            return Some((at + 1, has_src));
        }
        let name_start = at;
        while !matches!(html.get(at)?, b'=' | b'>' | b'/') && !html[at].is_ascii_whitespace() {
            at += 1;
        }
        has_src |= html[name_start..at].eq_ignore_ascii_case(b"src");
        while html.get(at)?.is_ascii_whitespace() {
            at += 1;
        }
        if html[at] != b'=' {
            continue;
        }
        at += 1;
        while html.get(at)?.is_ascii_whitespace() {
            at += 1;
        }
        match html[at] {
            quote @ (b'"' | b'\'') => {
                at += 1;
                at += html.get(at..)?.iter().position(|&byte| byte == quote)? + 1;
            }
            _ => {
                while !html.get(at)?.is_ascii_whitespace() && html[at] != b'>' {
                    at += 1;
                }
            }
        }
    }
}

pub fn script_hash(body: &[u8]) -> String {
    format!(
        "sha256-{}",
        base64::engine::general_purpose::STANDARD.encode(Sha256::digest(body))
    )
}

/// Enforced policy. Inline scripts are allowed only by hash; DuckDB needs `'wasm-unsafe-eval'`
/// (never plain unsafe-eval). DuckDB's parquet extension is served from `/duckdb/`, so nothing
/// connects to a third-party origin.
pub fn policy(script_hashes: &[&str]) -> String {
    let mut script_src = String::from("'self' 'wasm-unsafe-eval'");
    for hash in script_hashes {
        script_src.push_str(&format!(" '{hash}'"));
    }
    format!(
        "default-src 'self'; script-src {script_src}; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data: blob:; font-src 'self' data:; connect-src 'self'; \
         worker-src 'self' blob:; frame-ancestors 'none'; base-uri 'self'; \
         form-action 'self'; object-src 'none'"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_only_inline_script_bodies_byte_for_byte() {
        let html = br#"<html><head><script src="/a.js" async=""></script><SCRIPT id="_R_">self.x="a>b"&amp;</SCRIPT ><script data-x='>src='>1</script><script nosrc>2</script></head></html>"#;
        let scripts: Vec<_> = inline_scripts(html);
        assert_eq!(
            scripts,
            vec![&b"self.x=\"a>b\"&amp;"[..], &b"1"[..], &b"2"[..]]
        );
    }

    #[test]
    fn unterminated_script_is_ignored() {
        assert!(inline_scripts(b"<script>alert(1)").is_empty());
        assert!(inline_scripts(b"<scripts>x</scripts>").is_empty());
    }

    #[test]
    fn hash_matches_the_csp_source_expression_format() {
        // printf 'alert(1)' | openssl dgst -sha256 -binary | base64
        assert_eq!(
            script_hash(b"alert(1)"),
            "sha256-bhHHL3z2vDgxUt0W3dWQOrprscmda2Y5pLsLg4GF+pI="
        );
    }

    #[test]
    fn policy_enforces_self_with_hashes_and_wasm() {
        let policy = policy(&["sha256-a=", "sha256-b="]);
        assert!(policy.contains("script-src 'self' 'wasm-unsafe-eval' 'sha256-a=' 'sha256-b='; "));
        for directive in [
            "default-src 'self'",
            "style-src 'self' 'unsafe-inline'",
            "worker-src 'self' blob:",
            "connect-src 'self';",
            "frame-ancestors 'none'",
            "base-uri 'self'",
            "object-src 'none'",
        ] {
            assert!(policy.contains(directive), "{directive} missing: {policy}");
        }
        assert!(!policy.contains(concat!("'unsafe-", "eval'")));
        assert!(!policy.contains("script-src 'self' 'unsafe-inline'"));
        assert!(
            !policy.contains("https:"),
            "no third-party origin: {policy}"
        );
    }
}
