use std::collections::BTreeMap;

pub const PUBLIC_BODY_LIMIT: usize = 65_536;
/// Peer requests carry the ingest watermark root, which outgrows the public cap.
pub const INTERNAL_BODY_LIMIT: usize = 2 * 1024 * 1024;

pub fn is_internal_body_route(request: &Request) -> bool {
    request.method == "POST"
        && [
            crate::ingest::worker::PATH,
            crate::ingest::worker::RANGE_PATH,
        ]
        .contains(&request.path.as_str())
}

/// Bytes the entrypoint reads before handlers apply their own, tighter limits.
/// Only grants `INTERNAL_BODY_LIMIT` after verifying `x-flp-worker-key` in
/// constant time against `worker_secret` so unauthenticated callers cannot
/// amplify request-body buffering to 2 MiB via header presence alone.
pub fn body_limit_for_runtime<R: crate::shell::Runtime>(runtime: &R, request: &Request) -> usize {
    if is_internal_body_route(request) && crate::ingest::worker::authorized(runtime, request) {
        INTERNAL_BODY_LIMIT
    } else {
        PUBLIC_BODY_LIMIT
    }
}

/// Strict pre-auth body limit when no verified worker runtime is available.
pub fn body_limit(_request: &Request) -> usize {
    PUBLIC_BODY_LIMIT
}

#[derive(Default)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    pub fn path_only(&self) -> &str {
        self.path
            .split_once('?')
            .map_or(self.path.as_str(), |(path, _)| path)
    }

    pub fn query_param(&self, name: &str) -> Option<String> {
        let (_, query) = self.path.split_once('?')?;
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.into_owned())
            .filter(|value| !value.is_empty())
    }

    pub fn cookie(&self, name: &str) -> Option<String> {
        compute_common::http::extract_cookie_value(self.header("cookie")?, name).map(str::to_owned)
    }

    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }
}

pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub set_cookies: Vec<String>,
    pub body: Vec<u8>,
    /// A body to stream from the front's `/fos/<service>/<key>` path when the response is sent,
    /// replacing `body`; the upstream's status, length and range headers pass through.
    pub upstream: Option<Box<Upstream>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upstream {
    pub service_id: String,
    pub key: String,
    pub cdn_key: String,
    /// The client's `Range` header, sent on unchanged.
    pub range: Option<String>,
    pub head_only: bool,
}

impl Response {
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: BTreeMap::new(),
            set_cookies: Vec::new(),
            body: body.into(),
            upstream: None,
        }
    }

    pub fn json(status: u16, value: &serde_json::Value) -> Self {
        Self::new(status, value.to_string())
            .header("content-type", "application/json")
            .private()
            .compress_hint()
    }

    pub fn json_error(status: u16, code: &str) -> Self {
        Self::json(status, &serde_json::json!({"error": code}))
    }

    pub fn streamed(upstream: Upstream) -> Self {
        Self {
            upstream: Some(Box::new(upstream)),
            ..Self::new(200, Vec::new())
        }
    }

    pub fn cookie(mut self, value: impl Into<String>) -> Self {
        let value = value.into();
        self.headers
            .entry("set-cookie".to_owned())
            .or_insert_with(|| value.clone());
        if !self.set_cookies.contains(&value) {
            self.set_cookies.push(value);
        }
        self
    }

    pub fn cookies(&self) -> &[String] {
        &self.set_cookies
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        if name.eq_ignore_ascii_case("set-cookie") {
            return self.cookie(value);
        }
        self.headers.insert(name.to_owned(), value.into());
        self
    }

    /// Appends `name;dur=<ms>` entries to any existing `Server-Timing` value.
    pub fn server_timing(mut self, phases: &[(&str, std::time::Duration)]) -> Self {
        let added = phases
            .iter()
            .map(|(name, dur)| format!("{name};dur={:.1}", dur.as_secs_f64() * 1000.0))
            .collect::<Vec<_>>()
            .join(", ");
        if added.is_empty() {
            return self;
        }
        let value = match self.headers.remove("server-timing") {
            Some(existing) if !existing.is_empty() => format!("{existing}, {added}"),
            _ => added,
        };
        self.header("server-timing", value)
    }

    pub fn private(self) -> Self {
        self.header("cache-control", "private, no-store")
            .header("vary", "Cookie")
    }

    /// Marks unencoded JSON bodies of at least 1 KiB so the front VCL gzips them.
    pub fn compress_hint(self) -> Self {
        let json = self
            .headers
            .get("content-type")
            .is_some_and(|value| value.starts_with("application/json"));
        if json && self.body.len() >= 1024 && !self.headers.contains_key("content-encoding") {
            self.header("x-compress-hint", "on")
        } else {
            self
        }
    }

    /// Headers every Compute response carries. Inline script hashes are attached only to
    /// HTML/script responses so JSON `/api/*` responses stay compact under HTTP header limits.
    /// A dedicated worker is governed by its own script response's policy.
    pub fn secured(self) -> Self {
        let csp = csp_header(
            self.headers.get("content-type").map(String::as_str),
            crate::INLINE_SCRIPT_HASHES,
        );
        self.header("content-security-policy", csp)
            .header(
                "strict-transport-security",
                "max-age=31536000; includeSubDomains",
            )
            .header("x-content-type-options", "nosniff")
            .header("referrer-policy", "same-origin")
    }

    pub fn redirect(location: &str) -> Self {
        Self::new(303, Vec::new())
            .header("location", location)
            .private()
    }
}

fn csp_header(content_type: Option<&str>, script_hashes: &[&str]) -> String {
    let needs_inline_hashes = content_type.is_some_and(|ct| {
        let mime = ct.split(';').next().unwrap_or("").trim();
        mime.eq_ignore_ascii_case("text/html")
            || mime.eq_ignore_ascii_case("application/javascript")
            || mime.eq_ignore_ascii_case("text/javascript")
    });
    crate::csp::policy(if needs_inline_hashes {
        script_hashes
    } else {
        &[]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_unencoded_json_over_a_kilobyte_gets_the_compress_hint() {
        let hint = |response: Response| {
            response
                .compress_hint()
                .headers
                .get("x-compress-hint")
                .cloned()
        };
        let large = "x".repeat(1024);
        assert_eq!(
            hint(Response::new(200, large.clone()).header("content-type", "application/json")),
            Some("on".to_owned())
        );
        assert_eq!(
            hint(Response::new(200, "{}").header("content-type", "application/json")),
            None
        );
        assert_eq!(
            hint(
                Response::new(200, large.clone())
                    .header("content-type", "application/vnd.apache.parquet")
            ),
            None
        );
        assert_eq!(
            hint(
                Response::new(200, large)
                    .header("content-type", "application/json")
                    .header("content-encoding", "gzip")
            ),
            None
        );
    }

    #[test]
    fn every_response_carries_the_enforced_csp_and_transport_headers() {
        let response = Response::new(200, "<html></html>")
            .header("content-type", "text/html")
            .secured();
        let csp = &response.headers["content-security-policy"];
        assert_eq!(csp, &crate::csp::policy(crate::INLINE_SCRIPT_HASHES));
        for hash in crate::INLINE_SCRIPT_HASHES {
            assert!(csp.contains(&format!(" '{hash}'")));
        }
        assert!(!response
            .headers
            .contains_key("content-security-policy-report-only"));
        assert_eq!(
            response.headers["strict-transport-security"],
            "max-age=31536000; includeSubDomains"
        );
        assert_eq!(response.headers["x-content-type-options"], "nosniff");
        assert_eq!(response.headers["referrer-policy"], "same-origin");
    }

    #[test]
    fn non_html_non_script_responses_omit_inline_script_hashes_from_csp() {
        let hashes = &["sha256-inlineScriptHashForTest="];
        let json_csp = csp_header(Some("application/json"), hashes);
        assert_eq!(json_csp, crate::csp::policy(&[]));
        assert!(!json_csp.contains("sha256-inlineScriptHashForTest="));

        let none_csp = csp_header(None, hashes);
        assert_eq!(none_csp, crate::csp::policy(&[]));

        for content_type in [
            "text/html",
            "text/html; charset=utf-8",
            "application/javascript",
            "text/javascript",
        ] {
            let ct_csp = csp_header(Some(content_type), hashes);
            assert_eq!(ct_csp, crate::csp::policy(hashes));
            assert!(ct_csp.contains("sha256-inlineScriptHashForTest="));
        }

        let json_response = Response::json(200, &serde_json::json!({"ok": true})).secured();
        assert_eq!(
            json_response.headers["content-security-policy"],
            crate::csp::policy(&[])
        );

        let js_response = Response::new(200, "console.log(1)")
            .header("content-type", "application/javascript")
            .secured();
        assert_eq!(
            js_response.headers["content-security-policy"],
            crate::csp::policy(crate::INLINE_SCRIPT_HASHES)
        );
    }

    #[test]
    fn cookies_do_not_match_names_with_a_shared_suffix() {
        let mut request = Request::default();
        request.headers.insert(
            "cookie".into(),
            "not_flp_session=attacker; flp_session=verified; other=third".into(),
        );
        assert_eq!(request.cookie("flp_session").as_deref(), Some("verified"));
        assert_eq!(request.cookie("missing"), None);
    }

    #[test]
    fn requests_and_responses_share_json_encoding() {
        #[derive(Debug, PartialEq, serde::Deserialize)]
        struct Payload {
            count: u32,
        }

        let response = Response::json(200, &serde_json::json!({"count": 3}));
        let request = Request {
            body: response.body.clone(),
            ..Default::default()
        };

        assert_eq!(request.json::<Payload>().unwrap(), Payload { count: 3 });
        assert_eq!(response.headers["content-type"], "application/json");
        assert!(response.headers.contains_key("cache-control"));
    }

    #[test]
    fn query_params_are_decoded_and_empty_values_are_ignored() {
        let request = Request {
            path: "/api?service_id=hello%20world&limit=&filter=a%2Bb".into(),
            ..Default::default()
        };

        assert_eq!(
            request.query_param("service_id").as_deref(),
            Some("hello world")
        );
        assert_eq!(request.query_param("limit"), None);
        assert_eq!(request.query_param("filter").as_deref(), Some("a+b"));
    }

    #[test]
    fn multiple_set_cookie_headers_preserve_order_and_first_map_entry() {
        let a = "flp_session=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax";
        let b = "flp_cdn=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax";
        let c = "__Host-flp_cdn=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax";
        let response = Response::new(200, "ok")
            .cookie(a)
            .cookie(b)
            .cookie(a)
            .header("Set-Cookie", c);

        assert_eq!(
            response.cookies(),
            &[a.to_owned(), b.to_owned(), c.to_owned()]
        );
        assert_eq!(
            response.headers.get("set-cookie").map(String::as_str),
            Some(a)
        );
    }
}
