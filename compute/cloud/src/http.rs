use std::collections::BTreeMap;

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
        query.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == name).then(|| value.to_owned())
        })
    }

    pub fn cookie(&self, name: &str) -> Option<String> {
        compute_common::http::extract_cookie_value(self.header("cookie")?, name).map(str::to_owned)
    }
}

pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
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

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.insert(name.to_owned(), value.into());
        self
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

    /// Headers every Compute response carries. The CSP applies to scripts and workers too:
    /// a dedicated worker is governed by its own script response's policy.
    pub fn secured(self) -> Self {
        self.header(
            "content-security-policy",
            crate::csp::policy(crate::INLINE_SCRIPT_HASHES),
        )
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
    fn cookies_do_not_match_names_with_a_shared_suffix() {
        let mut request = Request::default();
        request.headers.insert(
            "cookie".into(),
            "not_flp_session=attacker; flp_session=verified; other=third".into(),
        );
        assert_eq!(request.cookie("flp_session").as_deref(), Some("verified"));
        assert_eq!(request.cookie("missing"), None);
    }
}
