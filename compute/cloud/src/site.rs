pub struct Asset {
    pub path: &'static str,
    pub content_type: &'static str,
    pub identity: Option<&'static [u8]>,
    pub gzip: Option<&'static [u8]>,
    pub br: Option<&'static [u8]>,
}

impl Asset {
    fn is_static_bundle(&self) -> bool {
        self.path.starts_with("/_next/static/")
            || self.path.starts_with("/duckdb/")
            || self.path.starts_with("/geo/")
            || self.path.starts_with("/maplibre-gl-")
    }

    pub fn cache_control(&self) -> &'static str {
        if self.is_static_bundle() {
            "public, max-age=31536000, immutable"
        } else {
            "public, max-age=60"
        }
    }

    pub fn surrogate_key(&self) -> &'static str {
        if self.is_static_bundle() {
            "flp-static"
        } else {
            "flp-html"
        }
    }

    pub fn varies_by_encoding(&self) -> bool {
        self.gzip.is_some() || self.br.is_some()
    }

    /// Picks the build-time variant the client accepts, preferring Brotli, then gzip, then
    /// identity. `None` means no stored variant is acceptable; Compute never decodes.
    pub fn negotiate(
        &self,
        accept_encoding: Option<&str>,
    ) -> Option<(&'static [u8], Option<&'static str>)> {
        if let Some(body) = self.br.filter(|_| accepts(accept_encoding, "br")) {
            return Some((body, Some("br")));
        }
        if let Some(body) = self.gzip.filter(|_| accepts(accept_encoding, "gzip")) {
            return Some((body, Some("gzip")));
        }
        self.identity.map(|body| (body, None))
    }
}

fn accepts(accept_encoding: Option<&str>, wanted: &str) -> bool {
    let Some(value) = accept_encoding else {
        return false;
    };
    value.split(',').any(|part| {
        let mut segments = part.trim().split(';');
        let coding = segments.next().unwrap_or("").trim();
        if !coding.eq_ignore_ascii_case(wanted) && coding != "*" {
            return false;
        }
        !segments.any(|param| {
            let param = param.trim();
            param
                .strip_prefix("q=")
                .or_else(|| param.strip_prefix("Q="))
                .and_then(|q| q.trim().parse::<f32>().ok())
                .is_some_and(|q| q == 0.0)
        })
    })
}

pub fn resolve<'a>(assets: &'a [Asset], path: &str) -> Option<&'a Asset> {
    if !path.starts_with('/')
        || path.contains('%')
        || path.contains('\\')
        || path.split('/').any(|part| part == "." || part == "..")
    {
        return None;
    }
    let path = path.trim_end_matches('/');
    let html = if path.is_empty() {
        "/index.html".to_owned()
    } else {
        format!("{path}.html")
    };
    assets
        .iter()
        .find(|asset| asset.path == path || asset.path == html)
}

#[cfg(test)]
mod tests {
    use super::*;

    static ASSETS: &[Asset] = &[
        Asset {
            path: "/index.html",
            identity: Some(b"home"),
            gzip: None,
            br: None,
            content_type: "text/html",
        },
        Asset {
            path: "/admin.html",
            identity: Some(b"admin"),
            gzip: None,
            br: None,
            content_type: "text/html",
        },
        Asset {
            path: "/404.html",
            identity: Some(b"not found"),
            gzip: None,
            br: None,
            content_type: "text/html",
        },
        Asset {
            path: "/_next/static/chunk-abc.js",
            identity: Some(b"script"),
            gzip: None,
            br: None,
            content_type: "application/javascript",
        },
    ];

    #[test]
    fn static_routes_resolve_exported_html_but_not_traversal() {
        assert_eq!(resolve(ASSETS, "/").unwrap().identity, Some(&b"home"[..]));
        assert_eq!(
            resolve(ASSETS, "/admin/").unwrap().identity,
            Some(&b"admin"[..])
        );
        assert!(resolve(ASSETS, "/../admin").is_none());
        assert!(resolve(ASSETS, "/%2e%2e/admin").is_none());
        assert!(resolve(ASSETS, "/missing").is_none());
    }

    #[test]
    fn html_and_hashed_assets_have_distinct_caching() {
        let html = resolve(ASSETS, "/admin").unwrap();
        assert_eq!(html.cache_control(), "public, max-age=60");
        assert_eq!(html.surrogate_key(), "flp-html");
        let script = resolve(ASSETS, "/_next/static/chunk-abc.js").unwrap();
        assert_eq!(
            script.cache_control(),
            "public, max-age=31536000, immutable"
        );
        assert_eq!(script.surrogate_key(), "flp-static");
    }

    static ENCODED: Asset = Asset {
        path: "/_next/static/chunk-abc.js",
        content_type: "application/javascript",
        identity: Some(b"script"),
        gzip: Some(b"gz-script"),
        br: Some(b"br-script"),
    };

    static WASM: Asset = Asset {
        path: "/duckdb/1.0.0/duckdb-eh.wasm",
        content_type: "application/wasm",
        identity: None,
        gzip: Some(b"gz-wasm"),
        br: Some(b"br-wasm"),
    };

    #[test]
    fn precompressed_variants_are_chosen_by_accept_encoding() {
        assert_eq!(
            ENCODED.negotiate(Some("gzip, deflate, br")),
            Some((&b"br-script"[..], Some("br")))
        );
        assert_eq!(
            ENCODED.negotiate(Some("br")),
            Some((&b"br-script"[..], Some("br")))
        );
        assert_eq!(
            ENCODED.negotiate(Some("br;q=0, gzip")),
            Some((&b"gz-script"[..], Some("gzip")))
        );
        assert_eq!(
            ENCODED.negotiate(Some("gzip")),
            Some((&b"gz-script"[..], Some("gzip")))
        );
        assert_eq!(
            ENCODED.negotiate(Some("*")),
            Some((&b"br-script"[..], Some("br")))
        );
        assert_eq!(ENCODED.negotiate(None), Some((&b"script"[..], None)));
        assert_eq!(
            ENCODED.negotiate(Some("gzip;q=0")),
            Some((&b"script"[..], None))
        );
        assert!(ENCODED.varies_by_encoding());
    }

    #[test]
    fn missing_variants_fall_back_and_never_decode_in_compute() {
        let gzip_only = Asset { br: None, ..WASM };
        assert_eq!(
            gzip_only.negotiate(Some("gzip, br")),
            Some((&b"gz-wasm"[..], Some("gzip")))
        );
        assert_eq!(
            WASM.negotiate(Some("br")),
            Some((&b"br-wasm"[..], Some("br")))
        );
        assert_eq!(WASM.negotiate(None), None);
        assert_eq!(WASM.negotiate(Some("deflate")), None);
        assert_eq!(WASM.cache_control(), "public, max-age=31536000, immutable");
        assert_eq!(WASM.surrogate_key(), "flp-static");

        let plain = &ASSETS[0];
        assert_eq!(
            plain.negotiate(Some("gzip, br")),
            Some((&b"home"[..], None))
        );
        assert!(!plain.varies_by_encoding());
    }
}
