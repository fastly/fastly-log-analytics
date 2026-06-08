//! URL → canonical (route, category) pair.
//!
//! Mirrors `backend/scoring/normalize.py`. Same patterns, same category map,
//! same lowercase-then-collapse order. Anything that normalizes differently
//! between Python and Rust would corrupt matrix lookups at runtime — there's
//! a cross-language fixture test in `tests/cross_lang_normalize.rs`.

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
        // Look for the FIRST '/' after the scheme separator.
        let rest = &path[idx + 3..];
        if let Some(slash) = rest.find('/') {
            return &rest[slash..];
        }
        return "/";
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
    if let Some(idx) = segment.find(|c: char| c == '-' || c == '_') {
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

/// Normalize a URL to its (canonical route, category) pair.
pub fn normalize(url: &str) -> Route {
    let path = strip_query(url);
    if path.is_empty() || path == "/" {
        return Route {
            path: "/".to_string(),
            category: "home".to_string(),
        };
    }
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Route {
            path: "/".to_string(),
            category: "home".to_string(),
        };
    }

    let normalized: Vec<String> = segments
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
    let category = category_for(segments[0]).to_string();
    Route {
        path: canonical,
        category,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn known_limitation_word_like_user_id() {
        // Documents the deliberate v1 limitation that /users/drew/profile
        // doesn't auto-collapse without per-site route-template config.
        let r = normalize("/users/drew/profile");
        assert_eq!(r.path, "/users/drew/profile");
        assert_eq!(r.category, "account");
    }
}
