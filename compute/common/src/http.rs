/// Extracts the non-empty value of cookie `name` from a raw `Cookie` header string.
pub fn extract_cookie_value<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    cookie_header.split(';').find_map(|part| {
        let (k, v) = part.trim().split_once('=')?;
        (k.trim() == name)
            .then(|| v.trim())
            .filter(|v| !v.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_named_cookie_and_skips_empty_values() {
        assert_eq!(
            extract_cookie_value("foo=bar; flp_session=abc.def; baz=1", "flp_session"),
            Some("abc.def")
        );
        assert_eq!(
            extract_cookie_value("flp_session=; foo=bar", "flp_session"),
            None
        );
        assert_eq!(extract_cookie_value("foo=bar", "flp_session"), None);
    }
}
