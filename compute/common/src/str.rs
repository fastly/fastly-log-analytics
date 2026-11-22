/// Returns the longest prefix of `s` with length `<= max_bytes` that ends on a valid UTF-8
/// character boundary.
#[inline]
pub fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Truncates `value` in-place to at most `max_bytes` on a valid UTF-8 character boundary.
#[inline]
pub fn truncate_string_utf8(value: &mut String, max_bytes: usize) {
    if value.len() > max_bytes {
        let len = truncate_utf8(value, max_bytes).len();
        value.truncate(len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncates_multibyte_utf8_at_char_boundary() {
        let text = "aé😊b";
        assert_eq!(truncate_utf8(text, 2), "a");
        assert_eq!(truncate_utf8(text, 3), "aé");
        assert_eq!(truncate_utf8(text, 6), "aé");
        assert_eq!(truncate_utf8(text, 7), "aé😊");

        let mut owned = text.to_owned();
        truncate_string_utf8(&mut owned, 5);
        assert_eq!(owned, "aé");
    }
}
