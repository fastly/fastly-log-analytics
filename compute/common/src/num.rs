/// Banker's rounding (round half to even) matching Python's `round()` and Rust 1.77+
/// `f64::round_ties_even`.
#[inline]
pub fn round_ties_even(x: f64) -> f64 {
    x.round_ties_even()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banker_rounding_matches_half_to_even() {
        assert_eq!(round_ties_even(2.5), 2.0);
        assert_eq!(round_ties_even(3.5), 4.0);
        assert_eq!(round_ties_even(-2.5), -2.0);
        assert_eq!(round_ties_even(-3.5), -4.0);
        assert_eq!(round_ties_even(2.51), 3.0);
    }
}
