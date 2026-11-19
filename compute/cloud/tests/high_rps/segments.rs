use super::support::*;
use fastly_log_analytics::ingest::convert::{self, RequestSchema, Segment};
use std::{cell::Cell, collections::BTreeSet};

#[test]
fn segments_count_every_line_exactly_once_for_gzip_and_zstd() {
    let (text, ids) = ndjson(3000);
    let total = text.len() as u64;
    let whole = convert::convert_object(&gz(&text), 60, BASE).unwrap();
    // Cuts on line starts, mid-line, inside the over-long line, past the end and at 0.
    let first_line = text.iter().position(|&b| b == b'\n').unwrap() as u64 + 1;
    let long_at = text.windows(4).position(|w| w == b"xxxx").unwrap() as u64;
    let cut_sets: Vec<Vec<u64>> = vec![
        vec![first_line],
        vec![1, 2, 3],
        vec![total / 7, total / 3, long_at + 5, total / 2, total - 1],
        vec![0, total / 2, total, total + 100],
        (1..40).map(|i| i * total / 40 + i % 3).collect(),
    ];
    for object in [gz(&text), zst(&text)] {
        for cuts in &cut_sets {
            let (mut seen, lines, bad) = convert_segments(&object, cuts);
            assert_eq!(lines, whole.lines_read, "cuts {cuts:?}");
            assert_eq!(bad, whole.bad_lines);
            assert_eq!(seen.len(), ids.len(), "cuts {cuts:?}");
            seen.sort();
            assert_eq!(seen, ids);
            assert_eq!(seen.iter().collect::<BTreeSet<_>>().len(), ids.len());
        }
    }
}

#[test]
fn segment_content_cap_and_measured_length() {
    let (text, _) = ndjson(2000);
    let object = gz(&text);
    let total = text.len() as u64;
    assert_eq!(
        convert::decompressed_len(&object, total, &|| false),
        Ok(total)
    );
    assert_eq!(
        convert::decompressed_len(&object, total - 1, &|| false),
        Err(convert::ConvertError::TooLarge)
    );
    let open = |start, cap| {
        convert::convert_object_range(
            &object,
            60,
            BASE,
            Segment { start, end: None },
            cap,
            RequestSchema::base(),
            &|| false,
        )
    };
    // The cap counts only the segment's own bytes, not the skipped prefix.
    assert!(open(total / 2, total / 2 + 1024).is_ok());
    assert_eq!(
        open(1, total / 2).err(),
        Some(convert::ConvertError::TooLarge)
    );
    let mut broken = object.clone();
    let mid = broken.len() / 2;
    broken[mid..mid + 64].fill(0);
    assert!(convert::decompressed_len(&broken, u64::MAX, &|| false).is_err());
}

#[test]
fn prefix_decompression_checks_its_budget_every_checkpoint_and_stops() {
    let (text, _) = ndjson(40_000);
    let total = text.len() as u64;
    assert!(total > 3 * convert::CHECKPOINT_BYTES);
    let tail = Segment {
        start: total - 1000,
        end: None,
    };
    for object in [gz(&text), zst(&text)] {
        let checks = Cell::new(0u64);
        let never = || {
            checks.set(checks.get() + 1);
            false
        };
        assert_eq!(convert::decompressed_len(&object, total, &never), Ok(total));
        assert!(checks.get() >= total / convert::CHECKPOINT_BYTES);
        checks.set(0);
        let rows = convert::convert_object_range(
            &object,
            60,
            BASE,
            tail,
            convert::MAX_DECOMPRESSED_BYTES,
            RequestSchema::base(),
            &never,
        )
        .unwrap();
        assert!(rows.lines_read > 0);
        assert!(checks.get() >= total / convert::CHECKPOINT_BYTES);

        let second = || {
            checks.set(checks.get() + 1);
            checks.get() >= 2
        };
        checks.set(0);
        assert_eq!(
            convert::decompressed_len(&object, total, &second),
            Err(convert::ConvertError::Stopped)
        );
        assert_eq!(checks.get(), 2);
        checks.set(0);
        assert_eq!(
            convert::convert_object_range(
                &object,
                60,
                BASE,
                tail,
                convert::MAX_DECOMPRESSED_BYTES,
                RequestSchema::base(),
                &second,
            )
            .err(),
            Some(convert::ConvertError::Stopped)
        );
        assert_eq!(checks.get(), 2);
    }
}
