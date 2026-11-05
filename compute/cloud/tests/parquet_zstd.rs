//! Every page the shared Parquet writer emits is a ZSTD level-1 frame. The footer only records
//! the codec, so the test recompresses each page frame and compares bytes.

use fastly_log_analytics::ingest::convert;
use parquet::file::{metadata::ParquetMetaDataReader, reader::ChunkReader};
use serde_json::json;

const BASE: i64 = 1_800_000_000;
const STATUS: [u16; 5] = [200, 200, 304, 404, 503];

fn object() -> Vec<u8> {
    let mut text = Vec::new();
    for i in 0..20_000i64 {
        let line = json!({
            "timestamp": fastly_log_analytics::ingest::rfc3339(BASE + i % 60),
            "ip": format!("10.{}.{}.{}", i % 7, (i * 31) % 251, (i * 17) % 253),
            "status": STATUS[(i % 5) as usize],
            "url": format!("/assets/{}/item-{}.js?v={}", i % 13, (i * 7919) % 4099, i % 97),
            "fastly_req_id": format!("{:032x}", (i as u128).wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c834)),
        });
        text.extend_from_slice(line.to_string().as_bytes());
        text.push(b'\n');
    }
    zstd::stream::encode_all(&text[..], 1).unwrap()
}

#[test]
fn window_pages_are_zstd_level_one_frames() {
    let rows = convert::convert_object(&object(), 60, BASE).unwrap();
    let bytes = bytes::Bytes::from(
        convert::write_parquet(&rows.windows.values().collect::<Vec<_>>()).unwrap(),
    );
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .unwrap();

    let (mut frames, mut differs_from_three) = (0, false);
    for group in metadata.row_groups() {
        for column in group.columns() {
            let start = column
                .dictionary_page_offset()
                .unwrap_or(column.data_page_offset()) as u64;
            let chunk = bytes
                .get_bytes(start, column.compressed_size() as usize)
                .unwrap();
            let mut at = 0;
            while at + 4 <= chunk.len() {
                if chunk[at..at + 4] != [0x28, 0xb5, 0x2f, 0xfd] {
                    at += 1;
                    continue;
                }
                let len = zstd::zstd_safe::find_frame_compressed_size(&chunk[at..]).unwrap();
                let frame = &chunk[at..at + len];
                let raw = zstd::stream::decode_all(frame).unwrap();
                let one = zstd::bulk::compress(&raw, 1).unwrap();
                assert!(
                    one == frame,
                    "column {} page at {at} is not a level-1 frame ({} bytes, level 1 gives {})",
                    column.column_path(),
                    frame.len(),
                    one.len()
                );
                differs_from_three |= zstd::bulk::compress(&raw, 3).unwrap() != frame;
                frames += 1;
                at += len;
            }
        }
    }
    assert!(frames > 0);
    assert!(differs_from_three);
}
