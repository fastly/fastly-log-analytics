use super::support::*;
use fastly_log_analytics::ingest::{self, convert::Segment, worker};
use serde_json::json;

#[test]
fn large_objects_split_into_segments_convert_every_line_once() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let minute = BASE;
    let mut expected = put_object(&fake, minute, "a-small", 50, 10, false);
    expected.extend(put_object(&fake, minute, "b-big", 6000, 40, false));
    expected.extend(put_object(&fake, minute, "c-big", 5000, 40, true));
    expected.extend(put_object(&fake, minute, "d-small", 30, 10, false));
    let big = raw_key(minute, "b-big", "zst");
    assert!(fake.bucket.borrow()[&big].len() * 8 > 2 * 64 * 1024);
    tick(&fake).unwrap();
    let first = plan(&fake);
    assert_eq!(first["version"], 3, "{first}");
    assert!(first["segments"].as_object().unwrap().len() >= 4);
    drain(&fake, 10);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert!(fake.cdn_gets.borrow().iter().filter(|k| **k == big).count() >= 2);
    let queued = deleteq_keys(&fake);
    assert_eq!(queued.iter().filter(|k| **k == big).count(), 1);
    assert_eq!(queued.len(), 4);
    assert!(fake.paths.borrow().iter().any(|p| p == worker::RANGE_PATH));
}

#[test]
fn small_objects_keep_version_two_plans_and_the_whole_object_path() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let expected = put_object(&fake, BASE, "a", 40, 10, false);
    tick(&fake).unwrap();
    assert_eq!(plan(&fake)["version"], 2);
    assert!(plan(&fake).get("segments").is_none());
    drain(&fake, 4);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert!(fake.paths.borrow().iter().all(|p| p == worker::PATH));
}

#[test]
fn an_object_that_decompresses_past_the_cap_is_measured_and_segmented() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let expected = put_object(&fake, BASE, "dense", 400, 4000, false);
    let key = raw_key(BASE, "dense", "zst");
    let gz_len = fake.bucket.borrow()[&key].len();
    assert!(
        gz_len * 8 <= 2 * 64 * 1024,
        "gz {gz_len} would be pre-segmented"
    );
    tick(&fake).unwrap();
    let after = plan(&fake);
    assert_eq!(after["version"], 3, "{after}");
    assert!(after["segments"].as_object().unwrap().len() >= 10);
    drain(&fake, 10);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert_eq!(deleteq_keys(&fake), vec![key]);
}

#[test]
fn crashes_and_lost_replies_mid_split_neither_lose_nor_duplicate_lines() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let mut expected = put_object(&fake, BASE, "big", 6000, 40, false);
    expected.extend(put_object(&fake, BASE + 60, "next", 3000, 40, false));
    // The first segment writes but its reply is lost; the second never runs.
    fake.faults.borrow_mut().extend([
        Fault::LostReply,
        Fault::Unreachable,
        Fault::Ok,
        Fault::LostReply,
    ]);
    tick(&fake).unwrap();
    assert!(!committed(&fake));
    // The next tick dies after re-taking the lease, before any worker answers.
    fake.crash_after.set(Some(4));
    assert!(tick(&fake).is_err());
    fake.crash_after.set(None);
    drain(&fake, 12);
    let got = processed_ips(&fake);
    assert_eq!(got.len(), expected.len());
    assert_eq!(got, sorted(expected));
}

#[test]
fn a_segment_whose_prefix_skip_outlasts_the_worker_stop_fails_visibly_and_keeps_its_raw() {
    let fake = Fake::new();
    let (text, _) = ndjson(40_000);
    let key = raw_key(BASE, "skip", "zst");
    let body = gz(&text);
    fake.bucket.borrow_mut().insert(key.clone(), body.clone());
    let mut shard = shard_request(std::slice::from_ref(&key));
    shard.segment = Some(Segment {
        start: text.len() as u64 - 1000,
        end: None,
    });
    shard.segment_bytes = 64 * 1024;
    fake.stall_after_get.set(true);
    let reply = run_worker(&fake, &shard, worker::RANGE_PATH);
    assert_eq!(reply["status"], "error", "{reply}");
    assert_eq!(reply["error"], "worker_budget_stopped");
    assert!(fake.keys("processed/").is_empty());
    assert_eq!(fake.bucket.borrow()[&key], body);
}

#[test]
fn a_large_window_is_written_as_multipart_parts() {
    use md5::{Digest, Md5};
    let fake = Fake::new();
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut keys = Vec::new();
    let mut expected = Vec::new();
    for name in ["p0", "p1"] {
        let mut text = Vec::new();
        for i in 0..2000 {
            let id = format!("{name}-{i:05}");
            let url = format!("/{}", random_text(&mut seed, 4000));
            text.extend_from_slice(
                json!({"timestamp": iso(BASE + i % 60), "ip": id, "status": 200, "url": url})
                    .to_string()
                    .as_bytes(),
            );
            text.push(b'\n');
            expected.push(id);
        }
        let key = raw_key(BASE, name, "zst");
        let body = gz(&text);
        assert!(body.len() < 8 * 1024 * 1024);
        fake.bucket.borrow_mut().insert(key.clone(), body);
        keys.push(key);
    }
    let reply = run_worker(&fake, &shard_request(&keys), worker::PATH);
    assert_eq!(reply["status"], "ok", "{reply}");
    let output = &reply["result"]["outputs"][0];
    let key = output["key"].as_str().unwrap().to_owned();
    let completed = fake.completed.borrow().clone();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].0, key);
    assert!(completed[0].1 >= 2);
    let body = fake.bucket.borrow()[&key].clone();
    assert!(body.len() > ingest::compaction::PART_BYTES);
    assert_eq!(output["md5"], hex::encode(Md5::digest(&body)));
    assert_eq!(output["bytes"], body.len() as u64);
    assert!(fake.uploads.borrow().is_empty());
    let mut got = processed_ips(&fake);
    got.dedup();
    assert_eq!(got, sorted(expected));
    assert!(reply["calls"].as_u64().unwrap() <= u64::from(BUDGET));
}

#[test]
fn a_large_window_without_spare_calls_falls_back_to_one_put() {
    let fake = Fake::new();
    let mut seed = 7u64;
    let mut keys = Vec::new();
    for name in ["q0", "q1", "q2"] {
        let mut text = Vec::new();
        for i in 0..1900 {
            let url = format!("/{}", random_text(&mut seed, 4000));
            text.extend_from_slice(
                json!({"timestamp": iso(BASE + i % 60), "ip": format!("{name}-{i}"), "status": 200, "url": url})
                    .to_string()
                    .as_bytes(),
            );
            text.push(b'\n');
        }
        let key = raw_key(BASE, name, "zst");
        fake.bucket.borrow_mut().insert(key.clone(), gz(&text));
        keys.push(key);
    }
    // With 22 objects only 6 calls remain after the manifest: a 3-part upload needs 7.
    for i in 0..19 {
        put_object(&fake, BASE, &format!("r{i:02}"), 1, 10, false);
        keys.push(raw_key(BASE, &format!("r{i:02}"), "zst"));
    }
    keys.sort();
    let reply = run_worker(&fake, &shard_request(&keys), worker::PATH);
    assert_eq!(reply["status"], "ok", "{reply}");
    assert!(
        reply["result"]["outputs"][0]["bytes"].as_u64().unwrap()
            > 2 * ingest::compaction::PART_BYTES as u64
    );
    assert_eq!(fake.upload_seq.get(), 0);
    assert_eq!(processed_ips(&fake).len(), 3 * 1900 + 19);
}
