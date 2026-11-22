use super::support::*;
use fastly_log_analytics::ingest::{
    self,
    convert::{self, RequestSchema, Segment},
    worker,
};
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn a_fine_segment_size_cuts_a_large_object_into_at_most_the_object_segment_cap() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let mut seed = 7;
    let mut text = Vec::new();
    let mut expected = Vec::new();
    for i in 0..20_000 {
        let id = format!("big-{i:06}");
        let url = format!("/{}", random_text(&mut seed, 80));
        text.extend_from_slice(
            json!({"timestamp": iso(BASE + (i % 60)), "ip": id, "status": 200, "url": url})
                .to_string()
                .as_bytes(),
        );
        text.push(b'\n');
        expected.push(id);
    }
    let name = long_name("big", 900);
    let key = raw_key(BASE, &name, "zst");
    let body = gz(&text);
    assert!(body.len() as u64 * 8 / (64 * 1024) > 3 * 22);
    fake.bucket.borrow_mut().insert(key.clone(), body);
    tick(&fake).unwrap();
    let first = plan(&fake);
    let segments = first["segments"].as_object().unwrap().len();
    assert!((3..=22).contains(&segments), "{segments} segments");
    drain(&fake, 30);
    assert_eq!(processed_ips(&fake), sorted(expected));
    assert_eq!(deleteq_keys(&fake), vec![key]);
    assert!(fake.plan_peak.get() <= KV_READ_LIMIT);
}

#[test]
fn a_resegment_past_the_plan_bound_holds_its_object_visibly_and_loses_no_line() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    fake.set("ingest_relay", "16");
    let mut by_key = BTreeMap::new();
    for i in 0..100 {
        let name = long_name(&format!("dense{i:03}"), 900);
        let ids = put_object(&fake, BASE, &name, 600, 1000, false);
        by_key.insert(raw_key(BASE, &name, "zst"), ids);
    }
    drain(&fake, 40);
    let held = held_keys(&fake);
    assert!(!held.is_empty(), "no object was held");
    let mut expected: Vec<String> = by_key
        .iter()
        .filter(|(k, _)| !held.contains(k))
        .flat_map(|(_, ids)| ids.clone())
        .collect();
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
    for key in &held {
        assert!(fake.bucket.borrow().contains_key(key));
    }
    let mut accounted = deleteq_keys(&fake);
    accounted.extend(held);
    accounted.sort();
    assert_eq!(accounted, by_key.keys().cloned().collect::<Vec<_>>());
    assert!(
        fake.plan_peak.get() <= KV_READ_LIMIT,
        "{}",
        fake.plan_peak.get()
    );
}

#[test]
fn straggler_replan_fits_within_kv_limit_and_preserves_sweep_cursor() {
    let fake = Fake::new();
    let prior_cursor = raw_key(BASE, "cursor-before-straggler-batch", "zst");
    let mark_key = raw_key(BASE + 3600, "watermark", "zst");
    fake.kv.borrow_mut().insert(
        ingest::WATERMARK_KEY.into(),
        (
            1,
            json!({
                "key": mark_key,
                "run_id": "wm0",
                "at": BASE as u64 + 3600,
                "sweep": {"after": prior_cursor, "at": BASE as u64 + 3600},
                "passes": [],
                "swept": [],
            }),
        ),
    );
    let mut raw_keys = Vec::new();
    let mut sizes = Vec::new();
    for i in 0..240 {
        let name = long_name(&format!("strag{i:03}"), 900);
        put_object(&fake, BASE + 60, &name, 2, 10, false);
        let key = raw_key(BASE + 60, &name, "zst");
        let sz = fake.bucket.borrow()[&key].len() as u64;
        raw_keys.push(key);
        sizes.push(sz);
    }
    let mut shards = BTreeMap::new();
    for (idx, chunk) in raw_keys.chunks(worker::SHARD_MAX_OBJECTS).enumerate() {
        shards.insert(idx.to_string(), chunk.to_vec());
    }
    let straggler_plan = json!({
        "version": 2,
        "plan_id": "abandonedstr",
        "nonce": "0102030405060708",
        "stream": "request",
        "table": "request",
        "schema": RequestSchema::base().hash(),
        "custom": [],
        "window_s": 60,
        "sizes": sizes,
        "raw_keys": raw_keys,
        "shards": shards,
        "segments": {},
        "segment_bytes": 0,
        "results": {},
        "failures": {"0": 3},
        "attempt": 3,
        "lease_until": 0,
        "created_at": BASE as u64 + 3600,
        "abandoning": "shard_failures",
        "replans": 3,
        "straggler": true,
        "sweep_next": "",
    });
    fake.kv
        .borrow_mut()
        .insert(ingest::PLAN_KEY.into(), (1, straggler_plan));
    tick(&fake).unwrap();
    let replanned = plan(&fake);
    assert_eq!(replanned["straggler"], true, "{replanned}");
    assert!(replanned["abandoning"].is_null(), "{replanned}");
    let kept = replanned["raw_keys"].as_array().unwrap().len();
    assert!(kept > 0 && kept < raw_keys.len(), "kept={kept}");
    assert_eq!(replanned["sweep_next"], prior_cursor);
    assert!(
        fake.plan_peak.get() <= KV_READ_LIMIT,
        "plan_peak={}",
        fake.plan_peak.get()
    );
}

#[test]
fn worker_segment_prefix_skip_stops_at_vcpu_budget_before_wall_stop() {
    let fake = Fake::new();
    let (text, _) = ndjson(40_000);
    let key = raw_key(BASE, "vcpu-skip", "zst");
    let body = gz(&text);
    fake.bucket.borrow_mut().insert(key.clone(), body.clone());
    let mut shard = shard_request(std::slice::from_ref(&key));
    shard.segment = Some(Segment {
        start: text.len() as u64 - 1000,
        end: None,
    });
    shard.segment_bytes = 64 * 1024;
    // Wall clock does not advance (stall_after_get is false), but worker vCPU reaches 800 ms.
    fake.vcpu_after_get.set(worker::VCPU_STOP_MS);
    let reply = run_worker(&fake, &shard, worker::RANGE_PATH);
    assert_eq!(reply["status"], "error", "{reply}");
    assert_eq!(reply["error"], "worker_budget_stopped");
    assert!(fake.keys("processed/").is_empty());
    assert_eq!(fake.bucket.borrow()[&key], body);
}

#[test]
fn persistently_budget_stopping_single_segment_shard_is_held_on_replan_and_other_shards_commit() {
    let fake = Fake::new();
    fake.set("ingest_segment_kib", "64");
    let (bad_text, _) = ndjson(25_000);
    assert!(bad_text.len() as u64 > 2 * convert::CHECKPOINT_BYTES);
    let bad_key = raw_key(BASE, "a-bad-seg", "zst");
    let bad_body = gz(&bad_text);
    assert!(bad_body.len() * 8 > 2 * 64 * 1024);
    fake.bucket
        .borrow_mut()
        .insert(bad_key.clone(), bad_body.clone());
    let good_ids = put_object(&fake, BASE + 60, "b-good", 40, 10, false);
    let good_key = raw_key(BASE + 60, "b-good", "zst");
    // Fetching `bad_key` burns the worker vCPU budget, so segments whose prefix >= CHECKPOINT_BYTES
    // fail with `worker_budget_stopped` on every attempt.
    fake.stall_keys.borrow_mut().insert(bad_key.clone());

    drain(&fake, 15);
    assert!(committed(&fake));
    assert_eq!(processed_ips(&fake), sorted(good_ids));
    assert_eq!(deleteq_keys(&fake), vec![good_key]);
    assert_eq!(held_keys(&fake), vec![bad_key.clone()]);
    assert_eq!(fake.bucket.borrow()[&bad_key], bad_body);
    let held_reasons: Vec<String> = fake
        .kv
        .borrow()
        .iter()
        .filter(|(k, _)| k.starts_with(ingest::DELETEQ_PREFIX))
        .flat_map(|(_, (_, v))| {
            v["held"]
                .as_array()
                .unwrap()
                .iter()
                .map(|h| {
                    assert_eq!(h["status"], "failed");
                    h["reason"].as_str().unwrap().to_owned()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(held_reasons, vec!["worker_budget_stopped".to_owned()]);
}
