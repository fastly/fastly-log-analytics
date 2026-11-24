use super::{freshness, support::*};
use fastly_log_analytics::{
    ingest::{self, api},
    shell::Runtime,
    telemetry::{self, LogBuffer, LogKind},
};
use serde_json::{json, Value};
use std::{collections::BTreeSet, time::Instant};

#[test]
fn catalog_discovery_reads_an_active_day_in_few_concurrent_rounds() {
    let fake = Fake::new(BASE);
    fake.heap_mib.set(Some(40));
    let (query, want_files, want_summaries) = publish_active_day(&fake);
    let walk = || walk_files(&fake, &query);
    let (files, summaries, pages) = walk();
    assert_eq!(files, want_files);
    assert_eq!(summaries, want_summaries);
    {
        let rounds = fake.fos_rounds.borrow();
        let reads: usize = rounds.iter().map(Vec::len).sum();
        // Each hour reads its head block; one at a time that is one round trip per hour.
        assert!(reads >= 13, "{reads} reads");
        assert!(
            rounds.len() <= 2 * pages + 1,
            "{} rounds for {reads} reads",
            rounds.len()
        );
    }

    // Near the heap admission, speculative reads shrink to the headroom left below 96 MiB:
    // at most 1 MiB, two 512 KiB manifests at their full limit, beside the required read.
    fake.heap_mib.set(Some(95));
    let (files, summaries, _) = walk();
    assert_eq!(
        (files, summaries),
        (want_files.clone(), want_summaries.clone())
    );
    assert!(fake
        .fos_rounds
        .borrow()
        .iter()
        .all(|round| round.len() <= 3));
    assert!(fake.fos_rounds.borrow().iter().any(|round| round.len() > 1));
    fake.heap_mib.set(None);
    let (files, summaries, _) = walk();
    assert_eq!(
        (files, summaries),
        (want_files.clone(), want_summaries.clone())
    );
    {
        let rounds = fake.fos_rounds.borrow();
        assert!(!rounds.is_empty());
        assert!(rounds.iter().all(|round| round.len() == 1), "{rounds:?}");
    }

    // A runtime that cannot send concurrently reads one object at a time, never speculatively.
    fake.heap_mib.set(Some(40));
    fake.fos_concurrent.set(false);
    let (files, summaries, _) = walk();
    assert_eq!((files, summaries), (want_files, want_summaries));
    assert!(fake.fos_rounds.borrow().is_empty());
}

#[test]
fn catalog_fork_listing_lists_the_sequential_walk_in_parallel_chains() {
    let fake = Fake::new(BASE);
    fake.heap_mib.set(Some(40));
    let (query, want_files, want_summaries) = publish_active_day(&fake);
    walk_files(&fake, &query);
    let sequential = fake.fos_rounds.borrow().len();
    let forked = walk_forked(&fake, &query);
    assert_eq!(forked.files, want_files);
    assert_eq!(forked.summaries, want_summaries);
    assert!(forked.forks >= 4, "{} forks", forked.forks);
    // Setup reads the range's two day indexes in one round, then writes the forks.
    assert_eq!(forked.setup_rounds, 1);
    assert!(
        forked.setup_calls <= 7,
        "{} setup calls",
        forked.setup_calls
    );
    // Every fork finishes in one page; setup plus the slowest fork take fewer round trips than
    // one walk of every hour.
    assert_eq!(forked.longest, 1);
    assert!(
        forked.setup_rounds + forked.longest_rounds < sequential,
        "{} + {} rounds forked, {sequential} sequential",
        forked.setup_rounds,
        forked.longest_rounds
    );

    // Forks are pinned to the generation setup read; a later commit shows up in the next listing.
    let setup = files_page(&fake, &format!("{query}&fork=1")).unwrap();
    let hour = BASE - BASE.rem_euclid(3600) - 3 * 3600;
    let late = ingest::processed_key("processed/request/", hour + 60, hour + 120, "late", 3);
    fake.bucket.borrow_mut().insert(late.clone(), vec![1]);
    publish_files(&fake, std::slice::from_ref(&late), &[], &[]);
    let mut pinned = BTreeSet::new();
    for fork in setup["forks"].as_array().unwrap() {
        let mut cursor = fork.as_str().map(str::to_owned);
        while let Some(value) = cursor {
            let page = files_page(&fake, &format!("{query}&cursor={value}")).unwrap();
            assert_eq!(page["catalog_generation"], setup["catalog_generation"]);
            for file in page["files"].as_array().unwrap() {
                assert!(pinned.insert(file["key"].as_str().unwrap().to_owned()));
            }
            cursor = page["next_cursor"].as_str().map(str::to_owned);
        }
    }
    assert_eq!(pinned, want_files);
    assert!(walk_forked(&fake, &query).files.contains(&late));
}

#[test]
fn catalog_fork_cursors_bind_their_range_and_expire_with_the_snapshot() {
    let fake = Fake::new(BASE);
    let (query, _, _) = publish_active_day(&fake);
    let setup = files_page(&fake, &format!("{query}&fork=1")).unwrap();
    let fork = setup["forks"][1].as_str().unwrap().to_owned();
    let open = BASE - BASE.rem_euclid(3600);
    let other = format!(
        "summaries=1&from={}&to={}",
        iso_url(open - 11 * 3600),
        iso_url(open + 3600)
    );
    assert_eq!(
        files_page(&fake, &format!("{other}&cursor={fork}"))
            .unwrap_err()
            .0,
        400
    );
    fake.now
        .set(fake.now.get() + ingest::catalog::discovery::SNAPSHOT_SECONDS);
    assert_eq!(
        files_page(&fake, &format!("{query}&cursor={fork}"))
            .unwrap_err()
            .0,
        409
    );
}

#[test]
fn catalog_fork_is_skipped_when_the_range_has_one_shard() {
    let fake = Fake::new(BASE);
    publish_active_day(&fake);
    let open = BASE - BASE.rem_euclid(3600);
    let query = format!(
        "fork=1&from={}&to={}",
        iso_url(open - 3600),
        iso_url(open - 1800)
    );
    let page = files_page(&fake, &query).unwrap();
    assert!(page.get("forks").is_none_or(Value::is_null));
    assert_eq!(page["complete"], true);
    assert_eq!(page["files"].as_array().unwrap().len(), 4);
}

#[test]
fn catalog_fork_listing_of_many_days_splits_by_day_and_stays_exact() {
    let fake = Fake::new(BASE);
    let mut want = BTreeSet::new();
    for day in 0..20 {
        let start = BASE - day * 86_400 - 7200;
        let keys: Vec<String> = (0..3)
            .map(|i| {
                let key = ingest::processed_key(
                    "processed/request/",
                    start + i * 600,
                    start + i * 600 + 60,
                    "abc",
                    1,
                );
                fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
                key
            })
            .collect();
        want.extend(keys.iter().cloned());
        publish_files(&fake, &keys, &[], &[]);
    }
    let query = format!(
        "from={}&to={}",
        iso_url(BASE - 21 * 86_400),
        iso_url(BASE + 3600)
    );
    assert_eq!(walk_files(&fake, &query).0, want);
    let forked = walk_forked(&fake, &query);
    assert_eq!(forked.files, want);
    assert!(forked.forks >= 8 && forked.forks <= 16, "{}", forked.forks);
    // Splitting by day reads no day index; each fork reads its own.
    assert_eq!(forked.setup_rounds, 0);
    assert!(
        forked.setup_calls <= 5,
        "{} setup calls",
        forked.setup_calls
    );
}

#[test]
fn catalog_prefetch_bounds_unread_reads_by_their_full_limits_across_batches() {
    let fake = Fake::new(BASE);
    let (query, want_files, want_summaries) = publish_active_day(&fake);
    fake.fos_defer.set(true);
    // Unread speculative reads stay outstanding across batches; at 95 MiB their declared
    // limits together stay within the 1 MiB left below the 96 MiB admission.
    for (heap, bound) in [(Some(95), 1 << 20), (Some(64), 32 << 20)] {
        fake.heap_mib.set(heap);
        fake.fos_unread_peak.set(0);
        let (files, summaries, _) = walk_files(&fake, &query);
        assert_eq!(
            (files, summaries),
            (want_files.clone(), want_summaries.clone())
        );
        let peak = fake.fos_unread_peak.get();
        assert!(peak > 0 && peak <= bound, "{peak} outstanding bytes");
        assert!(fake.fos_unread.borrow().is_empty());
    }
    fake.heap_mib.set(None);
    fake.fos_unread_peak.set(0);
    let (files, summaries, _) = walk_files(&fake, &query);
    assert_eq!((files, summaries), (want_files, want_summaries));
    assert_eq!(fake.fos_unread_peak.get(), 0);
}

#[test]
fn catalog_prefetch_accounts_for_allocated_held_response_capacity() {
    let fake = Fake::new(BASE);
    fake.heap_mib.set(Some(95));
    let svc = service();
    let store = ingest::storage::Store {
        runtime: &fake,
        service: &svc,
        access_key: String::new(),
        secret_key: String::new(),
        started: Instant::now(),
        vcpu_start: 0,
    };
    let mut prefetched = ingest::catalog::Prefetched::new();
    let mut held = Vec::with_capacity(1 << 20);
    held.push(0);
    prefetched.insert("held".into(), Ok(Some(held)));

    assert_eq!(
        ingest::catalog::fetch_batched(
            &store,
            &mut prefetched,
            "required",
            512 << 10,
            16,
            || vec![("next-a".into(), 512 << 10), ("next-b".into(), 512 << 10)],
        ),
        Ok(None)
    );
    assert_eq!(
        fake.fos_rounds.borrow().as_slice(),
        &[vec!["required".to_owned()]]
    );
}

#[test]
fn catalog_pins_generations_and_replaces_exact_sources_only_after_cas() {
    let fake = Fake::new(BASE);
    let hour = BASE - BASE.rem_euclid(3600);
    let keys = (0..1005)
        .map(|i| {
            let key = ingest::processed_key(
                "processed/request/",
                hour,
                hour + 60,
                &format!("source{i:04}"),
                1,
            );
            fake.bucket.borrow_mut().insert(key.clone(), vec![1, 2, 3]);
            key
        })
        .collect::<Vec<_>>();
    publish_files(&fake, &keys, &[], &[]);
    let query = format!("from={}&to={}", iso_url(hour), iso_url(hour + 3600));
    let first = files_page(&fake, &query).unwrap();
    assert_eq!(first["files"].as_array().unwrap().len(), 500);
    let cursor = first["next_cursor"].as_str().unwrap().to_owned();
    let replacement = telemetry::compact::compact_key(
        &format!("processed/request/{}", ingest::hour_dir(hour)),
        hour as u64,
        (hour + 60) as u64,
        &keys[..32],
        32,
    );
    fake.bucket
        .borrow_mut()
        .insert(replacement.clone(), vec![4, 5, 6]);
    fake.new_request();
    let record = fake.admin_get(ingest::WATERMARK_KEY).unwrap().unwrap();
    let output = ingest::worker::Output {
        key: replacement.clone(),
        rows: 32,
        bytes: 3,
        ..Default::default()
    };
    let candidate = ingest::catalog::prepare(
        &store(&fake, &service()),
        &record.value,
        "compact",
        "schema",
        &[output],
        &[],
        &[],
        &keys[..32],
    )
    .unwrap();
    assert_eq!(
        catalog_keys(&fake, &query).1,
        keys.iter().cloned().collect()
    );
    fake.new_request();
    assert!(fake
        .admin_cas(ingest::WATERMARK_KEY, Some(&record.generation), &candidate)
        .unwrap());
    let current = catalog_keys(&fake, &query);
    assert_eq!(current.0, 2);
    assert_eq!(current.1.len(), 974);
    assert!(current.1.contains(&replacement));
    assert!(keys[..32].iter().all(|key| !current.1.contains(key)));
    let mut pinned = first["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["key"].as_str().unwrap().to_owned())
        .collect::<BTreeSet<_>>();
    let mut cursor = Some(cursor);
    while let Some(value) = cursor {
        let page = files_page(&fake, &format!("{query}&cursor={value}")).unwrap();
        assert_eq!(page["catalog_generation"], 1);
        for file in page["files"].as_array().unwrap() {
            assert!(pinned.insert(file["key"].as_str().unwrap().to_owned()));
        }
        cursor = page["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(pinned, keys.into_iter().collect());
}

#[test]
fn catalog_retention_retires_expired_hours_before_deleting_their_objects() {
    let fake = Fake::new(BASE + 86_400 + 7 * 3600);
    let key = ingest::processed_key("processed/request/", BASE, BASE + 60, "retained", 1);
    let active = ingest::processed_key(
        "processed/request/",
        BASE + 8 * 3600,
        BASE + 8 * 3600 + 60,
        "active",
        2,
    );
    fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    fake.bucket.borrow_mut().insert(active.clone(), vec![2]);
    publish_files(&fake, &[key.clone(), active.clone()], &[], &[]);
    let mut svc = service();
    svc.log_retention_days = 1;
    for _ in 0..10 {
        fake.new_request();
        let report =
            ingest::retention::retention_sweep(&store(&fake, &svc), &Default::default(), 4, None)
                .unwrap();
        if !report.more_pending {
            break;
        }
    }
    assert!(fake.bucket.borrow().contains_key(&key));
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    assert!(!catalog_keys(&fake, &query).1.contains(&key));
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 24 * 3600));
    assert_eq!(
        catalog_keys(&fake, &query).1,
        BTreeSet::from([active.clone()])
    );
    fake.new_request();
    assert_eq!(
        api::extents(&fake, &svc, Instant::now()).unwrap()["total_rows"],
        2
    );
    fake.now
        .set(fake.now.get() + ingest::compaction::DELETE_GRACE_S + 1);
    for _ in 0..10 {
        fake.new_request();
        let report =
            ingest::retention::retention_sweep(&store(&fake, &svc), &Default::default(), 4, None)
                .unwrap();
        if !report.more_pending {
            break;
        }
    }
    assert!(!fake.bucket.borrow().contains_key(&key));
    assert!(fake.bucket.borrow().contains_key(&active));
}

#[test]
fn catalog_delta_requires_proven_base_ancestry_and_pins_empty_continuation_pages() {
    let fake = Fake::new(BASE + 3600);
    let keys = (0..3)
        .map(|n| {
            ingest::processed_key(
                "processed/request/",
                BASE,
                BASE + 60,
                &format!("delta{n}"),
                1,
            )
        })
        .collect::<Vec<_>>();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    publish_files(&fake, &keys[..1], &[], &[]);
    let base_manifest = fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    publish_files(&fake, &keys[1..2], &[], &[]);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.bucket.borrow_mut().remove(&base_manifest);
    assert_eq!(
        files_page(&fake, &query).unwrap_err(),
        (409, "catalog_resync_required")
    );
}

#[test]
fn catalog_delta_reads_recent_commit_manifests_in_one_round() {
    let fake = Fake::new(BASE + 3600);
    fake.heap_mib.set(Some(40));
    let keys = (0..7)
        .map(|n| {
            let start = BASE + n * 120;
            ingest::processed_key("processed/request/", start, start + 120, "delta", 1)
        })
        .collect::<Vec<_>>();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        publish_files(&fake, std::slice::from_ref(key), &[], &[]);
    }
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.fos_rounds.borrow_mut().clear();
    let page = files_page(&fake, &query).unwrap();
    let changed: Vec<&str> = page["changes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|change| change["files"].as_array().unwrap())
        .map(|file| file["key"].as_str().unwrap())
        .collect();
    assert_eq!(
        changed,
        keys[1..].iter().map(String::as_str).collect::<Vec<_>>()
    );
    let rounds = fake.fos_rounds.borrow();
    assert_eq!(rounds.len(), 1, "{rounds:?}");
    assert_eq!(rounds[0].len(), 7);
}

#[test]
fn catalog_delta_keeps_a_prefetched_manifest_answer_instead_of_reading_it_twice() {
    let fake = Fake::new(BASE + 3600);
    fake.heap_mib.set(Some(40));
    let keys = (0..4)
        .map(|n| {
            let start = BASE + n * 120;
            ingest::processed_key("processed/request/", start, start + 120, "delta", 1)
        })
        .collect::<Vec<_>>();
    let mut manifests = Vec::new();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
        publish_files(&fake, std::slice::from_ref(key), &[], &[]);
        manifests.push(
            fake.kv.borrow()[ingest::WATERMARK_KEY].1["catalog"]["manifest"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    // The second-newest commit's manifest is gone: its prefetched 404 must decide the page.
    fake.bucket.borrow_mut().remove(&manifests[2]);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.fos_rounds.borrow_mut().clear();
    fake.fos_gets.borrow_mut().clear();
    assert_eq!(
        files_page(&fake, &query).unwrap_err(),
        (409, "catalog_resync_required")
    );
    let rounds = fake.fos_rounds.borrow();
    assert_eq!(rounds.len(), 1, "{rounds:?}");
    let gets = fake.fos_gets.borrow();
    assert_eq!(gets.iter().filter(|key| **key == manifests[2]).count(), 1);
    assert_eq!(gets.len(), rounds[0].len(), "{gets:?}");
}

#[test]
fn catalog_delta_pins_a_target_across_empty_pages_and_reports_expired_cursors() {
    let fake = Fake::new(BASE + 3600);
    let keys = (0..3)
        .map(|n| {
            ingest::processed_key(
                "processed/request/",
                BASE,
                BASE + 60,
                &format!("delta{n}"),
                1,
            )
        })
        .collect::<Vec<_>>();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    publish_files(&fake, &keys[..1], &[], &[]);
    publish_files(&fake, &keys[1..2], &[], &[]);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    fake.new_request();
    fake.calls.set(BUDGET - 6);
    let first = api::files(&fake, &service(), &query, Instant::now()).unwrap();
    assert_eq!(first["complete"], false);
    assert!(fake.calls.get() <= BUDGET);
    let cursor = first["next_cursor"].as_str().unwrap();
    publish_files(&fake, &keys[2..], &[], &[]);
    let next = files_page(&fake, &format!("{query}&cursor={cursor}")).unwrap();
    assert_eq!(next["base_generation"], 1);
    assert_eq!(next["catalog_generation"], 2);
    assert_eq!(next["complete"], true);
    assert_eq!(next["changes"][0]["files"][0]["key"], keys[1]);
    fake.now.set(fake.now.get() + 601);
    assert_eq!(
        files_page(&fake, &format!("{query}&cursor={cursor}")).unwrap_err(),
        (409, "catalog_resync_required")
    );
}

#[test]
fn catalog_delta_treats_a_temporarily_missing_head_as_retryable_not_a_resync() {
    let fake = Fake::new(BASE);
    let query = format!(
        "from={}&to={}&since_generation=1&previous_from={}&previous_to={}",
        iso_url(BASE),
        iso_url(BASE + 3600),
        iso_url(BASE),
        iso_url(BASE + 3600)
    );
    assert_eq!(
        files_page(&fake, &query).unwrap_err(),
        (503, "catalog_head_behind")
    );
}

#[test]
fn catalog_delta_returns_only_new_raw_changes_and_authoritative_summary_partitions() {
    let fake = Fake::new(BASE + 3600);
    let first = ingest::processed_key("processed/request/", BASE, BASE + 60, "first", 1);
    let second = ingest::processed_key("processed/request/", BASE + 60, BASE + 120, "second", 1);
    let summary_first =
        ingest::summary::summary_key("processed/summary/request/", BASE, BASE + 60, "first", 1);
    let summary_second = ingest::summary::summary_key(
        "processed/summary/request/",
        BASE + 60,
        BASE + 120,
        "second",
        1,
    );
    for key in [&first, &second, &summary_first, &summary_second] {
        fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    }
    publish_files(&fake, &[first], &[summary_first], &[]);
    publish_files(&fake, std::slice::from_ref(&second), &[summary_second], &[]);
    let range = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let page = files_page(
        &fake,
        &format!(
            "{range}&summaries=1&since_generation=1&previous_from={}&previous_to={}",
            iso_url(BASE),
            iso_url(BASE + 3600)
        ),
    )
    .unwrap();
    assert_eq!(page["base_generation"], 1);
    assert_eq!(page["catalog_generation"], 2);
    assert_eq!(page["complete"], true);
    assert_eq!(page["files"].as_array().unwrap().len(), 0);
    let changes = page["changes"].as_array().unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["generation"], 2);
    assert_eq!(changes[0]["files"][0]["key"], second);
    assert_eq!(page["summary_partitions"].as_array().unwrap().len(), 1);
    assert_eq!(page["summary_files"].as_array().unwrap().len(), 2);
}

#[test]
fn catalog_pending_compaction_does_not_lease_block_ingestion_publication() {
    let fake = Fake::new(BASE);
    let hour = BASE - BASE.rem_euclid(3600);
    let sources = (0..2)
        .map(|n| {
            let key =
                ingest::processed_key("processed/request/", hour, hour + 60, &format!("old{n}"), 1);
            fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
            key
        })
        .collect::<Vec<_>>();
    publish_files(&fake, &sources, &[], &[]);
    let replacement = ingest::worker::Output {
        key: telemetry::compact::compact_key(
            &format!("processed/request/{}", ingest::hour_dir(hour)),
            hour as u64,
            (hour + 60) as u64,
            &sources,
            2,
        ),
        rows: 2,
        bytes: 1,
        ..Default::default()
    };
    fake.bucket
        .borrow_mut()
        .insert(replacement.key.clone(), vec![2]);
    fake.new_request();
    let previous = fake.admin_get(ingest::WATERMARK_KEY).unwrap().unwrap();
    assert_eq!(
        ingest::catalog::prepare_reserving(
            &store(&fake, &service()),
            &previous.value,
            "compaction",
            "",
            std::slice::from_ref(&replacement),
            &[],
            &[],
            &sources,
            26,
        )
        .unwrap_err(),
        "catalog_prepare_pending"
    );
    assert!(fake.calls.get() <= 32);
    let arriving = ingest::worker::Output {
        key: ingest::processed_key("processed/request/", hour, hour + 60, "arriving", 1),
        rows: 1,
        bytes: 1,
        ..Default::default()
    };
    fake.bucket
        .borrow_mut()
        .insert(arriving.key.clone(), vec![3]);
    let committed_at = fake.now.get();
    let mut published = false;
    for _ in 0..3 {
        fake.new_request();
        match ingest::catalog::prepare(
            &store(&fake, &service()),
            &previous.value,
            "ingestion",
            "schema",
            std::slice::from_ref(&arriving),
            &[],
            &[],
            &[],
        ) {
            Ok(value) => {
                assert!(fake
                    .admin_cas(ingest::WATERMARK_KEY, Some(&previous.generation), &value)
                    .unwrap());
                published = true;
                break;
            }
            Err(error) if error == "catalog_prepare_pending" => {}
            Err(error) => panic!("{error}"),
        }
        assert!(fake.calls.get() <= 32);
    }
    assert!(
        published,
        "ingestion waited for an unrelated compaction lease"
    );
    assert_eq!(fake.now.get(), committed_at);
    publish_outputs(&fake, std::slice::from_ref(&replacement), &[], &sources);
    let query = format!("from={}&to={}", iso_url(hour), iso_url(hour + 3600));
    let visible = catalog_keys(&fake, &query);
    assert_eq!(visible.0, 3);
    assert_eq!(visible.1, BTreeSet::from([replacement.key, arriving.key]));
}

#[test]
fn catalog_missing_immutable_root_is_a_retryable_error_not_an_empty_dataset() {
    let fake = Fake::new(BASE);
    let key = ingest::processed_key("processed/request/", BASE, BASE + 60, "source", 1);
    fake.bucket.borrow_mut().insert(key.clone(), vec![1]);
    let value = publish_files(&fake, std::slice::from_ref(&key), &[], &[]);
    let root = value["catalog"]["days"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    let bytes = fake.bucket.borrow_mut().remove(&root).unwrap();
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 60));
    assert_eq!(files_page(&fake, &query).unwrap_err().0, 503);
    fake.bucket.borrow_mut().insert(root, bytes);
    assert_eq!(catalog_keys(&fake, &query).1, BTreeSet::from([key]));
}

#[test]
fn catalog_discovery_excludes_uploaded_but_uncommitted_managed_files() {
    let fake = Fake::new(BASE);
    let key = ingest::processed_key(
        "processed/request/",
        BASE,
        BASE + 60,
        "catalog1_uncommitted",
        7,
    );
    fake.bucket.borrow_mut().insert(key, vec![1, 2, 3]);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 60));
    let mut query = query.clone();
    for _ in 0..100 {
        let page = files_page(&fake, &query).unwrap();
        assert!(
            page["files"].as_array().unwrap().is_empty(),
            "an uploaded output is not published until the catalog CAS"
        );
        if page["complete"] == true {
            return;
        }
        query = format!(
            "from={}&to={}&cursor={}",
            iso_url(BASE),
            iso_url(BASE + 60),
            page["next_cursor"].as_str().unwrap()
        );
    }
    panic!("bounded backfill must eventually complete");
}

#[test]
fn files_paginates_and_completes_only_on_the_last_page() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 1200);
    let base_query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 1200 * 60));
    let (mut cursor, mut seen, mut pages) = (None::<String>, BTreeSet::new(), 0);
    loop {
        let query = match &cursor {
            Some(c) => format!("{base_query}&cursor={c}"),
            None => base_query.clone(),
        };
        let page = files_page(&fake, &query).unwrap();
        let files = page["files"].as_array().unwrap();
        assert!(files.len() <= api::FILES_PAGE);
        for f in files {
            assert!(seen.insert(f["key"].as_str().unwrap().to_owned()));
        }
        pages += 1;
        if page["complete"] == true {
            assert!(page["next_cursor"].is_null());
            break;
        }
        cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
        assert!(pages < 10);
    }
    assert_eq!(seen.len(), 1200);
    assert!(pages >= 3);
}

#[test]
fn files_filters_by_overlap_and_shapes_entries() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 10);
    let query = format!("from={}&to={}", iso_url(BASE + 120), iso_url(BASE + 240));
    let page = files_page(&fake, &query).unwrap();
    let files = page["files"].as_array().unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0]["start"], ingest::rfc3339(BASE + 120));
    assert_eq!(files[0]["end"], ingest::rfc3339(BASE + 180));
    assert_eq!(
        (files[0]["size"].as_u64(), files[0]["rows"].as_u64()),
        (Some(3), Some(3))
    );
    assert_eq!(page["complete"], true);
}

#[test]
fn files_returns_a_compact_in_place_of_its_sources_and_keeps_uncovered_windows() {
    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 4);
    let windows = request_windows(&fake);
    let compact = request_compact_key(&windows[..3]);
    fake.bucket.borrow_mut().insert(compact.clone(), vec![9; 7]);
    publish_files(&fake, std::slice::from_ref(&compact), &[], &windows[..3]);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let page = files_page(&fake, &query).unwrap();
    let files = page["files"].as_array().unwrap();
    let keys: Vec<&str> = files.iter().map(|f| f["key"].as_str().unwrap()).collect();
    assert_eq!(keys, vec![compact.as_str(), windows[3].as_str()]);
    assert_eq!(files[0]["start"], ingest::rfc3339(BASE));
    assert_eq!(files[0]["end"], ingest::rfc3339(BASE + 180));
    assert_eq!(files[0]["rows"], 6);
    assert_eq!(files[0]["sources"], 3);
    assert_eq!(files[0]["size"], 7);
    // The same answer from the cached closed hour.
    assert_eq!(files_page(&fake, &query).unwrap()["files"], page["files"]);
}

#[test]
fn files_hides_sources_on_a_later_page_of_an_hour_than_their_compact() {
    let fake = Fake::new(BASE + 10 * 86_400);
    for i in 0..(api::FILES_PAGE as i64 + 20) {
        let start = BASE + i * 6;
        let key = ingest::processed_key("processed/request/", start, start + 6, "abcdef012345", 1);
        fake.bucket.borrow_mut().insert(key, vec![1]);
    }
    let windows = request_windows(&fake);
    publish_files(&fake, &windows, &[], &[]);
    let late = &windows[windows.len() - 3..];
    let compact = request_compact_key(late);
    fake.bucket.borrow_mut().insert(compact.clone(), vec![1]);
    publish_files(&fake, std::slice::from_ref(&compact), &[], late);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 3600));
    let (mut cursor, mut seen) = (None::<String>, Vec::new());
    for _ in 0..5 {
        let q = match &cursor {
            Some(c) => format!("{query}&cursor={c}"),
            None => query.clone(),
        };
        let page = files_page(&fake, &q).unwrap();
        seen.extend(
            page["files"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["key"].as_str().unwrap().to_owned()),
        );
        if page["complete"] == true {
            break;
        }
        cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
    }
    assert!(seen.contains(&compact));
    for source in late {
        assert!(!seen.contains(source), "{source} is covered");
    }
    assert_eq!(seen.len(), windows.len() - 3 + 1);
}

#[test]
fn files_stops_listing_after_the_last_hour_of_the_range() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 960);
    let query = format!("from={}&to={}", iso_url(BASE + 3600), iso_url(BASE + 7200));
    let before = lists();
    let page = files_page(&fake, &query).unwrap();
    assert_eq!(
        lists() - before,
        0,
        "catalog discovery never lists uploaded objects"
    );
    assert_eq!(page["complete"], true);
    assert_eq!(page["files"].as_array().unwrap().len(), 60);
}

#[test]
fn closed_hours_are_listed_once_and_a_late_commit_reveals_its_window() {
    let fake = Fake::new(BASE + 3 * 3600);
    put_raw(&fake, BASE + 60, "b", &[line(BASE + 60, "b")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 2 * 3600));
    let keys = |page: &Value| -> Vec<String> {
        page["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["key"].as_str().unwrap().to_owned())
            .collect()
    };
    let first = files_page(&fake, &query).unwrap();
    assert_eq!(keys(&first).len(), 1);
    assert_eq!(
        first["catalog_generation"], 1,
        "the catalog atomically committed the first window"
    );
    let before = lists();
    let again = files_page(&fake, &query).unwrap();
    assert_eq!(
        lists() - before,
        0,
        "both closed hours come from the immutable catalog"
    );
    assert_eq!(keys(&again), keys(&first));

    // A late upload for the closed hour commits a new window and a new generation.
    put_raw(&fake, BASE + 120, "late", &[line(BASE + 120, "late")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1["manifest_gen"], 2);
    let after = files_page(&fake, &query).unwrap();
    assert_eq!(keys(&after).len(), 2);
    assert_eq!(after["catalog_generation"], 2);

    // A commit into an hour still open to caches keeps the generation.
    put_raw(&fake, BASE + 7260, "open", &[line(BASE + 7260, "open")]);
    assert_eq!(tick(&fake).unwrap().converted, 1);
    assert_eq!(fake.kv.borrow()[ingest::WATERMARK_KEY].1["manifest_gen"], 2);
}

#[test]
fn open_hours_and_active_plans_do_not_expose_uncommitted_catalog_outputs() {
    let fake = Fake::new(BASE + 1800);
    seed_files(&fake, 30);
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 1800));
    files_page(&fake, &query).unwrap();
    let before = lists();
    assert_eq!(
        files_page(&fake, &query).unwrap()["files"]
            .as_array()
            .unwrap()
            .len(),
        30
    );
    assert_eq!(
        lists() - before,
        0,
        "open hours also use committed catalog metadata"
    );

    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 120);
    fake.kv
        .borrow_mut()
        .insert(ingest::PLAN_KEY.into(), (1, json!({"plan_id": "r2"})));
    let query = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 7200));
    let page = files_page(&fake, &query).unwrap();
    assert_eq!(page["catalog_generation"], 1);
    let before = lists();
    files_page(&fake, &query).unwrap();
    assert_eq!(lists() - before, 0);

    fake.kv.borrow_mut().remove(ingest::PLAN_KEY);
    files_page(&fake, &query).unwrap();
    let before = lists();
    assert_eq!(
        files_page(&fake, &query).unwrap()["files"]
            .as_array()
            .unwrap()
            .len(),
        120
    );
    assert_eq!(lists() - before, 0);
}

#[test]
fn a_cached_listing_stays_within_the_cache_op_budget_and_pages_exactly() {
    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 1200);
    let base = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 1200 * 60));
    let collect = |fake: &Fake, spare_ops: u32| -> (Vec<String>, u32) {
        let (mut cursor, mut keys, mut list_calls) = (None::<String>, Vec::new(), 0);
        for _ in 0..20 {
            let query = match &cursor {
                Some(c) => format!("{base}&cursor={c}"),
                None => base.clone(),
            };
            fake.new_request();
            fake.cache_ops
                .set(fastly_log_analytics::shell::CACHE_OP_BUDGET - spare_ops);
            let before = lists();
            let page = api::files(fake, &service(), &query, Instant::now()).unwrap();
            list_calls += lists() - before;
            assert!(fake.cache_ops.get() <= fastly_log_analytics::shell::CACHE_OP_BUDGET);
            assert!(page["files"].as_array().unwrap().len() <= api::FILES_PAGE);
            keys.extend(
                page["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|f| f["key"].as_str().unwrap().to_owned()),
            );
            if page["complete"] == true {
                return (keys, list_calls);
            }
            cursor = Some(page["next_cursor"].as_str().unwrap().to_owned());
        }
        panic!("listing did not complete");
    };
    let want: Vec<String> = fake.keys("processed/request/");
    let (cold, _) = collect(&fake, 0);
    assert_eq!(cold, want, "no cache ops left: plain listing");
    let (warm_up, _) = collect(&fake, 64);
    assert_eq!(warm_up, want);
    let (warm, list_calls) = collect(&fake, 64);
    assert_eq!(warm, want);
    assert_eq!(list_calls, 0);
}

#[test]
fn files_accepts_browser_iso_timestamps_with_milliseconds() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 10);
    let from = ingest::rfc3339(BASE)
        .replace('Z', ".123Z")
        .replace(':', "%3A");
    let to = ingest::rfc3339(BASE + 600)
        .replace('Z', ".456Z")
        .replace(':', "%3A");
    let page = files_page(&fake, &format!("from={from}&to={to}")).unwrap();
    assert_eq!(page["files"].as_array().unwrap().len(), 10);
}

#[test]
fn files_rejects_forged_or_mismatched_cursors_and_bad_ranges() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 600);
    let q = format!("from={}&to={}", iso_url(BASE), iso_url(BASE + 36_000));
    let page = files_page(&fake, &q).unwrap();
    let cursor = page["next_cursor"].as_str().unwrap().to_owned();
    let other = format!(
        "from={}&to={}&cursor={cursor}",
        iso_url(BASE),
        iso_url(BASE + 7200)
    );
    assert_eq!(files_page(&fake, &other).unwrap_err().0, 400);
    let forged = format!("{q}&cursor=x{cursor}");
    assert_eq!(files_page(&fake, &forged).unwrap_err().0, 400);
    let backwards = format!("from={}&to={}", iso_url(BASE + 60), iso_url(BASE));
    assert_eq!(files_page(&fake, &backwards).unwrap_err().0, 400);
}

#[test]
fn file_serves_only_processed_keys_of_this_service() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 1);
    let key = fake.keys("processed/request/")[0].clone();
    let get = |k: &str| {
        fake.new_request();
        api::file(
            &fake,
            &service(),
            &format!(
                "key={}",
                url::form_urlencoded::byte_serialize(k.as_bytes()).collect::<String>()
            ),
            None,
        )
        .map(|upstream| fastly_log_analytics::shell::fetch_upstream(&fake, &upstream))
    };
    assert_eq!(get(&key).unwrap().body, vec![1, 2, 3]);
    for bad in [
        "raw/request/year=2027/month=01/day=15/hour=08/minute=00/a.gz",
        "processed/request/../raw/request/a.parquet",
        "processed/requestx/a.parquet",
        "other/processed/request/a.parquet",
        "processed/request/year=2027/a.gz",
        "processed/request//a.parquet",
    ] {
        assert_eq!(get(bad).err().unwrap().0, 400, "{bad}");
    }
    assert_eq!(
        get("processed/request/year=2027/missing.parquet")
            .unwrap()
            .status,
        404
    );
}

#[test]
fn file_streams_past_the_8_mib_cap_and_passes_a_range_through() {
    let fake = Fake::new(BASE);
    let key = "processed/request/year=2027/big.parquet";
    let body: Vec<u8> = (0..9 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    fake.bucket.borrow_mut().insert(key.into(), body.clone());
    let get = |range: Option<&str>| {
        fake.new_request();
        fake.cdn_gets.borrow_mut().clear();
        let upstream = api::file(&fake, &service(), &format!("key={key}"), range).unwrap();
        assert_eq!(upstream.key, key);
        assert_eq!(upstream.range.as_deref(), range);
        assert!(
            fake.cdn_gets.borrow().is_empty(),
            "the body is fetched when the response is sent"
        );
        fastly_log_analytics::shell::fetch_upstream(&fake, &upstream)
    };
    let full = get(None);
    assert_eq!(full.status, 200);
    assert!(full.body == body);
    assert_eq!(full.headers["accept-ranges"], "bytes");
    let part = get(Some("bytes=10-19"));
    assert_eq!(part.status, 206);
    assert_eq!(part.body, body[10..20].to_vec());
    assert_eq!(
        part.headers["content-range"],
        format!("bytes 10-19/{}", body.len())
    );
    let tail = get(Some("bytes=-4"));
    assert_eq!(tail.body, body[body.len() - 4..].to_vec());
    fake.new_request();
    let mut upstream =
        api::file(&fake, &service(), &format!("key={key}"), Some("bytes=0-")).unwrap();
    upstream.head_only = true;
    let head = fastly_log_analytics::shell::fetch_upstream(&fake, &upstream);
    assert_eq!(head.status, 206);
    assert!(head.body.is_empty());
    assert_eq!(head.headers["content-length"], body.len().to_string());
    assert_eq!(
        head.headers["content-range"],
        format!("bytes 0-{}/{}", body.len() - 1, body.len())
    );
    let beyond = get(Some(&format!("bytes={}-", body.len())));
    assert_eq!(beyond.status, 416);
    assert_eq!(
        beyond.headers["content-range"],
        format!("bytes */{}", body.len())
    );
}

#[test]
fn log_extents_reports_first_and_last_windows_or_nulls() {
    let fake = Fake::new(BASE);
    let extents = |fake: &Fake| {
        fake.new_request();
        api::extents(fake, &service(), Instant::now()).unwrap()
    };
    let empty = extents(&fake);
    assert!(empty["earliest"].is_null() && empty["latest"].is_null());
    assert!(empty["total_rows"].is_null());
    seed_files(&fake, 1);
    for (start, run) in [(BASE + 86_400 * 3 + 7200, "a"), (BASE - 86_400 * 40, "b")] {
        let key = ingest::processed_key("processed/request/", start, start + 60, run, 1);
        fake.bucket.borrow_mut().insert(key.clone(), vec![0]);
        publish_files(&fake, &[key], &[], &[]);
    }
    let found = extents(&fake);
    assert_eq!(found["earliest"], ingest::rfc3339(BASE - 86_400 * 40));
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 86_400 * 3 + 7260));
    assert_eq!(found["total_rows"], 3);
    assert_eq!(found["total_rows_partial"], false);
}

#[test]
fn log_extents_count_a_compact_once_while_its_sources_remain_and_after_they_go() {
    let fake = Fake::new(BASE + 10 * 86_400);
    seed_files(&fake, 4);
    let windows = request_windows(&fake);
    let compact = request_compact_key(&windows[1..]);
    fake.bucket.borrow_mut().insert(compact.clone(), vec![1]);
    publish_files(&fake, &[compact], &[], &windows[1..]);
    let extents = |fake: &Fake| {
        fake.new_request();
        api::extents(fake, &service(), Instant::now()).unwrap()
    };
    let found = extents(&fake);
    assert_eq!(found["total_rows"], 10);
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 240));
    for source in &windows[1..] {
        fake.bucket.borrow_mut().remove(source);
    }
    let found = extents(&fake);
    assert_eq!(found["total_rows"], 10);
    assert_eq!(found["earliest"], ingest::rfc3339(BASE));
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 240));
}

#[test]
fn log_extents_uses_complete_committed_statistics_beyond_the_old_list_cap() {
    let fake = Fake::new(BASE);
    seed_files(&fake, 4001);
    fake.new_request();
    let found = api::extents(&fake, &service(), Instant::now()).unwrap();
    assert_eq!(found["total_rows_partial"], false);
    assert_eq!(found["total_rows"], 4001u64 * 4002 / 2);
    assert_eq!(found["latest"], ingest::rfc3339(BASE + 4001 * 60));
}

#[test]
fn trees_list_one_folder_per_request_without_object_gets() {
    let fake = Fake::new(BASE);
    for minute in 0..130 {
        put_raw(&fake, BASE + minute * 60, "a", &[line(BASE, "a")]);
    }
    fake.bucket
        .borrow_mut()
        .insert("raw/request/odd.gz".into(), vec![1, 2]);
    let top = admin_page(&fake, Some(true), "").unwrap();
    assert_eq!(top["root"], "raw/");
    assert_eq!(top["nodes"].as_array().unwrap().len(), 1);
    assert_eq!(top["nodes"][0]["name"], "request");
    assert_eq!(top["nodes"][0]["prefix"], "request/");
    let root = admin_page(&fake, Some(true), "prefix=request%2F").unwrap();
    assert_eq!(root["complete"], true);
    let nodes = root["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["name"], "year=2027");
    assert_eq!(nodes[0]["type"], "directory");
    assert_eq!(nodes[0]["prefix"], "request/year=2027/");
    assert_eq!(nodes[1]["name"], "odd.gz");
    assert_eq!(nodes[1]["size"], 2);
    assert_eq!(nodes[1]["mtime"], "2027-01-15T08:00:00.000Z");

    let hour = admin_page(
        &fake,
        Some(true),
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2F",
    )
    .unwrap();
    let names: Vec<_> = hour["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["hour=08", "hour=09", "hour=10"]);
    let minute = admin_page(
        &fake,
        Some(true),
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2Fhour%3D08%2Fminute%3D00%2F",
    )
    .unwrap();
    assert_eq!(minute["nodes"][0]["name"], "a.zst");
    assert_eq!(
        minute["nodes"][0]["key"],
        "raw/request/year=2027/month=01/day=15/hour=08/minute=00/a.zst"
    );
    assert!(fake.cdn_gets.borrow().is_empty());

    seed_files(&fake, 3);
    let storage = admin_page(
        &fake,
        Some(false),
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2Fhour%3D08%2F",
    )
    .unwrap();
    assert_eq!(storage["root"], "processed/");
    let top = admin_page(&fake, Some(false), "").unwrap();
    let names: Vec<_> = top["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["request"]);
    assert_eq!(storage["nodes"].as_array().unwrap().len(), 3);
    assert_eq!(storage["nodes"][0]["rows"], 1);
    assert_eq!(storage["nodes"][0]["size"], 3);
}

#[test]
fn tree_pages_are_bounded_and_cursors_bind_endpoint_and_prefix() {
    let fake = Fake::new(BASE);
    for i in 0..1500 {
        fake.bucket.borrow_mut().insert(
            format!("raw/request/year=2027/month=01/day=15/hour=08/minute=00/{i:05}.gz"),
            vec![0],
        );
    }
    let folder =
        "prefix=request%2Fyear%3D2027%2Fmonth%3D01%2Fday%3D15%2Fhour%3D08%2Fminute%3D00%2F";
    let page = admin_page(&fake, Some(true), folder).unwrap();
    assert_eq!(page["nodes"].as_array().unwrap().len(), 1000);
    assert_eq!(page["complete"], false);
    let next = cursor_query(page["next_cursor"].as_str().unwrap());
    let rest = admin_page(&fake, Some(true), &format!("{folder}&{next}")).unwrap();
    assert_eq!(rest["nodes"].as_array().unwrap().len(), 500);
    assert_eq!(rest["complete"], true);
    for bad in [next.clone(), format!("{folder}x%2F&{next}")] {
        assert_eq!(admin_page(&fake, Some(true), &bad).unwrap_err().0, 400);
    }
    assert_eq!(
        admin_page(&fake, Some(false), &format!("{folder}&{next}"))
            .unwrap_err()
            .0,
        400
    );
    for prefix in [
        "prefix=..%2F",
        "prefix=request%2Fyear%3D2027",
        "prefix=%2Fetc%2F",
        "prefix=a%2F%2Fb%2F",
    ] {
        assert_eq!(admin_page(&fake, Some(true), prefix).unwrap_err().0, 400);
    }
}

#[test]
fn multipart_uploads_assemble_their_parts_or_abort() {
    let fake = Fake::new(BASE);
    let service = service();
    let store = store(&fake, &service);
    let big = vec![7u8; 5 * 1024 * 1024];
    let id = store.multipart_create("a/b.parquet").unwrap();
    let mut parts = vec![store
        .multipart_part("a/b.parquet", &id, 1, big.clone())
        .unwrap()];
    parts.push(
        store
            .multipart_part("a/b.parquet", &id, 2, vec![1, 2])
            .unwrap(),
    );
    store
        .multipart_complete("a/b.parquet", &id, &parts)
        .unwrap();
    let stored = fake.bucket.borrow()["a/b.parquet"].clone();
    assert_eq!(stored.len(), big.len() + 2);
    assert_eq!(&stored[big.len()..], &[1, 2]);
    assert!(fake.uploads.borrow().is_empty());

    let id = store.multipart_create("a/c.parquet").unwrap();
    let small = store
        .multipart_part("a/c.parquet", &id, 1, vec![1])
        .unwrap();
    let last = store
        .multipart_part("a/c.parquet", &id, 2, vec![2])
        .unwrap();
    assert!(store
        .multipart_complete("a/c.parquet", &id, &[small, last])
        .is_err());
    store.multipart_abort("a/c.parquet", &id).unwrap();
    assert!(fake.uploads.borrow().is_empty());
    assert!(!fake.bucket.borrow().contains_key("a/c.parquet"));
}

#[test]
fn services_default_to_delete_after_and_thirty_day_retention() {
    let svc = service();
    assert!(svc.delete_after);
    assert_eq!((svc.log_retention_days, svc.rum_retention_days), (30, 30));
}

#[test]
fn raw_delete_removes_only_queued_keys_and_writes_version_two_rows() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 3);
    let bystander = raw_key(BASE + 1800, "unqueued");
    fake.bucket.borrow_mut().insert(bystander.clone(), vec![1]);
    let svc = service();
    let report = delete_tick(&fake, &svc);
    assert_eq!(
        (report.deleted, report.failed, report.more_pending),
        (3, 0, false)
    );
    for key in &keys {
        assert!(!fake.bucket.borrow().contains_key(key));
    }
    assert!(fake.bucket.borrow().contains_key(&bystander));
    assert!(deleteq(&fake).is_empty());
    let latest = telemetry::latest_ingest_state(&rows(&fake, LogKind::IngestFiles));
    assert_eq!(latest.len(), 3);
    for row in latest {
        assert_eq!(row["state_version"], 2);
        assert_eq!(row["status"], "converted");
        assert_eq!(row["raw_deleted"], true);
        assert!(row["raw_deleted_at"].is_i64() || row["raw_deleted_at"].is_string());
        assert!(keys.contains(&row["raw_key"].as_str().unwrap().to_owned()));
    }
    // The three raw objects plus the committed plan's one intent manifest, in one request.
    assert_eq!(*fake.delete_posts.borrow(), vec![4]);
    assert!(fake.keys("processed/_inflight/").is_empty());
}

#[test]
fn manifest_cleanup_has_one_queue_owner_when_raw_deletion_is_disabled() {
    for inline in [false, true] {
        let fake = Fake::new(BASE + 3600);
        let raw = put_raw(&fake, BASE, "retained", &[line(BASE, "one")]);
        let mut svc = service();
        svc.delete_after = false;
        let buffer = LogBuffer::new(SID, "commit");
        let mut options = ingest::coordinator::Options::request(&fake);
        options.defer_deleteq = inline;
        let report = ingest::coordinator::coordinate(
            &fake,
            &svc,
            "ak",
            "sk",
            Instant::now(),
            &buffer,
            &options,
        )
        .unwrap();
        assert_eq!(report.converted, 1);
        assert!(
            fake.delete_posts.borrow().is_empty(),
            "commit must not eagerly delete"
        );
        let queues: Vec<_> = report.pending_deleteq.into_iter().collect();
        let cleanup = ingest::retention::raw_delete_with_inline(
            &store(&fake, &svc),
            &buffer,
            4,
            &Default::default(),
            &queues,
            !inline,
        )
        .unwrap();
        assert_eq!((cleanup.queues, cleanup.failed, cleanup.deleted), (1, 0, 0));
        assert_eq!(cleanup.backend_calls, if inline { 1 } else { 5 });
        assert_eq!(*fake.delete_posts.borrow(), vec![1]);
        assert!(fake.bucket.borrow().contains_key(&raw));
        assert!(fake.keys("processed/_inflight/").is_empty());
        assert!(deleteq(&fake).is_empty());
        assert!(fake.calls.get() <= BUDGET);
    }
}

#[test]
fn all_held_commit_keeps_a_durable_manifest_cleanup_queue() {
    let fake = Fake::new(BASE + 3600);
    let bad = raw_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"broken".to_vec());
    let mut svc = service();
    svc.delete_after = false;
    let buffer = LogBuffer::new(SID, "commit");
    let mut options = ingest::coordinator::Options::request(&fake);
    options.defer_deleteq = true;
    let report =
        ingest::coordinator::coordinate(&fake, &svc, "ak", "sk", Instant::now(), &buffer, &options)
            .unwrap();
    assert_eq!((report.converted, report.failed), (0, 1));
    assert!(report.pending_deleteq.is_none());
    assert_eq!(deleteq(&fake).len(), 1);
    assert!(fake.delete_posts.borrow().is_empty());
    let cleanup = delete_tick(&fake, &svc);
    assert_eq!(
        (cleanup.queues, cleanup.quarantined, cleanup.failed),
        (1, 1, 0)
    );
    assert_eq!(*fake.delete_posts.borrow(), vec![1]);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn inline_manifest_failure_or_budget_deferral_keeps_a_durable_retry() {
    for (remaining, elapsed) in [(5, 0), (6, 0), (BUDGET, 0), (7, 40)] {
        let fake = Fake::new(BASE + 3600);
        let raw = put_raw(&fake, BASE, "retained", &[line(BASE, "one")]);
        let mut svc = service();
        svc.delete_after = false;
        let buffer = LogBuffer::new(SID, "commit");
        let mut options = ingest::coordinator::Options::request(&fake);
        options.defer_deleteq = true;
        let report = ingest::coordinator::coordinate(
            &fake,
            &svc,
            "ak",
            "sk",
            Instant::now(),
            &buffer,
            &options,
        )
        .unwrap();
        let queue = report.pending_deleteq.unwrap();
        let manifest = queue.1["inflight_keys"][0].as_str().unwrap().to_owned();
        fake.fail_delete_keys.borrow_mut().insert(manifest.clone());
        fake.new_request();
        fake.calls.set(BUDGET - remaining);
        fake.elapsed_s.set(elapsed);
        let cleanup = ingest::retention::raw_delete_with_inline(
            &store(&fake, &svc),
            &buffer,
            4,
            &Default::default(),
            &[queue],
            false,
        )
        .unwrap();
        assert!(cleanup.more_pending, "remaining={remaining}");
        assert_eq!(cleanup.failed, 1, "remaining={remaining}");
        assert_eq!(
            cleanup.backend_calls,
            if remaining == 5 || elapsed == 40 {
                1
            } else {
                2
            }
        );
        assert_eq!(deleteq(&fake).len(), 1, "remaining={remaining}");
        assert_eq!(deleteq(&fake)[0]["inflight_keys"], json!([manifest]));
        assert!(fake.calls.get() <= BUDGET - 4, "tail reserve must survive");
        if remaining == 5 || elapsed == 40 {
            assert!(
                fake.delete_posts.borrow().is_empty(),
                "reserve retry persistence before FOS"
            );
        }
        fake.fail_delete_keys.borrow_mut().clear();
        fake.elapsed_s.set(0);
        let retry = delete_tick(&fake, &svc);
        assert_eq!((retry.queues, retry.failed, retry.deleted), (1, 0, 0));
        assert!(fake.bucket.borrow().contains_key(&raw));
        assert!(deleteq(&fake).is_empty());
        assert!(fake.keys("processed/_inflight/").is_empty());
    }
}

#[test]
fn scheduler_consumes_all_held_queues_with_raw_retention_enabled_from_the_start() {
    for stream in ["request", "rum"] {
        let fake = Fake::new(BASE + 3600);
        tick_runtime(&fake);
        let mut svc = rum_service();
        svc.delete_after = false;
        store_service(&fake, &svc);
        let bad = if stream == "rum" {
            rum_key(BASE, "bad")
        } else {
            raw_key(BASE, "bad")
        };
        fake.bucket
            .borrow_mut()
            .insert(bad.clone(), b"broken".to_vec());
        for _ in 0..3 {
            freshness::finish_tick(&fake);
            if !quarantine_records(&fake).is_empty() {
                break;
            }
            fake.now.set(fake.now.get() + 120);
        }
        assert_eq!(quarantine_records(&fake).len(), 1, "{stream}");
        assert!(deleteq(&fake).is_empty(), "{stream}");
        assert!(fake.keys("processed/_inflight/").is_empty(), "{stream}");
        assert!(fake.bucket.borrow().contains_key(&bad), "{stream}");
        assert_eq!(*fake.delete_posts.borrow(), vec![1], "{stream}");
    }
}

#[test]
fn scheduler_retries_a_durable_rum_held_queue_alongside_inline_request_cleanup() {
    let fake = Fake::new(BASE + 3600);
    tick_runtime(&fake);
    let mut svc = rum_service();
    svc.delete_after = false;
    store_service(&fake, &svc);
    let bad = rum_key(BASE, "bad");
    fake.bucket
        .borrow_mut()
        .insert(bad.clone(), b"broken".to_vec());
    assert_eq!(rum_tick(&fake, &svc).unwrap().failed, 1);
    assert_eq!(deleteq(&fake).len(), 1);
    for minute in 0..3 {
        put_raw(
            &fake,
            BASE + minute * 60,
            "retained",
            &[line(BASE + minute * 60, "request")],
        );
        run_tick(&fake);
        fake.now.set(fake.now.get() + 120);
    }
    assert!(
        deleteq(&fake).is_empty(),
        "due raw-delete discovery must not starve behind inline commits: {:?}; jobs={}",
        deleteq(&fake),
        fake.kv.borrow()["cron:state"].1["jobs"],
    );
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert!(fake.keys("processed/_inflight/").is_empty());
}

#[test]
fn delete_queue_decoding_preserves_legacy_times_streams_and_partial_retries() {
    for inline in [false, true] {
        let fake = Fake::new(BASE + 3600);
        let mut svc = service();
        svc.delete_after = false;
        let mut queues = Vec::new();
        for stream in ["request", "rum"] {
            let run = format!("legacy-{stream}");
            let manifest = format!("processed/_inflight/{run}.json");
            let raw = format!("raw/{stream}/held.zst");
            fake.bucket.borrow_mut().insert(manifest.clone(), vec![1]);
            fake.bucket.borrow_mut().insert(raw.clone(), vec![2]);
            fake.fail_delete_keys.borrow_mut().insert(manifest.clone());
            let value = json!({
                "run_id": run, "stream": stream, "keys": [], "sizes": "legacy-invalid",
                "inflight_keys": [manifest], "held": [{"key": raw, "reason": "corrupt_gzip"}],
                "at": BASE,
            });
            let key = format!("{}{run}", ingest::DELETEQ_PREFIX);
            if !inline {
                fake.kv.borrow_mut().insert(key.clone(), (7, value.clone()));
            }
            queues.push((key, value));
        }
        let buffer = LogBuffer::new(SID, "delete");
        let report = ingest::retention::raw_delete_with_inline(
            &store(&fake, &svc),
            &buffer,
            4,
            &Default::default(),
            if inline { &queues } else { &[] },
            !inline,
        )
        .unwrap();
        assert_eq!(
            (report.failed, report.quarantined, report.deleted),
            (2, 2, 0)
        );
        let quarantine = quarantine_records(&fake);
        assert_eq!(quarantine.len(), 2);
        assert!(quarantine.iter().all(|record| record["first_seen"] == BASE));
        for (key, original) in queues {
            let kv = fake.kv.borrow();
            let (generation, value) = &kv[&key];
            assert_eq!(*generation, if inline { 1 } else { 8 });
            assert_eq!(value["stream"], original["stream"]);
            assert_eq!(value["run_id"], original["run_id"]);
            assert_eq!(value["inflight_keys"], original["inflight_keys"]);
            assert_eq!(value["held"], json!([]));
            assert_eq!(value["planned_at"], BASE);
            assert_eq!(value["sizes"], json!([]));
        }
        fake.fail_delete_keys.borrow_mut().clear();
        assert_eq!(delete_tick(&fake, &svc).queues, 2);
        assert!(deleteq(&fake).is_empty());
        assert_eq!(quarantine_records(&fake).len(), 2);
    }
}

#[test]
fn malformed_inline_and_stored_delete_queues_return_the_existing_error() {
    for inline in [false, true] {
        let fake = Fake::new(BASE + 3600);
        let value = json!({"run_id": "invalid", "keys": [null], "held": []});
        let key = format!("{}invalid", ingest::DELETEQ_PREFIX);
        if !inline {
            fake.kv.borrow_mut().insert(key.clone(), (1, value.clone()));
        }
        let buffer = LogBuffer::new(SID, "delete");
        let queues = vec![(key, value)];
        let result = ingest::retention::raw_delete_with_inline(
            &store(&fake, &service()),
            &buffer,
            4,
            &Default::default(),
            if inline { &queues } else { &[] },
            !inline,
        );
        assert_eq!(result.unwrap_err(), "deleteq_invalid");
        assert!(fake.delete_posts.borrow().is_empty());
    }
}

#[test]
fn raw_delete_keeps_raw_objects_but_consumes_the_queue_when_delete_after_is_off() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 2);
    let mut svc = service();
    svc.delete_after = false;
    let report = delete_tick(&fake, &svc);
    assert_eq!(report.deleted, 0);
    // Only the plan's intent manifest is deleted; the queue record is consumed, not kept.
    assert_eq!(*fake.delete_posts.borrow(), vec![1]);
    assert!(deleteq(&fake).is_empty());
    assert!(fake.keys("processed/_inflight/").is_empty());
    assert!(keys.iter().all(|k| fake.bucket.borrow().contains_key(k)));
}

#[test]
fn a_failed_inflight_delete_counts_as_failed_when_delete_after_is_off() {
    let fake = Fake::new(BASE + 3600);
    converted_fixture(&fake, 2);
    let inflight = deleteq(&fake)[0]["inflight_keys"][0]
        .as_str()
        .unwrap()
        .to_owned();
    fake.fail_delete_keys.borrow_mut().insert(inflight.clone());
    let mut svc = service();
    svc.delete_after = false;
    let report = delete_tick(&fake, &svc);
    assert_eq!((report.deleted, report.failed), (0, 1));
    assert_eq!(deleteq(&fake)[0]["inflight_keys"], json!([inflight]));
    fake.fail_delete_keys.borrow_mut().clear();
    assert_eq!(delete_tick(&fake, &svc).failed, 0);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn per_key_delete_errors_are_requeued_and_retried() {
    let fake = Fake::new(BASE + 3600);
    let keys = converted_fixture(&fake, 3);
    fake.fail_delete_keys.borrow_mut().insert(keys[1].clone());
    let svc = service();
    let report = delete_tick(&fake, &svc);
    assert_eq!((report.deleted, report.failed), (2, 1));
    let queued = deleteq(&fake);
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0]["keys"], json!([keys[1]]));
    assert_eq!(queued[0]["sizes"].as_array().unwrap().len(), 1);
    assert!(fake.bucket.borrow().contains_key(&keys[1]));
    let rows_now = telemetry::latest_ingest_state(&rows(&fake, LogKind::IngestFiles));
    assert_eq!(
        rows_now.iter().filter(|r| r["raw_deleted"] == true).count(),
        2
    );
    fake.fail_delete_keys.borrow_mut().clear();
    assert_eq!(delete_tick(&fake, &svc).deleted, 1);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn delete_objects_batches_at_one_thousand_keys() {
    let fake = Fake::new(BASE);
    let keys: Vec<String> = (0..2500).map(|i| format!("raw/request/k{i:04}")).collect();
    for key in &keys {
        fake.bucket.borrow_mut().insert(key.clone(), vec![0; 3]);
    }
    let svc = service();
    fake.new_request();
    let result = store(&fake, &svc).delete_objects(&keys).unwrap();
    assert_eq!(result.deleted.len(), 2500);
    assert!(result.failed.is_empty());
    assert_eq!(*fake.delete_posts.borrow(), vec![1000, 1000, 500]);
}

#[test]
fn raw_delete_leaves_the_queue_when_the_budget_cannot_cover_it() {
    let fake = Fake::new(BASE + 3600);
    converted_fixture(&fake, 2);
    let svc = service();
    fake.new_request();
    fake.calls.set(BUDGET - 3);
    let buffer = LogBuffer::new(SID, "del");
    let report =
        ingest::retention::raw_delete(&store(&fake, &svc), &buffer, 4, &Default::default())
            .unwrap();
    assert_eq!(report.deleted, 0);
    assert!(report.more_pending);
    assert_eq!(deleteq(&fake).len(), 1);
    assert!(fake.delete_posts.borrow().is_empty());
}

#[test]
fn held_objects_move_to_a_quarantine_record_once_their_plan_is_committed() {
    let fake = Fake::new(BASE + 3600);
    let (bad, good) = quarantined_fixture(&fake);
    let queues = deleteq(&fake);
    assert_eq!(queues[0]["held"][0]["key"], bad);
    assert_eq!(queues[0]["held"][0]["reason"], "corrupt_gzip");
    assert_eq!(queues[0]["held"][0]["attempts"], 1);
    assert!(quarantine_records(&fake).is_empty());
    let report = delete_tick(&fake, &service());
    assert_eq!((report.deleted, report.quarantined), (1, 1));
    let records = quarantine_records(&fake);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["objects"][0]["key"], bad);
    assert_eq!(records[0]["objects"][0]["status"], "quarantined");
    assert!(records[0]["first_seen"].as_u64().unwrap() > 0);
    assert!(deleteq(&fake).is_empty());
    let bucket = fake.bucket.borrow();
    assert!(bucket.contains_key(&bad) && !bucket.contains_key(&good));
}

#[test]
fn held_objects_of_an_uncommitted_plan_are_never_quarantined() {
    let fake = Fake::new(BASE + 3600);
    let held = raw_key(BASE, "held");
    fake.kv.borrow_mut().insert(
        ingest::PLAN_KEY.into(),
        (1, json!({"plan_id": "p-open", "version": 99})),
    );
    fake.kv.borrow_mut().insert(
        "ingest:deleteq:p-open".into(),
        (
            1,
            json!({"run_id": "p-open", "keys": [], "sizes": [], "inflight_keys": [],
            "held": [{"key": held, "size": 1, "status": "failed", "reason": "x", "attempts": 1}],
            "planned_at": BASE, "at": BASE}),
        ),
    );
    delete_tick(&fake, &service());
    assert!(quarantine_records(&fake).is_empty());
    assert_eq!(deleteq(&fake).len(), 1);
}

#[test]
fn a_held_only_queue_is_quarantined_even_when_delete_after_is_off() {
    let fake = Fake::new(BASE + 3600);
    let (bad, _) = quarantined_fixture(&fake);
    let mut svc = service();
    svc.delete_after = false;
    delete_tick(&fake, &svc);
    assert_eq!(quarantine_records(&fake)[0]["objects"][0]["key"], bad);
    assert!(deleteq(&fake).is_empty());
}

#[test]
fn quarantine_lists_reason_size_first_seen_and_attempts_in_signed_pages() {
    let fake = Fake::new(BASE + 3600);
    let (bad, _) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    for i in 0..12 {
        seed_quarantine(
            &fake,
            &format!("seed{i:02}"),
            &[raw_key(BASE - 3600, &format!("s{i}"))],
        );
    }
    let first = quarantine_page(&fake, "").unwrap();
    assert_eq!(first["complete"], false);
    let items = first["items"].as_array().unwrap();
    assert_eq!(items.len(), 10);
    let mut all: Vec<Value> = items.clone();
    let cursor = first["next_cursor"].as_str().unwrap();
    let second = quarantine_page(&fake, &cursor_query(cursor)).unwrap();
    assert_eq!(second["complete"], true);
    all.extend(second["items"].as_array().unwrap().iter().cloned());
    assert_eq!(all.len(), 13);
    let item = all.iter().find(|i| i["key"] == bad).unwrap();
    assert_eq!(item["reason"], "corrupt_gzip");
    assert_eq!(item["status"], "quarantined");
    assert_eq!(item["size"], 8);
    assert_eq!(item["attempts"], 1);
    assert!(item["first_seen"].as_str().unwrap().ends_with('Z'));
    assert_eq!(item["purgeable"], true);
    let seeded = all.iter().find(|i| i["record"] == "seed03").unwrap();
    assert_eq!(seeded["attempts"], 2);
    assert_eq!(
        quarantine_page(&fake, "cursor=forged.abc").unwrap_err().0,
        400
    );
}

#[test]
fn purge_requires_a_confirm_token_bound_to_actor_target_and_time() {
    let fake = Fake::new(BASE + 3600);
    let (bad, _) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    let issued = purge_as(&fake, "u-admin", json!({"target": "all"})).unwrap();
    assert_eq!(issued["status"], "confirm_required");
    assert!(
        fake.bucket.borrow().contains_key(&bad),
        "issuing a token deletes nothing"
    );
    let token = issued["confirm_token"].clone();
    let other_actor = purge_as(&fake, "u-other", json!({"target": "all", "confirm": token}));
    let other_target = purge_as(
        &fake,
        "u-admin",
        json!({"target": bad, "record": "x", "confirm": token}),
    );
    let forged = purge_as(
        &fake,
        "u-admin",
        json!({"target": "all", "confirm": "e30.00"}),
    );
    for rejected in [other_actor, other_target, forged] {
        assert_eq!(rejected.unwrap_err().0, 400);
    }
    fake.now.set(fake.now.get() + 601);
    let expired = purge_as(&fake, "u-admin", json!({"target": "all", "confirm": token}));
    assert_eq!(expired.unwrap_err().0, 400);
    assert!(fake.bucket.borrow().contains_key(&bad));
    assert_eq!(quarantine_records(&fake).len(), 1);
}

#[test]
fn purge_all_deletes_only_quarantined_keys_at_or_below_the_watermark_and_never_reingests() {
    let fake = Fake::new(BASE + 3600);
    let (bad, good) = quarantined_fixture(&fake);
    delete_tick(&fake, &service());
    let bystander = raw_key(BASE - 600, "unqueued");
    fake.bucket.borrow_mut().insert(bystander.clone(), vec![1]);
    let pending = put_raw(&fake, BASE + 120, "pending", &[line(BASE + 120, "pending")]);
    let ahead = raw_key(BASE + 1200, "ahead");
    seed_quarantine(&fake, "zz-ahead", std::slice::from_ref(&ahead));
    let ips = processed_ips(&fake);
    let done = purge(&fake, json!({"target": "all"})).unwrap();
    assert_eq!(
        (
            done["purged"].as_u64(),
            done["skipped"].as_u64(),
            done["complete"].as_bool()
        ),
        (Some(1), Some(1), Some(true))
    );
    assert_eq!(done["bytes"], 8);
    let bucket = fake.bucket.borrow();
    assert!(!bucket.contains_key(&bad));
    for kept in [&bystander, &pending, &ahead] {
        assert!(bucket.contains_key(kept), "{kept} must survive a purge");
    }
    assert!(!bucket.contains_key(&good));
    drop(bucket);
    let records = quarantine_records(&fake);
    assert_eq!(
        records.len(),
        1,
        "only the record above the watermark is left"
    );
    assert_eq!(records[0]["objects"][0]["key"], ahead);
    let latest = telemetry::latest_ingest_state(&rows(&fake, LogKind::IngestFiles));
    let row = latest.iter().find(|r| r["raw_key"] == bad).unwrap();
    assert_eq!(
        (&row["state_version"], &row["status"], &row["raw_deleted"]),
        (&json!(2), &json!("quarantined"), &json!(true))
    );
    let next = tick(&fake).unwrap();
    assert_eq!(
        next.listed, 2,
        "only the unconverted objects above the watermark"
    );
    assert_eq!(next.converted, 1);
    let mut expected = ips;
    expected.push("pending".into());
    expected.sort();
    assert_eq!(processed_ips(&fake), expected);
}

#[test]
fn purge_one_deletes_just_that_object_and_keeps_the_rest_of_its_record() {
    let fake = Fake::new(BASE + 3600);
    let keys: Vec<String> = (0..3)
        .map(|i| raw_key(BASE - 600, &format!("q{i}")))
        .collect();
    seed_quarantine(&fake, "run1", &keys);
    set_watermark(&fake, &raw_key(BASE, "mark"));
    let done = purge(&fake, json!({"target": keys[1], "record": "run1"})).unwrap();
    assert_eq!(
        (done["purged"].as_u64(), done["complete"].as_bool()),
        (Some(1), Some(true))
    );
    let bucket = fake.bucket.borrow();
    assert!(bucket.contains_key(&keys[0]) && !bucket.contains_key(&keys[1]));
    drop(bucket);
    let left: Vec<Value> = quarantine_records(&fake)[0]["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["key"].clone())
        .collect();
    assert_eq!(left, vec![json!(keys[0]), json!(keys[2])]);
    let unknown = raw_key(BASE - 600, "never-quarantined");
    fake.bucket.borrow_mut().insert(unknown.clone(), vec![1]);
    let missing = purge(&fake, json!({"target": unknown, "record": "run1"}));
    assert_eq!(missing.unwrap_err().0, 404);
    assert!(fake.bucket.borrow().contains_key(&unknown));
    let ahead = raw_key(BASE + 600, "ahead");
    seed_quarantine(&fake, "run2", std::slice::from_ref(&ahead));
    let refused = purge(&fake, json!({"target": ahead, "record": "run2"}));
    assert_eq!(refused.unwrap_err().0, 409);
    assert!(fake.bucket.borrow().contains_key(&ahead));
    let bad_record = purge_as(
        &fake,
        "u-admin",
        json!({"target": keys[0], "record": "../x"}),
    );
    assert_eq!(bad_record.unwrap_err().0, 400);
}

#[test]
fn purge_all_is_bounded_per_step_and_resumes_from_its_cursor() {
    let fake = Fake::new(BASE + 3600);
    let mut keys = Vec::new();
    for run in 0..25 {
        let pair: Vec<String> = (0..2)
            .map(|i| raw_key(BASE - 3600 + run * 60, &format!("r{i}")))
            .collect();
        seed_quarantine(&fake, &format!("run{run:02}"), &pair);
        keys.extend(pair);
    }
    set_watermark(&fake, &raw_key(BASE, "mark"));
    let mut body = json!({"target": "all"});
    let issued = purge_as(&fake, "u-admin", body.clone()).unwrap();
    body["confirm"] = issued["confirm_token"].clone();
    let (mut steps, mut purged) = (0, 0);
    loop {
        steps += 1;
        assert!(steps < 20, "purge never completed");
        let posts = fake.delete_posts.borrow().len();
        let step = purge_as(&fake, "u-admin", body.clone()).unwrap();
        assert!(fake.delete_posts.borrow().len() - posts <= 1);
        purged += step["purged"].as_u64().unwrap();
        if step["complete"] == true {
            break;
        }
        assert!(
            step["purged"].as_u64().unwrap() > 0,
            "a step must make progress"
        );
        body["cursor"] = step["next_cursor"].clone();
        body["confirm"] = step["confirm_token"].clone();
    }
    assert!(steps > 2, "25 records cannot fit one step");
    assert_eq!(purged, 50);
    assert!(keys.iter().all(|k| !fake.bucket.borrow().contains_key(k)));
    assert!(quarantine_records(&fake).is_empty());
    body["cursor"] = json!("forged.cursor");
    assert_eq!(purge_as(&fake, "u-admin", body).unwrap_err().0, 400);
}

#[test]
fn a_purge_step_with_too_little_budget_deletes_nothing() {
    let fake = Fake::new(BASE + 3600);
    let key = raw_key(BASE - 600, "q");
    seed_quarantine(&fake, "run1", std::slice::from_ref(&key));
    set_watermark(&fake, &raw_key(BASE, "mark"));
    let issued = purge_as(&fake, "u-admin", json!({"target": "all"})).unwrap();
    fake.new_request();
    fake.calls.set(BUDGET - api::PURGE_RESERVE - 5);
    let buffer = LogBuffer::new(SID, "purge");
    let body = json!({"target": "all", "confirm": issued["confirm_token"]});
    let result = api::quarantine_purge(
        &fake,
        &service(),
        "u-admin",
        body.to_string().as_bytes(),
        &buffer,
        Instant::now(),
    );
    assert_eq!(result.err().map(|e| e.0), Some(503));
    assert!(fake.bucket.borrow().contains_key(&key));
    assert_eq!(quarantine_records(&fake).len(), 1);
}
