//! Fastly NGWAF Verified Bot polling and daily summary storage for Cloud installs.
//!
//! Polls `GET /ngwaf/v1/workspaces/{workspace_id}/requests?q=tag:VERIFIED-BOT ...` on a bounded
//! cadence inside the maintenance rotation, deduplicates by `waf_req_id` hash, and persists
//! minute-bucketed bot counts per UTC day in FOS at `{scope}processed/ngwaf_bots/day=YYYY-MM-DD.json`.

use super::{rfc3339, scope, storage::Store};
use crate::{admin::ApiRequest, shell::Runtime};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

pub const STATE_KEY: &str = "ingest:ngwaf_bots_state";
pub const SYNC_INTERVAL_S: u64 = 120;
/// NGWAF `/requests` entries carry full headers (~4–6 KB each); 100 fits well under the 2 MiB
/// `bounded_json` backend response cap.
const PAGE_LIMIT: usize = 100;
const MAX_PAGES_PER_RUN: usize = 6;
const DAY_FILE_LIMIT: usize = 2 * 1024 * 1024;
const MAX_SEEN_PER_DAY: usize = 50_000;
const MAX_QUERY_DAYS: usize = 31;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SyncState {
    #[serde(default)]
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ts: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_last_ts: Option<String>,
    #[serde(default)]
    pub last_run: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MinuteBucket {
    /// UTC unix seconds floored to the minute.
    pub m: i64,
    /// NGWAF verified bot name (from `VERIFIED-BOT` signal `value`).
    pub b: String,
    /// NGWAF verified bot subcategory (from `VERIFIED-BOT.<cat>` signal), if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
    /// Sample User-Agent string seen for this bot, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub u: Option<String>,
    /// Deduplicated request count in this minute bucket.
    pub n: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DaySummary {
    pub version: u32,
    pub day: String,
    #[serde(default)]
    pub buckets: Vec<MinuteBucket>,
    /// Truncated SHA-256 hashes of `waf_req_id` values already counted in `buckets`.
    #[serde(default)]
    pub seen: BTreeSet<String>,
}

#[derive(Debug, Default)]
pub struct SyncReport {
    pub pages: u64,
    pub records: u64,
    pub added: u64,
    pub days_written: u64,
    pub more_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotRecord {
    pub waf_req_id: String,
    pub bot_name: String,
    pub category: Option<String>,
    pub user_agent: Option<String>,
    pub timestamp_s: i64,
    pub timestamp_iso: String,
}

pub fn day_key(service: &crate::admin::Service, day: &str) -> String {
    format!("{}processed/ngwaf_bots/day={day}.json", scope(service))
}

fn hash_req_id(id: &str) -> String {
    let digest = Sha256::digest(id.as_bytes());
    hex::encode(&digest[..8])
}

fn parse_rfc3339_secs(ts: &str) -> Option<i64> {
    time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|dt| dt.unix_timestamp())
}

fn day_string(unix_s: i64) -> Option<String> {
    let dt = time::OffsetDateTime::from_unix_timestamp(unix_s).ok()?;
    Some(format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day()
    ))
}

/// Converts a last-synced RFC3339 timestamp to a rounded-up relative NGWAF query range
/// (e.g. `-15min`, `-2h`, `-3d`), matching High-Scale's `_get_relative_time_range`.
pub fn relative_time_range(last_ts: Option<&str>, now_s: i64) -> String {
    let Some(ts_str) = last_ts else {
        return "-24h".into();
    };
    let Some(ts) = parse_rfc3339_secs(ts_str) else {
        return if ts_str.starts_with('-') {
            ts_str.to_owned()
        } else {
            "-24h".into()
        };
    };
    let seconds = now_s.saturating_sub(ts).max(60);
    if seconds > 86_400 {
        let days = ((seconds + 86_399) / 86_400).min(30);
        format!("-{days}d")
    } else if seconds > 3_600 {
        let hours = (seconds + 3_599) / 3_600;
        format!("-{hours}h")
    } else {
        let mins = ((seconds + 899) / 900) * 15;
        format!("-{}min", mins.max(15))
    }
}

/// Extracts a verified-bot record from one NGWAF `/requests` response entry.
pub fn extract_bot_record(entry: &Value) -> Option<BotRecord> {
    let signals = entry["signals"].as_array()?;
    let verified = signals
        .iter()
        .find(|s| s["id"].as_str() == Some("VERIFIED-BOT"))?;
    let bot_name = verified["value"]
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())?
        .to_owned();
    let category = signals.iter().find_map(|s| {
        let id = s["id"].as_str()?;
        id.strip_prefix("VERIFIED-BOT.")
            .filter(|c| !c.is_empty())
            .map(str::to_owned)
    });
    let waf_req_id = entry["id"].as_str().filter(|id| !id.is_empty())?.to_owned();
    let timestamp_iso = entry["timestamp"].as_str()?.to_owned();
    let timestamp_s = parse_rfc3339_secs(&timestamp_iso)?;
    let user_agent = entry["user_agent"]
        .as_str()
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            entry["request_headers"].as_array()?.iter().find_map(|h| {
                let name = h["name"].as_str()?;
                if name.eq_ignore_ascii_case("user-agent") {
                    h["value"]
                        .as_str()
                        .filter(|v| !v.is_empty())
                        .map(str::to_owned)
                } else {
                    None
                }
            })
        });
    Some(BotRecord {
        waf_req_id,
        bot_name,
        category,
        user_agent,
        timestamp_s,
        timestamp_iso,
    })
}

fn merge_records(summary: &mut DaySummary, records: &[BotRecord]) -> u64 {
    let mut map: BTreeMap<(i64, String), MinuteBucket> = summary
        .buckets
        .drain(..)
        .map(|b| ((b.m, b.b.clone()), b))
        .collect();
    let mut added = 0u64;
    for rec in records {
        let h = hash_req_id(&rec.waf_req_id);
        if summary.seen.contains(&h) {
            continue;
        }
        if summary.seen.len() < MAX_SEEN_PER_DAY {
            summary.seen.insert(h);
        }
        let minute = rec.timestamp_s - rec.timestamp_s.rem_euclid(60);
        let entry = map
            .entry((minute, rec.bot_name.clone()))
            .or_insert_with(|| MinuteBucket {
                m: minute,
                b: rec.bot_name.clone(),
                c: rec.category.clone(),
                u: rec.user_agent.clone(),
                n: 0,
            });
        entry.n += 1;
        if entry.c.is_none() && rec.category.is_some() {
            entry.c = rec.category.clone();
        }
        if entry.u.is_none() && rec.user_agent.is_some() {
            entry.u = rec.user_agent.clone();
        }
        added += 1;
    }
    summary.buckets = map.into_values().collect();
    added
}

/// Runs one bounded NGWAF verified-bot poll step for the active service, if configured and due.
pub fn sync_step<R: Runtime>(
    store: &Store<'_, R>,
    tail: u32,
) -> Result<Option<SyncReport>, String> {
    let runtime = store.runtime;
    let Some(workspace_id) = store
        .service
        .ngwaf_workspace_id
        .as_deref()
        .map(str::trim)
        .filter(|w| !w.is_empty())
    else {
        return Ok(None);
    };
    if runtime.admin_remaining_calls() <= tail + 5 {
        return Ok(None);
    }
    let token = runtime
        .secret(&store.service.fastly_token_ref)?
        .filter(|t| !t.is_empty())
        .ok_or("fastly_token_missing")?;
    let now = runtime.now();
    let existing = runtime.admin_get(STATE_KEY)?;
    let mut state = existing
        .as_ref()
        .and_then(|r| serde_json::from_value::<SyncState>(r.value.clone()).ok())
        .unwrap_or_default();
    if state.workspace_id != workspace_id {
        state = SyncState {
            workspace_id: workspace_id.to_owned(),
            ..Default::default()
        };
    }
    if state.next_cursor.is_some() && state.active_query.is_none() {
        state.next_cursor = None;
        state.pending_last_ts = None;
    }
    if state.next_cursor.is_none() && state.last_run.saturating_add(SYNC_INTERVAL_S) > now {
        return Ok(None);
    }

    let mut report = SyncReport::default();
    let mut by_day: BTreeMap<String, Vec<BotRecord>> = BTreeMap::new();
    let prev_last_ts = state.last_ts.clone();
    let mut newest_ts = state
        .pending_last_ts
        .clone()
        .or_else(|| state.last_ts.clone());
    let mut cursor_reset = false;
    let api_path = format!("/ngwaf/v1/workspaces/{workspace_id}/requests");

    for _ in 0..MAX_PAGES_PER_RUN {
        if runtime.admin_remaining_calls() <= tail + 4 || !store.within_wall() {
            report.more_pending = true;
            break;
        }
        let q = state
            .active_query
            .get_or_insert_with(|| {
                let from_rel = relative_time_range(state.last_ts.as_deref(), now as i64);
                format!("tag:VERIFIED-BOT from:{from_rel}")
            })
            .clone();
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("q", &q);
        query.append_pair("limit", &PAGE_LIMIT.to_string());
        if let Some(cursor) = state.next_cursor.as_deref() {
            query.append_pair("cursor", cursor);
        }
        let resp = runtime.admin_api(ApiRequest {
            method: "GET",
            path: &api_path,
            query: &query.finish(),
            token: &token,
            body: None,
            timeout: Duration::from_secs(15),
        })?;
        if resp.status == 401 || resp.status == 403 {
            return Err("ngwaf_api_unauthorized".into());
        }
        if resp.status == 400 && state.next_cursor.is_some() {
            state.next_cursor = None;
            state.active_query = None;
            state.pending_last_ts = None;
            newest_ts = state.last_ts.clone();
            if !cursor_reset {
                cursor_reset = true;
                continue;
            }
            let val = serde_json::to_value(&state).map_err(|e| e.to_string())?;
            let _ = runtime.admin_cas(
                STATE_KEY,
                existing.as_ref().map(|r| r.generation.as_str()),
                &val,
            )?;
            return Err("ngwaf_api_status_400".into());
        }
        if !(200..300).contains(&resp.status) {
            return Err(format!("ngwaf_api_status_{}", resp.status));
        }
        report.pages += 1;
        let mut reached_prev = false;
        if let Some(items) = resp.body["data"].as_array() {
            for item in items {
                if let Some(rec) = extract_bot_record(item) {
                    if newest_ts
                        .as_deref()
                        .is_none_or(|prev| rec.timestamp_iso.as_str() > prev)
                    {
                        newest_ts = Some(rec.timestamp_iso.clone());
                    }
                    if prev_last_ts
                        .as_deref()
                        .is_some_and(|prev| rec.timestamp_iso.as_str() <= prev)
                    {
                        reached_prev = true;
                    }
                    if let Some(day) = day_string(rec.timestamp_s) {
                        by_day.entry(day).or_default().push(rec);
                        report.records += 1;
                    }
                }
            }
        }
        if reached_prev {
            state.next_cursor = None;
            break;
        }
        let next = resp.body["meta"]["next_cursor"]
            .as_str()
            .or_else(|| resp.body["next_cursor"].as_str())
            .filter(|c| !c.is_empty())
            .map(str::to_owned);
        state.next_cursor = next;
        if state.next_cursor.is_none() {
            break;
        }
    }

    let mut write_truncated = false;
    for (day, records) in by_day {
        if runtime.admin_remaining_calls() <= tail + 2 || !store.within_wall() {
            write_truncated = true;
            report.more_pending = true;
            break;
        }
        let key = day_key(store.service, &day);
        let mut summary = match store.get_direct(&key, DAY_FILE_LIMIT)? {
            Some(bytes) => {
                serde_json::from_slice::<DaySummary>(&bytes).unwrap_or_else(|_| DaySummary {
                    version: 1,
                    day: day.clone(),
                    ..Default::default()
                })
            }
            None => DaySummary {
                version: 1,
                day: day.clone(),
                ..Default::default()
            },
        };
        let added = merge_records(&mut summary, &records);
        if added > 0 {
            let body = serde_json::to_vec(&summary).map_err(|e| e.to_string())?;
            store.put_verified(&key, body)?;
            report.added += added;
            report.days_written += 1;
        }
    }

    if state.next_cursor.is_none() && !write_truncated {
        state.last_ts = newest_ts.or_else(|| Some(rfc3339(now as i64)));
        state.pending_last_ts = None;
        state.active_query = None;
        state.last_run = now;
    } else if state.next_cursor.is_some() {
        state.pending_last_ts = newest_ts;
        report.more_pending = true;
    } else {
        state.active_query = None;
        state.pending_last_ts = None;
        report.more_pending = true;
    }
    let val = serde_json::to_value(&state).map_err(|e| e.to_string())?;
    let _ = runtime.admin_cas(
        STATE_KEY,
        existing.as_ref().map(|r| r.generation.as_str()),
        &val,
    )?;
    Ok(Some(report))
}

/// Queries stored NGWAF verified-bot daily summaries over `[from_s, to_s)` and returns the
/// `ngwaf_configured`, `ngwaf_verified_bots`, `ngwaf_verified_bots_ts`, and `ngwaf_bots` fields.
pub fn query_range<R: Runtime>(
    store: &Store<'_, R>,
    from_s: i64,
    to_s: i64,
    bucket_seconds: i64,
) -> Result<Value, String> {
    let configured = store
        .service
        .ngwaf_workspace_id
        .as_deref()
        .map(str::trim)
        .is_some_and(|w| !w.is_empty());
    if !configured {
        return Ok(json!({
            "ngwaf_configured": false,
            "ngwaf_verified_bots": [],
            "ngwaf_verified_bots_ts": [],
            "ngwaf_bots": [],
        }));
    }
    let bucket = bucket_seconds.max(60);
    let start_day = from_s - from_s.rem_euclid(86_400);
    let end_day = to_s.saturating_sub(1) - to_s.saturating_sub(1).rem_euclid(86_400);
    let mut days = Vec::new();
    let mut cur = start_day;
    while cur <= end_day {
        if let Some(d) = day_string(cur) {
            days.push(d);
        }
        cur = cur.saturating_add(86_400);
    }
    if days.len() > MAX_QUERY_DAYS {
        days = days.split_off(days.len() - MAX_QUERY_DAYS);
    }

    let mut ts_totals: BTreeMap<(i64, String), u64> = BTreeMap::new();
    let mut bot_totals: BTreeMap<String, u64> = BTreeMap::new();
    let mut bot_cats: BTreeMap<String, String> = BTreeMap::new();

    for day in days {
        if store.runtime.admin_remaining_calls() <= 2 {
            break;
        }
        let key = day_key(store.service, &day);
        let Some(bytes) = store.get_direct(&key, DAY_FILE_LIMIT)? else {
            continue;
        };
        let Ok(summary) = serde_json::from_slice::<DaySummary>(&bytes) else {
            continue;
        };
        for entry in summary.buckets {
            if entry.m < from_s || entry.m >= to_s || entry.n == 0 {
                continue;
            }
            let b_ts = entry.m - entry.m.rem_euclid(bucket);
            *ts_totals.entry((b_ts, entry.b.clone())).or_insert(0) += entry.n;
            *bot_totals.entry(entry.b.clone()).or_insert(0) += entry.n;
            if let Some(cat) = entry.c {
                bot_cats.entry(entry.b).or_insert(cat);
            }
        }
    }

    let mut verified_bots: Vec<Value> = bot_totals
        .iter()
        .map(|(name, count)| {
            json!({
                "bot_name": name,
                "wellknown_bot_name": Value::Null,
                "category": bot_cats.get(name).cloned(),
                "request_count": count,
            })
        })
        .collect();
    verified_bots.sort_by(|a, b| {
        b["request_count"]
            .as_u64()
            .cmp(&a["request_count"].as_u64())
            .then_with(|| a["bot_name"].as_str().cmp(&b["bot_name"].as_str()))
    });

    let top_ngwaf_bots: Vec<Value> = verified_bots
        .iter()
        .take(10)
        .map(|row| {
            json!({
                "bot_name": row["bot_name"],
                "category": row["category"],
                "request_count": row["request_count"],
            })
        })
        .collect();

    let verified_bots_ts: Vec<Value> = ts_totals
        .into_iter()
        .map(|((ts, name), count)| {
            json!({
                "time": rfc3339(ts),
                "bot_name": name,
                "count": count,
            })
        })
        .collect();

    Ok(json!({
        "ngwaf_configured": true,
        "ngwaf_verified_bots": verified_bots,
        "ngwaf_verified_bots_ts": verified_bots_ts,
        "ngwaf_bots": top_ngwaf_bots,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_verified_bot_and_subcategory() {
        let entry = json!({
            "id": "req-abc-123",
            "timestamp": "2026-11-26T20:15:30Z",
            "signals": [
                {"id": "VERIFIED-BOT", "value": "Googlebot"},
                {"id": "VERIFIED-BOT.SEARCH-ENGINE"}
            ],
            "request_headers": [
                {"name": "User-Agent", "value": "Mozilla/5.0 (compatible; Googlebot/2.1)"}
            ]
        });
        let rec = extract_bot_record(&entry).expect("record");
        assert_eq!(rec.waf_req_id, "req-abc-123");
        assert_eq!(rec.bot_name, "Googlebot");
        assert_eq!(rec.category.as_deref(), Some("SEARCH-ENGINE"));
        assert_eq!(
            rec.user_agent.as_deref(),
            Some("Mozilla/5.0 (compatible; Googlebot/2.1)")
        );
    }

    #[test]
    fn merge_deduplicates_overlapping_polls() {
        let mut summary = DaySummary {
            version: 1,
            day: "2026-11-26".into(),
            ..Default::default()
        };
        let rec = BotRecord {
            waf_req_id: "req-1".into(),
            bot_name: "Googlebot".into(),
            category: Some("SEARCH-ENGINE".into()),
            user_agent: None,
            timestamp_s: 1_795_724_130,
            timestamp_iso: "2026-11-26T20:15:30Z".into(),
        };
        assert_eq!(merge_records(&mut summary, std::slice::from_ref(&rec)), 1);
        assert_eq!(merge_records(&mut summary, &[rec]), 0);
        assert_eq!(summary.buckets.len(), 1);
        assert_eq!(summary.buckets[0].n, 1);
    }

    use crate::admin::{ApiResponse, FosRequest, FosResponse, Record, Service};
    use crate::setup::SetupStores;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::time::Instant;

    struct MockRt {
        now: Cell<u64>,
        kv: RefCell<BTreeMap<String, (u64, Value)>>,
        fos: RefCell<BTreeMap<String, Vec<u8>>>,
        api_responses: RefCell<VecDeque<ApiResponse>>,
        api_queries: RefCell<Vec<String>>,
    }

    impl MockRt {
        fn new(now: u64, responses: Vec<ApiResponse>) -> Self {
            Self {
                now: Cell::new(now),
                kv: RefCell::new(BTreeMap::new()),
                fos: RefCell::new(BTreeMap::new()),
                api_responses: RefCell::new(responses.into()),
                api_queries: RefCell::new(Vec::new()),
            }
        }

        fn state(&self) -> SyncState {
            let v = self
                .kv
                .borrow()
                .get(STATE_KEY)
                .map(|(_, v)| v.clone())
                .expect("state saved");
            serde_json::from_value(v).expect("valid SyncState")
        }

        fn seed_state(&self, state: &SyncState) {
            self.kv.borrow_mut().insert(
                STATE_KEY.into(),
                (1, serde_json::to_value(state).expect("json")),
            );
        }
    }

    impl SetupStores for MockRt {
        fn config_present(&self, _: &str) -> Result<bool, String> {
            Ok(true)
        }
        fn secret_present(&self, _: &str) -> Result<bool, String> {
            Ok(true)
        }
        fn state_linked(&self) -> Result<bool, String> {
            Ok(true)
        }
    }

    impl Runtime for MockRt {
        fn config(&self, _: &str) -> Result<Option<String>, String> {
            Ok(None)
        }
        fn secret(&self, _: &str) -> Result<Option<String>, String> {
            Ok(Some("tok".into()))
        }
        fn random(&self) -> Result<[u8; 32], String> {
            Ok([0; 32])
        }
        fn now(&self) -> u64 {
            self.now.get()
        }
        fn checks(&self, _: bool) -> Result<Value, String> {
            Ok(json!({}))
        }
        fn versions(&self) -> Result<Value, String> {
            Ok(json!({}))
        }
        fn compute_service_id(&self) -> Option<String> {
            None
        }
        fn compute_service_version(&self) -> Option<String> {
            None
        }
        fn admin_remaining_calls(&self) -> u32 {
            100
        }
        fn admin_get(&self, key: &str) -> Result<Option<Record>, String> {
            Ok(self.kv.borrow().get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            }))
        }
        fn admin_cas(
            &self,
            key: &str,
            generation: Option<&str>,
            value: &Value,
        ) -> Result<bool, String> {
            let mut kv = self.kv.borrow_mut();
            let current = kv.get(key).map(|(g, _)| *g);
            if current.map(|g| g.to_string()).as_deref() != generation {
                return Ok(false);
            }
            kv.insert(key.into(), (current.unwrap_or(0) + 1, value.clone()));
            Ok(true)
        }
        fn admin_api(&self, req: ApiRequest<'_>) -> Result<ApiResponse, String> {
            self.api_queries.borrow_mut().push(req.query.to_owned());
            Ok(self
                .api_responses
                .borrow_mut()
                .pop_front()
                .expect("queued api response"))
        }
        fn admin_fos(&self, req: FosRequest<'_>) -> Result<FosResponse, String> {
            match req.method {
                "GET" => match self.fos.borrow().get(req.key) {
                    Some(bytes) => Ok(FosResponse {
                        status: 200,
                        body: bytes.clone(),
                        cdn_cache: None,
                        retry_after: None,
                    }),
                    None => Ok(FosResponse {
                        status: 404,
                        body: Vec::new(),
                        cdn_cache: None,
                        retry_after: None,
                    }),
                },
                "PUT" => {
                    self.fos
                        .borrow_mut()
                        .insert(req.key.to_owned(), req.body.to_vec());
                    Ok(FosResponse {
                        status: 200,
                        body: Vec::new(),
                        cdn_cache: None,
                        retry_after: None,
                    })
                }
                other => Err(format!("unexpected_fos_{other}")),
            }
        }
        fn admin_fos_etag(
            &self,
            req: FosRequest<'_>,
        ) -> Result<(FosResponse, Option<String>), String> {
            use md5::{Digest as _, Md5};
            let etag = (req.method == "PUT")
                .then(|| format!("\"{}\"", hex::encode(Md5::digest(&req.body))));
            Ok((self.admin_fos(req)?, etag))
        }
    }

    fn test_service() -> Service {
        serde_json::from_value(json!({
            "service_id": "svc-1",
            "name": "demo",
            "status": "active",
            "created_at": 1,
            "updated_at": 1,
            "config_version": 1,
            "logging_enabled": true,
            "logging_endpoint_name": "ep",
            "log_period": 60,
            "sample_rate": 100,
            "edge_only": false,
            "custom_condition": "",
            "fos_bucket": "bucket",
            "fos_prefix": "logs/",
            "fos_region": "us-east-1",
            "fos_endpoint": "us-east.object.fastlystorage.app",
            "log_fields": {},
            "field_limits": {},
            "fastly_token_ref": "tok",
            "fos_access_key_ref": "ak",
            "fos_secret_key_ref": "sk",
            "owner": "owner",
            "bucket_adopted": true,
            "bucket_checked": true,
            "ngwaf_workspace_id": "ws-demo"
        }))
        .expect("service")
    }

    fn make_entry(id: &str, ts: &str, bot: &str) -> Value {
        json!({
            "id": id,
            "timestamp": ts,
            "signals": [
                {"id": "VERIFIED-BOT", "value": bot},
                {"id": "VERIFIED-BOT.SEARCH-ENGINE"}
            ]
        })
    }

    fn make_store<'a>(rt: &'a MockRt, svc: &'a Service) -> Store<'a, MockRt> {
        Store {
            runtime: rt,
            service: svc,
            access_key: "ak".into(),
            secret_key: "sk".into(),
            started: Instant::now(),
            vcpu_start: 0,
        }
    }

    fn api_resp(status: u16, body: Value) -> ApiResponse {
        ApiResponse {
            status,
            body,
            retry_after: None,
        }
    }

    #[test]
    fn sync_step_clears_expired_cursor_on_400_and_recovers() {
        let now = 1_795_800_000;
        let rt = MockRt::new(
            now,
            vec![
                api_resp(400, json!({"detail": "invalid cursor"})),
                api_resp(
                    200,
                    json!({
                        "data": [make_entry("r1", "2026-11-27T17:00:00Z", "GoogleBot")],
                        "meta": {}
                    }),
                ),
            ],
        );
        rt.seed_state(&SyncState {
            workspace_id: "ws-demo".into(),
            last_ts: Some("2026-11-27T16:00:00Z".into()),
            next_cursor: Some("expired-cur".into()),
            active_query: Some("tag:VERIFIED-BOT from:-24h".into()),
            pending_last_ts: Some("2026-11-27T16:30:00Z".into()),
            last_run: now - 600,
        });
        let svc = test_service();
        let store = make_store(&rt, &svc);
        let report = sync_step(&store, 0).expect("ok").expect("ran");
        assert_eq!(report.pages, 1);
        assert_eq!(report.added, 1);
        assert!(!report.more_pending);
        let queries = rt.api_queries.borrow();
        assert_eq!(queries.len(), 2);
        assert!(queries[0].contains("cursor=expired-cur"));
        assert!(!queries[1].contains("cursor="));
        let st = rt.state();
        assert_eq!(st.next_cursor, None);
        assert_eq!(st.active_query, None);
        assert_eq!(st.pending_last_ts, None);
        assert_eq!(st.last_ts.as_deref(), Some("2026-11-27T17:00:00Z"));
    }

    #[test]
    fn sync_step_reuses_active_query_and_preserves_pending_last_ts_across_turns() {
        let now = 1_795_800_000;
        let mut responses = Vec::new();
        for i in 0..MAX_PAGES_PER_RUN {
            responses.push(api_resp(
                200,
                json!({
                    "data": [make_entry(
                        &format!("r{i}"),
                        &format!("2026-11-27T17:{:02}:00Z", 50 - i),
                        "GoogleBot"
                    )],
                    "meta": {"next_cursor": format!("cur-{}", i + 1)}
                }),
            ));
        }
        responses.push(api_resp(
            200,
            json!({
                "data": [make_entry("r-last", "2026-11-27T17:40:00Z", "GoogleBot")],
                "meta": {}
            }),
        ));
        let rt = MockRt::new(now, responses);
        let svc = test_service();
        let store = make_store(&rt, &svc);

        let r1 = sync_step(&store, 0).expect("turn 1").expect("ran");
        assert_eq!(r1.pages, MAX_PAGES_PER_RUN as u64);
        assert!(r1.more_pending);
        let st1 = rt.state();
        assert_eq!(st1.next_cursor.as_deref(), Some("cur-6"));
        assert_eq!(
            st1.active_query.as_deref(),
            Some("tag:VERIFIED-BOT from:-24h")
        );
        assert_eq!(st1.pending_last_ts.as_deref(), Some("2026-11-27T17:50:00Z"));
        assert_eq!(st1.last_ts, None);

        rt.now.set(now + 360);
        let r2 = sync_step(&store, 0).expect("turn 2").expect("ran");
        assert_eq!(r2.pages, 1);
        assert!(!r2.more_pending);
        let queries = rt.api_queries.borrow();
        let last_q = queries.last().expect("turn 2 query");
        assert!(last_q.contains("q=tag%3AVERIFIED-BOT+from%3A-24h"));
        assert!(last_q.contains("cursor=cur-6"));
        let st2 = rt.state();
        assert_eq!(st2.next_cursor, None);
        assert_eq!(st2.active_query, None);
        assert_eq!(st2.pending_last_ts, None);
        assert_eq!(st2.last_ts.as_deref(), Some("2026-11-27T17:50:00Z"));
    }

    #[test]
    fn sync_step_stops_early_when_reaching_last_ts() {
        let now = 1_795_800_000;
        let rt = MockRt::new(
            now,
            vec![api_resp(
                200,
                json!({
                    "data": [
                        make_entry("r-new", "2026-11-27T17:05:00Z", "GoogleBot"),
                        make_entry("r-old", "2026-11-27T17:00:00Z", "GoogleBot")
                    ],
                    "meta": {"next_cursor": "should-not-follow"}
                }),
            )],
        );
        rt.seed_state(&SyncState {
            workspace_id: "ws-demo".into(),
            last_ts: Some("2026-11-27T17:00:00Z".into()),
            last_run: now - 300,
            ..Default::default()
        });
        let svc = test_service();
        let store = make_store(&rt, &svc);
        let report = sync_step(&store, 0).expect("ok").expect("ran");
        assert_eq!(report.pages, 1);
        assert!(!report.more_pending);
        let st = rt.state();
        assert_eq!(st.next_cursor, None);
        assert_eq!(st.last_ts.as_deref(), Some("2026-11-27T17:05:00Z"));
    }

    #[test]
    fn sync_step_resets_legacy_cursor_without_active_query() {
        let now = 1_795_800_000;
        let rt = MockRt::new(
            now,
            vec![api_resp(
                200,
                json!({
                    "data": [make_entry("r1", "2026-11-27T17:10:00Z", "GoogleBot")],
                    "meta": {}
                }),
            )],
        );
        rt.seed_state(&SyncState {
            workspace_id: "ws-demo".into(),
            last_ts: Some("2026-11-27T00:00:00Z".into()),
            next_cursor: Some("legacy-wedged-cursor".into()),
            active_query: None,
            pending_last_ts: None,
            last_run: now - 600,
        });
        let svc = test_service();
        let store = make_store(&rt, &svc);
        let report = sync_step(&store, 0).expect("ok").expect("ran");
        assert_eq!(report.pages, 1);
        assert_eq!(report.added, 1);
        let queries = rt.api_queries.borrow();
        assert_eq!(queries.len(), 1);
        assert!(!queries[0].contains("cursor="));
        let st = rt.state();
        assert_eq!(st.next_cursor, None);
        assert_eq!(st.last_ts.as_deref(), Some("2026-11-27T17:10:00Z"));
    }
}
