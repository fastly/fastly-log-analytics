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
const PAGE_LIMIT: usize = 500;
const MAX_PAGES_PER_RUN: usize = 3;
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
    if state.next_cursor.is_none() && state.last_run.saturating_add(SYNC_INTERVAL_S) > now {
        return Ok(None);
    }

    let mut report = SyncReport::default();
    let mut by_day: BTreeMap<String, Vec<BotRecord>> = BTreeMap::new();
    let from_rel = relative_time_range(state.last_ts.as_deref(), now as i64);
    let mut newest_ts = state.last_ts.clone();
    let api_path = format!("/ngwaf/v1/workspaces/{workspace_id}/requests");

    for _ in 0..MAX_PAGES_PER_RUN {
        if runtime.admin_remaining_calls() <= tail + 4 || !store.within_wall() {
            report.more_pending = true;
            break;
        }
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("q", &format!("tag:VERIFIED-BOT from:{from_rel}"));
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
        if !(200..300).contains(&resp.status) {
            return Err(format!("ngwaf_api_status_{}", resp.status));
        }
        report.pages += 1;
        if let Some(items) = resp.body["data"].as_array() {
            for item in items {
                if let Some(rec) = extract_bot_record(item) {
                    if newest_ts
                        .as_deref()
                        .is_none_or(|prev| rec.timestamp_iso.as_str() > prev)
                    {
                        newest_ts = Some(rec.timestamp_iso.clone());
                    }
                    if let Some(day) = day_string(rec.timestamp_s) {
                        by_day.entry(day).or_default().push(rec);
                        report.records += 1;
                    }
                }
            }
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

    for (day, records) in by_day {
        if runtime.admin_remaining_calls() <= tail + 2 || !store.within_wall() {
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

    if state.next_cursor.is_none() {
        state.last_ts = newest_ts.or_else(|| Some(rfc3339(now as i64)));
        state.last_run = now;
    } else {
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
}
