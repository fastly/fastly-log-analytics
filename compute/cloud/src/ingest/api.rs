use super::{
    day_dir, day_start, parse_processed, processed_prefix, raw_key_minute, raw_prefix, rfc3339,
    scope,
    storage::{Object, Store, CDN_READ_LIMIT},
};
use crate::{
    admin::Service,
    shell::Runtime,
    telemetry::{self, write_log, LogBuffer, LogKind, WritableLogKind},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;

pub const FILES_PAGE: usize = 500;
pub const MAX_RANGE_S: i64 = 62 * 24 * 3600;
const MAX_LIST_CALLS: u32 = 20;
const MAX_KEY_BYTES: usize = 512;

pub type Failure = (u16, &'static str);
pub type Api = Result<Value, Failure>;

#[derive(Serialize, Deserialize)]
struct Cursor {
    from: i64,
    to: i64,
    day: usize,
    token: Option<String>,
    #[serde(default = "default_dataset")]
    dataset: String,
    /// `summary` for `view=summary` pages, whose `token` is a LIST `start-after` key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    view: Option<String>,
    /// LIST `start-after` key a raw listing of an uncompacted dataset resumes from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    after: Option<String>,
    /// The table folder (one of `RUM_TABLES`) a `rum` listing is reading.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    table: Option<String>,
}

/// RUM writes one folder per table under `processed/rum/`; a `rum` listing reads them in order.
const RUM_TABLES: [&str; 2] = [super::rum::ERRORS, super::rum::VITALS];

fn default_dataset() -> String {
    "request".into()
}

/// Folder holding one dataset's Parquet files: `request`, `rum` or `ops:{kind}`.
pub fn dataset_root(service: &Service, dataset: &str) -> Option<String> {
    let scope = scope(service);
    match dataset {
        "request" => Some(processed_prefix(service)),
        "rum" => Some(format!("{scope}processed/rum/")),
        _ => {
            let kind = LogKind::parse(dataset.strip_prefix("ops:")?)?;
            Some(format!("{scope}processed/ops/{}/", kind.as_str()))
        }
    }
}

pub fn is_ops_key(service: &Service, key: &str) -> bool {
    key.starts_with(&format!("{}processed/ops/", scope(service)))
}

#[derive(Serialize, Deserialize)]
struct TokenCursor {
    kind: String,
    prefix: String,
    token: String,
}

pub struct Access<'a, R: Runtime> {
    pub store: Store<'a, R>,
}

pub fn open<'a, R: Runtime>(
    runtime: &'a R,
    service: &'a Service,
    started: Instant,
) -> Result<Store<'a, R>, Failure> {
    let (access_key, secret_key) =
        crate::admin::fos_credentials(runtime, service).ok_or_else(|| {
            let lookup = runtime
                .secret(&service.fos_access_key_ref)
                .and_then(|_| runtime.secret(&service.fos_secret_key_ref));
            match lookup {
                Err(reason) if reason == "secret_read_budget_exhausted" => {
                    (503, "Secret read budget exhausted")
                }
                Err(_) => (503, "Secret lookup failed"),
                Ok(_) => (503, "Object storage credentials are not available"),
            }
        })?;
    Ok(Store {
        runtime,
        service,
        access_key,
        secret_key,
        started,
        vcpu_start: 0,
    })
}

fn parse_instant(value: &str) -> Option<i64> {
    let value = value.trim();
    if let Ok(parsed) =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
    {
        return Some(parsed.unix_timestamp());
    }
    let date = time::Date::parse(
        value,
        &time::format_description::parse_borrowed::<2>("[year]-[month]-[day]").ok()?,
    )
    .ok()?;
    Some(date.midnight().assume_utc().unix_timestamp())
}

fn fractional(value: &str) -> i64 {
    i64::from(
        time::OffsetDateTime::parse(value.trim(), &time::format_description::well_known::Rfc3339)
            .is_ok_and(|at| at.nanosecond() != 0),
    )
}

fn encode_cursor(secret: &[u8], cursor: &Cursor) -> Result<String, Failure> {
    compute_common::crypto::seal_json(secret, cursor).map_err(|_| (500, "Cursor"))
}

fn decode_cursor(secret: &[u8], value: &str) -> Result<Cursor, Failure> {
    compute_common::crypto::unseal_json(secret, value).map_err(|_| (400, "Invalid cursor"))
}

fn failure(error: String) -> Failure {
    match error.as_str() {
        "backend_budget_exhausted" => (503, "Backend call budget exhausted; retry"),
        _ => (502, "Object storage request failed"),
    }
}

/// Datasets whose closed days `log_compact` merges into day-level `compact=` files.
fn compacted(dataset: &str) -> bool {
    dataset.starts_with("ops:")
}

/// Day folders whose compacts may sit on an earlier page than `objects`. A page starting inside a
/// day folder (`resumed`) may have passed that day's compacts, which sort before its `hour=`
/// folders; a day that starts later on the page is listed from its beginning.
fn days_to_relist(objects: &[Object], resumed: bool) -> Vec<String> {
    let Some(first) = objects.first().filter(|_| resumed) else {
        return Vec::new();
    };
    let prefix = telemetry::compact::day_compact_prefix(&first.key);
    let first_day_has_window = objects.iter().any(|o| {
        telemetry::compact::day_compact_prefix(&o.key) == prefix
            && parse_processed(&o.key).is_some()
    });
    if first_day_has_window {
        vec![prefix]
    } else {
        Vec::new()
    }
}

/// Keys of `objects` a reader returns: valid compacts and windows no valid compact covers.
/// Each `relist` prefix costs one LIST of that day's compacts.
fn visible_keys<R: Runtime>(
    store: &Store<'_, R>,
    objects: &[Object],
    relist: &[String],
) -> Result<std::collections::BTreeSet<String>, Failure> {
    let mut context = Vec::new();
    for prefix in relist {
        let page = store
            .list(prefix, None, None, 1000, None)
            .map_err(failure)?;
        context.extend(page.objects.into_iter().map(|o| o.key));
    }
    let listed: Vec<String> = objects.iter().map(|o| o.key.clone()).collect();
    Ok(telemetry::compact::visible(&listed, &context))
}

/// End of the rows a compact can hold: ops windows only hold rows of the hour they are filed
/// under, so the last source window ends with the hour that `b` starts in.
fn compact_end(b_seconds: i64) -> i64 {
    b_seconds.div_euclid(3600) * 3600 + 3600
}

/// `GET /api/files?from&to&cursor`: one page of window files overlapping `[from, to)`.
pub fn files<R: Runtime>(runtime: &R, service: &Service, query: &str, started: Instant) -> Api {
    let params: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let param = |name: &str| {
        params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .filter(|v| !v.is_empty())
    };
    let from = param("from")
        .and_then(parse_instant)
        .ok_or((400, "from must be an ISO 8601 time"))?;
    let to = param("to")
        .and_then(parse_instant)
        .ok_or((400, "to must be an ISO 8601 time"))?;
    if to <= from || to - from > MAX_RANGE_S {
        return Err((400, "to must be after from and within 62 days"));
    }
    let summary_bounds = (from + fractional(param("from").unwrap_or_default()), to);
    let to = to + fractional(param("to").unwrap_or_default());
    let secret = runtime
        .secret("session_key")
        .ok()
        .flatten()
        .filter(|s| !s.is_empty())
        .ok_or((503, "Signing key unavailable"))?;
    let dataset = param("dataset").unwrap_or("request");
    let root = dataset_root(service, dataset).ok_or((400, "Unknown dataset"))?;
    if dataset == "request" {
        let summary = match param("view") {
            None | Some("raw") => false,
            Some("summary") => true,
            _ => return Err((400, "view must be raw or summary")),
        };
        let since = param("since_generation")
            .map(|value| {
                value
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n <= 9_007_199_254_740_991)
                    .ok_or((400, "since_generation must be a nonnegative safe integer"))
            })
            .transpose()?;
        if since.is_some() && summary {
            return Err((400, "Delta discovery requires raw view"));
        }
        let marker = if since.is_some() {
            "catalog_delta"
        } else if summary {
            "catalog_summary"
        } else {
            "catalog_raw"
        };
        let cursor = param("cursor")
            .map(|value| decode_cursor(secret.as_bytes(), value))
            .transpose()?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.from != from
                || cursor.to != to
                || cursor.dataset != dataset
                || cursor.view.as_deref() != Some(marker)
        }) {
            return Err((400, "Invalid cursor"));
        }
        let store = open(runtime, service, started)?;
        if let Some(base) = since {
            let previous_from = param("previous_from")
                .and_then(parse_instant)
                .ok_or((400, "previous_from must be an ISO 8601 time"))?;
            let previous_to = param("previous_to")
                .and_then(parse_instant)
                .ok_or((400, "previous_to must be an ISO 8601 time"))?;
            if previous_to <= previous_from || previous_to - previous_from > MAX_RANGE_S {
                return Err((400, "previous range must be ordered and within 62 days"));
            }
            let previous_summary_bounds = (
                previous_from + fractional(param("previous_from").unwrap_or_default()),
                previous_to,
            );
            let previous_to = previous_to + fractional(param("previous_to").unwrap_or_default());
            let mut page = super::catalog::delta::page(
                &store,
                from,
                to,
                previous_from,
                previous_to,
                base,
                param("summaries") == Some("1"),
                summary_bounds,
                previous_summary_bounds,
                cursor.as_ref().and_then(|c| c.token.as_deref()),
            )
            .map_err(|error| match error.as_str() {
                "catalog_resync_required" | "catalog_cursor_expired" => {
                    (409, "catalog_resync_required")
                }
                "catalog_cursor_invalid" => (400, "Invalid cursor"),
                "catalog_head_behind" => (503, "catalog_head_behind"),
                _ => {
                    runtime.admin_event(json!({"event":"catalog.delta_error","error":error}));
                    (503, "Catalog discovery unavailable; retry")
                }
            })?;
            if let Some(token) = page["next_cursor"].as_str() {
                page["next_cursor"] = encode_cursor(
                    secret.as_bytes(),
                    &Cursor {
                        from,
                        to,
                        day: 0,
                        token: Some(token.to_owned()),
                        dataset: dataset.to_owned(),
                        view: Some(marker.to_owned()),
                        after: None,
                        table: None,
                    },
                )?
                .into();
            }
            return Ok(page);
        }
        let page = super::catalog::discovery::pinned_page_precise(
            &store,
            from,
            to,
            summary,
            param("summaries") == Some("1"),
            cursor.as_ref().and_then(|cursor| cursor.token.as_deref()),
            None,
            summary_bounds,
        )
        .map_err(|error| match error.as_str() {
            "catalog_cursor_expired" => (409, "Catalog snapshot expired; restart discovery"),
            "catalog_cursor_invalid" => (400, "Invalid cursor"),
            _ => {
                runtime.admin_event(json!({"event":"catalog.discovery_error","error":error}));
                (503, "Catalog discovery unavailable; retry")
            }
        })?;
        let complete = page.next.is_none();
        let next = page
            .next
            .map(|token| {
                encode_cursor(
                    secret.as_bytes(),
                    &Cursor {
                        from,
                        to,
                        day: 0,
                        token: Some(token),
                        dataset: dataset.to_owned(),
                        view: Some(marker.to_owned()),
                        after: None,
                        table: None,
                    },
                )
            })
            .transpose()?;
        return Ok(json!({
            "files":page.files, "summary_files":page.summaries,
            "next_cursor":next, "complete":complete, "catalog_generation":page.generation,
            "approximate": if summary { summary_semantics() } else { Value::Null },
        }));
    }
    match param("view") {
        None | Some("raw") => {}
        Some("summary") if dataset == "request" => {
            return summary_files(
                runtime,
                service,
                from,
                to,
                &secret,
                param("cursor"),
                started,
            );
        }
        Some("summary") => return Err((400, "Summaries exist only for the request dataset")),
        Some(_) => return Err((400, "view must be raw or summary")),
    }
    let (mut day, mut token, mut after) = (0usize, None, None);
    let mut table = (dataset == "rum").then_some(RUM_TABLES[0]);
    if let Some(value) = param("cursor") {
        let cursor = decode_cursor(secret.as_bytes(), value)?;
        let cursor_table = match (&cursor.table, dataset == "rum") {
            (None, false) => None,
            (Some(name), true) => Some(
                *RUM_TABLES
                    .iter()
                    .find(|t| **t == name.as_str())
                    .ok_or((400, "Invalid cursor"))?,
            ),
            _ => return Err((400, "Invalid cursor")),
        };
        if cursor.from != from
            || cursor.to != to
            || cursor.dataset != dataset
            || cursor.view.is_some()
            || (compacted(dataset) && cursor.after.is_some())
            || (!compacted(dataset) && cursor.token.is_some())
        {
            return Err((400, "Invalid cursor"));
        }
        day = cursor.day;
        token = cursor.token;
        after = cursor.after;
        table = cursor_table;
    }
    let include_summaries =
        dataset == "request" && param("summaries") == Some("1") && param("cursor").is_none();
    let store = open(runtime, service, started)?;
    if !compacted(dataset) {
        loop {
            let table_root = match table {
                Some(table) => format!("{root}{table}/"),
                None => root.clone(),
            };
            let mut page = hour_files(
                &store,
                &table_root,
                (dataset, table),
                (from, to),
                (day, after.take()),
                secret.as_bytes(),
                include_summaries,
            )?;
            let next_table = table.and_then(|table| {
                let at = RUM_TABLES.iter().position(|t| *t == table)?;
                RUM_TABLES.get(at + 1).copied()
            });
            let Some(next) = next_table.filter(|_| page["complete"] == true) else {
                return Ok(page);
            };
            // An empty finished table rolls straight into the next one, so no page is empty.
            if page["files"].as_array().is_some_and(Vec::is_empty) {
                (table, day) = (Some(next), 0);
                continue;
            }
            page["complete"] = false.into();
            page["next_cursor"] = encode_cursor(
                secret.as_bytes(),
                &Cursor {
                    from,
                    to,
                    day: 0,
                    token: None,
                    dataset: dataset.to_owned(),
                    view: None,
                    after: None,
                    table: Some(next.to_owned()),
                },
            )?
            .into();
            return Ok(page);
        }
    }
    let prefix = root;
    // Operational file names carry minutes, every other dataset seconds.
    let unit = if dataset.starts_with("ops:") { 60 } else { 1 };
    // A window can start in the hour before `from` and still overlap it.
    let first_day = day_start(from - 3600);
    let last_day = day_start(to - 1);
    let day_count = usize::try_from((last_day - first_day) / 86_400 + 1).unwrap_or(0);
    let calls = runtime
        .admin_remaining_calls()
        .saturating_sub(2)
        .min(MAX_LIST_CALLS);
    let mut used = 0;
    let mut files = Vec::new();
    while files.len() < FILES_PAGE && day < day_count && used < calls {
        let at = first_day + day as i64 * 86_400;
        let day_prefix = format!("{prefix}{}", day_dir(at));
        // `compact=` sorts before `hour=`, so a compacted day is listed from its start.
        let start_after = (day == 0 && token.is_none() && !compacted(dataset))
            .then(|| format!("{prefix}{}", super::hour_dir(from - 3600)));
        let resumed = token.is_some() && compacted(dataset);
        // A resumed day may need its compacts listed again.
        if used + 1 + u32::from(resumed) > calls {
            break;
        }
        let page = store
            .list(
                &day_prefix,
                start_after.as_deref(),
                token.as_deref(),
                FILES_PAGE - files.len(),
                None,
            )
            .map_err(failure)?;
        used += 1;
        let relist = days_to_relist(&page.objects, resumed);
        used += relist.len() as u32;
        let visible = visible_keys(&store, &page.objects, &relist)?;
        let last_folder = format!("{prefix}{}", super::hour_dir(to - 1));
        let past = page
            .objects
            .iter()
            .any(|o| o.key > last_folder && !o.key.starts_with(&last_folder));
        for object in page.objects {
            if !visible.contains(&object.key) {
                continue;
            }
            let entry = if let Some(name) = parse_processed(&object.key) {
                let (start, end) = if unit == 1 {
                    (name.start, name.end)
                } else {
                    (name.start * unit, name.end * unit + unit)
                };
                (start < to && end > from).then(|| {
                    json!({
                        "key": object.key,
                        "start": rfc3339(start),
                        "end": rfc3339(end),
                        "size": object.size,
                        "rows": name.rows,
                    })
                })
            } else if let Some(name) = telemetry::compact::parse_compact_name(&object.key) {
                let (a, b) = (name.a as i64 * unit, name.b as i64 * unit);
                let end = compact_end(b);
                (a < to && end > from).then(|| {
                    json!({
                        "key": object.key,
                        "start": rfc3339(a),
                        "end": rfc3339(end),
                        "size": object.size,
                        "rows": name.rows,
                        "span": [rfc3339(a), rfc3339(b)],
                        "sources": name.sources,
                    })
                })
            } else {
                None
            };
            files.extend(entry);
        }
        match page.next_token.filter(|_| !past) {
            Some(next) => token = Some(next),
            None if past => {
                day = day_count;
                token = None;
            }
            None => {
                day += 1;
                token = None;
            }
        }
    }
    let complete = day >= day_count;
    let next_cursor = if complete {
        Value::Null
    } else {
        json!(encode_cursor(
            secret.as_bytes(),
            &Cursor {
                from,
                to,
                day,
                token,
                dataset: dataset.to_owned(),
                view: None,
                after: None,
                table: None,
            }
        )?)
    };
    Ok(json!({"files": files, "next_cursor": next_cursor, "complete": complete}))
}

/// `files` for a dataset without compacts, listed hour by hour so that closed hours of the
/// request dataset come from Core Cache. Pages end at an hour boundary unless one hour alone
/// overflows a page; `cache` tells browsers which hours they may keep, and under which
/// generation.
fn hour_files<R: Runtime>(
    store: &Store<'_, R>,
    root: &str,
    (dataset, table): (&str, Option<&str>),
    (from, to): (i64, i64),
    (mut day, mut after): (usize, Option<String>),
    secret: &[u8],
    include_summaries: bool,
) -> Api {
    let runtime = store.runtime;
    // A window can start in the hour before `from` and still overlap it.
    let first_day = day_start(from - 3600);
    let last_hour = (to - 1) - (to - 1).rem_euclid(3600);
    let day_count = usize::try_from((day_start(to - 1) - first_day) / 86_400 + 1).unwrap_or(0);
    let mut calls = runtime
        .admin_remaining_calls()
        .saturating_sub(2)
        .min(MAX_LIST_CALLS);
    let first_hour = (from - 3600) - (from - 3600).rem_euclid(3600);
    let mut lister = super::listing::Lister::new(
        store,
        store.service,
        root,
        dataset == "request",
        (first_hour, last_hour),
        &mut calls,
    );
    let (mut used, mut full) = (0, false);
    let mut files = Vec::new();
    let mut raw_objects = Vec::new();
    while !full && day < day_count && used < calls {
        let at = first_day + day as i64 * 86_400;
        let pos = after.take().unwrap_or_else(|| {
            if day == 0 {
                lister.start(first_hour)
            } else {
                lister.day_start(at)
            }
        });
        let block = lister
            .next_block(at, &pos, last_hour, &mut used)
            .map_err(failure)?;
        if include_summaries && dataset == "request" {
            raw_objects.extend(block.objects.iter().cloned());
        }
        let mut next = block.next;
        // A page resumed inside an hour has passed that hour's compacts, which sort first.
        let relist: Vec<String> = match pos.rsplit_once('/') {
            Some((folder, name))
                if dataset == "request"
                    && !name.is_empty()
                    && block
                        .objects
                        .iter()
                        .any(|o| parse_processed(&o.key).is_some()) =>
            {
                vec![format!("{folder}/compact=")]
            }
            _ => Vec::new(),
        };
        used += relist.len() as u32;
        let visible = visible_keys(store, &block.objects, &relist)?;
        // Where the hour being read began in `files`, if the page holds it from its start.
        let mut hour_mark = (!pos.ends_with('/'))
            .then(|| parse_hour(&pos))
            .flatten()
            .map(|h| (h, None));
        for (i, object) in block.objects.iter().enumerate() {
            let hour = parse_hour(&object.key);
            if hour_mark.is_none_or(|(h, _)| Some(h) != hour) {
                hour_mark = hour.map(|h| (h, Some(files.len())));
            }
            if files.len() == FILES_PAGE {
                full = true;
                next = Some(match hour_mark {
                    Some((h, Some(mark))) if mark > 0 => {
                        files.truncate(mark);
                        lister.start(h)
                    }
                    _ => block.objects[i - 1].key.clone(),
                });
                break;
            }
            if !visible.contains(&object.key) {
                continue;
            }
            if let Some(name) = parse_processed(&object.key) {
                if name.start < to && name.end > from {
                    files.push(json!({
                        "key": object.key,
                        "start": rfc3339(name.start),
                        "end": rfc3339(name.end),
                        "size": object.size,
                        "rows": name.rows,
                    }));
                }
            } else if let Some(name) = telemetry::compact::parse_compact_name(&object.key) {
                // A request compact spans its first source's start to its last source's end.
                let (a, b) = (name.a as i64, name.b as i64);
                if a < to && b > from {
                    files.push(json!({
                        "key": object.key,
                        "start": rfc3339(a),
                        "end": rfc3339(b),
                        "size": object.size,
                        "rows": name.rows,
                        "span": [rfc3339(a), rfc3339(b)],
                        "sources": name.sources,
                    }));
                }
            }
        }
        full |= files.len() == FILES_PAGE;
        match next {
            Some(next) => after = Some(next),
            None => day += 1,
        }
    }
    let cache = lister.finish(dataset == "request");
    let complete = day >= day_count;
    let next_cursor = if complete {
        Value::Null
    } else {
        json!(encode_cursor(
            secret,
            &Cursor {
                from,
                to,
                day,
                token: None,
                dataset: dataset.to_owned(),
                view: None,
                after,
                table: table.map(str::to_owned),
            }
        )?)
    };
    let mut page = json!({"files": files, "next_cursor": next_cursor, "complete": complete});
    if !cache.is_null() {
        page["cache"] = cache;
    }
    if include_summaries && complete {
        if let Some(summary_files) = inline_summary_files(store, (from, to), &raw_objects) {
            page["summary_files"] = Value::Array(summary_files);
            page["summary_semantics"] = summary_semantics();
        }
    }
    Ok(page)
}

fn inline_summary_files<R: Runtime>(
    store: &Store<'_, R>,
    (from, to): (i64, i64),
    raw_objects: &[Object],
) -> Option<Vec<Value>> {
    use super::{bundle, summary};
    let runtime = store.runtime;
    let service = store.service;
    let scope = scope(service);
    let windows_root = summary::summary_prefix(&scope, "request");
    let bundles_root = bundle::bundle_prefix(&scope, "request");
    let first_day = day_start(from - 3600);
    let last_day = day_start(to - 1);
    let day_count = usize::try_from((last_day - first_day) / 86_400 + 1).unwrap_or(0);
    let mut calls = runtime
        .admin_remaining_calls()
        .saturating_sub(2)
        .min(MAX_LIST_CALLS);
    if calls < 1 {
        return None;
    }
    let first_hour = (from - 3600) - (from - 3600).rem_euclid(3600);
    let last_hour = (to - 1) - (to - 1).rem_euclid(3600);
    let mut lister = super::listing::Lister::new(
        store,
        service,
        &windows_root,
        true,
        (first_hour, last_hour),
        &mut calls,
    );
    let mut used = 0;
    let mut files = Vec::new();
    let list_all = |prefix: &str, used: &mut u32, calls: u32| -> Option<Vec<Object>> {
        let mut objects = Vec::new();
        let mut next = None::<String>;
        loop {
            if *used >= calls {
                return None;
            }
            let page = store.list(prefix, None, next.as_deref(), 1000, None).ok()?;
            *used += 1;
            objects.extend(page.objects);
            match page.next_token {
                Some(n) => next = Some(n),
                None => return Some(objects),
            }
        }
    };
    for day in 0..day_count {
        if files.len() >= FILES_PAGE || used >= calls {
            return None;
        }
        let at = first_day + day as i64 * 86_400;
        let bundles = list_all(&format!("{bundles_root}{}", day_dir(at)), &mut used, calls)?;
        let day_first_hour = ((first_hour.max(at) - at) / 3600) as u32;
        let day_last_hour = ((last_hour.min(at + 86_400 - 3600) - at) / 3600) as u32;
        let mut raw_hours: std::collections::BTreeMap<u32, Vec<Object>> = Default::default();
        let mut day_has_compact = false;
        for o in raw_objects {
            let Some(h_ts) = parse_hour(&o.key) else {
                continue;
            };
            if day_start(h_ts) != at {
                continue;
            }
            if telemetry::compact::parse_compact_name(&o.key).is_some() {
                day_has_compact = true;
                break;
            }
            if let Some(suffix) = o
                .key
                .rsplit('/')
                .next()
                .and_then(|n| n.strip_prefix("window="))
            {
                let h = ((h_ts - at) / 3600) as u32;
                let key = format!("{windows_root}{}summary={suffix}", super::hour_dir(h_ts));
                if summary_window(&key).is_some() {
                    raw_hours.entry(h).or_default().push(Object {
                        key,
                        size: 0,
                        last_modified: None,
                    });
                }
            }
        }
        let hour_resolved = |h: u32| -> Option<Vec<Object>> {
            if day_has_compact || raw_hours.is_empty() {
                return None;
            }
            let start = at + i64::from(h) * 3600;
            let ws = raw_hours.get(&h).cloned().unwrap_or_default();
            if !ws.is_empty() {
                let inside = (start >= from && start + 3600 <= to)
                    || ws.iter().all(|o| {
                        summary_window(&o.key).is_some_and(|(a, b, _)| a >= from && b <= to)
                    });
                if inside {
                    let view = bundle::hour_view(h, &ws, &bundles);
                    if view.bundle.is_some() && view.windows.is_empty() {
                        return Some(ws);
                    }
                } else if start + 3600 <= from
                    && ws.iter().all(|o| {
                        summary_window(&o.key).is_some_and(|(a, b, _)| !(a < to && b > from))
                    })
                {
                    return Some(ws);
                }
            } else if !bundles.iter().any(|o| {
                bundle::parse_name(&o.key)
                    .is_some_and(|n| n.kind == bundle::NameKind::Hour && n.unit == h)
            }) {
                return Some(Vec::new());
            }
            None
        };
        let mut hours: std::collections::BTreeMap<u32, Vec<Object>> = Default::default();
        let mut resume_from_hour = day_first_hour;
        while resume_from_hour <= day_last_hour {
            if let Some(ws) = hour_resolved(resume_from_hour) {
                if !ws.is_empty() {
                    hours.insert(resume_from_hour, ws);
                }
                resume_from_hour += 1;
            } else {
                break;
            }
        }
        let mut resume_to_hour = day_last_hour;
        while resume_to_hour > resume_from_hour {
            if let Some(ws) = hour_resolved(resume_to_hour) {
                if !ws.is_empty() {
                    hours.insert(resume_to_hour, ws);
                }
                resume_to_hour -= 1;
            } else {
                break;
            }
        }
        if resume_from_hour <= resume_to_hour {
            let mut pos = if resume_from_hour == 0 && day > 0 {
                lister.day_start(at)
            } else {
                lister.start(at + i64::from(resume_from_hour) * 3600)
            };
            let day_end = at + i64::from(resume_to_hour) * 3600;
            let mut windows = Vec::new();
            let mut windows_done = false;
            while used < calls {
                let block = lister.next_block(at, &pos, day_end, &mut used).ok()?;
                windows.extend(block.objects);
                match block.next {
                    Some(next) => pos = next,
                    None => {
                        windows_done = true;
                        break;
                    }
                }
            }
            if !windows_done {
                return None;
            }
            windows.retain(|o| summary_window(&o.key).is_some());
            for object in windows {
                let (start, _, _) = summary_window(&object.key).expect("retained");
                let hour = (start.rem_euclid(86_400) / 3600) as u32;
                hours.entry(hour).or_default().push(object);
            }
        }
        let current = bundle::current_hours(&bundles);
        let day_bundle = (at >= from && at + 86_400 <= to)
            .then(|| bundle::day_view(&bundles))
            .flatten()
            .filter(|_| {
                hours.keys().eq(current.keys())
                    && hours.iter().all(|(hour, ws)| {
                        let view = bundle::hour_view(*hour, ws, &bundles);
                        view.windows.is_empty()
                            && view.bundle == current.get(hour).map(|o| o.key.clone())
                    })
            });
        if let Some(key) = day_bundle {
            let object = bundles.iter().find(|o| o.key == key).expect("listed");
            let name = bundle::parse_name(&key).expect("a day bundle");
            files.push(summary_entry(
                "day",
                object,
                at,
                at + 86_400,
                name.rows,
                name.count,
            ));
        } else {
            for (hour, ws) in &hours {
                let start = at + i64::from(*hour) * 3600;
                let inside = (start >= from && start + 3600 <= to)
                    || (!ws.is_empty()
                        && ws.iter().all(|o| {
                            summary_window(&o.key).is_some_and(|(a, b, _)| a >= from && b <= to)
                        }));
                let view = if inside {
                    bundle::hour_view(*hour, ws, &bundles)
                } else {
                    bundle::HourView {
                        bundle: None,
                        windows: ws.iter().map(|o| o.key.clone()).collect(),
                    }
                };
                if let Some(key) = &view.bundle {
                    let object = bundles.iter().find(|o| &o.key == key).expect("listed");
                    let name = bundle::parse_name(key).expect("an hour bundle");
                    files.push(summary_entry(
                        "hour",
                        object,
                        start,
                        start + 3600,
                        name.rows,
                        name.count,
                    ));
                }
                for object in ws.iter().filter(|o| view.windows.contains(&o.key)) {
                    let (a, b, rows) = summary_window(&object.key).expect("retained");
                    if a < to && b > from {
                        files.push(summary_entry("window", object, a, b, rows, 1));
                    }
                }
            }
        }
        if files.len() >= FILES_PAGE {
            return None;
        }
    }
    lister.finish(false);
    Some(files)
}

/// Start of the hour folder a processed key is filed under.
fn parse_hour(key: &str) -> Option<i64> {
    let at = key.find("year=")?;
    let mut parts = key[at..].split('/');
    let mut number = |name: &str| -> Option<i64> { parts.next()?.strip_prefix(name)?.parse().ok() };
    let (year, month, day, hour) = (
        number("year=")?,
        number("month=")?,
        number("day=")?,
        number("hour=")?,
    );
    let date = time::Date::from_calendar_date(
        i32::try_from(year).ok()?,
        time::Month::try_from(u8::try_from(month).ok()?).ok()?,
        u8::try_from(day).ok()?,
    )
    .ok()?;
    Some(date.midnight().assume_utc().unix_timestamp() + hour * 3600)
}

/// What a `view=summary` reader must treat as approximate. Counts and sums are exact; the
/// files' own `field_totals.truncated` and `sample_capped` flags say where these apply.
fn summary_semantics() -> Value {
    use super::{bundle, summary};
    json!({
        "summary_version": summary::SUMMARY_VERSION,
        "top_k": {
            "window": summary::TOP_K,
            "hour": bundle::HOUR_TOP_K,
            "day": bundle::DAY_TOP_K,
        },
        "ip_top_k": summary::IP_TOP_K,
        "other": "__other__ = field_totals.count - sum of kept all_fields counts; set when field_totals.truncated",
        "truncated_flag": "field_totals.truncated: values beyond top_k were dropped, so per-value counts and distinct counts are lower bounds",
        "distinct": {"method": "hll", "precision": summary::HLL_P},
        "ip_sample_cap": summary::IP_SAMPLE_CAP,
        "sample_capped_flag": "sample_capped: ip_count_observed reached ip_sample_cap",
    })
}

/// `(start, end, rows)` of a `summary={start}-{end}-{run}-n{rows}.parquet` key.
fn summary_window(key: &str) -> Option<(i64, i64, u64)> {
    let name = key.rsplit('/').next()?.strip_prefix("summary=")?;
    let name = name.strip_suffix(".parquet")?;
    let (head, rows) = name.rsplit_once("-n")?;
    let mut parts = head.splitn(3, '-');
    let start = parts.next()?.parse().ok()?;
    let end = parts.next()?.parse().ok()?;
    parts.next()?;
    Some((start, end, rows.parse().ok()?))
}

fn summary_entry(
    kind: &str,
    object: &Object,
    start: i64,
    end: i64,
    rows: u64,
    sources: usize,
) -> Value {
    json!({
        "key": object.key,
        "kind": kind,
        "start": rfc3339(start),
        "end": rfc3339(end),
        "size": object.size,
        "rows": rows,
        "sources": sources,
    })
}

/// `GET /api/files?view=summary&from&to&cursor`: the fewest summary files that answer
/// `[from, to)` exactly. Per UTC day it lists the day's `summary_bundle` folder and its window
/// summaries, then returns the day bundle for a whole day inside the range when it covers
/// exactly that day's windows, else per hour inside the range its valid hour bundle plus the
/// windows written after it, and every other overlapping window summary. Pages end at hour
/// boundaries; `token` in the cursor is the `start-after` key of the next hour to list.
fn summary_files<R: Runtime>(
    runtime: &R,
    service: &Service,
    from: i64,
    to: i64,
    secret: &str,
    cursor: Option<&str>,
    started: Instant,
) -> Api {
    use super::{bundle, summary};
    let (mut day, mut token) = (0usize, None::<String>);
    if let Some(value) = cursor {
        let cursor = decode_cursor(secret.as_bytes(), value)?;
        if cursor.from != from
            || cursor.to != to
            || cursor.dataset != "request"
            || cursor.view.as_deref() != Some("summary")
        {
            return Err((400, "Invalid cursor"));
        }
        day = cursor.day;
        token = cursor.token;
    }
    let (start_day, start_token) = (day, token.clone());
    let store = open(runtime, service, started)?;
    let scope = scope(service);
    let windows_root = summary::summary_prefix(&scope, "request");
    let bundles_root = bundle::bundle_prefix(&scope, "request");
    let first_day = day_start(from - 3600);
    let last_day = day_start(to - 1);
    let day_count = usize::try_from((last_day - first_day) / 86_400 + 1).unwrap_or(0);
    let mut calls = runtime
        .admin_remaining_calls()
        .saturating_sub(2)
        .min(MAX_LIST_CALLS);
    let first_hour = (from - 3600) - (from - 3600).rem_euclid(3600);
    let last_hour = (to - 1) - (to - 1).rem_euclid(3600);
    let mut lister = super::listing::Lister::new(
        &store,
        service,
        &windows_root,
        true,
        (first_hour, last_hour),
        &mut calls,
    );
    let mut used = 0;
    let mut files = Vec::new();
    let list_all = |prefix: &str, start_after: Option<&str>, used: &mut u32| {
        let mut objects = Vec::new();
        let mut next = None::<String>;
        loop {
            if *used >= calls {
                return Ok((objects, false));
            }
            let page = store
                .list(prefix, start_after, next.as_deref(), 1000, None)
                .map_err(failure)?;
            *used += 1;
            objects.extend(page.objects);
            match page.next_token {
                Some(n) => next = Some(n),
                None => return Ok::<_, Failure>((objects, true)),
            }
        }
    };
    while day < day_count && files.len() < FILES_PAGE && used + 2 <= calls {
        let at = first_day + day as i64 * 86_400;
        let (bundles, bundles_done) =
            list_all(&format!("{bundles_root}{}", day_dir(at)), None, &mut used)?;
        if !bundles_done {
            break;
        }
        let whole_day = token.is_none();
        let mut pos = token.clone().unwrap_or_else(|| {
            if day == 0 {
                lister.start(first_hour)
            } else {
                lister.day_start(at)
            }
        });
        let (mut windows, mut windows_done) = (Vec::new(), false);
        while used < calls {
            let block = lister
                .next_block(at, &pos, last_hour, &mut used)
                .map_err(failure)?;
            windows.extend(block.objects);
            match block.next {
                Some(next) => pos = next,
                None => {
                    windows_done = true;
                    break;
                }
            }
        }
        windows.retain(|o| summary_window(&o.key).is_some());
        // Blocks end at hour boundaries unless one hour fills a LIST page.
        let at_boundary = !windows_done && pos.ends_with('/') && Some(&pos) != token.as_ref();
        // An unfinished listing resolves only the hours before its last listed hour.
        let resume_hour = (!windows_done && !at_boundary)
            .then(|| windows.last().and_then(|o| summary_window(&o.key)))
            .flatten()
            .map(|(start, _, _)| start - start.rem_euclid(3600));
        if at_boundary {
            token = Some(pos);
        } else if !windows_done {
            let Some(hour) = resume_hour.filter(|h| {
                windows.first().is_some_and(|o| {
                    summary_window(&o.key).is_some_and(|(s, _, _)| s - s.rem_euclid(3600) < *h)
                })
            }) else {
                break;
            };
            windows.retain(|o| summary_window(&o.key).is_some_and(|(s, _, _)| s < hour));
            token = Some(format!("{windows_root}{}", super::hour_dir(hour)));
        }
        let mut hours: std::collections::BTreeMap<u32, Vec<Object>> = Default::default();
        for object in windows {
            let (start, _, _) = summary_window(&object.key).expect("retained");
            let hour = (start.rem_euclid(86_400) / 3600) as u32;
            hours.entry(hour).or_default().push(object);
        }
        let current = bundle::current_hours(&bundles);
        let day_bundle = (whole_day && windows_done && at >= from && at + 86_400 <= to)
            .then(|| bundle::day_view(&bundles))
            .flatten()
            .filter(|_| {
                hours.keys().eq(current.keys())
                    && hours.iter().all(|(hour, ws)| {
                        let view = bundle::hour_view(*hour, ws, &bundles);
                        view.windows.is_empty()
                            && view.bundle == current.get(hour).map(|o| o.key.clone())
                    })
            });
        if let Some(key) = day_bundle {
            let object = bundles.iter().find(|o| o.key == key).expect("listed");
            let name = bundle::parse_name(&key).expect("a day bundle");
            files.push(summary_entry(
                "day",
                object,
                at,
                at + 86_400,
                name.rows,
                name.count,
            ));
        } else {
            for (hour, ws) in &hours {
                let start = at + i64::from(*hour) * 3600;
                let inside = (start >= from && start + 3600 <= to)
                    || (!ws.is_empty()
                        && ws.iter().all(|o| {
                            summary_window(&o.key).is_some_and(|(a, b, _)| a >= from && b <= to)
                        }));
                let view = if inside {
                    bundle::hour_view(*hour, ws, &bundles)
                } else {
                    bundle::HourView {
                        bundle: None,
                        windows: ws.iter().map(|o| o.key.clone()).collect(),
                    }
                };
                if let Some(key) = &view.bundle {
                    let object = bundles.iter().find(|o| &o.key == key).expect("listed");
                    let name = bundle::parse_name(key).expect("an hour bundle");
                    files.push(summary_entry(
                        "hour",
                        object,
                        start,
                        start + 3600,
                        name.rows,
                        name.count,
                    ));
                }
                for object in ws.iter().filter(|o| view.windows.contains(&o.key)) {
                    let (a, b, rows) = summary_window(&object.key).expect("retained");
                    if a < to && b > from {
                        files.push(summary_entry("window", object, a, b, rows, 1));
                    }
                }
            }
        }
        if windows_done {
            day += 1;
            token = None;
        }
    }
    if (day, &token) == (start_day, &start_token) {
        return Err((503, "Backend call budget exhausted; retry"));
    }
    let complete = day >= day_count;
    let next_cursor = if complete {
        Value::Null
    } else {
        json!(encode_cursor(
            secret.as_bytes(),
            &Cursor {
                from,
                to,
                day,
                token,
                dataset: "request".to_owned(),
                view: Some("summary".to_owned()),
                after: None,
                table: None,
            }
        )?)
    };
    lister.finish(false);
    Ok(json!({
        "files": files,
        "next_cursor": next_cursor,
        "complete": complete,
        "approximate": summary_semantics(),
    }))
}

fn token_from_cursor(
    secret: &[u8],
    query: &str,
    kind: &str,
    prefix: &str,
) -> Result<Option<String>, Failure> {
    let cursor = url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, v)| k == "cursor" && !v.is_empty())
        .map(|(_, v)| v.into_owned());
    let Some(value) = cursor else {
        return Ok(None);
    };
    let cursor: TokenCursor =
        compute_common::crypto::unseal_json(secret, &value).map_err(|_| (400, "Invalid cursor"))?;
    if cursor.kind != kind || cursor.prefix != prefix {
        return Err((400, "Invalid cursor"));
    }
    Ok(Some(cursor.token))
}

fn next_token_cursor(
    secret: &[u8],
    kind: &str,
    prefix: &str,
    token: Option<String>,
) -> Result<Value, Failure> {
    let Some(token) = token else {
        return Ok(Value::Null);
    };
    let sealed = compute_common::crypto::seal_json(
        secret,
        &TokenCursor {
            kind: kind.into(),
            prefix: prefix.into(),
            token,
        },
    )
    .map_err(|_| (500, "Cursor"))?;
    Ok(json!(sealed))
}

fn signing_key(runtime: &impl Runtime) -> Result<String, Failure> {
    runtime
        .secret("session_key")
        .ok()
        .flatten()
        .filter(|s| !s.is_empty())
        .ok_or((503, "Signing key unavailable"))
}

const INGEST_FILES_PER_PAGE: usize = 10;
const OUTBOX_PAGES: usize = 5;

fn ingest_file_json(service: &Service, row: &Value) -> Option<Value> {
    let key = row["raw_key"].as_str()?;
    let start = raw_key_minute(key, &raw_prefix(service))?;
    let at = row["timestamp"]
        .as_i64()
        .map(|ms| rfc3339(ms.div_euclid(1000)));
    Some(json!({
        "key": key,
        "start": rfc3339(start),
        "end": rfc3339(start + 60),
        "size": row["raw_size"],
        "rows": row["rows"],
        "last_modified": at,
        "status": row["status"],
        "raw_deleted": row["raw_deleted"].as_bool().unwrap_or(false),
        "raw_deleted_at": row["raw_deleted_at"].as_i64().map(|ms| rfc3339(ms.div_euclid(1000))),
        "dataset": row["dataset"],
    }))
}

/// `GET /api/admin/ingested-files?cursor`: raw files with their latest `ingest_files` state.
/// Each page decodes a few archived `ingest_files` Parquet files in key (time) order; the final
/// page also folds in the unarchived outbox rows, which are the newest. A raw key can recur on
/// a later page with a newer state, so clients keep the last occurrence. `complete` is true only
/// on the last page.
pub fn ingested_files<R: Runtime>(
    runtime: &R,
    service: &Service,
    query: &str,
    started: Instant,
) -> Api {
    let secret = signing_key(runtime)?;
    let token = token_from_cursor(secret.as_bytes(), query, "ingested", "")?;
    let store = open(runtime, service, started)?;
    let root = dataset_root(service, "ops:ingest_files").ok_or((500, "Dataset"))?;
    let page = store
        .list(&root, None, token.as_deref(), INGEST_FILES_PER_PAGE, None)
        .map_err(failure)?;
    let cdn_key = runtime
        .secret("cdn_key")
        .ok()
        .flatten()
        .filter(|k| !k.is_empty())
        .ok_or((503, "CDN key unavailable"))?;
    let reader = Store {
        runtime,
        service,
        access_key: String::new(),
        secret_key: String::new(),
        started,
        vcpu_start: 0,
    };
    let (mut rows, mut skipped) = (Vec::new(), 0u64);
    let relist = days_to_relist(&page.objects, token.is_some());
    let visible = visible_keys(&store, &page.objects, &relist)?;
    for object in page.objects {
        let compact = telemetry::compact::parse_compact_name(&object.key).is_some();
        if parse_processed(&object.key).is_none() && !compact {
            skipped += 1;
            continue;
        }
        if !visible.contains(&object.key) {
            continue;
        }
        let body = reader
            .get(&cdn_key, &object.key, CDN_READ_LIMIT)
            .map_err(failure)?;
        match body
            .and_then(|bytes| telemetry::schema::from_parquet(LogKind::IngestFiles, bytes).ok())
        {
            Some(decoded) => rows.extend(decoded),
            None => skipped += 1,
        }
    }
    let complete = page.next_token.is_none();
    if complete {
        let mut cursor: Option<String> = None;
        for _ in 0..OUTBOX_PAGES {
            let live = telemetry::read_live(
                runtime,
                &service.service_id,
                LogKind::IngestFiles,
                None,
                Some(telemetry::MAX_PAGE),
                cursor.as_deref(),
            )
            .map_err(|_| (502, "Live log read failed"))?;
            rows.extend(live.rows);
            cursor = live.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
    }
    // Later states omit the row count and size they inherit from the conversion state.
    let known = |key: &Value, field: &str| {
        rows.iter()
            .filter(|row| row["raw_key"] == *key)
            .map(|row| &row[field])
            .find(|value| !value.is_null())
            .cloned()
    };
    let files: Vec<Value> = telemetry::latest_ingest_state(&rows)
        .iter()
        .filter_map(|row| {
            let mut file = ingest_file_json(service, row)?;
            for field in ["rows", "size"] {
                if file[field].is_null() {
                    let source = if field == "size" { "raw_size" } else { field };
                    file[field] = known(&row["raw_key"], source).unwrap_or(Value::Null);
                }
            }
            Some(file)
        })
        .collect();
    Ok(json!({
        "files": files,
        "skipped_objects": skipped,
        "next_cursor": next_token_cursor(secret.as_bytes(), "ingested", "", page.next_token)?,
        "complete": complete,
    }))
}

const TREE_PAGE: usize = 1000;
const MAX_TREE_PREFIX: usize = 256;

fn valid_tree_prefix(prefix: &str) -> bool {
    prefix.len() <= MAX_TREE_PREFIX
        && (prefix.is_empty() || prefix.ends_with('/'))
        && !prefix.starts_with('/')
        && !prefix.contains("..")
        && !prefix.contains("//")
        && prefix
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'=' | b'/'))
}

/// `GET /api/admin/raw-tree` and `/api/admin/storage-tree?prefix&cursor`: one delimiter LIST of a
/// folder under the raw or Parquet root. `prefix` is relative to that root. Folders carry no size
/// because a delimiter LIST cannot know it.
pub fn tree<R: Runtime>(
    runtime: &R,
    service: &Service,
    query: &str,
    started: Instant,
    raw: bool,
) -> Api {
    let kind = if raw { "raw-tree" } else { "storage-tree" };
    let pairs: Vec<(String, String)> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let find = |name: &str| {
        pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
    };
    let root = match (raw, find("dataset").as_deref()) {
        (true, None) => format!("{}raw/", scope(service)),
        (false, None) => format!("{}processed/", scope(service)),
        (false, Some("request")) => processed_prefix(service),
        (false, Some("rum")) => format!("{}processed/rum/", scope(service)),
        (false, Some("ops")) => format!("{}processed/ops/", scope(service)),
        _ => return Err((400, "Unknown dataset")),
    };
    let relative = find("prefix").unwrap_or_default();
    if !valid_tree_prefix(&relative) {
        return Err((400, "Invalid prefix"));
    }
    let secret = signing_key(runtime)?;
    let token = token_from_cursor(secret.as_bytes(), query, kind, &relative)?;
    let store = open(runtime, service, started)?;
    let full = format!("{root}{relative}");
    let page = store
        .list(&full, None, token.as_deref(), TREE_PAGE, Some("/"))
        .map_err(failure)?;
    let mut nodes = Vec::new();
    for dir in &page.prefixes {
        let Some(name) = dir.strip_prefix(&full).map(|n| n.trim_end_matches('/')) else {
            continue;
        };
        if relative.is_empty() && (name == "_inflight" || name == "_catalog") {
            continue;
        }
        nodes.push(json!({
            "name": name,
            "type": "directory",
            "prefix": format!("{relative}{name}/"),
        }));
    }
    for object in page.objects {
        let Some(name) = object.key.strip_prefix(&full) else {
            continue;
        };
        nodes.push(json!({
            "name": name,
            "type": "file",
            "key": object.key,
            "size": object.size,
            "mtime": object.last_modified,
            "rows": parse_processed(&object.key).and_then(|n| n.rows),
        }));
    }
    let complete = page.next_token.is_none();
    Ok(json!({
        "root": root,
        "prefix": relative,
        "nodes": nodes,
        "next_cursor": next_token_cursor(secret.as_bytes(), kind, &relative, page.next_token)?,
        "complete": complete,
    }))
}

pub fn valid_processed_key(service: &Service, key: &str) -> bool {
    key.len() <= MAX_KEY_BYTES
        && ["request", "rum", "ops", "summary", "summary_bundle"]
            .iter()
            .any(|root| key.starts_with(&format!("{}processed/{root}/", scope(service))))
        && key.ends_with(".parquet")
        && !key.contains("..")
        && !key.contains("//")
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'=' | b'/'))
}

/// `GET /api/file?key=`: where to stream one window, compact, summary or summary bundle file
/// from. The body isn't read here: it streams from the CDN path as the response is sent, with
/// the client's `range`, so no size cap applies.
pub fn file<R: Runtime>(
    runtime: &R,
    service: &Service,
    query: &str,
    range: Option<&str>,
) -> Result<crate::http::Upstream, Failure> {
    let key = url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.into_owned())
        .ok_or((400, "key is required"))?;
    if !valid_processed_key(service, &key) {
        return Err((400, "key is outside this service's processed files"));
    }
    let cdn_key = runtime
        .secret("cdn_key")
        .ok()
        .flatten()
        .filter(|k| !k.is_empty())
        .ok_or((503, "CDN key unavailable"))?;
    Ok(crate::http::Upstream {
        service_id: service.service_id.clone(),
        key,
        cdn_key,
        range: range.map(str::to_owned),
        head_only: false,
    })
}

/// Extents and row totals belong to the same committed epoch as file discovery.
pub fn extents<R: Runtime>(runtime: &R, service: &Service, started: Instant) -> Api {
    let _store = open(runtime, service, started)?;
    let watermark = runtime.admin_get(super::WATERMARK_KEY).map_err(failure)?;
    let catalog =
        super::catalog::head(watermark.as_ref().map(|r| &r.value).unwrap_or(&Value::Null))
            .map_err(failure)?;
    let now_s = runtime.now() as i64;
    let statistics = catalog.statistics.values().collect::<Vec<_>>();
    let earliest_ts = statistics.iter().filter_map(|s| s.earliest).min();
    let latest_ts = statistics.iter().filter_map(|s| s.latest).max();
    let earliest_log_ts = statistics
        .iter()
        .filter_map(|s| s.earliest_log.or(s.earliest))
        .min();
    let latest_log_ts = statistics
        .iter()
        .filter_map(|s| {
            s.latest_log.or_else(|| {
                let end = s.latest?;
                let start = s.earliest.unwrap_or(end);
                Some(if start <= now_s {
                    end.min(now_s).max(start)
                } else {
                    end
                })
            })
        })
        .max();
    let total_rows = statistics.iter().map(|s| s.rows).sum::<u64>();
    let earliest = earliest_ts.map(rfc3339);
    let latest = latest_ts.map(rfc3339);
    let earliest_log_at = earliest_log_ts.map(rfc3339);
    let latest_log_at = latest_log_ts.map(rfc3339);
    let rows = earliest_ts.is_some().then_some(total_rows);
    Ok(json!({
        "configured": true,
        "earliest": earliest,
        "latest": latest,
        "earliest_log_at": earliest_log_at,
        "latest_log_at": latest_log_at,
        "total_rows": rows,
        "total_rows_partial": false,
        "catalog_generation": catalog.generation,
    }))
}

const QUARANTINE_PAGE: u32 = 10;
const PURGE_LIST: u32 = 20;
const PURGE_RECORDS: u32 = 8;
const PURGE_KEYS: usize = super::storage::DELETE_BATCH;
/// Calls kept free for the request's telemetry flush and the audit write.
pub const PURGE_RESERVE: u32 = 4;
const CONFIRM_TTL_S: u64 = 600;
const MAX_RECORD_ID: usize = 128;

fn state_failure(error: String) -> Failure {
    match error.as_str() {
        "backend_budget_exhausted" => (503, "Backend call budget exhausted; retry"),
        _ => (502, "State store request failed"),
    }
}

fn seal(secret: &[u8], value: &Value) -> Result<String, Failure> {
    compute_common::crypto::seal_json(secret, value).map_err(|_| (500, "Token"))
}

fn unseal(secret: &[u8], value: &str, invalid: Failure) -> Result<Value, Failure> {
    compute_common::crypto::unseal_json(secret, value).map_err(|_| invalid)
}

/// The committed watermark key: only raw objects at or below it can be purged, since anything
/// above it may still be listed and converted.
fn watermark_key<R: Runtime>(runtime: &R) -> Result<Option<String>, Failure> {
    Ok(runtime
        .admin_get(super::WATERMARK_KEY)
        .map_err(state_failure)?
        .and_then(|r| r.value["key"].as_str().map(str::to_owned)))
}

fn purgeable(service: &Service, key: &str, watermark: Option<&str>) -> bool {
    key.starts_with(&raw_prefix(service))
        && super::retention::safe_key(key, &scope(service))
        && watermark.is_some_and(|mark| key <= mark)
}

fn record_objects(record: &Value) -> Vec<Value> {
    record["objects"].as_array().cloned().unwrap_or_default()
}

fn quarantine_item(service: &Service, record: &Value, object: &Value, mark: Option<&str>) -> Value {
    let key = object["key"].as_str().unwrap_or_default();
    let first_seen = record["first_seen"]
        .as_i64()
        .filter(|s| *s > 0)
        .map(rfc3339);
    json!({
        "key": key,
        "record": record["run_id"],
        "status": object["status"],
        "reason": object["reason"],
        "size": object["size"],
        "attempts": object["attempts"],
        "first_seen": first_seen,
        "quarantined_at": record["at"].as_i64().map(rfc3339),
        "purgeable": purgeable(service, key, mark),
    })
}

/// `GET /api/admin/quarantine?cursor`: failed and quarantined raw objects of committed plans, one
/// page of `ingest:quarantine:*` records at a time.
pub fn quarantine<R: Runtime>(runtime: &R, service: &Service, query: &str) -> Api {
    let secret = signing_key(runtime)?;
    let token = token_from_cursor(secret.as_bytes(), query, "quarantine", "")?;
    let page = runtime
        .kv_list(super::QUARANTINE_PREFIX, token.as_deref(), QUARANTINE_PAGE)
        .map_err(state_failure)?;
    let mark = watermark_key(runtime)?;
    let mut items = Vec::new();
    for key in &page.keys {
        let Some(record) = runtime.admin_get(key).map_err(state_failure)? else {
            continue;
        };
        for object in record_objects(&record.value) {
            items.push(quarantine_item(
                service,
                &record.value,
                &object,
                mark.as_deref(),
            ));
        }
    }
    let complete = page.next_cursor.is_none();
    Ok(json!({
        "items": items,
        "next_cursor": next_token_cursor(secret.as_bytes(), "quarantine", "", page.next_cursor)?,
        "complete": complete,
    }))
}

#[derive(Deserialize)]
struct PurgeRequest {
    target: String,
    #[serde(default)]
    record: Option<String>,
    #[serde(default)]
    confirm: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
}

pub struct Purge {
    pub body: Value,
    /// Audit target text for a confirmed step.
    pub audit: Option<String>,
}

fn confirm_token(
    secret: &[u8],
    actor: &str,
    request: &PurgeRequest,
    exp: u64,
) -> Result<String, Failure> {
    seal(
        secret,
        &json!({"kind": "quarantine-purge", "actor": actor, "target": request.target,
            "record": request.record, "exp": exp}),
    )
}

/// `POST /api/admin/quarantine/purge {target: "all" | raw key, record?, confirm?, cursor?}`.
///
/// Without `confirm` it only issues a confirm token bound to the actor and target. With one, it
/// runs one bounded step: at most `PURGE_RECORDS` quarantine records and one `DeleteObjects`,
/// keeping [`PURGE_RESERVE`] calls free. It deletes only keys a quarantine record names, under
/// the raw prefix and at or below the watermark, so a purged object is never listed or ledgered
/// again. The raw delete comes before the record update, so a step cut short repeats safely.
pub fn quarantine_purge<R: Runtime>(
    runtime: &R,
    service: &Service,
    actor: &str,
    body: &[u8],
    buffer: &LogBuffer,
    started: Instant,
) -> Result<Purge, Failure> {
    if crate::admin::reset::fencing(service) {
        return Err((409, "data_reset_in_progress"));
    }
    let request: PurgeRequest =
        serde_json::from_slice(body).map_err(|_| (400, "Invalid purge request"))?;
    let all = request.target == "all";
    let record_id = request.record.as_deref().unwrap_or_default();
    if !all
        && (record_id.is_empty()
            || record_id.len() > MAX_RECORD_ID
            || !record_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            || request.target.len() > 1024)
    {
        return Err((400, "Invalid purge request"));
    }
    let secret = signing_key(runtime)?;
    let now = runtime.now();
    let Some(confirm) = request.confirm.as_deref() else {
        let exp = now + CONFIRM_TTL_S;
        return Ok(Purge {
            body: json!({
                "status": "confirm_required",
                "confirm_token": confirm_token(secret.as_bytes(), actor, &request, exp)?,
                "expires_at": rfc3339(exp as i64),
            }),
            audit: None,
        });
    };
    let invalid = (400, "Invalid or expired confirmation");
    let claims = unseal(secret.as_bytes(), confirm, invalid)?;
    if claims["kind"] != "quarantine-purge"
        || claims["actor"] != actor
        || claims["target"] != request.target.as_str()
        || claims["record"] != json!(request.record)
        || claims["exp"].as_u64().is_none_or(|exp| exp < now)
    {
        return Err(invalid);
    }
    let resume = match request.cursor.as_deref().filter(|c| !c.is_empty()) {
        Some(cursor) => {
            let value = unseal(secret.as_bytes(), cursor, (400, "Invalid cursor"))?;
            if value["kind"] != "quarantine-purge" || value["target"] != request.target.as_str() {
                return Err((400, "Invalid cursor"));
            }
            Some(value)
        }
        None => None,
    };
    let store = open(runtime, service, started)?;
    let mark = watermark_key(runtime)?;
    let room = |need: u32| runtime.admin_remaining_calls() >= PURGE_RESERVE + need;

    // (record key, generation, record value, keys chosen for deletion)
    let mut chosen: Vec<(String, String, Value, Vec<Value>)> = Vec::new();
    let mut skipped = 0u64;
    let mut next: Option<Value> = None;
    if all {
        let token = resume
            .as_ref()
            .and_then(|c| c["token"].as_str().map(str::to_owned));
        let after = resume
            .as_ref()
            .and_then(|c| c["after"].as_str().map(str::to_owned));
        if !room(4) {
            return Err((503, "Backend call budget exhausted; retry"));
        }
        let page = runtime
            .kv_list(super::QUARANTINE_PREFIX, token.as_deref(), PURGE_LIST)
            .map_err(state_failure)?;
        let pending: Vec<&String> = page
            .keys
            .iter()
            .filter(|k| after.as_deref().is_none_or(|a| k.as_str() > a))
            .collect();
        // Set when the step stops inside the page: the last record fully handled.
        let mut stop: Option<Option<String>> = None;
        let mut keys = 0usize;
        for (index, key) in pending.iter().enumerate() {
            let previous = || {
                index
                    .checked_sub(1)
                    .map(|i| pending[i].to_string())
                    .or_else(|| after.clone())
            };
            // A get and an update per record, plus the one DeleteObjects.
            let next_records = chosen.len() as u32 + 1;
            if next_records > PURGE_RECORDS || !room(2 * next_records + 1) || keys >= PURGE_KEYS {
                stop = Some(previous());
                break;
            }
            let Some(record) = runtime.admin_get(key).map_err(state_failure)? else {
                continue;
            };
            let objects = record_objects(&record.value);
            let eligible: Vec<Value> = objects
                .iter()
                .filter(|o| {
                    purgeable(
                        service,
                        o["key"].as_str().unwrap_or_default(),
                        mark.as_deref(),
                    )
                })
                .cloned()
                .collect();
            skipped += (objects.len() - eligible.len()) as u64;
            let room_left = PURGE_KEYS - keys;
            let truncated = eligible.len() > room_left;
            let take: Vec<Value> = eligible.into_iter().take(room_left).collect();
            keys += take.len();
            chosen.push((key.to_string(), record.generation, record.value, take));
            if truncated {
                stop = Some(previous());
                break;
            }
        }
        next = match stop {
            Some(after) => Some(json!({"kind": "quarantine-purge", "target": "all",
                "token": token, "after": after})),
            None => page
                .next_cursor
                .map(|t| json!({"kind": "quarantine-purge", "target": "all", "token": t})),
        };
    } else {
        if !room(3) {
            return Err((503, "Backend call budget exhausted; retry"));
        }
        let key = format!("{}{record_id}", super::QUARANTINE_PREFIX);
        let record = runtime
            .admin_get(&key)
            .map_err(state_failure)?
            .ok_or((404, "Quarantined object not found"))?;
        let object = record_objects(&record.value)
            .into_iter()
            .find(|o| o["key"] == request.target.as_str())
            .ok_or((404, "Quarantined object not found"))?;
        if !purgeable(service, &request.target, mark.as_deref()) {
            return Err((409, "Object is not purgeable"));
        }
        chosen.push((key, record.generation, record.value, vec![object]));
    }

    let keys: Vec<String> = chosen
        .iter()
        .flat_map(|(_, _, _, take)| {
            take.iter()
                .filter_map(|o| o["key"].as_str().map(str::to_owned))
        })
        .collect();
    let outcome = if keys.is_empty() {
        Default::default()
    } else {
        store.delete_objects(&keys).map_err(failure)?
    };
    let deleted: std::collections::BTreeSet<&String> = outcome.deleted.iter().collect();
    let now_ms = now as i64 * 1000;
    let (mut purged, mut bytes) = (0u64, 0u64);
    for (key, generation, record, take) in &chosen {
        let mut gone = 0;
        for object in take {
            let raw = object["key"].as_str().unwrap_or_default();
            if !deleted.contains(&raw.to_owned()) {
                continue;
            }
            gone += 1;
            purged += 1;
            bytes += object["size"].as_u64().unwrap_or(0);
            write_log(
                runtime,
                buffer,
                WritableLogKind::IngestFiles,
                json!({
                    "run_id": record["run_id"], "dataset": "request", "raw_key": raw,
                    "raw_size": object["size"], "status": object["status"],
                    "reason": object["reason"], "state_version": 2, "raw_deleted": true,
                    "raw_deleted_at": now_ms,
                }),
            );
        }
        if gone == 0 {
            continue;
        }
        let remaining: Vec<Value> = record_objects(record)
            .into_iter()
            .filter(|o| !deleted.contains(&o["key"].as_str().unwrap_or_default().to_owned()))
            .collect();
        if remaining.is_empty() {
            runtime.kv_delete(key).map_err(state_failure)?;
        } else {
            let mut value = record.clone();
            value["objects"] = json!(remaining);
            runtime
                .admin_cas(key, Some(generation), &value)
                .map_err(state_failure)?;
        }
    }
    let failed = (keys.len() as u64).saturating_sub(purged);
    let complete = next.is_none();
    let target = if all { "all" } else { request.target.as_str() };
    Ok(Purge {
        body: json!({
            "status": if complete { "complete" } else { "partial" },
            "purged": purged,
            "bytes": bytes,
            "failed": failed,
            "skipped": skipped,
            "next_cursor": next.map(|c| seal(secret.as_bytes(), &c)).transpose()?,
            "confirm_token": if complete { None } else {
                Some(confirm_token(secret.as_bytes(), actor, &request, now + CONFIRM_TTL_S)?)
            },
            "complete": complete,
        }),
        audit: Some(format!("{target}: {purged} purged, {failed} failed")),
    })
}
