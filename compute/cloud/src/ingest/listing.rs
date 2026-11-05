//! Hour-aligned listing of a dataset's day folders that serves closed hours from Core Cache.
//!
//! A cached hour is keyed by the watermark's `manifest_gen`, which every commit that adds a
//! window to a closed hour bumps (`super::manifest_hour_closed`), so a late window is never
//! hidden behind a cached listing. Hours are stored only when no plan is in flight, because an
//! uncommitted plan's outputs are listed but may still be deleted by its cleanup, and only
//! between the retention edge and the closed edge.

use super::{
    hour_dir, rfc3339, settle_s, storage::Object, storage::Store, PLAN_KEY, WATERMARK_KEY,
};
use crate::{
    admin::Service,
    shell::{cache_ops_remaining, Runtime},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Seconds a reader waits past `manifest_hour_closed` before caching an hour, so a writer whose
/// clock runs behind the reader's still bumps the generation for that hour.
pub const CLOSE_MARGIN_S: i64 = 600;
/// Hours this close to the retention cutoff are never cached, since retention may delete them.
const RETENTION_MARGIN_S: i64 = 2 * 86_400;
const TTL: Duration = Duration::from_secs(3600);
/// Core Cache operations left for the rest of the execution.
const CACHE_OPS_RESERVE: u32 = 4;
const LIST_PAGE: usize = 1000;

/// Objects in key order, and the `start-after` key the day continues from (`None`: day done).
pub struct Block {
    pub objects: Vec<Object>,
    pub next: Option<String>,
}

struct Cache {
    generation: u64,
    run_id: Option<String>,
    /// Hours in `[from, before)` may be cached.
    from: i64,
    before: i64,
    namespace: String,
    surrogate: String,
}

pub struct Lister<'a, 's, R: Runtime> {
    store: &'s Store<'a, R>,
    root: String,
    cache: Option<Cache>,
    pending: Vec<(String, Vec<u8>)>,
}

fn hour_floor(seconds: i64) -> i64 {
    seconds - seconds.rem_euclid(3600)
}

/// `{root}year=/month=/day=/` folder of the day starting at `day`.
pub fn day_dir(day: i64) -> String {
    let dir = hour_dir(day);
    dir.split("hour=").next().unwrap_or("").to_owned()
}

/// Start of the hour a key under `root` is filed in, and whether the key is exactly that hour's
/// (or day's) folder rather than an object inside it.
fn key_hour(root: &str, key: &str) -> Option<(i64, bool)> {
    let mut parts = key.strip_prefix(root)?.split('/');
    let mut number = |name: &str| -> Option<u32> { parts.next()?.strip_prefix(name)?.parse().ok() };
    let (year, month, day) = (number("year=")?, number("month=")?, number("day=")?);
    let date = time::Date::from_calendar_date(
        i32::try_from(year).ok()?,
        time::Month::try_from(u8::try_from(month).ok()?).ok()?,
        u8::try_from(day).ok()?,
    )
    .ok()?;
    let start = date.midnight().assume_utc().unix_timestamp();
    match parts.next() {
        None | Some("") => Some((start, true)),
        Some(hour) => {
            let hour: i64 = hour.strip_prefix("hour=")?.parse().ok()?;
            if !(0..24).contains(&hour) {
                return None;
            }
            let rest: Vec<&str> = parts.collect();
            Some((start + hour * 3600, rest.is_empty() || rest == [""]))
        }
    }
}

impl<'a, 's, R: Runtime> Lister<'a, 's, R> {
    /// Lists `root` (a dataset folder ending in `/`). With `cached`, closed hours of
    /// `[first_hour, last_hour]` come from and go to Core Cache; that costs a KV read now and
    /// one in `finish`, both taken from `calls`.
    pub fn new(
        store: &'s Store<'a, R>,
        service: &Service,
        root: &str,
        cached: bool,
        (first_hour, last_hour): (i64, i64),
        calls: &mut u32,
    ) -> Self {
        let runtime = store.runtime;
        let now = i64::try_from(runtime.now()).unwrap_or(i64::MAX);
        let settle = i64::try_from(settle_s(service.log_period)).unwrap_or(i64::MAX);
        let before = hour_floor(now.saturating_sub(settle).saturating_sub(CLOSE_MARGIN_S));
        let oldest = now - i64::from(service.log_retention_days) * 86_400 + RETENTION_MARGIN_S;
        let from = hour_floor(oldest + 3599);
        let useful = cached
            && first_hour < before
            && last_hour >= from
            && *calls >= 4
            && cache_ops_remaining(runtime) > CACHE_OPS_RESERVE;
        let cache = useful
            .then(|| {
                *calls -= 2;
                let watermark = runtime.admin_get(WATERMARK_KEY).ok()?;
                let value = watermark.map(|r| r.value).unwrap_or(Value::Null);
                let digest = Sha256::digest(format!("{}\n{root}", service.fos_bucket).as_bytes());
                Some(Cache {
                    generation: value["manifest_gen"].as_u64().unwrap_or(0),
                    run_id: value["run_id"].as_str().map(str::to_owned),
                    from,
                    before,
                    namespace: hex::encode(&digest[..8]),
                    surrogate: format!("flp-manifest-{}", service.service_id),
                })
            })
            .flatten();
        Self {
            store,
            root: root.to_owned(),
            cache,
            pending: Vec::new(),
        }
    }

    /// Where a listing of the day starting at `day` begins, or of the hour `hour` within it.
    pub fn start(&self, hour: i64) -> String {
        format!("{}{}", self.root, hour_dir(hour))
    }

    pub fn day_start(&self, day: i64) -> String {
        format!("{}{}", self.root, day_dir(day))
    }

    fn cache_key(&self, hour: i64) -> Option<String> {
        let cache = self.cache.as_ref()?;
        (hour >= cache.from && hour < cache.before).then(|| {
            format!(
                "flp-manifest:{}:g{}:{hour}",
                cache.namespace, cache.generation
            )
        })
    }

    fn lookup(&self, hour: i64) -> Option<Vec<Object>> {
        let key = self.cache_key(hour)?;
        let runtime = self.store.runtime;
        if cache_ops_remaining(runtime) <= CACHE_OPS_RESERVE + self.pending.len() as u32 {
            return None;
        }
        let (body, _) = runtime.core_cache_lookup(&key).ok()??;
        let entries: Vec<(String, u64, Option<String>)> = serde_json::from_slice(&body).ok()?;
        Some(
            entries
                .into_iter()
                .map(|(name, size, last_modified)| Object {
                    key: format!("{}{name}", self.root),
                    size,
                    last_modified,
                })
                .collect(),
        )
    }

    fn remember(&mut self, hour: i64, objects: &[Object]) {
        let Some(key) = self.cache_key(hour) else {
            return;
        };
        let entries: Vec<(&str, u64, Option<&str>)> = objects
            .iter()
            .map(|o| {
                (
                    &o.key[self.root.len()..],
                    o.size,
                    o.last_modified.as_deref(),
                )
            })
            .collect();
        if let Ok(body) = serde_json::to_vec(&entries) {
            self.pending.push((key, body));
        }
    }

    /// The next objects of the day starting at `day`, from the `start-after` key `pos` through
    /// hour `last_hour`: one cached hour, or one LIST page cut back to whole hours when it has
    /// more than one. Every whole hour a LIST returns is kept for `finish` to cache.
    pub fn next_block(
        &mut self,
        day: i64,
        pos: &str,
        last_hour: i64,
        used: &mut u32,
    ) -> Result<Block, String> {
        let end = last_hour.min(day + 86_400 - 3600);
        let at = key_hour(&self.root, pos);
        if let Some((hour, true)) = at {
            if hour > end {
                return Ok(Block {
                    objects: Vec::new(),
                    next: None,
                });
            }
            if let Some(objects) = self.lookup(hour) {
                let next = hour + 3600;
                return Ok(Block {
                    objects,
                    next: (next <= end).then(|| self.start(next)),
                });
            }
        }
        *used += 1;
        let page = self.store.list(
            &format!("{}{}", self.root, day_dir(day)),
            Some(pos),
            None,
            LIST_PAGE,
            None,
        )?;
        let mut objects = page.objects;
        let last_folder = self.start(end);
        let past = objects
            .iter()
            .position(|o| o.key > last_folder && !o.key.starts_with(&last_folder));
        let done = page.next_token.is_none() || past.is_some();
        objects.truncate(past.unwrap_or(objects.len()));
        let hours: Vec<Option<i64>> = objects
            .iter()
            .map(|o| key_hour(&self.root, &o.key).map(|(h, _)| h))
            .collect();
        let (next, known_to) = if done {
            (None, Some(end + 3600))
        } else {
            match hours.last().copied().flatten() {
                Some(last) => match hours.iter().position(|h| *h == Some(last)) {
                    Some(cut) if cut > 0 => {
                        objects.truncate(cut);
                        (Some(self.start(last)), Some(last))
                    }
                    _ => (objects.last().map(|o| o.key.clone()), Some(last)),
                },
                None => (objects.last().map(|o| o.key.clone()), None),
            }
        };
        let known_from = match at {
            Some((hour, true)) => Some(hour),
            Some((hour, false)) => Some(hour + 3600),
            None => None,
        };
        let hours = &hours[..objects.len()];
        if let (Some(mut hour), Some(to), true) =
            (known_from, known_to, hours.iter().all(Option::is_some))
        {
            // Objects of the hour `pos` falls inside were not listed from its start.
            let mut index = hours.iter().take_while(|h| **h < Some(hour)).count();
            while hour < to {
                let begin = index;
                while index < objects.len() && hours[index] == Some(hour) {
                    index += 1;
                }
                let listed = objects[begin..index].to_vec();
                self.remember(hour, &listed);
                hour += 3600;
            }
        }
        Ok(Block { objects, next })
    }

    /// Caches the whole hours listed unless a plan is in flight. With `advertise`, returns what
    /// a browser may key its own closed-hour cache on (`Null` when it must not cache).
    pub fn finish(self, advertise: bool) -> Value {
        let Some(cache) = &self.cache else {
            return Value::Null;
        };
        if self.pending.is_empty() && !advertise {
            return Value::Null;
        }
        let runtime = self.store.runtime;
        let in_flight = match runtime.admin_get(PLAN_KEY) {
            Ok(None) => false,
            Ok(Some(record)) => {
                let id = record.value["plan_id"]
                    .as_str()
                    .or_else(|| record.value["run_id"].as_str());
                id != cache.run_id.as_deref()
            }
            Err(_) => true,
        };
        if in_flight {
            return Value::Null;
        }
        for (key, body) in &self.pending {
            if cache_ops_remaining(runtime) <= CACHE_OPS_RESERVE {
                break;
            }
            let _ = runtime.core_cache_replace(key, &[cache.surrogate.as_str()], TTL, &|_| {
                (body.clone(), Vec::new())
            });
        }
        if advertise {
            json!({
                "gen": cache.generation,
                "from": rfc3339(cache.from),
                "before": rfc3339(cache.before),
            })
        } else {
            Value::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hour_tells_folders_from_objects() {
        let root = "s/processed/request/";
        let day = 1_800_000_000 - 8 * 3600;
        assert_eq!(
            key_hour(root, "s/processed/request/year=2027/month=01/day=15/"),
            Some((day, true))
        );
        assert_eq!(
            key_hour(
                root,
                "s/processed/request/year=2027/month=01/day=15/hour=09/"
            ),
            Some((day + 9 * 3600, true))
        );
        assert_eq!(
            key_hour(
                root,
                "s/processed/request/year=2027/month=01/day=15/hour=09/window=1-2-r.parquet"
            ),
            Some((day + 9 * 3600, false))
        );
        assert_eq!(key_hour(root, "s/processed/request/year=2027/x"), None);
        assert_eq!(key_hour(root, "other/year=2027/month=01/day=15/"), None);
    }
}
