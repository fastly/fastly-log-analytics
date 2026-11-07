use crate::{
    admin::{
        transport::{encode, request_timeout, FOS_TIMEOUT},
        FosRequest, Service,
    },
    shell::{Runtime, PREFETCH_UNREAD},
};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    time::{Duration, Instant},
};

pub const DELETE_BATCH: usize = 1000;
pub const CDN_READ_LIMIT: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Object {
    pub key: String,
    pub size: u64,
    pub last_modified: Option<String>,
}

#[derive(Debug, Default)]
pub struct Page {
    pub objects: Vec<Object>,
    pub prefixes: Vec<String>,
    pub next_token: Option<String>,
}

#[derive(Deserialize)]
struct ListResult {
    #[serde(rename = "Contents", default)]
    contents: Vec<Entry>,
    #[serde(rename = "CommonPrefixes", default)]
    prefixes: Vec<Prefix>,
    #[serde(rename = "IsTruncated", default)]
    truncated: bool,
    #[serde(rename = "NextContinuationToken", default)]
    token: Option<String>,
}
#[derive(Deserialize)]
struct Entry {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Size", default)]
    size: u64,
    #[serde(rename = "LastModified", default)]
    last_modified: Option<String>,
}
#[derive(Deserialize)]
struct Prefix {
    #[serde(rename = "Prefix")]
    prefix: String,
}

pub fn parse_list(xml: &[u8]) -> Result<Page, String> {
    let text = std::str::from_utf8(xml).map_err(|_| "list_invalid")?;
    if !text.contains("<ListBucketResult") {
        return Err("list_invalid".into());
    }
    let parsed: ListResult = quick_xml::de::from_str(text).map_err(|_| "list_invalid")?;
    let next_token = if parsed.truncated {
        Some(
            parsed
                .token
                .filter(|t| !t.is_empty())
                .ok_or("list_missing_cursor")?,
        )
    } else {
        None
    };
    Ok(Page {
        objects: parsed
            .contents
            .into_iter()
            .map(|e| Object {
                key: e.key,
                size: e.size,
                last_modified: e.last_modified.filter(|v| !v.is_empty()),
            })
            .collect(),
        prefixes: parsed.prefixes.into_iter().map(|p| p.prefix).collect(),
        next_token,
    })
}

#[derive(Debug, Default)]
pub struct DeleteResult {
    pub deleted: Vec<String>,
    pub failed: Vec<String>,
}

#[derive(Deserialize)]
struct DeleteResponse {
    #[serde(rename = "$value", default)]
    items: Vec<DeleteItem>,
}
#[derive(Deserialize)]
enum DeleteItem {
    Deleted(KeyOnly),
    Error(KeyOnly),
    #[serde(other)]
    Other,
}
#[derive(Deserialize)]
struct KeyOnly {
    #[serde(rename = "Key")]
    key: String,
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Text of the first `<tag>` element in an XML body.
fn xml_text(xml: &[u8], tag: &str) -> Option<String> {
    let text = std::str::from_utf8(xml).ok()?;
    let start = text.find(&format!("<{tag}>"))? + tag.len() + 2;
    let end = start + text[start..].find(&format!("</{tag}>"))?;
    Some(
        text[start..end]
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&"),
    )
}

/// The MD5 hex of a single-part upload's `ETag`; `None` for multipart (`-n`) or malformed tags.
pub fn normalize_etag(etag: &str) -> Option<String> {
    let tag = etag.trim().trim_matches('"').to_ascii_lowercase();
    (tag.len() == 32 && tag.bytes().all(|b| b.is_ascii_hexdigit())).then_some(tag)
}

/// Bucket access for one provisioned service. LIST, PUT and DELETE go straight to object
/// storage; object reads go through the front's `/fos/<service>/<key>` path.
pub struct Store<'a, R: Runtime> {
    pub runtime: &'a R,
    pub service: &'a Service,
    pub access_key: String,
    pub secret_key: String,
    pub started: Instant,
    /// Execution vCPU already spent when this store's work began; vCPU admission counts from here.
    pub vcpu_start: u64,
}

/// Loops stop starting new work past this elapsed time, well inside the 60s trial runtime.
pub const WALL_STOP: std::time::Duration = std::time::Duration::from_secs(40);
/// The wait before a retry whose transient answer named no `Retry-After`.
pub const RETRY_DELAY: Duration = Duration::from_millis(500);

/// Calls a job holds back for retrying idempotent GETs and PUTs after a transient answer, and
/// the elapsed time past which a retry would no longer start.
pub struct Retry {
    spare: Cell<u32>,
    used: Cell<u32>,
    stop: Duration,
}

impl Retry {
    pub fn new(spare: u32, stop: Duration) -> Self {
        Self {
            spare: Cell::new(spare),
            used: Cell::new(0),
            stop,
        }
    }
    pub fn used(&self) -> u32 {
        self.used.get()
    }
    pub fn stop(&self) -> Duration {
        self.stop
    }
}

/// A 5xx, 429 or S3 `SlowDown` answer: the same request may succeed if sent again.
pub fn transient_status(status: u16, body: &[u8]) -> bool {
    status == 429
        || (500..600).contains(&status)
        || body.windows(15).any(|w| w == b"<Code>SlowDown<")
}

/// Whether a job's error came from a transient object storage answer or transport failure
/// rather than the job itself, so retrying the job unchanged may succeed. A worker that never
/// answered (`worker_deadline`, `worker_http_*`) is not transient: it may have run out of memory.
pub fn transient_error(code: &str) -> bool {
    if code.starts_with("worker_") {
        return false;
    }
    let status = code
        .rsplit_once("_http_")
        .and_then(|(_, status)| status.parse::<u16>().ok());
    status.is_some_and(|status| transient_status(status, b""))
        || crate::admin::transport::retryable_transport(code)
}

enum Sent {
    Done(Result<String, String>),
    Transient(String, Option<Duration>),
}

impl<R: Runtime> Store<'_, R> {
    pub fn within_wall(&self) -> bool {
        self.runtime.admin_elapsed(self.started) < WALL_STOP
    }

    pub fn can_afford(&self, calls: u32, reserve: u32, vcpu_stop_ms: u64) -> bool {
        self.within_wall()
            && self.runtime.admin_remaining_calls() >= calls + reserve
            && self.vcpu_ms() < vcpu_stop_ms
    }

    /// vCPU spent since [`Self::vcpu_start`].
    pub fn vcpu_ms(&self) -> u64 {
        self.runtime.vcpu_ms().saturating_sub(self.vcpu_start)
    }

    fn fos(
        &self,
        method: &str,
        key: &str,
        query: &str,
        body: Vec<u8>,
    ) -> Result<crate::admin::FosResponse, String> {
        self.fos_with_limit(
            method,
            key,
            query,
            body,
            crate::admin::transport::FOS_RESPONSE_LIMIT,
        )
    }

    fn fos_with_limit(
        &self,
        method: &str,
        key: &str,
        query: &str,
        body: Vec<u8>,
        response_limit: usize,
    ) -> Result<crate::admin::FosResponse, String> {
        self.runtime.admin_fos(FosRequest {
            method,
            region: &self.service.fos_region,
            bucket: &self.service.fos_bucket,
            key,
            query,
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            body,
            timeout: request_timeout(FOS_TIMEOUT, self.runtime.admin_elapsed(self.started)),
            response_limit,
            content_md5: None,
        })
    }

    pub fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        token: Option<&str>,
        max_keys: usize,
        delimiter: Option<&str>,
    ) -> Result<Page, String> {
        let mut query = format!(
            "list-type=2&max-keys={}&prefix={}",
            max_keys.clamp(1, 1000),
            encode(prefix)
        );
        if let Some(delimiter) = delimiter {
            query.push_str(&format!("&delimiter={}", encode(delimiter)));
        }
        if let Some(token) = token {
            query.push_str(&format!("&continuation-token={}", encode(token)));
        } else if let Some(after) = start_after {
            query.push_str(&format!("&start-after={}", encode(after)));
        }
        let response = self.fos("GET", "", &query, Vec::new())?;
        if response.status != 200 {
            return Err(format!("list_http_{}", response.status));
        }
        let page = parse_list(&response.body)?;
        if let Some(usage) = self.runtime.usage_agg() {
            usage.note_sizes(page.objects.iter().map(|o| (o.key.as_str(), o.size)));
        }
        Ok(page)
    }

    pub fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        let response = self.fos("PUT", key, "", body)?;
        if (200..300).contains(&response.status) {
            Ok(())
        } else {
            Err(format!("put_http_{}", response.status))
        }
    }

    /// Single PUT that succeeds only if the returned `ETag` is the body's MD5, i.e. object
    /// storage stored exactly these bytes. Returns the MD5 hex.
    pub fn put_verified(&self, key: &str, body: Vec<u8>) -> Result<String, String> {
        self.put_verified_retrying(key, body, None)
    }

    /// [`Self::put_verified`], sent once more after a transient answer while `retry` has a spare
    /// call. The body is kept for that only while a retry is possible.
    pub fn put_verified_retrying(
        &self,
        key: &str,
        mut body: Vec<u8>,
        retry: Option<&Retry>,
    ) -> Result<String, String> {
        use base64::Engine;
        use md5::{Digest, Md5};
        let digest = Md5::digest(&body);
        let md5 = hex::encode(digest);
        let content_md5 = base64::engine::general_purpose::STANDARD.encode(digest);
        let send = |body: Vec<u8>| -> Result<Sent, String> {
            let (response, etag) = self.runtime.admin_fos_etag(FosRequest {
                method: "PUT",
                region: &self.service.fos_region,
                bucket: &self.service.fos_bucket,
                key,
                query: "",
                access_key: &self.access_key,
                secret_key: &self.secret_key,
                body,
                timeout: request_timeout(FOS_TIMEOUT, self.runtime.admin_elapsed(self.started)),
                response_limit: crate::admin::transport::FOS_RESPONSE_LIMIT,
                content_md5: Some(&content_md5),
            })?;
            let error = format!("put_http_{}", response.status);
            if transient_status(response.status, &response.body) {
                return Ok(Sent::Transient(error, response.retry_after));
            }
            if !(200..300).contains(&response.status) {
                return Ok(Sent::Done(Err(error)));
            }
            let stored = etag.as_deref().and_then(normalize_etag).as_deref() == Some(md5.as_str());
            Ok(Sent::Done(if stored {
                Ok(md5.clone())
            } else {
                Err("etag_mismatch".into())
            }))
        };
        let keep = retry.is_some_and(|r| r.spare.get() > 0);
        let first = if keep {
            body.clone()
        } else {
            std::mem::take(&mut body)
        };
        match send(first)? {
            Sent::Done(result) => result,
            Sent::Transient(_, wait) if keep && self.retry_wait(retry, wait) => match send(body)? {
                Sent::Done(result) => result,
                Sent::Transient(error, _) => Err(error),
            },
            Sent::Transient(error, _) => Err(error),
        }
    }

    /// Starts a multipart upload of `key` and returns its upload id.
    pub fn multipart_create(&self, key: &str) -> Result<String, String> {
        let response = self.fos("POST", key, "uploads", Vec::new())?;
        if response.status != 200 {
            return Err(format!("multipart_create_http_{}", response.status));
        }
        xml_text(&response.body, "UploadId").ok_or_else(|| "multipart_create_no_id".into())
    }

    /// Uploads part `number` (from 1) and returns its `ETag`, which must be the part's MD5.
    pub fn multipart_part(
        &self,
        key: &str,
        upload_id: &str,
        number: u32,
        body: Vec<u8>,
    ) -> Result<String, String> {
        use base64::Engine;
        use md5::{Digest, Md5};
        let digest = Md5::digest(&body);
        let md5 = hex::encode(digest);
        let content_md5 = base64::engine::general_purpose::STANDARD.encode(digest);
        let query = format!("partNumber={number}&uploadId={}", encode(upload_id));
        let (response, etag) = self.runtime.admin_fos_etag(FosRequest {
            method: "PUT",
            region: &self.service.fos_region,
            bucket: &self.service.fos_bucket,
            key,
            query: &query,
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            body,
            timeout: request_timeout(FOS_TIMEOUT, self.runtime.admin_elapsed(self.started)),
            response_limit: crate::admin::transport::FOS_RESPONSE_LIMIT,
            content_md5: Some(&content_md5),
        })?;
        if !(200..300).contains(&response.status) {
            if response.status == 404
                && xml_text(&response.body, "Code").as_deref() == Some("NoSuchUpload")
            {
                return Err("multipart_upload_missing".into());
            }
            return Err(format!("multipart_part_http_{}", response.status));
        }
        match etag.as_deref().and_then(normalize_etag) {
            Some(tag) if tag == md5 => Ok(md5),
            _ => Err("etag_mismatch".into()),
        }
    }

    /// Completes the upload from `etags`, part 1 first. Object storage can answer 200 with an
    /// error document, so the body is checked too.
    pub fn multipart_complete(
        &self,
        key: &str,
        upload_id: &str,
        etags: &[String],
    ) -> Result<(), String> {
        let mut body = String::from("<CompleteMultipartUpload>");
        for (index, etag) in etags.iter().enumerate() {
            body.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>\"{etag}\"</ETag></Part>",
                index + 1
            ));
        }
        body.push_str("</CompleteMultipartUpload>");
        let query = format!("uploadId={}", encode(upload_id));
        let response = self.fos("POST", key, &query, body.into_bytes())?;
        if response.status != 200 || xml_text(&response.body, "Code").is_some() {
            return Err(format!("multipart_complete_http_{}", response.status));
        }
        Ok(())
    }

    pub fn multipart_abort(&self, key: &str, upload_id: &str) -> Result<(), String> {
        let query = format!("uploadId={}", encode(upload_id));
        let response = self.fos("DELETE", key, &query, Vec::new())?;
        match response.status {
            200..=299 | 404 => Ok(()),
            status => Err(format!("multipart_abort_http_{status}")),
        }
    }

    /// Spends `retry`'s spare call after waiting out `retry_after` (or [`RETRY_DELAY`]), unless
    /// none is left or the wait would end past its stop.
    fn retry_wait(&self, retry: Option<&Retry>, retry_after: Option<Duration>) -> bool {
        let Some(retry) = retry.filter(|r| r.spare.get() > 0) else {
            return false;
        };
        let wait = retry_after.unwrap_or(RETRY_DELAY);
        if self.runtime.admin_elapsed(self.started) + wait >= retry.stop {
            return false;
        }
        retry.spare.set(retry.spare.get() - 1);
        retry.used.set(retry.used.get() + 1);
        self.runtime.admin_wait(wait.as_millis() as u64);
        true
    }

    /// Direct GET; `Ok(None)` is a missing object. On Wasm (`FastlyRuntime`), routes write-once
    /// object reads through the Front VCL CDN (`/fos/<service>/<key>`) when `cdn_key` is present.
    pub fn get_direct(&self, key: &str, limit: usize) -> Result<Option<Vec<u8>>, String> {
        #[cfg(target_arch = "wasm32")]
        if let Ok(Some(cdn_key)) = self
            .runtime
            .secret("cdn_key")
            .map(|k| k.filter(|s| !s.is_empty()))
        {
            return match self.get(&cdn_key, key, limit) {
                Ok(body) => Ok(body),
                Err(err) if err == "cdn_body_too_large" => Err("get_body_too_large".into()),
                Err(err) => Err(err),
            };
        }
        let response = self.fos_with_limit("GET", key, "", Vec::new(), limit)?;
        match response.status {
            200 if response.body.len() <= limit => Ok(Some(response.body)),
            200 => Err("get_body_too_large".into()),
            404 => Ok(None),
            status => Err(format!("get_http_{status}")),
        }
    }

    /// [`Self::get_direct`] of the first `(key, limit)`, with the rest sent alongside it as
    /// speculative reads. Results keep request order; each sent request is one backend call. A
    /// speculative read whose answer was not read here answers [`PREFETCH_UNREAD`].
    pub fn get_direct_many(
        &self,
        requests: &[(String, usize)],
    ) -> Vec<Result<Option<Vec<u8>>, String>> {
        let Some((first, rest)) = requests.split_first() else {
            return Vec::new();
        };
        let abandoned = || Err(PREFETCH_UNREAD.to_owned());
        #[cfg(target_arch = "wasm32")]
        if let Ok(Some(cdn_key)) = self
            .runtime
            .secret("cdn_key")
            .map(|k| k.filter(|s| !s.is_empty()))
        {
            let timeout = request_timeout(FOS_TIMEOUT, self.runtime.admin_elapsed(self.started));
            return self
                .runtime
                .admin_cdn_get_many(&self.service.service_id, requests, &cdn_key, timeout)
                .into_iter()
                .map(|response| match response {
                    Ok(response) => match response.status {
                        200 => Ok(Some(response.body)),
                        404 => Ok(None),
                        status => Err(format!("get_http_{status}")),
                    },
                    Err(err) if err == "cdn_body_too_large" => Err("get_body_too_large".into()),
                    Err(err) => Err(err),
                })
                .collect();
        }
        if !self.runtime.admin_fos_concurrent() {
            return std::iter::once(self.get_direct(&first.0, first.1))
                .chain(rest.iter().map(|_| abandoned()))
                .collect();
        }
        let timeout = request_timeout(FOS_TIMEOUT, self.runtime.admin_elapsed(self.started));
        self.runtime
            .admin_fos_many(
                requests
                    .iter()
                    .map(|(key, limit)| FosRequest {
                        method: "GET",
                        region: &self.service.fos_region,
                        bucket: &self.service.fos_bucket,
                        key,
                        query: "",
                        access_key: &self.access_key,
                        secret_key: &self.secret_key,
                        body: Vec::new(),
                        timeout,
                        response_limit: *limit,
                        content_md5: None,
                    })
                    .collect(),
            )
            .into_iter()
            .zip(requests)
            .map(|(response, (_, limit))| {
                let response = response?;
                match response.status {
                    200 if response.body.len() <= *limit => Ok(Some(response.body)),
                    200 => Err("get_body_too_large".into()),
                    404 => Ok(None),
                    status => Err(format!("get_http_{status}")),
                }
            })
            .collect()
    }

    pub fn exists(&self, key: &str) -> Result<bool, String> {
        let response = self.fos("HEAD", key, "", Vec::new())?;
        match response.status {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(format!("head_http_{status}")),
        }
    }

    /// A missing object counts as deleted.
    pub fn delete(&self, key: &str) -> Result<(), String> {
        let response = self.fos("DELETE", key, "", Vec::new())?;
        if (200..300).contains(&response.status) || response.status == 404 {
            Ok(())
        } else {
            Err(format!("delete_http_{}", response.status))
        }
    }

    /// One `DeleteObjects` POST per 1,000 keys. A key the response neither confirms nor
    /// reports as an error counts as failed, so callers requeue it.
    pub fn delete_objects(&self, keys: &[String]) -> Result<DeleteResult, String> {
        let mut result = DeleteResult::default();
        for (index, chunk) in keys.chunks(DELETE_BATCH).enumerate() {
            let mut body = String::from("<Delete>");
            for key in chunk {
                body.push_str(&format!("<Object><Key>{}</Key></Object>", xml_escape(key)));
            }
            body.push_str("</Delete>");
            let response = match self.fos("POST", "", "delete", body.into_bytes()) {
                Ok(response) => response,
                Err(error) if index == 0 => return Err(error),
                Err(_) => {
                    result
                        .failed
                        .extend(keys[index * DELETE_BATCH..].iter().cloned());
                    break;
                }
            };
            let parsed = (response.status == 200)
                .then(|| std::str::from_utf8(&response.body).ok())
                .flatten()
                .and_then(|text| quick_xml::de::from_str::<DeleteResponse>(text).ok());
            let Some(parsed) = parsed else {
                if index == 0 {
                    return Err(format!("delete_objects_http_{}", response.status));
                }
                result
                    .failed
                    .extend(keys[index * DELETE_BATCH..].iter().cloned());
                break;
            };
            let (mut confirmed, mut errored) = (
                std::collections::BTreeSet::new(),
                std::collections::BTreeSet::new(),
            );
            for item in parsed.items {
                match item {
                    DeleteItem::Deleted(k) => confirmed.insert(k.key),
                    DeleteItem::Error(k) => errored.insert(k.key),
                    DeleteItem::Other => continue,
                };
            }
            let mut done = Vec::new();
            for key in chunk {
                if confirmed.contains(key) && !errored.contains(key) {
                    done.push(key.clone());
                } else {
                    result.failed.push(key.clone());
                }
            }
            if let Some(usage) = self.runtime.usage_agg() {
                usage.record_deleted(&done);
            }
            result.deleted.extend(done);
        }
        Ok(result)
    }

    /// `Ok(None)` is a missing object. Every read goes through the CDN path.
    pub fn get(&self, cdn_key: &str, key: &str, limit: usize) -> Result<Option<Vec<u8>>, String> {
        self.get_retrying(cdn_key, key, limit, None)
    }

    pub fn range(
        &self,
        cdn_key: &str,
        key: &str,
        start: u64,
        length: usize,
    ) -> Result<Vec<u8>, String> {
        let end = start
            .checked_add(length as u64)
            .and_then(|end| end.checked_sub(1))
            .ok_or("compact_range_invalid")?;
        let response = self.runtime.admin_cdn_read(
            &self.service.service_id,
            key,
            cdn_key,
            length,
            request_timeout(FOS_TIMEOUT, self.runtime.admin_elapsed(self.started)),
            Some((start, end)),
        )?;
        if response.status != 206 {
            return Err(format!("get_http_{}", response.status));
        }
        if response.body.len() != length {
            return Err("compact_range_invalid_response".into());
        }
        Ok(response.body)
    }

    /// [`Self::get`], sent once more after a transient answer while `retry` has a spare call.
    pub fn get_retrying(
        &self,
        cdn_key: &str,
        key: &str,
        limit: usize,
        retry: Option<&Retry>,
    ) -> Result<Option<Vec<u8>>, String> {
        let send = || {
            self.runtime.admin_cdn_get(
                &self.service.service_id,
                key,
                cdn_key,
                limit,
                request_timeout(FOS_TIMEOUT, self.runtime.admin_elapsed(self.started)),
            )
        };
        let mut response = send()?;
        if transient_status(response.status, &response.body)
            && self.retry_wait(retry, response.retry_after)
        {
            response = send()?;
        }
        match response.status {
            200 => Ok(Some(response.body)),
            404 => Ok(None),
            status => Err(format!("get_http_{status}")),
        }
    }
}
