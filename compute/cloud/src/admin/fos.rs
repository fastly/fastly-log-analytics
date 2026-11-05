use super::*;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

fn temporary_description(service: &Service) -> String {
    format!("flp-tmp-{}-{}", service.owner, service.service_id)
}
fn permanent_description(service: &Service) -> String {
    format!("flp-{}-{}", service.owner, service.service_id)
}
fn temporary_refs(service: &Service) -> (String, String) {
    (
        format!("fos_tmp_ak.{}", service.service_id),
        format!("fos_tmp_sk.{}", service.service_id),
    )
}
fn recovery_ref(service: &Service, access_key: &str) -> String {
    format!(
        "fos_recovery.{}.{}",
        service.service_id,
        hex::encode(Sha256::digest(access_key.as_bytes()))
    )
}
fn secret_metadata(
    context: &Context<'_, impl Runtime>,
    token: &str,
    name: &str,
) -> Result<u16, Error> {
    let store = config(context.runtime, "secret_store_id")?;
    let (status, _) = context.api_status(
        "GET",
        &format!("/resources/stores/secret/{store}/secrets/{}", encode(name)),
        token,
        None,
    )?;
    if matches!(status, 200 | 404) {
        Ok(status)
    } else {
        Err(Error::new(502, "secret_metadata_unavailable"))
    }
}
fn keys(context: &Context<'_, impl Runtime>, token: &str) -> Result<Vec<Value>, Error> {
    let response = context.api("GET", "/resources/object-storage/access-keys", token, None)?;
    let values = list(&response)?;
    if values.len() > 1000 {
        return Err(Error::new(409, "key_inventory_limit"));
    }
    Ok(values.clone())
}
fn delete_key(
    context: &Context<'_, impl Runtime>,
    token: &str,
    key: &Value,
    service: &Service,
) -> Result<(), Error> {
    let key = required(key, "access_key")?;
    let store = config(context.runtime, "secret_store_id")?;
    let (status, _) = context.api_status(
        "DELETE",
        &format!(
            "/resources/stores/secret/{store}/secrets/{}",
            encode(&recovery_ref(service, key))
        ),
        token,
        None,
    )?;
    if !(200..300).contains(&status) && status != 404 {
        return Err(Error::new(502, "secret_delete_failed"));
    }
    let (status, _) = context.api_status(
        "DELETE",
        &format!("/resources/object-storage/access-keys/{}", encode(key)),
        token,
        None,
    )?;
    if (200..300).contains(&status) || status == 404 {
        Ok(())
    } else {
        Err(Error::new(502, "key_revocation_failed"))
    }
}

fn ensure_key(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    token: &str,
    temporary: bool,
) -> Result<bool, Error> {
    let description = if temporary {
        temporary_description(service)
    } else {
        permanent_description(service)
    };
    let mut body = json!({"permission":if temporary {"read-write-admin"} else {"read-write-objects"},"description":description});
    if !temporary {
        body["buckets"] = json!([service.fos_bucket]);
    }
    let (ak_ref, sk_ref) = if temporary {
        temporary_refs(service)
    } else {
        (
            service.fos_access_key_ref.clone(),
            service.fos_secret_key_ref.clone(),
        )
    };
    let inventory = keys(context, token)?;
    let managed: Vec<_> = inventory
        .iter()
        .filter(|v| v["description"] == description)
        .collect();
    if managed.len() > 1 {
        delete_key(context, token, managed[0], service)?;
        return Ok(false);
    }
    if let Some(key) = managed.first() {
        let expected_permission = if temporary {
            "read-write-admin"
        } else {
            "read-write-objects"
        };
        if key["permission"] != expected_permission
            || !temporary
                && key["buckets"]
                    .as_array()
                    .is_none_or(|b| b.len() != 1 || b[0] != service.fos_bucket)
        {
            return Err(Error::new(409, "managed_key_scope_mismatch"));
        }
        let access_key = required(key, "access_key")?;
        context.confirm_api_effect("POST", "/resources/object-storage/access-keys", Some(&body));
        let store = config(context.runtime, "secret_store_id")?;
        if context.api_uncertain(
            "PUT",
            &format!("/resources/stores/secret/{store}/secrets"),
            Some(&json!({"name":ak_ref})),
        ) {
            put_secret(context, token, &ak_ref, access_key)?;
            return Ok(false);
        }
        match context.secret(&sk_ref) {
            Ok(value) => {
                let pair: Value = serde_json::from_str(&value)
                    .map_err(|_| Error::new(409, "credentials_invalid"))?;
                if pair["access_key"] == access_key
                    && pair["secret_key"].as_str().is_some_and(|s| !s.is_empty())
                {
                    put_secret(context, token, &ak_ref, access_key)?;
                    return Ok(true);
                }
            }
            Err(error) if error.code == "credentials_pending" => (),
            Err(error) => return Err(error),
        }
        let recovery = recovery_ref(service, access_key);
        match context.secret(&recovery) {
            Ok(value) => {
                let pair: Value = serde_json::from_str(&value)
                    .map_err(|_| Error::new(409, "credentials_invalid"))?;
                if pair["access_key"] != access_key
                    || pair["secret_key"].as_str().is_none_or(str::is_empty)
                {
                    return Err(Error::new(409, "credentials_invalid"));
                }
                put_secret(context, token, &ak_ref, access_key)?;
                put_secret(context, token, &sk_ref, &value)?;
                return Ok(true);
            }
            Err(error) if error.code == "credentials_pending" => {
                if secret_metadata(context, token, &recovery)? == 200 {
                    return Err(Error::new(409, "credentials_pending"));
                }
                delete_key(context, token, key, service)?;
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
    }
    let key = context.api(
        "POST",
        "/resources/object-storage/access-keys",
        token,
        Some(body),
    )?;
    let ak = required(&key, "access_key").map_err(|_| Error::new(502, "key_response_invalid"))?;
    let sk = required(&key, "secret_key").map_err(|_| Error::new(502, "key_response_invalid"))?;
    let pair = json!({"access_key":ak,"secret_key":sk}).to_string();
    put_secret(context, token, &recovery_ref(service, ak), &pair)?;
    put_secret(context, token, &ak_ref, ak)?;
    put_secret(context, token, &sk_ref, &pair)?;
    Ok(true)
}

fn request(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    method: &str,
    key: &str,
    query: &str,
    temporary: bool,
) -> Result<FosResponse, Error> {
    let (ak_ref, sk_ref) = if temporary {
        temporary_refs(service)
    } else {
        (
            service.fos_access_key_ref.clone(),
            service.fos_secret_key_ref.clone(),
        )
    };
    let ak = context.secret(&ak_ref)?;
    let envelope = context.secret(&sk_ref)?;
    let envelope: Value =
        serde_json::from_str(&envelope).map_err(|_| Error::new(409, "credentials_pending"))?;
    if envelope["access_key"] != ak {
        return Err(Error::new(409, "credentials_pending"));
    }
    let sk = required(&envelope, "secret_key")?;
    let body = if method == "PUT" && key.is_empty() && service.fos_region != "us-east-1" {
        create_bucket_body(&service.fos_region)?
    } else {
        Vec::new()
    };
    context.fos(FosRequest {
        method,
        region: &service.fos_region,
        bucket: &service.fos_bucket,
        key,
        query,
        access_key: &ak,
        secret_key: sk,
        body,
        timeout: super::transport::FOS_TIMEOUT,
        response_limit: super::transport::FOS_RESPONSE_LIMIT,
        content_md5: None,
    })
}
pub fn credentials(
    context: &Context<'_, impl Runtime>,
    service: &Service,
) -> Result<(String, String), Error> {
    let ak = context.secret(&service.fos_access_key_ref)?;
    let envelope = context.secret(&service.fos_secret_key_ref)?;
    let envelope: Value =
        serde_json::from_str(&envelope).map_err(|_| Error::new(409, "credentials_pending"))?;
    if envelope["access_key"] != ak {
        return Err(Error::new(409, "credentials_pending"));
    }
    Ok((ak, required(&envelope, "secret_key")?.to_owned()))
}
pub fn put_object(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    (ak, sk): &(String, String),
    key: &str,
    body: Vec<u8>,
) -> Result<(), Error> {
    let response = context.fos(FosRequest {
        method: "PUT",
        region: &service.fos_region,
        bucket: &service.fos_bucket,
        key,
        query: "",
        access_key: ak,
        secret_key: sk,
        body,
        timeout: super::transport::FOS_TIMEOUT,
        response_limit: super::transport::FOS_RESPONSE_LIMIT,
        content_md5: None,
    })?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(Error::new(502, "object_put_failed"))
    }
}
pub fn delete_object(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    (ak, sk): &(String, String),
    key: &str,
) -> Result<(), Error> {
    let response = context.fos(FosRequest {
        method: "DELETE",
        region: &service.fos_region,
        bucket: &service.fos_bucket,
        key,
        query: "",
        access_key: ak,
        secret_key: sk,
        body: Vec::new(),
        timeout: super::transport::FOS_TIMEOUT,
        response_limit: super::transport::FOS_RESPONSE_LIMIT,
        content_md5: None,
    })?;
    if (200..300).contains(&response.status) || response.status == 404 {
        Ok(())
    } else {
        Err(Error::new(502, "object_delete_failed"))
    }
}
pub fn bucket(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &mut Service,
    record: &Record,
    token: &str,
) -> Result<(), Error> {
    match job.step.as_str() {
        "bucket" => {
            if ensure_key(context, service, token, true)? {
                advance(job, "bucket_probe");
            }
        }
        "bucket_probe" => {
            let result = request(context, service, "HEAD", "", "", true)?;
            match result.status {
                200 => service.bucket_adopted = true,
                404 => service.bucket_adopted = false,
                _ => return Err(Error::new(502, "bucket_head_failed")),
            }
            service.bucket_checked = true;
            save_job_service(
                context,
                record,
                service,
                job,
                "bucket.checked",
                "bucket_create",
            )?;
            let current = context
                .get(&format!("svc:{}", service.service_id))?
                .ok_or_else(|| Error::new(503, "state_unavailable"))?;
            job.expected_generation = current.generation;
            advance(job, "bucket_create");
        }
        "bucket_create" => {
            if !service.bucket_checked {
                return Err(Error::new(409, "bucket_not_checked"));
            }
            if !service.bucket_adopted {
                let result = request(context, service, "PUT", "", "", true)?;
                if !(200..300).contains(&result.status) {
                    if result.status == 409 {
                        service.bucket_adopted = true;
                        save_job_service(context, record, service, job, "bucket.adopted", "key")?;
                        job.expected_generation = context
                            .get(&format!("svc:{}", service.service_id))?
                            .ok_or_else(|| Error::new(503, "state_unavailable"))?
                            .generation;
                    } else {
                        return Err(Error::new(502, "bucket_create_failed"));
                    }
                }
            }
            advance(job, "key");
        }
        _ => return Err(Error::new(409, "invalid_bucket_step")),
    }
    Ok(())
}
pub fn permanent_key(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    if ensure_key(context, service, token, false)? {
        advance(job, "cleanup_temp");
    }
    Ok(())
}
pub fn cleanup_temp(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    token: &str,
) -> Result<bool, Error> {
    let inventory = keys(context, token)?;
    let description = temporary_description(service);
    if let Some(key) = inventory.iter().find(|v| v["description"] == description) {
        if key["permission"] != "read-write-admin" {
            return Err(Error::new(409, "temporary_key_scope_mismatch"));
        }
        delete_key(context, token, key, service)?;
        return Ok(false);
    }
    Ok(true)
}
pub fn cleanup_temp_secrets(
    context: &Context<'_, impl Runtime>,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    let store = config(context.runtime, "secret_store_id")?;
    let (ak, sk) = temporary_refs(service);
    let mut names = vec![ak, sk];
    if let Ok(perm_ak) = context.secret(&service.fos_access_key_ref) {
        if !perm_ak.is_empty() {
            names.push(recovery_ref(service, &perm_ak));
        }
    }
    for name in names {
        let (status, _) = context.api_status(
            "DELETE",
            &format!("/resources/stores/secret/{store}/secrets/{}", encode(&name)),
            token,
            None,
        )?;
        if !(200..300).contains(&status) && status != 404 {
            return Err(Error::new(502, "secret_delete_failed"));
        }
    }
    Ok(())
}

pub fn delete(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    if !job.options.delete_data && !job.options.delete_fos_bucket {
        advance(job, "revoke");
        return Ok(());
    }
    if service.bucket_adopted && (job.options.delete_fos_bucket || service.fos_prefix.is_empty()) {
        return Err(Error::new(409, "adopted_bucket_protected"));
    }
    match job.step.as_str() {
        "delete_objects" => {
            if ensure_key(context, service, token, true)? {
                advance(job, "delete_page");
            }
        }
        "delete_page" => {
            let prefix = scope_prefix(service);
            let response = request(
                context,
                service,
                "GET",
                "",
                &format!("list-type=2&max-keys=1&prefix={}", encode(&prefix)),
                true,
            )?;
            if response.status == 404 {
                advance(job, "revoke");
                return Ok(());
            }
            if response.status != 200 {
                return Err(Error::new(502, "object_list_failed"));
            }
            let xml = std::str::from_utf8(&response.body)
                .map_err(|_| Error::new(502, "invalid_object_list"))?;
            if let Some(key) = parse_first_object_key(xml)? {
                if !key.starts_with(&prefix) {
                    return Err(Error::new(409, "object_outside_service_prefix"));
                }
                let result = request(context, service, "DELETE", &key, "", true)?;
                if !(200..300).contains(&result.status) && result.status != 404 {
                    return Err(Error::new(502, "object_delete_failed"));
                }
            } else {
                advance(
                    job,
                    if job.options.delete_fos_bucket {
                        "delete_bucket"
                    } else {
                        "revoke"
                    },
                );
            }
        }
        "delete_bucket" => {
            if service.bucket_adopted {
                return Err(Error::new(409, "adopted_bucket_protected"));
            }
            let response = request(context, service, "DELETE", "", "", true)?;
            if !(200..300).contains(&response.status) && response.status != 404 {
                return Err(Error::new(502, "bucket_delete_failed"));
            }
            advance(job, "revoke");
        }
        _ => return Err(Error::new(409, "invalid_delete_step")),
    }
    Ok(())
}

pub fn revoke(
    context: &Context<'_, impl Runtime>,
    job: &mut Job,
    service: &Service,
    token: &str,
) -> Result<(), Error> {
    if job.step == "revoke_secret_token" {
        advance(job, "finalize");
        return Ok(());
    }
    match job.step.as_str() {
        "revoke" => {
            if job.options.delete_fos_keys {
                let description = permanent_description(service);
                let inventory = keys(context, token)?;
                if let Some(key) = inventory.iter().find(|v| v["description"] == description) {
                    if key["permission"] != "read-write-objects"
                        || key["buckets"]
                            .as_array()
                            .is_none_or(|v| v.len() != 1 || v[0] != service.fos_bucket)
                    {
                        return Err(Error::new(409, "managed_key_scope_mismatch"));
                    }

                    delete_key(context, token, key, service)?;
                    return Ok(());
                }
            }
            advance(job, "revoke_temp");
        }
        "revoke_temp" => {
            if cleanup_temp(context, service, token)? {
                advance(job, "revoke_secret_ak");
            }
        }
        "revoke_secret_ak"
        | "revoke_secret_sk"
        | "revoke_secret_tmp_ak"
        | "revoke_secret_tmp_sk"
        | "revoke_secret_token" => {
            let (name, next) = match job.step.as_str() {
                "revoke_secret_ak" => (service.fos_access_key_ref.clone(), "revoke_secret_sk"),
                "revoke_secret_sk" => (service.fos_secret_key_ref.clone(), "revoke_secret_tmp_ak"),
                "revoke_secret_tmp_ak" => (temporary_refs(service).0, "revoke_secret_tmp_sk"),
                "revoke_secret_tmp_sk" => (temporary_refs(service).1, "revoke_secret_token"),
                _ => (service.fastly_token_ref.clone(), "finalize"),
            };
            if job.options.delete_fos_keys
                || name.starts_with("fos_tmp_")
                || name.starts_with("fastly_token.")
            {
                let store = config(context.runtime, "secret_store_id")?;
                let (status, _) = context.api_status(
                    "DELETE",
                    &format!("/resources/stores/secret/{store}/secrets/{}", encode(&name)),
                    token,
                    None,
                )?;
                if !(200..300).contains(&status) && status != 404 {
                    return Err(Error::new(502, "secret_delete_failed"));
                }
            }
            advance(job, next);
        }
        _ => return Err(Error::new(409, "invalid_revoke_step")),
    }
    Ok(())
}

pub fn cleanup_token(context: &Context<'_, impl Runtime>, service: &Service) -> Result<(), Error> {
    match context.secret(&service.fastly_token_ref) {
        Ok(token) => {
            let store = config(context.runtime, "secret_store_id")?;
            let (status, _) = context.api_status(
                "DELETE",
                &format!(
                    "/resources/stores/secret/{store}/secrets/{}",
                    encode(&service.fastly_token_ref)
                ),
                &token,
                None,
            )?;
            if !(200..300).contains(&status) && status != 404 {
                return Err(Error::new(502, "secret_delete_failed"));
            }
            Ok(())
        }
        Err(error) if error.code == "credentials_pending" => {
            let token = context.secret("fastly_token")?;
            if secret_metadata(context, &token, &service.fastly_token_ref)? == 404 {
                Ok(())
            } else {
                Err(Error::new(409, "credentials_pending"))
            }
        }
        Err(error) => Err(error),
    }
}
fn scope_prefix(service: &Service) -> String {
    if service.fos_prefix.is_empty() {
        String::new()
    } else {
        format!("{}/", service.fos_prefix.trim_end_matches('/'))
    }
}
#[derive(serde::Deserialize)]
struct ListBucketResult {
    #[serde(rename = "Contents", default)]
    contents: Vec<ListBucketObject>,
}

#[derive(serde::Deserialize)]
struct ListBucketObject {
    #[serde(rename = "Key")]
    key: String,
}

fn parse_first_object_key(xml: &str) -> Result<Option<String>, Error> {
    if !xml.contains("<ListBucketResult") {
        return Err(Error::new(502, "invalid_object_list"));
    }
    let parsed: ListBucketResult =
        quick_xml::de::from_str(xml).map_err(|_| Error::new(502, "invalid_object_list"))?;
    Ok(parsed.contents.into_iter().next().map(|obj| obj.key))
}

#[derive(serde::Serialize)]
#[serde(rename = "CreateBucketConfiguration")]
struct CreateBucketConfiguration<'a> {
    #[serde(rename = "@xmlns")]
    xmlns: &'static str,
    #[serde(rename = "LocationConstraint")]
    location_constraint: &'a str,
}

fn create_bucket_body(region: &str) -> Result<Vec<u8>, Error> {
    quick_xml::se::to_string(&CreateBucketConfiguration {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
        location_constraint: region,
    })
    .map(String::into_bytes)
    .map_err(|_| Error::new(500, "request_encoding_failed"))
}

pub fn signed_headers(request: &FosRequest<'_>, now: u64) -> Result<Vec<(String, String)>, String> {
    let date =
        time::OffsetDateTime::from_unix_timestamp(now.try_into().map_err(|_| "invalid_timestamp")?)
            .map_err(|_| "invalid_timestamp")?;
    let timestamp = date
        .format(
            &time::format_description::parse_borrowed::<2>(
                "[year][month][day]T[hour][minute][second]Z",
            )
            .map_err(|_| "invalid_date_format")?,
        )
        .map_err(|_| "invalid_timestamp")?;
    let host = format!("{}.object.fastlystorage.app", request.region);
    let path = canonical_path(request.bucket, request.key);
    let payload = hex::encode(Sha256::digest(&request.body));
    // Object storage rejects a PUT whose body does not match its signed Content-MD5.
    let content_md5 = (request.method == "PUT").then(|| {
        request.content_md5.map(str::to_owned).unwrap_or_else(|| {
            use base64::Engine;
            use md5::{Digest, Md5};
            base64::engine::general_purpose::STANDARD.encode(Md5::digest(&request.body))
        })
    });
    let mut headers = vec![
        ("host", host.as_str()),
        ("x-amz-content-sha256", payload.as_str()),
        ("x-amz-date", timestamp.as_str()),
    ];
    if let Some(md5) = &content_md5 {
        headers.push(("content-md5", md5));
    }
    let signed = sigv4(&SigV4 {
        method: request.method,
        canonical_uri: &path,
        query: request.query,
        headers: &headers,
        payload_hash: &payload,
        timestamp: &timestamp,
        region: request.region,
        service: "s3",
        secret_key: request.secret_key,
    })?;
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        request.access_key, signed.scope, signed.signed_headers, signed.signature
    );
    let mut out = vec![
        ("host".into(), host),
        ("x-amz-date".into(), timestamp),
        ("x-amz-content-sha256".into(), payload),
        ("authorization".into(), authorization),
    ];
    out.extend(content_md5.map(|md5| ("content-md5".into(), md5)));
    Ok(out)
}

struct SigV4<'a> {
    method: &'a str,
    canonical_uri: &'a str,
    query: &'a str,
    /// Lowercase names with trimmed values; sorted here.
    headers: &'a [(&'a str, &'a str)],
    payload_hash: &'a str,
    /// `YYYYMMDDTHHMMSSZ`.
    timestamp: &'a str,
    region: &'a str,
    service: &'a str,
    secret_key: &'a str,
}

struct SigV4Signed {
    #[cfg_attr(not(test), allow(dead_code))]
    canonical_request: String,
    #[cfg_attr(not(test), allow(dead_code))]
    string_to_sign: String,
    scope: String,
    signed_headers: String,
    signature: String,
}

fn sigv4(input: &SigV4<'_>) -> Result<SigV4Signed, String> {
    let day = input.timestamp.get(..8).ok_or("invalid_timestamp")?;
    let scope = format!("{day}/{}/{}/aws4_request", input.region, input.service);
    let mut headers = input.headers.to_vec();
    headers.sort();
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let mut query: Vec<_> = url::form_urlencoded::parse(input.query.as_bytes())
        .map(|(k, v)| (encode(&k), encode(&v)))
        .collect();
    query.sort();
    let query = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_request = format!(
        "{}\n{}\n{query}\n{canonical_headers}\n{signed_headers}\n{}",
        input.method, input.canonical_uri, input.payload_hash
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        input.timestamp,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let key = hmac(
        format!("AWS4{}", input.secret_key).as_bytes(),
        day.as_bytes(),
    )?;
    let key = hmac(&key, input.region.as_bytes())?;
    let key = hmac(&key, input.service.as_bytes())?;
    let key = hmac(&key, b"aws4_request")?;
    let signature = hex::encode(hmac(&key, string_to_sign.as_bytes())?);
    Ok(SigV4Signed {
        canonical_request,
        string_to_sign,
        scope,
        signed_headers,
        signature,
    })
}
pub fn canonical_path(bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("/{}", encode(bucket))
    } else {
        format!(
            "/{}/{}",
            encode(bucket),
            key.split('/').map(encode).collect::<Vec<_>>().join("/")
        )
    }
}
fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| "invalid_signing_key")?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    #[test]
    fn create_bucket_body_uses_the_s3_namespace_and_location_constraint() {
        assert_eq!(
            super::create_bucket_body("us-west-2").unwrap(),
            br#"<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>"#
        );
    }

    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    /// One case from the AWS SigV4 test suite (awslabs/aws-c-auth
    /// tests/aws-signing-test-suite/v4): AKIDEXAMPLE, us-east-1, service "service",
    /// 2015-08-30T12:36:00Z, header signing.
    fn assert_vector(
        method: &str,
        uri: &str,
        query: &str,
        extra_headers: &[(&str, &str)],
        payload_hash: &str,
        [canonical_request, string_to_sign, signature]: [&str; 3],
    ) {
        let mut headers = vec![
            ("x-amz-date", "20150830T123600Z"),
            ("host", "example.amazonaws.com"),
        ];
        headers.extend_from_slice(extra_headers);
        let signed = super::sigv4(&super::SigV4 {
            method,
            canonical_uri: uri,
            query,
            headers: &headers,
            payload_hash,
            timestamp: "20150830T123600Z",
            region: "us-east-1",
            service: "service",
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        })
        .unwrap();
        assert_eq!(signed.canonical_request, canonical_request);
        assert_eq!(signed.string_to_sign, string_to_sign);
        assert_eq!(signed.signature, signature);
    }

    fn vanilla_canonical(method: &str, uri: &str, query: &str) -> String {
        format!(
            "{method}\n{uri}\n{query}\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\n{EMPTY_SHA256}"
        )
    }

    fn string_to_sign(hash: &str) -> String {
        format!(
            "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/service/aws4_request\n{hash}"
        )
    }

    #[test]
    fn sigv4_get_vanilla() {
        assert_vector(
            "GET",
            "/",
            "",
            &[],
            EMPTY_SHA256,
            [
                &vanilla_canonical("GET", "/", ""),
                &string_to_sign("bb579772317eb040ac9ed261061d46c1f17a8133879d6129b6e1c25292927e63"),
                "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
            ],
        );
    }

    #[test]
    fn sigv4_get_vanilla_query_order_key_case() {
        assert_vector(
            "GET",
            "/",
            "Param2=value2&Param1=value1",
            &[],
            EMPTY_SHA256,
            [
                &vanilla_canonical("GET", "/", "Param1=value1&Param2=value2"),
                &string_to_sign("816cd5b414d056048ba4f7c5386d6e0533120fb1fcfa93762cf0fc39e2cf19e0"),
                "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500",
            ],
        );
    }

    #[test]
    fn sigv4_get_vanilla_query_unreserved() {
        let unreserved = "-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let query = format!("{unreserved}={unreserved}");
        assert_vector(
            "GET",
            "/",
            &query,
            &[],
            EMPTY_SHA256,
            [
                &vanilla_canonical("GET", "/", &query),
                &string_to_sign("c30d4703d9f799439be92736156d47ccfb2d879ddf56f5befa6d1d6aab979177"),
                "9c3e54bfcdf0b19771a7f523ee5669cdf59bc7cc0884027167c21bb143a40197",
            ],
        );
    }

    #[test]
    fn sigv4_get_utf8_path_and_query() {
        let uri = super::canonical_path("\u{1234}", "");
        assert_vector(
            "GET",
            &uri,
            "",
            &[],
            EMPTY_SHA256,
            [
                &vanilla_canonical("GET", "/%E1%88%B4", ""),
                &string_to_sign("2a0a97d02205e45ce2e994789806b19270cfbbb0921b278ccf58f5249ac42102"),
                "8318018e0b0f223aa2bbf98705b62bb787dc9c0e678f255a891fd03141be5d85",
            ],
        );
        assert_vector(
            "GET",
            "/",
            "\u{1234}=bar",
            &[],
            EMPTY_SHA256,
            [
                &vanilla_canonical("GET", "/", "%E1%88%B4=bar"),
                &string_to_sign("eb30c5bed55734080471a834cc727ae56beb50e5f39d1bff6d0d38cb192a7073"),
                "2cdec8eed098649ff3a119c94853b13c643bcf08f8b0a1d91e12c9027818dd04",
            ],
        );
    }

    #[test]
    fn sigv4_post_vanilla() {
        assert_vector(
            "POST",
            "/",
            "",
            &[],
            EMPTY_SHA256,
            [
                &vanilla_canonical("POST", "/", ""),
                &string_to_sign("553f88c9e4d10fc9e109e2aeb65f030801b70c2f6468faca261d401ae622fc87"),
                "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b",
            ],
        );
    }

    #[test]
    fn sigv4_post_x_www_form_urlencoded_with_signed_body() {
        use sha2::Digest;
        let payload = hex::encode(sha2::Sha256::digest(b"Param1=value1"));
        assert_eq!(
            payload,
            "9095672bbd1f56dfc5b65f3e153adc8731a4a654192329106275f4c7b24d0b6e"
        );
        assert_vector(
            "POST",
            "/",
            "",
            &[
                ("content-type", "application/x-www-form-urlencoded"),
                ("content-length", "13"),
                ("x-amz-content-sha256", &payload),
            ],
            &payload,
            [
            &format!(
                "POST\n/\n\ncontent-length:13\ncontent-type:application/x-www-form-urlencoded\nhost:example.amazonaws.com\nx-amz-content-sha256:{payload}\nx-amz-date:20150830T123600Z\n\ncontent-length;content-type;host;x-amz-content-sha256;x-amz-date\n{payload}"
            ),
            &string_to_sign("b1edd1d03544c25390e32085d55b57acc9a3961bb59415ff86c45c3d89d16cfb"),
            "d3875051da38690788ef43de4db0d8f280229d82040bfac253562e56c3f20e0b",
            ],
        );
    }

    #[test]
    fn s3_request_signs_host_payload_hash_and_date() {
        let headers = super::signed_headers(
            &super::FosRequest {
                method: "PUT",
                region: "us-east-1",
                bucket: "my-bucket",
                key: "dir a/\u{1234}.gz",
                query: "prefix=x/y&list-type=2",
                access_key: "AKIDEXAMPLE",
                secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                body: b"hello".to_vec(),
                timeout: std::time::Duration::from_secs(1),
                response_limit: super::super::transport::FOS_RESPONSE_LIMIT,
                content_md5: None,
            },
            1_440_938_160,
        )
        .unwrap();
        let payload = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        assert_eq!(
            headers,
            vec![
                ("host".into(), "us-east-1.object.fastlystorage.app".into()),
                ("x-amz-date".into(), "20150830T123600Z".into()),
                ("x-amz-content-sha256".into(), payload.into()),
                (
                    "authorization".into(),
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request, \
                     SignedHeaders=content-md5;host;x-amz-content-sha256;x-amz-date, \
                     Signature=86249386251fde4d06c87d68f1590bf7793e9c6477473b55b293fe13f896852c"
                        .into()
                ),
                ("content-md5".into(), "XUFAKrxLKna5cZ2REBfFkg==".into()),
            ]
        );
    }
}
