use super::{Error, Role, User, GRANT_TTL, SESSION_TTL};
use cookie::{time::Duration, Cookie, SameSite};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct Claims {
    pub sub: String,
    pub role: Role,
    pub policy_version: u64,
    pub exp: u64,
    #[serde(default)]
    pub iat: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub jti: String,
}

fn next_session_jti_with<E>(
    fill: impl FnOnce(&mut [u8]) -> Result<(), E>,
) -> Result<String, Error> {
    let mut nonce = [0u8; 16];
    fill(&mut nonce).map_err(|_| Error::Internal)?;
    Ok(hex::encode(nonce))
}

fn next_session_jti() -> Result<String, Error> {
    next_session_jti_with(getrandom::getrandom)
}

pub(super) fn issue(
    user: &User,
    policy_version: u64,
    now: u64,
    key: &[u8],
) -> Result<String, Error> {
    let claims = Claims {
        sub: user.id.clone(),
        role: user.role,
        policy_version,
        exp: now.saturating_add(SESSION_TTL),
        iat: now,
        jti: next_session_jti()?,
    };
    compute_common::crypto::seal_json(key, &claims).map_err(|_| Error::InvalidInput)
}

pub(super) fn verify(cookie: &str, key: &[u8], now: u64) -> Result<Claims, Error> {
    if cookie.len() > 4096 {
        return Err(Error::Unauthorized);
    }
    let claims: Claims =
        compute_common::crypto::unseal_json(key, cookie).map_err(|_| Error::Unauthorized)?;
    if claims.exp <= now || claims.exp > now.saturating_add(SESSION_TTL) {
        return Err(Error::Unauthorized);
    }
    Ok(claims)
}

fn build_cookie(name: &str, value: &str, max_age_secs: u64) -> String {
    Cookie::build((name, value))
        .path("/")
        .http_only(true)
        .secure(true)
        .same_site(SameSite::Lax)
        .max_age(Duration::seconds(max_age_secs as i64))
        .build()
        .to_string()
}

pub fn cookie_header(value: &str) -> String {
    build_cookie("flp_session", value, SESSION_TTL)
}
pub fn logout_cookie() -> String {
    build_cookie("flp_session", "", 0)
}
pub fn setup_cookie(value: &str) -> String {
    build_cookie("flp_setup", value, GRANT_TTL)
}
pub fn clear_setup_cookie() -> String {
    build_cookie("flp_setup", "", 0)
}

pub const CDN_COOKIE_TTL: u64 = 180;
pub const CDN_COOKIE_MIN_REMAINING: u64 = 120;
pub const HOST_CDN_COOKIE_NAME: &str = "__Host-flp_cdn";

fn cdn_role(role: Role) -> &'static str {
    match role {
        Role::Admin => "admin",
        Role::Analyst => "analyst",
        Role::AnalystNoPii => "no_pii",
    }
}

pub fn clear_cdn_cookie() -> String {
    build_cookie("flp_cdn", "", 0)
}

pub fn clear_host_cdn_cookie() -> String {
    build_cookie(HOST_CDN_COOKIE_NAME, "", 0)
}

fn sign_cdn_cookie_value(
    service_id: &str,
    scope: &str,
    role: Role,
    session_cookie: &str,
    auth_epoch: u64,
    now: u64,
    cdn_key: &str,
) -> Option<String> {
    if service_id.len() != 22
        || !service_id.bytes().all(|b| b.is_ascii_alphanumeric())
        || session_cookie.is_empty()
        || cdn_key.is_empty()
        || !scope
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'/' | b'-'))
    {
        return None;
    }
    let exp = now.saturating_add(CDN_COOKIE_TTL);
    if exp > 9_999_999_999 {
        return None;
    }
    let session_hash = &compute_common::crypto::sha256_hex(session_cookie.as_bytes())[..16];
    let (payload, msg_prefix) = if auth_epoch == 0 {
        (
            format!(
                "{service_id}~{}~{exp:010}~{session_hash}~{scope}",
                cdn_role(role)
            ),
            "flp-cdn-v1",
        )
    } else {
        (
            format!(
                "{service_id}~{}~{exp:010}~{session_hash}~{auth_epoch}~{scope}",
                cdn_role(role)
            ),
            "flp-cdn-v2",
        )
    };
    let sig = compute_common::crypto::hmac_sha256_hex(
        cdn_key.as_bytes(),
        format!("{msg_prefix}~{payload}").as_bytes(),
    )
    .ok()?;
    Some(format!("{payload}~{sig}"))
}

pub fn issue_cdn_cookie(
    service_id: &str,
    scope: &str,
    role: Role,
    session_cookie: &str,
    auth_epoch: u64,
    now: u64,
    cdn_key: &str,
) -> Option<String> {
    let val = sign_cdn_cookie_value(
        service_id,
        scope,
        role,
        session_cookie,
        auth_epoch,
        now,
        cdn_key,
    )?;
    Some(build_cookie("flp_cdn", &val, CDN_COOKIE_TTL))
}

pub fn issue_cdn_cookies(
    service_id: &str,
    scope: &str,
    role: Role,
    session_cookie: &str,
    auth_epoch: u64,
    now: u64,
    cdn_key: &str,
) -> Option<(String, String)> {
    let val = sign_cdn_cookie_value(
        service_id,
        scope,
        role,
        session_cookie,
        auth_epoch,
        now,
        cdn_key,
    )?;
    Some((
        build_cookie("flp_cdn", &val, CDN_COOKIE_TTL),
        build_cookie(HOST_CDN_COOKIE_NAME, &val, CDN_COOKIE_TTL),
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn cdn_cookie_valid(
    cookie: &str,
    service_id: &str,
    scope: &str,
    role: Role,
    session_cookie: &str,
    auth_epoch: u64,
    now: u64,
    cdn_key: &str,
) -> bool {
    let Some((payload, sig_hex)) = cookie.rsplit_once('~') else {
        return false;
    };
    let parts: Vec<&str> = payload.split('~').collect();
    let (sid, r, exp_str, shash, parsed_epoch, sc, msg_prefix) = match parts.as_slice() {
        [sid, r, exp_str, shash, sc] => (*sid, *r, *exp_str, *shash, 0u64, *sc, "flp-cdn-v1"),
        [sid, r, exp_str, shash, epoch_str, sc] => {
            let Ok(parsed_epoch) = epoch_str.parse::<u64>() else {
                return false;
            };
            if parsed_epoch == 0 {
                return false;
            }
            (*sid, *r, *exp_str, *shash, parsed_epoch, *sc, "flp-cdn-v2")
        }
        _ => return false,
    };
    let Ok(expected_sig) = compute_common::crypto::hmac_sha256_hex(
        cdn_key.as_bytes(),
        format!("{msg_prefix}~{payload}").as_bytes(),
    ) else {
        return false;
    };
    if !compute_common::crypto::constant_time_eq(sig_hex.as_bytes(), expected_sig.as_bytes()) {
        return false;
    }
    let Ok(exp) = exp_str.parse::<u64>() else {
        return false;
    };
    let expected_shash = &compute_common::crypto::sha256_hex(session_cookie.as_bytes())[..16];
    sid == service_id
        && r == cdn_role(role)
        && parsed_epoch == auth_epoch
        && sc == scope
        && shash == expected_shash
        && exp > now.saturating_add(CDN_COOKIE_MIN_REMAINING)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdn_cookie_binds_service_role_scope_session_ttl_and_epoch() {
        let sid = "xa20dYfOsL4MhjIo66mVhh";
        let scope = "goal2-live-20261029/";
        let session = "session-token.signature";
        let cdn_key = "test-cdn-secret";
        let now = 1_762_000_000;
        let header =
            issue_cdn_cookie(sid, scope, Role::Admin, session, 0, now, cdn_key).expect("issued");
        assert!(header.contains("Max-Age=180"));
        let parsed = Cookie::parse(header).expect("cookie");
        assert_eq!(parsed.name(), "flp_cdn");
        let value = parsed.value();
        assert_eq!(value.split('~').count(), 6);
        assert!(cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Admin,
            session,
            0,
            now + 59,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Admin,
            session,
            0,
            now + 61,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Admin,
            session,
            1,
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Analyst,
            session,
            0,
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Admin,
            "other-session",
            0,
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            "other/",
            Role::Admin,
            session,
            0,
            now,
            cdn_key
        ));

        let v2_header =
            issue_cdn_cookie(sid, scope, Role::Analyst, session, 3, now, cdn_key).expect("v2");
        let v2_parsed = Cookie::parse(v2_header).expect("v2 cookie");
        let v2_value = v2_parsed.value();
        assert_eq!(v2_value.split('~').count(), 7);
        assert!(cdn_cookie_valid(
            v2_value,
            sid,
            scope,
            Role::Analyst,
            session,
            3,
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            v2_value,
            sid,
            scope,
            Role::Analyst,
            session,
            4,
            now,
            cdn_key
        ));

        assert!(clear_cdn_cookie().contains("Max-Age=0"));
        let host_clear = clear_host_cdn_cookie();
        assert!(host_clear.starts_with("__Host-flp_cdn=;"));
        assert!(host_clear.contains("Max-Age=0"));
        let setup_clear = clear_setup_cookie();
        assert!(setup_clear.starts_with("flp_setup=;"));
        assert!(setup_clear.contains("Max-Age=0"));
    }

    #[test]
    fn next_session_jti_fails_closed_when_csprng_errors() {
        let err = next_session_jti_with(|_| Err("rng unavailable")).unwrap_err();
        assert_eq!(err, Error::Internal);

        let jti = next_session_jti_with(|buf| -> Result<(), ()> {
            for (i, b) in buf.iter_mut().enumerate() {
                *b = i as u8;
            }
            Ok(())
        })
        .expect("csprng ok");
        assert_eq!(jti, "000102030405060708090a0b0c0d0e0f");
    }
}
