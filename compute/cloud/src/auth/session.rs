use super::{Error, Role, User, GRANT_TTL, SESSION_TTL};
use cookie::{time::Duration, Cookie, SameSite};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct Claims {
    pub sub: String,
    pub role: Role,
    pub policy_version: u64,
    pub exp: u64,
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

pub const CDN_COOKIE_TTL: u64 = 900;

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

pub fn issue_cdn_cookie(
    service_id: &str,
    scope: &str,
    role: Role,
    session_cookie: &str,
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
    let payload = format!(
        "{service_id}~{}~{exp:010}~{session_hash}~{scope}",
        cdn_role(role)
    );
    let sig = compute_common::crypto::hmac_sha256_hex(
        cdn_key.as_bytes(),
        format!("flp-cdn-v1~{payload}").as_bytes(),
    )
    .ok()?;
    Some(build_cookie(
        "flp_cdn",
        &format!("{payload}~{sig}"),
        CDN_COOKIE_TTL,
    ))
}

pub fn cdn_cookie_valid(
    cookie: &str,
    service_id: &str,
    scope: &str,
    role: Role,
    session_cookie: &str,
    now: u64,
    cdn_key: &str,
) -> bool {
    let Some((payload, sig_hex)) = cookie.rsplit_once('~') else {
        return false;
    };
    let Ok(expected_sig) = compute_common::crypto::hmac_sha256_hex(
        cdn_key.as_bytes(),
        format!("flp-cdn-v1~{payload}").as_bytes(),
    ) else {
        return false;
    };
    if !compute_common::crypto::constant_time_eq(sig_hex.as_bytes(), expected_sig.as_bytes()) {
        return false;
    }
    let mut parts = payload.splitn(5, '~');
    let (Some(sid), Some(r), Some(exp_str), Some(shash), Some(sc)) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return false;
    };
    let Ok(exp) = exp_str.parse::<u64>() else {
        return false;
    };
    let expected_shash = &compute_common::crypto::sha256_hex(session_cookie.as_bytes())[..16];
    sid == service_id
        && r == cdn_role(role)
        && sc == scope
        && shash == expected_shash
        && exp > now.saturating_add(600)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdn_cookie_binds_service_role_scope_and_session() {
        let sid = "xa20dYfOsL4MhjIo66mVhh";
        let scope = "goal2-live-20261029/";
        let session = "session-token.signature";
        let cdn_key = "test-cdn-secret";
        let now = 1_762_000_000;
        let header =
            issue_cdn_cookie(sid, scope, Role::Admin, session, now, cdn_key).expect("issued");
        let parsed = Cookie::parse(header).expect("cookie");
        assert_eq!(parsed.name(), "flp_cdn");
        let value = parsed.value();
        assert!(cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Admin,
            session,
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Analyst,
            session,
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Admin,
            "other-session",
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            "other/",
            Role::Admin,
            session,
            now,
            cdn_key
        ));
        assert!(!cdn_cookie_valid(
            value,
            sid,
            scope,
            Role::Admin,
            session,
            now + 301,
            cdn_key
        ));
        assert!(clear_cdn_cookie().contains("Max-Age=0"));
    }
}
