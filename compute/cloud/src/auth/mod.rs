mod session;
pub mod settings;
pub mod webauthn;
pub use session::{
    cdn_cookie_valid, clear_cdn_cookie, cookie_header, issue_cdn_cookie, logout_cookie,
    setup_cookie,
};

use crate::state::{StateError, StateStore};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use subtle::ConstantTimeEq;
pub use webauthn::{BrowserCredential, Credential};

const CHALLENGE_TTL: u64 = 300;
const GRANT_TTL: u64 = 600;
const INVITE_TTL: u64 = 7 * 86400;
pub const SESSION_TTL: u64 = 12 * 3600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Unauthorized,
    Forbidden,
    SetupClosed,
    InvalidCredential,
    CounterRegression,
    ChallengeExpired,
    Conflict,
    InvalidInput,
    NotFound,
    RateLimited { retry_after: u64 },
    Storage,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<StateError> for Error {
    fn from(_: StateError) -> Self {
        Self::Storage
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    #[serde(rename = "admin")]
    Admin,
    #[serde(rename = "analyst")]
    Analyst,
    #[serde(rename = "analyst-no-pii")]
    AnalystNoPii,
}
impl Role {
    pub fn variant(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Analyst => "analyst",
            Self::AnalystNoPii => "analyst_masked",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub display_name: String,
    pub role: Role,
    pub credentials: Vec<Credential>,
    pub revoked: bool,
    pub created_by: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invite {
    pub id: String,
    pub display_name: String,
    pub role: Role,
    pub expires_at: u64,
    pub revoked: bool,
    pub used: bool,
    pub created_by: String,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub actor: String,
    pub action: String,
    pub target: String,
    pub time: u64,
    pub ip: String,
}
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupState {
    pub hash_generation: String,
    pub failures: u32,
    pub spent: bool,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupGrant {
    pub generation: String,
    pub expires_at: u64,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegistrationAuthority {
    Setup {
        grant_hash: String,
        generation: String,
    },
    Invite {
        invite_hash: String,
    },
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Challenge {
    pub challenge: String,
    pub expires_at: u64,
    pub registration: Option<RegistrationAuthority>,
    pub display_name: String,
    pub user_id: String,
}
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateState {
    pub minute: u64,
    pub failures: u32,
    pub challenges: u32,
    pub blocked_until: u64,
}
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AuthState {
    pub users: BTreeMap<String, User>,
    pub invites: BTreeMap<String, Invite>,
    pub challenges: BTreeMap<String, Challenge>,
    pub setup: SetupState,
    #[serde(default)]
    pub retired_setup_generations: BTreeSet<String>,
    pub setup_grants: BTreeMap<String, SetupGrant>,
    pub rates: BTreeMap<String, RateState>,
    pub audit: Vec<AuditEntry>,
    #[serde(default)]
    pub recent_audit: Vec<AuditEntry>,
    #[serde(default)]
    pub audit_offset: u64,
    pub policy_version: u64,
    #[serde(default)]
    pub usage_logging: settings::UsageLogging,
}

#[derive(Clone)]
pub struct AuthConfig {
    pub origin: String,
    pub rp_id: String,
    pub session_key: Vec<u8>,
    pub setup_hash: Option<String>,
    pub setup_expires_at: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupSecret {
    sha256: String,
    expires_at: u64,
}

impl AuthConfig {
    /// Secret Store binds the setup hash and absolute Unix expiry in one JSON value.
    pub fn from_hash(
        origin: &str,
        session_key: Vec<u8>,
        setup_secret: Option<String>,
    ) -> Result<Self, Error> {
        let (setup_hash, setup_expires_at) = match setup_secret {
            Some(secret) if secret.starts_with('{') => {
                let secret: SetupSecret =
                    serde_json::from_str(&secret).map_err(|_| Error::InvalidInput)?;
                (Some(secret.sha256), Some(secret.expires_at))
            }
            secret => (secret, None),
        };
        let parsed = url::Url::parse(origin).map_err(|_| Error::InvalidInput)?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != "/"
            || session_key.len() < 32
            || setup_hash.as_ref().is_some_and(|s| {
                s.len() != 64
                    || !s
                        .bytes()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            })
        {
            return Err(Error::InvalidInput);
        }
        Ok(Self {
            origin: parsed.origin().ascii_serialization(),
            rp_id: parsed.host_str().ok_or(Error::InvalidInput)?.into(),
            session_key,
            setup_hash,
            setup_expires_at,
        })
    }
    pub fn new(
        origin: &str,
        session_key: Vec<u8>,
        setup_code: Option<(&str, u64)>,
    ) -> Result<Self, Error> {
        Self::from_hash(
            origin,
            session_key,
            setup_code.map(|(code, expires_at)| {
                json!({"sha256":hash(code),"expires_at":expires_at}).to_string()
            }),
        )
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Options {
    pub challenge_id: String,
    pub public_key: Value,
}
#[derive(Clone, Debug, Serialize)]
pub struct SignedIn {
    pub user: User,
    pub cookie: String,
}

pub fn encode(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}
pub fn decode(value: &str) -> Result<Vec<u8>, Error> {
    if value.len() > 64 * 1024 {
        return Err(Error::InvalidInput);
    }
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| Error::InvalidCredential)
}
pub fn hash(value: &str) -> String {
    compute_common::crypto::sha256_hex(value.as_bytes())
}

pub struct AuthService<'a, S: StateStore> {
    store: &'a S,
    config: &'a AuthConfig,
}

impl<'a, S: StateStore> AuthService<'a, S> {
    pub fn new(store: &'a S, config: &'a AuthConfig) -> Self {
        Self { store, config }
    }

    fn update<T>(
        &self,
        mut operation: impl FnMut(&mut AuthState) -> Result<T, Error>,
    ) -> Result<T, Error> {
        for _ in 0..3 {
            let mut current = self.store.load()?;
            let original = current.state.clone();
            let result = operation(&mut current.state);
            if current.state == original {
                return result;
            }
            if self
                .store
                .compare_and_swap(current.generation.as_deref(), &current.state)?
            {
                return result;
            }
        }
        Err(Error::Conflict)
    }

    fn sync_setup(&self, state: &mut AuthState, now: u64) -> Result<(), Error> {
        let generation = self
            .config
            .setup_hash
            .as_deref()
            .ok_or(Error::SetupClosed)?;
        if state.setup.hash_generation != generation {
            if state.retired_setup_generations.contains(generation) {
                return Err(Error::SetupClosed);
            }
            if !state.setup.hash_generation.is_empty() {
                state
                    .retired_setup_generations
                    .insert(state.setup.hash_generation.clone());
            }
            state.setup = SetupState {
                hash_generation: generation.into(),
                ..Default::default()
            };
            state.setup_grants.clear();
            state.challenges.retain(|_, c| {
                !matches!(c.registration, Some(RegistrationAuthority::Setup { .. }))
            });
            audit(state, "cli", "setup_generation", "", "", now);
        }
        Ok(())
    }

    pub fn setup_complete(&self) -> Result<bool, Error> {
        let state = self.store.load()?.state;
        if self.config.setup_hash.as_ref().is_some_and(|generation| {
            generation != &state.setup.hash_generation || !state.setup.spent
        }) {
            return Ok(false);
        }
        Ok(Self::has_users(&state))
    }

    pub fn has_users(state: &AuthState) -> bool {
        state
            .users
            .values()
            .any(|u| !u.revoked && u.role == Role::Admin)
    }

    pub fn accept_setup(
        &self,
        code: &str,
        ip: &str,
        now: u64,
        random: [u8; 32],
    ) -> Result<String, Error> {
        let token = encode(&random);
        self.update(|state| {
            self.sync_setup(state, now)?;
            if self
                .config
                .setup_expires_at
                .is_none_or(|expiry| expiry <= now)
                || state.setup.spent
                || state.setup.failures >= 20
            {
                audit(state, "anonymous", "setup_closed", "setup", ip, now);
                return Err(Error::SetupClosed);
            }
            if !bool::from(
                hash(code)
                    .as_bytes()
                    .ct_eq(state.setup.hash_generation.as_bytes()),
            ) {
                state.setup.failures += 1;
                audit(state, "anonymous", "setup_failure", "setup", ip, now);
                return Err(Error::Unauthorized);
            }
            prune(state, now);
            state.setup_grants.insert(
                hash(&token),
                SetupGrant {
                    generation: state.setup.hash_generation.clone(),
                    expires_at: now
                        .saturating_add(GRANT_TTL)
                        .min(self.config.setup_expires_at.ok_or(Error::SetupClosed)?),
                },
            );
            audit(state, "setup", "setup_grant", "setup", ip, now);
            Ok(token.clone())
        })
    }

    pub fn check_setup_grant(&self, token: &str, now: u64) -> Result<(), Error> {
        let state = self.store.load()?.state;
        self.valid_grant(&state, token, now)
    }
    fn valid_grant(&self, state: &AuthState, token: &str, now: u64) -> Result<(), Error> {
        let grant = state
            .setup_grants
            .get(&hash(token))
            .ok_or(Error::Unauthorized)?;
        if self
            .config
            .setup_expires_at
            .is_none_or(|expiry| expiry <= now)
            || grant.expires_at <= now
            || state.setup.spent
            || state.setup.failures >= 20
            || Some(&grant.generation) != self.config.setup_hash.as_ref()
        {
            return Err(Error::Unauthorized);
        }
        Ok(())
    }

    pub fn registration_options(
        &self,
        grant: Option<&str>,
        invite_token: Option<&str>,
        display_name: &str,
        ip: &str,
        now: u64,
        random: [u8; 32],
    ) -> Result<Options, Error> {
        if display_name.trim().is_empty() || display_name.len() > 200 {
            return Err(Error::InvalidInput);
        }
        let challenge_id = encode(&random);
        let challenge = encode(&Sha256::digest([b"challenge".as_slice(), &random].concat()));
        let user_id = encode(&Sha256::digest([b"user".as_slice(), &random].concat()));
        self.update(|state| {
            self.sync_setup(state, now)?;
            let authority = match (grant, invite_token) {
                (Some(token), None) => {
                    self.valid_grant(state, token, now)?;
                    RegistrationAuthority::Setup { grant_hash: hash(token), generation: state.setup.hash_generation.clone() }
                }
                (None, Some(token)) => {
                    valid_invite(state, &hash(token), now)?;
                    RegistrationAuthority::Invite { invite_hash: hash(token) }
                }
                _ => return Err(Error::Unauthorized),
            };
            prune(state, now);
            if state.challenges.contains_key(&challenge_id) { return Err(Error::Conflict); }
            state.challenges.insert(challenge_id.clone(), Challenge { challenge: challenge.clone(), expires_at: now.saturating_add(CHALLENGE_TTL), registration: Some(authority), display_name: display_name.trim().into(), user_id: user_id.clone() });
            audit(state, "anonymous", "registration_challenge", &challenge_id, ip, now);
            Ok(Options { challenge_id: challenge_id.clone(), public_key: json!({
                "challenge":challenge, "rp":{"name":"Fastly Log Analytics","id":self.config.rp_id},
                "user":{"id":user_id,"name":display_name,"displayName":display_name},
                "pubKeyCredParams":[{"type":"public-key","alg":-7},{"type":"public-key","alg":-8}],
                "timeout":300000,"attestation":"none",
                "authenticatorSelection":{"residentKey":"required","requireResidentKey":true,"userVerification":"required"}
            })})
        })
    }

    pub fn finish_registration(
        &self,
        challenge_id: &str,
        input: &BrowserCredential,
        ip: &str,
        now: u64,
    ) -> Result<SignedIn, Error> {
        self.update(|state| {
            self.sync_setup(state, now)?;
            let challenge = state
                .challenges
                .remove(challenge_id)
                .ok_or(Error::Unauthorized)?;
            audit(
                state,
                "anonymous",
                "registration_attempt",
                challenge_id,
                ip,
                now,
            );
            if challenge.expires_at <= now {
                return Err(Error::ChallengeExpired);
            }
            let authority = challenge.registration.ok_or(Error::Unauthorized)?;
            let (role, name, created_by) = match &authority {
                RegistrationAuthority::Setup {
                    grant_hash,
                    generation,
                } => {
                    let grant = state
                        .setup_grants
                        .get(grant_hash)
                        .ok_or(Error::Unauthorized)?;
                    if self
                        .config
                        .setup_expires_at
                        .is_none_or(|expiry| expiry <= now)
                        || grant.expires_at <= now
                        || state.setup.spent
                        || state.setup.failures >= 20
                        || generation != &state.setup.hash_generation
                        || Some(generation) != self.config.setup_hash.as_ref()
                    {
                        return Err(Error::Unauthorized);
                    }
                    (Role::Admin, challenge.display_name.clone(), "setup".into())
                }
                RegistrationAuthority::Invite { invite_hash } => {
                    let invite = valid_invite(state, invite_hash, now)?;
                    (
                        invite.role,
                        invite.display_name.clone(),
                        invite.created_by.clone(),
                    )
                }
            };
            let credential = webauthn::register(
                input,
                &challenge.challenge,
                &self.config.origin,
                &self.config.rp_id,
            )?;
            if state
                .users
                .values()
                .any(|u| u.credentials.iter().any(|c| c.id == credential.id))
            {
                return Err(Error::Conflict);
            }
            let user = User {
                id: challenge.user_id,
                display_name: name,
                role,
                credentials: vec![credential],
                revoked: false,
                created_by,
                created_at: now,
            };
            match authority {
                RegistrationAuthority::Setup { .. } => {
                    state.setup.spent = true;
                    state.setup_grants.clear();
                }
                RegistrationAuthority::Invite { invite_hash } => {
                    state
                        .invites
                        .get_mut(&invite_hash)
                        .ok_or(Error::Unauthorized)?
                        .used = true;
                }
            }
            state.users.insert(user.id.clone(), user.clone());
            audit(state, &user.id, "user_registered", &user.id, ip, now);
            self.signed_in(user, state.policy_version, now)
        })
    }

    pub fn login_options(&self, ip: &str, now: u64, random: [u8; 32]) -> Result<Options, Error> {
        let id = encode(&random);
        self.update(|state| {
            prune(state, now);
            if state.challenges.contains_key(&id) {return Err(Error::Conflict);}
            state.challenges.insert(id.clone(), Challenge {challenge:id.clone(),expires_at:now.saturating_add(CHALLENGE_TTL),registration:None,display_name:String::new(),user_id:String::new()});
            audit(state,"anonymous","login_challenge",&id,ip,now);
            Ok(Options {challenge_id:id.clone(), public_key:json!({"challenge":id,"rpId":self.config.rp_id,"timeout":300000,"userVerification":"required"})})
        })
    }

    pub fn finish_login(
        &self,
        challenge_id: &str,
        input: &BrowserCredential,
        ip: &str,
        now: u64,
    ) -> Result<SignedIn, Error> {
        self.update(|state| {
            let challenge = state
                .challenges
                .remove(challenge_id)
                .ok_or(Error::Unauthorized)?;
            audit(state, "anonymous", "login_attempt", challenge_id, ip, now);
            if challenge.registration.is_some() {
                return Err(Error::Unauthorized);
            }
            if challenge.expires_at <= now {
                return Err(Error::ChallengeExpired);
            }
            let user = state
                .users
                .values_mut()
                .find(|u| {
                    !u.revoked && u.credentials.iter().any(|c| !c.revoked && c.id == input.id)
                })
                .ok_or(Error::Unauthorized)?;
            if let Some(handle) = &input.response.user_handle {
                if handle != &user.id {
                    return Err(Error::Unauthorized);
                }
            }
            let credential = user
                .credentials
                .iter_mut()
                .find(|c| c.id == input.id)
                .ok_or(Error::Unauthorized)?;
            credential.sign_count = webauthn::assert(
                input,
                credential,
                &challenge.challenge,
                &self.config.origin,
                &self.config.rp_id,
            )?;
            let user = user.clone();
            audit(state, &user.id, "login", &user.id, ip, now);
            self.signed_in(user, state.policy_version, now)
        })
    }

    pub fn authenticate(&self, cookie: &str, now: u64) -> Result<User, Error> {
        self.authenticate_with_settings(cookie, now)
            .map(|(user, _)| user)
    }

    pub fn authenticate_with_settings(
        &self,
        cookie: &str,
        now: u64,
    ) -> Result<(User, settings::UsageLogging), Error> {
        let claims = session::verify(cookie, &self.config.session_key, now)?;
        let materialized = self
            .store
            .load_user(&claims.sub)?
            .ok_or(Error::Unauthorized)?;
        let state = self.store.load()?.state;
        let user = state
            .users
            .get(&claims.sub)
            .filter(|u| !u.revoked && u.credentials.iter().any(|c| !c.revoked))
            .ok_or(Error::Unauthorized)?;
        if &materialized != user {
            return Err(Error::Unauthorized);
        }
        if claims.policy_version != state.policy_version {
            return Err(Error::Unauthorized);
        }
        Ok((materialized, state.usage_logging))
    }
    fn signed_in(&self, user: User, policy_version: u64, now: u64) -> Result<SignedIn, Error> {
        let cookie = session::issue(&user, policy_version, now, &self.config.session_key)?;
        Ok(SignedIn { user, cookie })
    }

    pub fn users(&self, actor: &str) -> Result<Vec<User>, Error> {
        let state = self.store.load()?.state;
        admin(&state, actor)?;
        Ok(state.users.values().cloned().collect())
    }
    pub fn invites(&self, actor: &str) -> Result<Vec<Invite>, Error> {
        let state = self.store.load()?.state;
        admin(&state, actor)?;
        Ok(state
            .invites
            .values()
            .filter(|i| !i.revoked)
            .cloned()
            .collect())
    }
    pub fn audit_entries(&self) -> Result<Vec<AuditEntry>, Error> {
        let state = self.store.load()?.state;
        let mut entries: Vec<AuditEntry> =
            state.recent_audit.into_iter().chain(state.audit).collect();
        if entries.len() > 50 {
            let drain_count = entries.len() - 50;
            entries.drain(..drain_count);
        }
        entries.reverse();
        Ok(entries)
    }
    pub fn create_invite(
        &self,
        actor: &str,
        display_name: &str,
        role: Role,
        ip: &str,
        now: u64,
        random: [u8; 32],
    ) -> Result<(Invite, String), Error> {
        if display_name.trim().is_empty() || display_name.len() > 200 {
            return Err(Error::InvalidInput);
        }
        let token = encode(&random);
        let id = hash(&token);
        self.update(|state| {
            admin(state, actor)?;
            prune(state, now);
            if state.invites.contains_key(&id) {
                return Err(Error::Conflict);
            }
            let invite = Invite {
                id: id.clone(),
                display_name: display_name.trim().into(),
                role,
                expires_at: now.saturating_add(INVITE_TTL),
                revoked: false,
                used: false,
                created_by: actor.into(),
            };
            state.invites.insert(id.clone(), invite.clone());
            audit(state, actor, "invite_created", &id, ip, now);
            Ok((invite, token.clone()))
        })
    }
    pub fn revoke_invite(&self, actor: &str, id: &str, ip: &str, now: u64) -> Result<(), Error> {
        self.update(|state| {
            admin(state, actor)?;
            prune(state, now);
            if state.invites.get(id).ok_or(Error::NotFound)?.used {
                return Err(Error::Conflict);
            }
            state.invites.remove(id);
            audit(state, actor, "invite_revoked", id, ip, now);
            Ok(())
        })
    }
    pub fn set_role(
        &self,
        actor: &str,
        target: &str,
        role: Role,
        ip: &str,
        now: u64,
    ) -> Result<(), Error> {
        if role != Role::Admin {
            self.ensure_admin_survives(actor, target, None)?;
        }
        self.update(|state| {
            admin(state, actor)?;
            if actor == target {
                return Err(Error::Forbidden);
            }
            protect_last_admin(state, target, role != Role::Admin)?;
            state.users.get_mut(target).ok_or(Error::NotFound)?.role = role;
            audit(state, actor, "role_changed", target, ip, now);
            Ok(())
        })
    }
    pub fn revoke_user(&self, actor: &str, target: &str, ip: &str, now: u64) -> Result<(), Error> {
        self.ensure_admin_survives(actor, target, None)?;
        self.update(|state| {
            admin(state, actor)?;
            protect_last_admin(state, target, true)?;
            state.users.get_mut(target).ok_or(Error::NotFound)?.revoked = true;
            audit(state, actor, "user_revoked", target, ip, now);
            Ok(())
        })
    }
    pub fn revoke_credential(
        &self,
        actor: &str,
        target: &str,
        credential_id: &str,
        ip: &str,
        now: u64,
    ) -> Result<(), Error> {
        self.ensure_admin_survives(actor, target, Some(credential_id))?;
        self.update(|state| {
            admin(state, actor)?;
            let user = state.users.get(target).ok_or(Error::NotFound)?;
            if user.credentials.iter().filter(|c| !c.revoked).count() <= 1 {
                protect_last_admin(state, target, true)?;
            }
            state
                .users
                .get_mut(target)
                .ok_or(Error::NotFound)?
                .credentials
                .iter_mut()
                .find(|c| c.id == credential_id)
                .ok_or(Error::NotFound)?
                .revoked = true;
            audit(state, actor, "credential_revoked", target, ip, now);
            Ok(())
        })
    }
    fn ensure_admin_survives(
        &self,
        actor: &str,
        target: &str,
        credential: Option<&str>,
    ) -> Result<(), Error> {
        let state = self.store.load()?.state;
        admin(&state, actor)?;
        let user = state.users.get(target).ok_or(Error::NotFound)?;
        if user.role != Role::Admin
            || user.revoked
            || (credential.is_some() && user.credentials.iter().filter(|c| !c.revoked).count() > 1)
        {
            return Ok(());
        }
        for candidate in state
            .users
            .values()
            .filter(|u| u.id != target && u.role == Role::Admin && !u.revoked)
        {
            if self.store.load_user(&candidate.id)?.is_some_and(|u| {
                u.role == Role::Admin && !u.revoked && u.credentials.iter().any(|c| !c.revoked)
            }) {
                return Ok(());
            }
        }
        Err(Error::Forbidden)
    }

    pub fn check_rate_limit(&self, ip: &str, challenge: bool, now: u64) -> Result<(), Error> {
        if !challenge {
            let state = self.store.load()?.state;
            return match state.rates.get(&hash(ip)) {
                Some(rate) if rate.blocked_until > now => Err(Error::RateLimited {
                    retry_after: rate.blocked_until - now,
                }),
                _ => Ok(()),
            };
        }
        self.update(|state| {
            prune(state, now);
            let rate = state.rates.entry(hash(ip)).or_default();
            if rate.blocked_until > now {
                return Err(Error::RateLimited {
                    retry_after: rate.blocked_until - now,
                });
            }
            if rate.minute != now / 60 {
                rate.minute = now / 60;
                rate.failures = 0;
                rate.challenges = 0;
            }
            if challenge {
                if rate.challenges >= 30 {
                    return Err(Error::RateLimited {
                        retry_after: 60 - now % 60,
                    });
                }
                rate.challenges += 1;
                audit(state, "anonymous", "challenge_rate", ip, ip, now);
            }
            Ok(())
        })
    }
    pub fn record_failure(
        &self,
        ip: &str,
        endpoint: &str,
        reason: &str,
        now: u64,
    ) -> Result<(), Error> {
        // Only constant reason/endpoint identifiers belong here, never submitted values.
        if endpoint.len() > 100 || reason.len() > 100 {
            return Err(Error::InvalidInput);
        }
        self.update(|state| {
            let rate = state.rates.entry(hash(ip)).or_default();
            if rate.minute != now / 60 {
                rate.minute = now / 60;
                rate.failures = 0;
                rate.challenges = 0;
            }
            rate.failures = rate.failures.saturating_add(1);
            if rate.failures >= 10 {
                rate.blocked_until = now.saturating_add(900);
            }
            audit(state, "anonymous", reason, endpoint, ip, now);
            Ok(())
        })
    }
}

fn admin(state: &AuthState, actor: &str) -> Result<(), Error> {
    state
        .users
        .get(actor)
        .filter(|u| !u.revoked && u.role == Role::Admin && u.credentials.iter().any(|c| !c.revoked))
        .map(|_| ())
        .ok_or(Error::Forbidden)
}
fn protect_last_admin(state: &AuthState, target: &str, removes: bool) -> Result<(), Error> {
    let user = state.users.get(target).ok_or(Error::NotFound)?;
    if removes
        && !user.revoked
        && user.role == Role::Admin
        && state
            .users
            .values()
            .filter(|u| {
                !u.revoked && u.role == Role::Admin && u.credentials.iter().any(|c| !c.revoked)
            })
            .count()
            <= 1
    {
        return Err(Error::Forbidden);
    }
    Ok(())
}
fn valid_invite<'a>(state: &'a AuthState, id: &str, now: u64) -> Result<&'a Invite, Error> {
    state
        .invites
        .get(id)
        .filter(|i| !i.revoked && !i.used && i.expires_at > now)
        .ok_or(Error::Unauthorized)
}
fn audit(state: &mut AuthState, actor: &str, action: &str, target: &str, ip: &str, now: u64) {
    state.audit.push(AuditEntry {
        actor: actor.into(),
        action: action.into(),
        target: target.into(),
        time: now,
        ip: ip.into(),
    });
}
fn prune(state: &mut AuthState, now: u64) {
    state
        .invites
        .retain(|_, i| i.expires_at > now && !i.revoked);
    state.challenges.retain(|_, c| c.expires_at > now);
    state.setup_grants.retain(|_, g| g.expires_at > now);
    state
        .rates
        .retain(|_, r| r.blocked_until > now || r.minute == now / 60);
}
