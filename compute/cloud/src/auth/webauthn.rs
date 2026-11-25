use super::{decode, encode, Error};
use ciborium::value::Value;
use coset::{iana, iana::EnumI64, Algorithm, CborSerializable, CoseKey, KeyType, Label};
use p256::ecdsa::signature::Verifier;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Cursor;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credential {
    pub id: String,
    pub public_key: Vec<u8>,
    pub sign_count: u32,
    pub revoked: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserCredential {
    pub id: String,
    pub raw_id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub response: BrowserResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserResponse {
    #[serde(rename = "clientDataJSON")]
    pub client_data_json: String,
    pub attestation_object: Option<String>,
    pub authenticator_data: Option<String>,
    pub signature: Option<String>,
    pub user_handle: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientData {
    #[serde(rename = "type")]
    kind: String,
    challenge: String,
    origin: String,
    #[serde(default)]
    cross_origin: bool,
}

fn client_data(
    input: &BrowserCredential,
    challenge: &str,
    origin: &str,
    kind: &str,
) -> Result<Vec<u8>, Error> {
    if input.kind != "public-key"
        || input.id != input.raw_id
        || decode(&input.id)?.is_empty()
        || input.id.len() > 1400
    {
        return Err(Error::InvalidCredential);
    }
    let bytes = decode(&input.response.client_data_json)?;
    let data: ClientData = serde_json::from_slice(&bytes).map_err(|_| Error::InvalidCredential)?;
    if data.kind != kind
        || data.challenge != challenge
        || data.origin != origin
        || data.cross_origin
    {
        return Err(Error::InvalidCredential);
    }
    Ok(bytes)
}

fn auth_data(bytes: &[u8], rp_id: &str, registration: bool) -> Result<u32, Error> {
    if bytes.len() < 37
        || bytes[..32] != Sha256::digest(rp_id.as_bytes())[..]
        || bytes[32] & 0x05 != 0x05
        || (bytes[32] & 0x10 != 0 && bytes[32] & 0x08 == 0)
        || (registration && bytes[32] & 0x40 == 0)
        || (!registration && bytes[32] & 0x40 != 0)
    {
        return Err(Error::InvalidCredential);
    }
    Ok(u32::from_be_bytes(
        bytes[33..37]
            .try_into()
            .map_err(|_| Error::InvalidCredential)?,
    ))
}

fn map_get<'a>(value: &'a Value, key: &str) -> Result<&'a Value, Error> {
    let entries = value.as_map().ok_or(Error::InvalidCredential)?;
    let values: Vec<_> = entries
        .iter()
        .filter(|(k, _)| k.as_text() == Some(key))
        .collect();
    if values.len() != 1 {
        return Err(Error::InvalidCredential);
    }
    Ok(&values[0].1)
}

fn param(key: &CoseKey, label: i64) -> Result<&Value, Error> {
    let mut matches = key
        .params
        .iter()
        .filter(|(k, _)| *k == Label::Int(label))
        .map(|(_, v)| v);
    let first = matches.next().ok_or(Error::InvalidCredential)?;
    if matches.next().is_some() {
        return Err(Error::InvalidCredential);
    }
    Ok(first)
}

fn integer(value: &Value) -> Result<i128, Error> {
    value
        .as_integer()
        .map(i128::from)
        .ok_or(Error::InvalidCredential)
}

/// Maximum CBOR byte length accepted before decoding.
const MAX_CBOR_BYTES: usize = 64 * 1024;
/// Maximum CBOR container/tag nesting depth (legitimate WebAuthn payloads need <= 6).
const MAX_CBOR_DEPTH: usize = 32;
/// Maximum total CBOR data items/chunks accepted in a single decode.
const MAX_CBOR_ITEMS: usize = 4096;

#[derive(Clone, Copy)]
enum CborFrame {
    Definite(u64),
    IndefArray,
    IndefMap { expect_val: bool },
    IndefBytes,
    IndefText,
    Tag,
}

fn read_cbor_arg(bytes: &[u8], pos: &mut usize, ai: u8) -> Result<Option<u64>, Error> {
    match ai {
        0..=23 => Ok(Some(u64::from(ai))),
        24 => {
            let b = *bytes.get(*pos).ok_or(Error::InvalidCredential)?;
            *pos += 1;
            Ok(Some(u64::from(b)))
        }
        25 => {
            let slice: [u8; 2] = bytes
                .get(*pos..*pos + 2)
                .ok_or(Error::InvalidCredential)?
                .try_into()
                .map_err(|_| Error::InvalidCredential)?;
            *pos += 2;
            Ok(Some(u64::from(u16::from_be_bytes(slice))))
        }
        26 => {
            let slice: [u8; 4] = bytes
                .get(*pos..*pos + 4)
                .ok_or(Error::InvalidCredential)?
                .try_into()
                .map_err(|_| Error::InvalidCredential)?;
            *pos += 4;
            Ok(Some(u64::from(u32::from_be_bytes(slice))))
        }
        27 => {
            let slice: [u8; 8] = bytes
                .get(*pos..*pos + 8)
                .ok_or(Error::InvalidCredential)?
                .try_into()
                .map_err(|_| Error::InvalidCredential)?;
            *pos += 8;
            Ok(Some(u64::from_be_bytes(slice)))
        }
        31 => Ok(None),
        _ => Err(Error::InvalidCredential),
    }
}

fn finish_cbor_item(stack: &mut Vec<CborFrame>, remaining_top: &mut usize) {
    loop {
        match stack.last_mut() {
            None => {
                *remaining_top = remaining_top.saturating_sub(1);
                return;
            }
            Some(CborFrame::Tag) => {
                stack.pop();
            }
            Some(CborFrame::Definite(rem)) => {
                *rem -= 1;
                if *rem == 0 {
                    stack.pop();
                } else {
                    return;
                }
            }
            Some(CborFrame::IndefArray | CborFrame::IndefBytes | CborFrame::IndefText) => {
                return;
            }
            Some(CborFrame::IndefMap { expect_val }) => {
                *expect_val = !*expect_val;
                return;
            }
        }
    }
}

/// Iterative O(N) CBOR byte-stream pre-check enforcing bounded byte length,
/// nesting depth, and item count BEFORE invoking recursive CBOR decoders.
fn validate_cbor_prefix(bytes: &[u8], require_eof: bool) -> Result<(), Error> {
    if bytes.is_empty() || bytes.len() > MAX_CBOR_BYTES {
        return Err(Error::InvalidCredential);
    }
    let mut pos = 0usize;
    let mut stack: Vec<CborFrame> = Vec::new();
    let mut remaining_top = 1usize;
    let mut items = 0usize;

    while remaining_top > 0 || !stack.is_empty() {
        let initial = *bytes.get(pos).ok_or(Error::InvalidCredential)?;
        pos += 1;
        items += 1;
        if items > MAX_CBOR_ITEMS {
            return Err(Error::InvalidCredential);
        }

        if initial == 0xff {
            match stack.pop() {
                Some(
                    CborFrame::IndefArray
                    | CborFrame::IndefBytes
                    | CborFrame::IndefText
                    | CborFrame::IndefMap { expect_val: false },
                ) => {
                    finish_cbor_item(&mut stack, &mut remaining_top);
                    continue;
                }
                _ => return Err(Error::InvalidCredential),
            }
        }

        let major = initial >> 5;
        let ai = initial & 0x1f;

        if let Some(CborFrame::IndefBytes) = stack.last() {
            if major != 2 || ai == 31 {
                return Err(Error::InvalidCredential);
            }
            let len = read_cbor_arg(bytes, &mut pos, ai)?.ok_or(Error::InvalidCredential)?;
            if len > (bytes.len() - pos) as u64 {
                return Err(Error::InvalidCredential);
            }
            pos += len as usize;
            continue;
        }
        if let Some(CborFrame::IndefText) = stack.last() {
            if major != 3 || ai == 31 {
                return Err(Error::InvalidCredential);
            }
            let len = read_cbor_arg(bytes, &mut pos, ai)?.ok_or(Error::InvalidCredential)?;
            if len > (bytes.len() - pos) as u64 {
                return Err(Error::InvalidCredential);
            }
            pos += len as usize;
            continue;
        }

        match major {
            0 | 1 => {
                read_cbor_arg(bytes, &mut pos, ai)?.ok_or(Error::InvalidCredential)?;
                finish_cbor_item(&mut stack, &mut remaining_top);
            }
            2 | 3 => match read_cbor_arg(bytes, &mut pos, ai)? {
                Some(len) => {
                    if len > (bytes.len() - pos) as u64 {
                        return Err(Error::InvalidCredential);
                    }
                    pos += len as usize;
                    finish_cbor_item(&mut stack, &mut remaining_top);
                }
                None => {
                    if stack.len() >= MAX_CBOR_DEPTH {
                        return Err(Error::InvalidCredential);
                    }
                    stack.push(if major == 2 {
                        CborFrame::IndefBytes
                    } else {
                        CborFrame::IndefText
                    });
                }
            },
            4 => match read_cbor_arg(bytes, &mut pos, ai)? {
                Some(0) => finish_cbor_item(&mut stack, &mut remaining_top),
                Some(n) => {
                    if n > (bytes.len() - pos) as u64 || stack.len() >= MAX_CBOR_DEPTH {
                        return Err(Error::InvalidCredential);
                    }
                    stack.push(CborFrame::Definite(n));
                }
                None => {
                    if stack.len() >= MAX_CBOR_DEPTH {
                        return Err(Error::InvalidCredential);
                    }
                    stack.push(CborFrame::IndefArray);
                }
            },
            5 => match read_cbor_arg(bytes, &mut pos, ai)? {
                Some(0) => finish_cbor_item(&mut stack, &mut remaining_top),
                Some(n) => {
                    let entries = n.checked_mul(2).ok_or(Error::InvalidCredential)?;
                    if entries > (bytes.len() - pos) as u64 || stack.len() >= MAX_CBOR_DEPTH {
                        return Err(Error::InvalidCredential);
                    }
                    stack.push(CborFrame::Definite(entries));
                }
                None => {
                    if stack.len() >= MAX_CBOR_DEPTH {
                        return Err(Error::InvalidCredential);
                    }
                    stack.push(CborFrame::IndefMap { expect_val: false });
                }
            },
            6 => {
                read_cbor_arg(bytes, &mut pos, ai)?.ok_or(Error::InvalidCredential)?;
                if stack.len() >= MAX_CBOR_DEPTH {
                    return Err(Error::InvalidCredential);
                }
                stack.push(CborFrame::Tag);
            }
            7 => {
                let extra = match ai {
                    0..=23 => 0usize,
                    24 => 1,
                    25 => 2,
                    26 => 4,
                    27 => 8,
                    _ => return Err(Error::InvalidCredential),
                };
                if extra > bytes.len() - pos {
                    return Err(Error::InvalidCredential);
                }
                pos += extra;
                finish_cbor_item(&mut stack, &mut remaining_top);
            }
            _ => return Err(Error::InvalidCredential),
        }
    }

    if require_eof && pos != bytes.len() {
        return Err(Error::InvalidCredential);
    }
    Ok(())
}

fn cbor(bytes: &[u8]) -> Result<Value, Error> {
    validate_cbor_prefix(bytes, true)?;
    let mut cursor = Cursor::new(bytes);
    let value = ciborium::from_reader(&mut cursor).map_err(|_| Error::InvalidCredential)?;
    if cursor.position() != bytes.len() as u64 {
        return Err(Error::InvalidCredential);
    }
    Ok(value)
}

enum VerifyingKey {
    Es256(p256::ecdsa::VerifyingKey),
    Ed25519(ed25519_dalek::VerifyingKey),
}

impl VerifyingKey {
    fn verify(&self, message: &[u8], signature: &[u8]) -> Result<(), Error> {
        match self {
            Self::Es256(verifying) => {
                let sig = p256::ecdsa::Signature::from_der(signature)
                    .map_err(|_| Error::InvalidCredential)?;
                verifying
                    .verify(message, &sig)
                    .map_err(|_| Error::InvalidCredential)
            }
            Self::Ed25519(verifying) => {
                let sig = ed25519_dalek::Signature::from_slice(signature)
                    .map_err(|_| Error::InvalidCredential)?;
                verifying
                    .verify_strict(message, &sig)
                    .map_err(|_| Error::InvalidCredential)
            }
        }
    }
}

fn parse_cose_key(bytes: &[u8]) -> Result<(VerifyingKey, i128), Error> {
    validate_cbor_prefix(bytes, true)?;
    let key = CoseKey::from_slice(bytes).map_err(|_| Error::InvalidCredential)?;
    match (&key.kty, &key.alg) {
        (
            KeyType::Assigned(iana::KeyType::EC2),
            Some(Algorithm::Assigned(iana::Algorithm::ES256)),
        ) if integer(param(&key, iana::Ec2KeyParameter::Crv.to_i64())?)?
            == i128::from(iana::EllipticCurve::P_256.to_i64()) =>
        {
            let mut point = vec![4];
            for label in [
                iana::Ec2KeyParameter::X.to_i64(),
                iana::Ec2KeyParameter::Y.to_i64(),
            ] {
                let part = param(&key, label)?
                    .as_bytes()
                    .ok_or(Error::InvalidCredential)?;
                if part.len() != 32 {
                    return Err(Error::InvalidCredential);
                }
                point.extend(part);
            }
            let verifying = p256::ecdsa::VerifyingKey::from_sec1_bytes(&point)
                .map_err(|_| Error::InvalidCredential)?;
            Ok((
                VerifyingKey::Es256(verifying),
                i128::from(iana::Algorithm::ES256.to_i64()),
            ))
        }
        (
            KeyType::Assigned(iana::KeyType::OKP),
            Some(Algorithm::Assigned(iana::Algorithm::EdDSA)),
        ) if integer(param(&key, iana::OkpKeyParameter::Crv.to_i64())?)?
            == i128::from(iana::EllipticCurve::Ed25519.to_i64()) =>
        {
            let x: &[u8; 32] = param(&key, iana::OkpKeyParameter::X.to_i64())?
                .as_bytes()
                .ok_or(Error::InvalidCredential)?
                .as_slice()
                .try_into()
                .map_err(|_| Error::InvalidCredential)?;
            let verifying =
                ed25519_dalek::VerifyingKey::from_bytes(x).map_err(|_| Error::InvalidCredential)?;
            if verifying.is_weak() {
                return Err(Error::InvalidCredential);
            }
            Ok((
                VerifyingKey::Ed25519(verifying),
                i128::from(iana::Algorithm::EdDSA.to_i64()),
            ))
        }
        _ => Err(Error::InvalidCredential),
    }
}

pub fn register(
    input: &BrowserCredential,
    challenge: &str,
    origin: &str,
    rp_id: &str,
) -> Result<Credential, Error> {
    let client = client_data(input, challenge, origin, "webauthn.create")?;
    let attestation = cbor(&decode(
        input
            .response
            .attestation_object
            .as_deref()
            .ok_or(Error::InvalidCredential)?,
    )?)?;
    let auth = map_get(&attestation, "authData")?
        .as_bytes()
        .ok_or(Error::InvalidCredential)?;
    let count = auth_data(auth, rp_id, true)?;
    if auth.len() < 55 {
        return Err(Error::InvalidCredential);
    }
    let id_len = u16::from_be_bytes([auth[53], auth[54]]) as usize;
    let end = 55usize
        .checked_add(id_len)
        .ok_or(Error::InvalidCredential)?;
    if id_len == 0 || end >= auth.len() || encode(&auth[55..end]) != input.id {
        return Err(Error::InvalidCredential);
    }
    validate_cbor_prefix(&auth[end..], false)?;
    let mut cursor = Cursor::new(&auth[end..]);
    let key: Value = ciborium::from_reader(&mut cursor).map_err(|_| Error::InvalidCredential)?;
    let key_end = end + cursor.position() as usize;
    if auth[32] & 0x80 == 0 && key_end != auth.len() {
        return Err(Error::InvalidCredential);
    }
    if auth[32] & 0x80 != 0 {
        cbor(&auth[key_end..])?;
    }
    let mut public_key = Vec::new();
    ciborium::into_writer(&key, &mut public_key).map_err(|_| Error::InvalidCredential)?;
    let (verifying_key, key_alg) = parse_cose_key(&public_key)?;
    let stmt = map_get(&attestation, "attStmt")?;
    match map_get(&attestation, "fmt")?.as_text() {
        Some("none") if stmt.as_map().is_some_and(|v| v.is_empty()) => {}
        Some("packed") => {
            if stmt
                .as_map()
                .ok_or(Error::InvalidCredential)?
                .iter()
                .any(|(k, _)| matches!(k.as_text(), Some("x5c" | "ecdaaKeyId")))
            {
                return Err(Error::InvalidCredential);
            }
            if integer(map_get(stmt, "alg")?)? != key_alg {
                return Err(Error::InvalidCredential);
            }
            let mut signed = auth.clone();
            signed.extend(Sha256::digest(&client));
            verifying_key.verify(
                &signed,
                map_get(stmt, "sig")?
                    .as_bytes()
                    .ok_or(Error::InvalidCredential)?,
            )?;
        }
        _ => return Err(Error::InvalidCredential),
    }
    Ok(Credential {
        id: input.id.clone(),
        public_key,
        sign_count: count,
        revoked: false,
    })
}

pub fn assert(
    input: &BrowserCredential,
    credential: &Credential,
    challenge: &str,
    origin: &str,
    rp_id: &str,
) -> Result<u32, Error> {
    if credential.revoked || input.id != credential.id {
        return Err(Error::InvalidCredential);
    }
    let client = client_data(input, challenge, origin, "webauthn.get")?;
    let mut auth = decode(
        input
            .response
            .authenticator_data
            .as_deref()
            .ok_or(Error::InvalidCredential)?,
    )?;
    let count = auth_data(&auth, rp_id, false)?;
    if auth[32] & 0x80 == 0 && auth.len() != 37 {
        return Err(Error::InvalidCredential);
    }
    if auth[32] & 0x80 != 0 {
        cbor(&auth[37..])?;
    }
    if (count != 0 || credential.sign_count != 0) && count <= credential.sign_count {
        return Err(Error::CounterRegression);
    }
    auth.extend(Sha256::digest(client));
    let (verifying_key, _) = parse_cose_key(&credential.public_key)?;
    verifying_key.verify(
        &auth,
        &decode(
            input
                .response
                .signature
                .as_deref()
                .ok_or(Error::InvalidCredential)?,
        )?,
    )?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cbor_precheck_rejects_deeply_nested_definite_and_indefinite_containers() {
        // 10,000 nested 1-element definite arrays (0x81 * 10000 + 0x00)
        let mut deep_def_arr = vec![0x81u8; 10_000];
        deep_def_arr.push(0x00);
        assert_eq!(cbor(&deep_def_arr), Err(Error::InvalidCredential));

        // 10,000 nested indefinite arrays (0x9f * 10000 + 0xff * 10000)
        let mut deep_indef_arr = vec![0x9fu8; 10_000];
        deep_indef_arr.extend(std::iter::repeat_n(0xffu8, 10_000));
        assert_eq!(cbor(&deep_indef_arr), Err(Error::InvalidCredential));

        // Deeply nested definite maps (0xa1 0x00 * 1000 + 0x00)
        let mut deep_def_map = Vec::with_capacity(2001);
        for _ in 0..1000 {
            deep_def_map.push(0xa1);
            deep_def_map.push(0x00);
        }
        deep_def_map.push(0x00);
        assert_eq!(cbor(&deep_def_map), Err(Error::InvalidCredential));

        // Deeply nested indefinite maps (0xbf 0x00 * 1000 + 0x00 + 0xff * 1000)
        let mut deep_indef_map = Vec::with_capacity(3001);
        for _ in 0..1000 {
            deep_indef_map.push(0xbf);
            deep_indef_map.push(0x00);
        }
        deep_indef_map.push(0x00);
        deep_indef_map.extend(std::iter::repeat_n(0xffu8, 1000));
        assert_eq!(cbor(&deep_indef_map), Err(Error::InvalidCredential));

        // Deeply nested CBOR tags (0xc0 * 1000 + 0x00)
        let mut deep_tags = vec![0xc0u8; 1000];
        deep_tags.push(0x00);
        assert_eq!(cbor(&deep_tags), Err(Error::InvalidCredential));
    }

    #[test]
    fn cbor_precheck_enforces_exact_depth_and_item_bounds() {
        // Depth == MAX_CBOR_DEPTH is accepted.
        let mut at_limit = vec![0x81u8; MAX_CBOR_DEPTH];
        at_limit.push(0x00);
        assert!(cbor(&at_limit).is_ok());

        // Depth == MAX_CBOR_DEPTH + 1 is rejected.
        let mut over_limit = vec![0x81u8; MAX_CBOR_DEPTH + 1];
        over_limit.push(0x00);
        assert_eq!(cbor(&over_limit), Err(Error::InvalidCredential));

        // Excessive item count (array of MAX_CBOR_ITEMS + 1 zeros) is rejected.
        let mut many_items = vec![0x99u8]; // array with 16-bit length
        let count = (MAX_CBOR_ITEMS + 1) as u16;
        many_items.extend_from_slice(&count.to_be_bytes());
        many_items.extend(std::iter::repeat_n(0x00u8, count as usize));
        assert_eq!(cbor(&many_items), Err(Error::InvalidCredential));
    }
}
