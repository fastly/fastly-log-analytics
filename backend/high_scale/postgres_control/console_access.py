"""Transactional Postgres control-plane state for the isolated high-scale slice."""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import hmac
import logging
import os
import re
import threading
import urllib.parse
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any

from backend import config
from backend.core.clickhouse import ClickHouseClient
from backend.core.share_db import CMCD_SESSION_ALIAS_KEYS, SHARED_PII_KEYS, is_saved_view_pii_key
from backend.high_scale._common import clickhouse_string_literal

logger = logging.getLogger("backend.high_scale.postgres_control")

# ── Least-Privilege ClickHouse Console Access (folded from console_access.py) ─
FACT_DOMAINS = {"request_facts": "request", "rum_vitals_facts": "rum_vitals", "rum_error_facts": "rum_errors"}
PUBLICATIONS_TABLE = "high_scale_batch_publications"
PROFILE = "fla_console_ro"
MIN_SECRET_BYTES = 32
MIN_DISTINCT_SECRET_BYTES = 8
REDACTED = "[redacted]"
MASKED_KEYS = tuple(sorted(SHARED_PII_KEYS))
_EXTRA_MAP_MASKED_KEYS = frozenset(
    {
        "origin_ip",
        "cf_connecting_ip",
        "cf_connecting_ipv6",
        "connecting_ip",
        "connecting_ipv6",
        "x_real_ip",
        "x_client_ip",
        "x_cluster_client_ip",
        "x_original_forwarded_for",
        "x_forwarded_client_ip",
        "forwarded",
        "forwarded_for",
        "authorization",
        "proxy_authorization",
        "cookie",
        "set_cookie",
        "x_api_key",
        "api_key",
        "access_token",
        "x_access_token",
        "id_token",
        "refresh_token",
        "auth_token",
        "x_auth_token",
        "csrf_token",
        "xsrf_token",
        "x_csrf_token",
        "x_xsrf_token",
        "session",
        "session_token",
        "x_session_id",
        "x_session_token",
        "cmcd.sid",
        "cmcd.cid",
        "cmcd.session",
        "src_ip",
        "remote_ip",
        "peer_ip",
        "x_ip",
        "x_cookie",
        "x_authorization",
        "bearer_token",
        "session_cookie",
    }
)
_MAP_MASKED_KEYS = tuple(sorted(SHARED_PII_KEYS | CMCD_SESSION_ALIAS_KEYS | _EXTRA_MAP_MASKED_KEYS))
_MAP_MASKED_KEY_SET = frozenset(_MAP_MASKED_KEYS)
_MAP_MASKED_NONWORD_KEYS = tuple(sorted({re.sub(r"[^a-z0-9]+", "", k.lower()) for k in _MAP_MASKED_KEYS}))
_MAP_MASKED_NONWORD_SET = frozenset(_MAP_MASKED_NONWORD_KEYS)
_PII_WS_RE2_CHARS = (
    r"\x00-\x1f \x7f\x{0085}\x{00a0}\x{1680}\x{180e}\x{2000}-\x{200d}"
    r"\x{2028}\x{2029}\x{202f}\x{205f}\x{2060}\x{3000}\x{feff}"
)
_PII_WS_PY_CHARS = "\x00-\x1f \x7f\x85\u00a0\u1680\u180e\u2000-\u200d\u2028\u2029\u202f\u205f\u2060\u3000\ufeff"
_PII_WS_STRIP_CHARS = (
    "".join(chr(c) for c in range(0x20))
    + " \x7f\x85\u00a0\u1680\u180e"
    + "".join(chr(c) for c in range(0x2000, 0x200E))
    + "\u2028\u2029\u202f\u205f\u2060\u3000\ufeff"
)
_PII_WS_TRIM_RE2 = f"^[{_PII_WS_RE2_CHARS}]+|[{_PII_WS_RE2_CHARS}]+$"
_PII_SPLIT_OUTER_RE2 = f"[\\[\\]/:;|,&=?{_PII_WS_RE2_CHARS}]+"
_PII_SPLIT_ALL_RE2 = f"[.\\[\\]/:;|,&=?{_PII_WS_RE2_CHARS}]+"
_PII_CAMEL_IPV_RE = re.compile(r"IP[vV]([46])")
_PII_CAMEL_RE_1 = re.compile(r"([a-z0-9])([A-Z])")
_PII_CAMEL_RE_2 = re.compile(r"([A-Z]+)([A-Z][a-z])")
_PII_SPLIT_OUTER_RE = re.compile(f"[\\[\\]/:;|,&=?{_PII_WS_PY_CHARS}]+")
_PII_SPLIT_ALL_RE = re.compile(f"[.\\[\\]/:;|,&=?{_PII_WS_PY_CHARS}]+")
_PII_BRACKET_STRIP_RE = re.compile(r"\[[^\[\]]*\]")
_PII_IPV_RE = re.compile(r"([a-z_])ipv[46](?:_|$)")
_PII_AFFIX_PREFIX_RE = re.compile(
    r"^(?:(?:x_?)?filter_|_?cf_|(?:req|resp|request|response|bereq|beresp|obj)_http_|http_|(?:req|resp|request|response)_|headers?_|(?:raw|orig|original)_)+"
)
_PII_SUFFIX_RE = re.compile(r"(?:_v[0-9]+|_hash|_addr|_?[0-9]+)+$")


def _normalize_pii_unicode(value: str) -> str:
    # Fold U+0130 ('İ') -> 'I' and U+212A ('K') -> 'K' (uppercase) and strip U+0307 BEFORE camelCase/IPv splitting,
    # while remaining compatible with non-ICU ClickHouse/chdb.
    return value.replace("\u0130", "I").replace("\u0307", "").replace("\u212a", "K")


def _strip_pii_brackets(value: str) -> str:
    cur = value
    while True:
        nxt = _PII_BRACKET_STRIP_RE.sub("", cur)
        if nxt == cur:
            return cur
        cur = nxt


def _decode_pii_url_twice(value: str) -> str:
    return urllib.parse.unquote_to_bytes(urllib.parse.unquote_to_bytes(value)).decode("utf-8", errors="replace")


def _is_console_pii_identifier(name: str) -> bool:
    raw = _normalize_pii_unicode(name).strip(_PII_WS_STRIP_CHARS).lower()
    if not raw:
        return False
    if raw in MASKED_KEYS or raw in _MAP_MASKED_KEY_SET or is_saved_view_pii_key(name):
        return True
    decoded = _normalize_pii_unicode(_decode_pii_url_twice(name))
    raw_lower = decoded.strip(_PII_WS_STRIP_CHARS).lower()
    camel_src = _PII_CAMEL_IPV_RE.sub(r"Ipv\1", decoded)
    camel = _PII_CAMEL_RE_2.sub(r"\1_\2", _PII_CAMEL_RE_1.sub(r"\1_\2", camel_src)).strip(_PII_WS_STRIP_CHARS).lower()
    raw_unbrack = _strip_pii_brackets(raw_lower).strip(_PII_WS_STRIP_CHARS)
    camel_unbrack = _strip_pii_brackets(camel).strip(_PII_WS_STRIP_CHARS)
    candidates = [raw, raw_lower, camel, raw_unbrack, camel_unbrack]
    for base in (raw_lower, camel, raw_unbrack, camel_unbrack):
        if base:
            candidates.extend(_PII_SPLIT_OUTER_RE.split(base))
            candidates.extend(_PII_SPLIT_ALL_RE.split(base))
    for seg in candidates:
        if not seg:
            continue
        if seg in _MAP_MASKED_KEY_SET:
            return True
        canon = re.sub(r"[^a-z0-9]+", "_", seg).strip("_")
        nw_under = re.sub(r"[^a-z0-9_]+", "", seg)
        if not canon and not nw_under:
            continue
        ipv_norm = _PII_IPV_RE.sub(r"\1ip_", canon).strip("_") if canon else ""
        affix_stripped = _PII_AFFIX_PREFIX_RE.sub("", ipv_norm).strip("_") if ipv_norm else ""
        no_suffix = _PII_SUFFIX_RE.sub("", ipv_norm).strip("_") if ipv_norm else ""
        stripped = _PII_SUFFIX_RE.sub("", affix_stripped).strip("_") if affix_stripped else ""
        nw_ipv = _PII_IPV_RE.sub(r"\1ip_", nw_under) if nw_under else ""
        nw_affix = _PII_AFFIX_PREFIX_RE.sub("", nw_ipv).strip("_") if nw_ipv else ""
        nw_stripped = _PII_SUFFIX_RE.sub("", nw_affix).strip("_") if nw_affix else ""
        for cand in (canon, ipv_norm, affix_stripped, no_suffix, stripped, nw_affix, nw_stripped):
            if cand and (cand in _MAP_MASKED_KEY_SET or cand.replace("_", "") in _MAP_MASKED_NONWORD_SET):
                return True
    return False


_DATABASE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
_PROFILE_SETTINGS = (
    "readonly = 1, additional_table_filters CHANGEABLE_IN_READONLY, "
    "max_result_rows = 10001 MAX 10001 CHANGEABLE_IN_READONLY, result_overflow_mode CHANGEABLE_IN_READONLY, "
    "max_execution_time = 30 MAX 300 CHANGEABLE_IN_READONLY, "
    "max_memory_usage = 1073741824 MAX 1073741824 CHANGEABLE_IN_READONLY, "
    "max_threads = 2 MAX 2 CHANGEABLE_IN_READONLY, "
    "max_bytes_before_external_group_by = 134217728 MAX 134217728 CHANGEABLE_IN_READONLY, "
    "max_bytes_before_external_sort = 134217728 MAX 134217728 CHANGEABLE_IN_READONLY, "
    "timeout_overflow_mode CHANGEABLE_IN_READONLY, timeout_before_checking_execution_speed CHANGEABLE_IN_READONLY, "
    "output_format_json_quote_64bit_integers CHANGEABLE_IN_READONLY, "
    "memory_overcommit_ratio_denominator CHANGEABLE_IN_READONLY, "
    "memory_overcommit_ratio_denominator_for_user CHANGEABLE_IN_READONLY, "
    "max_memory_usage_for_user = 2147483648 MAX 2147483648, max_result_bytes = 67108864"
)


def _literal(value: str) -> str:
    return clickhouse_string_literal(value)


def _service_hash(service_id: str) -> str:
    return hashlib.sha256(service_id.encode()).hexdigest()[:16]


def _mode(masked: bool) -> str:
    return "m" if masked else "r"


def console_identity(service_id: str, secret: bytes, masked: bool = True) -> tuple[str, str]:
    """``(user, password)`` for ``service_id`` in the given mask mode; the password is an HMAC under the secret."""
    if len(secret) < MIN_SECRET_BYTES:
        raise ValueError(f"CLICKHOUSE_CONSOLE_SECRET must be at least {MIN_SECRET_BYTES} bytes")
    if len(set(secret)) < MIN_DISTINCT_SECRET_BYTES:
        raise ValueError(f"CLICKHOUSE_CONSOLE_SECRET needs at least {MIN_DISTINCT_SECRET_BYTES} distinct bytes")
    user = f"fla_console_{_service_hash(service_id)}_{_mode(masked)}"
    password = hmac.new(secret, f"console:{service_id}:{_mode(masked)}".encode(), hashlib.sha256).hexdigest()
    return user, password


def console_tables(service_id: str, masked: bool) -> dict[str, str]:
    """Canonical fact table name -> the view this identity reads it through."""
    return {table: f"fla_cv_{_service_hash(service_id)}_{_mode(masked)}_{table}" for table in FACT_DOMAINS}


def _redaction(name: str, type_: str, keys: str) -> str | None:
    """The expression that redacts column ``name`` for a masked identity, or None to pass it through unchanged."""
    redacted = _literal(REDACTED)
    if type_.startswith("Map("):
        match = re.fullmatch(r"Map\((.+?),\s*(.+)\)", type_)
        if not match or "String" not in match.group(1) or "String" not in match.group(2):
            raise RuntimeError(f"cannot redact column {name} of type {type_}")
        ws_trim_pat = _literal(_PII_WS_TRIM_RE2)
        u_idot_pat = _literal(r"\x{0130}")
        u_comb_dot_pat = _literal(r"\x{0307}")
        u_kelvin_pat = _literal(r"\x{212a}")

        def _ch_norm_u(expr: str) -> str:
            return (
                f"replaceRegexpAll(replaceRegexpAll(replaceRegexpAll({expr}, "
                f"{u_idot_pat}, 'I'), {u_comb_dot_pat}, ''), {u_kelvin_pat}, 'K')"
            )

        def _ch_url_decode_pass(expr: str) -> str:
            return (
                "arrayMap(parts -> arrayStringConcat(arrayMap((p, idx) -> "
                "if(idx = 1, p, if(match(p, '^[0-9A-Fa-f]{2}'), "
                "concat(unhex(substring(p, 1, 2)), substring(p, 3)), concat('%', p))), "
                f"parts, arrayEnumerate(parts))), [splitByChar('%', {expr})])[1]"
            )

        k_raw_pre = f"lower(replaceRegexpAll({_ch_norm_u('toValidUTF8(k)')}, {ws_trim_pat}, ''))"
        k_decoded = f"toValidUTF8({_ch_url_decode_pass(_ch_url_decode_pass('k'))})"
        k_norm = _ch_norm_u(k_decoded)
        k_raw_lower = f"lower(replaceRegexpAll({k_norm}, {ws_trim_pat}, ''))"
        camel_ipv_pat = _literal(_PII_CAMEL_IPV_RE.pattern)
        camel_ipv_repl = _literal(r"Ipv\1")
        camel1_pat = _literal(_PII_CAMEL_RE_1.pattern)
        camel2_pat = _literal(_PII_CAMEL_RE_2.pattern)
        camel_repl = _literal(r"\1_\2")
        k_camel_src = f"replaceRegexpAll({k_norm}, {camel_ipv_pat}, {camel_ipv_repl})"
        k_camel = (
            f"lower(replaceRegexpAll(replaceRegexpAll(replaceRegexpAll({k_camel_src}, "
            f"{camel1_pat}, {camel_repl}), {camel2_pat}, {camel_repl}), {ws_trim_pat}, ''))"
        )
        bracket_strip_pat = _literal(_PII_BRACKET_STRIP_RE.pattern)
        bracket_greedy_pat = _literal(r"\[.*\]")

        def _ch_strip_brackets(expr: str) -> str:
            cur_e = expr
            for _ in range(5):
                cur_e = f"replaceRegexpAll({cur_e}, {bracket_strip_pat}, '')"
            return (
                f"arrayStringConcat(arrayMap(bseg -> replaceRegexpAll(bseg, {bracket_greedy_pat}, ''), "
                f"splitByChar('.', {cur_e})), '.')"
            )

        k_raw_unbrack = f"replaceRegexpAll({_ch_strip_brackets(k_raw_lower)}, {ws_trim_pat}, '')"
        k_camel_unbrack = f"replaceRegexpAll({_ch_strip_brackets(k_camel)}, {ws_trim_pat}, '')"
        split_outer_pat = _literal(_PII_SPLIT_OUTER_RE2)
        split_all_pat = _literal(_PII_SPLIT_ALL_RE2)
        ipv_pat = _literal(_PII_IPV_RE.pattern)
        ipv_repl = _literal(r"\1ip_")
        affix_pat = _literal(_PII_AFFIX_PREFIX_RE.pattern)
        suffix_pat = _literal(_PII_SUFFIX_RE.pattern)
        seg_canon = "trimBoth(replaceRegexpAll(seg, '[^a-z0-9]+', '_'), '_')"
        seg_ipv = f"trimBoth(replaceRegexpAll({seg_canon}, {ipv_pat}, {ipv_repl}), '_')"
        seg_affix = f"trimBoth(replaceRegexpOne({seg_ipv}, {affix_pat}, ''), '_')"
        seg_no_suf = f"trimBoth(replaceRegexpOne({seg_ipv}, {suffix_pat}, ''), '_')"
        seg_stripped = f"trimBoth(replaceRegexpOne({seg_affix}, {suffix_pat}, ''), '_')"
        seg_nw_under = "replaceRegexpAll(seg, '[^a-z0-9_]+', '')"
        seg_nw_ipv = f"replaceRegexpAll({seg_nw_under}, {ipv_pat}, {ipv_repl})"
        seg_nw_affix = f"trimBoth(replaceRegexpOne({seg_nw_ipv}, {affix_pat}, ''), '_')"
        seg_nw_stripped = f"trimBoth(replaceRegexpOne({seg_nw_affix}, {suffix_pat}, ''), '_')"
        nw_keys = ", ".join(_literal(nk) for nk in _MAP_MASKED_NONWORD_KEYS)
        candidates_expr = (
            f"arrayConcat([{k_raw_pre}, {k_raw_lower}, {k_camel}, {k_raw_unbrack}, {k_camel_unbrack}], "
            f"splitByRegexp({split_outer_pat}, {k_raw_lower}), "
            f"splitByRegexp({split_all_pat}, {k_raw_lower}), "
            f"splitByRegexp({split_outer_pat}, {k_camel}), "
            f"splitByRegexp({split_all_pat}, {k_camel}), "
            f"splitByRegexp({split_outer_pat}, {k_raw_unbrack}), "
            f"splitByRegexp({split_all_pat}, {k_raw_unbrack}), "
            f"splitByRegexp({split_outer_pat}, {k_camel_unbrack}), "
            f"splitByRegexp({split_all_pat}, {k_camel_unbrack}))"
        )
        cmcd_extra = f" OR {k_camel} = 'session'" if name.lower() == "cmcd" else ""
        return (
            f"mapApply((k, v) -> (k, if((arrayExists(seg -> seg != '' AND ("
            f"seg IN ({keys}) OR arrayExists(cand -> cand != '' AND ("
            f"cand IN ({keys}) OR replaceRegexpAll(cand, '_', '') IN ({nw_keys})"
            f"), [{seg_canon}, {seg_ipv}, {seg_affix}, {seg_no_suf}, {seg_stripped}, "
            f"{seg_nw_affix}, {seg_nw_stripped}])"
            f"), {candidates_expr}){cmcd_extra}) "
            f"AND NOT (isNull(v) OR v = ''), {redacted}, v)), `{name}`) AS `{name}`"
        )
    if _is_console_pii_identifier(name):
        if "String" not in type_:
            raise RuntimeError(f"cannot redact column {name} of type {type_}")
        return f"if(isNull(`{name}`) OR `{name}` = '', `{name}`, {redacted}) AS `{name}`"
    return None


def _view_select(database: str, table: str, columns: list[tuple[str, str]], service_id: str, masked: bool) -> str:
    sid = _literal(service_id)
    fence = (
        f"batch_id IN (SELECT batch_id FROM `{database}`.`{PUBLICATIONS_TABLE}` FINAL "
        f"WHERE service_id = {sid} AND domain = '{FACT_DOMAINS[table]}' AND publication_state = 'visible')"
    )
    source = f"FROM `{database}`.`{table}` WHERE service_id = {sid} AND publication_state = 'visible' AND {fence}"
    if not masked:
        return f"SELECT * {source}"
    keys = ", ".join(_literal(key) for key in _MAP_MASKED_KEYS)
    projection = ", ".join(_redaction(name, type_, keys) or f"`{name}`" for name, type_ in columns)
    return f"SELECT {projection} {source}"


def provision_console_access(admin: Any, database: str, service_id: str, secret: bytes) -> dict[str, str]:
    """Idempotently create ``service_id``'s masked and raw console identities; returns ``{mode: user}``."""
    if not _DATABASE.fullmatch(database):
        raise ValueError("ClickHouse database must be a simple identifier")
    db = f"`{database}`"
    schemas = {
        table: [
            (str(row["name"]), str(row["type"]))
            for row in admin.execute(f"DESCRIBE TABLE {db}.`{table}`")
            if str(row.get("default_type") or "").upper() not in ("MATERIALIZED", "ALIAS")
        ]
        for table in FACT_DOMAINS
    }
    for table, columns in schemas.items():
        if not columns:
            raise RuntimeError(f"{table} has no columns; refusing to provision console access")
    admin.execute(f"CREATE SETTINGS PROFILE IF NOT EXISTS {PROFILE} SETTINGS {_PROFILE_SETTINGS}")
    admin.execute(f"ALTER SETTINGS PROFILE {PROFILE} SETTINGS {_PROFILE_SETTINGS}")
    users: dict[str, str] = {}
    for masked in (True, False):
        user, password = console_identity(service_id, secret, masked)
        views = console_tables(service_id, masked)
        admin.execute(
            f"CREATE USER IF NOT EXISTS `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)} "
            f"DEFAULT DATABASE {db} SETTINGS PROFILE '{PROFILE}'"
        )
        admin.execute(
            f"ALTER USER `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)} SETTINGS PROFILE '{PROFILE}'"
        )
        for table, view in views.items():
            admin.execute(f"DROP VIEW IF EXISTS {db}.`{view}`")
            admin.execute(
                f"CREATE VIEW {db}.`{view}` DEFINER = CURRENT_USER SQL SECURITY DEFINER AS "
                + _view_select(database, table, schemas[table], service_id, masked)
            )
        present = {
            row.get("name")
            for row in admin.execute(
                f"SELECT name FROM system.tables WHERE database = {_literal(database)} AND engine = 'View' "
                f"AND name IN ({', '.join(_literal(v) for v in views.values())})"
            )
        }
        missing = set(views.values()) - present
        if missing:
            raise RuntimeError(f"console views missing for {service_id}: {sorted(missing)}; nothing was granted")
        admin.execute(f"REVOKE ALL ON *.* FROM `{user}`")
        for view in views.values():
            admin.execute(f"GRANT SELECT ON {db}.`{view}` TO `{user}`")
        users["masked" if masked else "raw"] = user
    return users


@contextlib.contextmanager
def provisioning_client(default_admin: Any) -> Iterator[Any]:
    """The client to provision with: a separate startup-only account when ``CLICKHOUSE_PROVISION_USER`` is set."""
    settings = config.load_clickhouse_provisioner_config()
    if settings is None:
        yield default_admin
        return
    client = ClickHouseClient(settings)
    try:
        yield client
    finally:
        client.close()


def provision_configured_services(admin: Any, service_ids: Iterable[str]) -> tuple[str, ...]:
    """Provision every service at startup when a console secret is configured; failures are logged, not fatal."""
    secret = _env_secret()
    settings = config.load_clickhouse_config()
    if secret is None or settings is None:
        return ()
    done = []
    for service_id in service_ids:
        try:
            provision_console_access(admin, settings.database, service_id, secret)
            done.append(service_id)
        except Exception:
            logger.warning(
                "analyst console provisioning failed for %s; analysts get no console there", service_id, exc_info=True
            )
    return tuple(done)


def _env_secret() -> bytes | None:
    value = os.getenv("CLICKHOUSE_CONSOLE_SECRET", "").strip()
    return value.encode() if value else None


@dataclasses.dataclass(frozen=True)
class ConsoleAccess:
    """A console identity's client plus the views it must read each fact table through."""

    client: Any
    tables: Mapping[str, str]


class ConsoleClients:
    """Lazy per-(service, mask mode) ClickHouse clients that log in as that identity."""

    def __init__(
        self,
        *,
        settings: config.ClickHouseConfig | None,
        secret: bytes | None,
        client_factory: Callable[[config.ClickHouseConfig], Any] = ClickHouseClient,
    ) -> None:
        self._settings = settings
        self._secret = secret
        self._factory = client_factory
        self._access: dict[tuple[str, bool], ConsoleAccess] = {}
        self._lock = threading.Lock()

    def client_for(self, service_id: str, masked: bool = True) -> ConsoleAccess | None:
        """The identity for ``service_id``, or None when no secret/ClickHouse is configured (no analyst console)."""
        if self._secret is None or self._settings is None:
            return None
        key = (service_id, masked)
        with self._lock:
            existing = self._access.get(key)
        if existing is not None:
            return existing
        user, password = console_identity(service_id, self._secret, masked)
        client = self._factory(dataclasses.replace(self._settings, user=user, password=password))
        access = ConsoleAccess(client, console_tables(service_id, masked))
        try:
            self._assert_isolated(access, service_id, masked)
        except BaseException:
            client.close()
            raise
        with self._lock:
            winner = self._access.setdefault(key, access)
        if winner is not access:
            client.close()
        return winner

    @staticmethod
    def _assert_isolated(access: ConsoleAccess, service_id: str, masked: bool) -> None:
        """Fail closed unless the identity reads only its own service and, when masked, only redacted identifiers."""
        sid = _literal(service_id)
        for view in access.tables.values():
            rows = access.client.execute(f"SELECT count() AS n FROM {view} WHERE service_id != {sid}")
            if not rows or int(rows[0].get("n", 1)) != 0:
                raise RuntimeError(f"console identity for {service_id} failed the row isolation check on {view}")
        if masked:
            req_view = access.tables["request_facts"]
            desc_rows = access.client.execute(f"DESCRIBE TABLE {req_view}")
            if any(str(row.get("name") or "").lower().startswith("_cf_") for row in desc_rows):
                raise RuntimeError(
                    f"console identity for {service_id} failed the masking check on {req_view}: "
                    "materialized _cf_* columns exposed"
                )
            red = _literal(REDACTED)
            unmasked = {
                "request_facts": (
                    f"(client_ip != '' AND client_ip != {red}) "
                    f"OR custom_fields['cookie_session'] NOT IN ('', {red}) "
                    f"OR custom_fields['cid'] NOT IN ('', {red}) "
                    f"OR custom_fields['edge_sid'] NOT IN ('', {red}) "
                    f"OR custom_fields['oip'] NOT IN ('', {red}) "
                    f"OR custom_fields['cmcd_sid'] NOT IN ('', {red}) "
                    f"OR custom_fields['cmcd_cid'] NOT IN ('', {red}) "
                    f"OR custom_fields['cmcd_session'] NOT IN ('', {red}) "
                    f"OR custom_fields['cf-connecting-ip'] NOT IN ('', {red}) "
                    f"OR custom_fields['x-real-ip'] NOT IN ('', {red}) "
                    f"OR custom_fields['authorization'] NOT IN ('', {red}) "
                    f"OR custom_fields['cookie'] NOT IN ('', {red}) "
                    f"OR custom_fields['set-cookie'] NOT IN ('', {red}) "
                    f"OR custom_fields['filter-session-id'] NOT IN ('', {red}) "
                    f"OR custom_fields['req.http.session_id'] NOT IN ('', {red}) "
                    f"OR cmcd['sid'] NOT IN ('', {red}) "
                    f"OR cmcd['cid'] NOT IN ('', {red}) "
                    f"OR cmcd['cmcd_sid'] NOT IN ('', {red}) "
                    f"OR cmcd['cmcd_cid'] NOT IN ('', {red}) "
                    f"OR cmcd['cmcd_session'] NOT IN ('', {red}) "
                    f"OR cmcd['cmcd.sid'] NOT IN ('', {red}) "
                    f"OR cmcd['cmcd.cid'] NOT IN ('', {red}) "
                    f"OR cmcd['cmcd.session'] NOT IN ('', {red})"
                ),
                "rum_vitals_facts": f"client_id != '' AND client_id != {red}",
                "rum_error_facts": f"client_id != '' AND client_id != {red}",
            }
            for table, condition in unmasked.items():
                view = access.tables[table]
                rows = access.client.execute(f"SELECT count() AS n FROM {view} WHERE {condition}")
                if not rows or int(rows[0].get("n", 1)) != 0:
                    raise RuntimeError(f"console identity for {service_id} failed the masking check on {view}")

    def close(self) -> None:
        with self._lock:
            held, self._access = list(self._access.values()), {}
        for access in held:
            access.client.close()


_manager: ConsoleClients | None = None
_manager_lock = threading.Lock()


def get_console_clients() -> ConsoleClients:
    """Process singleton built from the environment (CLICKHOUSE_* plus CLICKHOUSE_CONSOLE_SECRET)."""
    global _manager
    with _manager_lock:
        if _manager is None:
            _manager = ConsoleClients(settings=config.load_clickhouse_config(), secret=_env_secret())
        return _manager


def close_console_clients() -> None:
    global _manager
    with _manager_lock:
        manager, _manager = _manager, None
    if manager is not None:
        manager.close()
