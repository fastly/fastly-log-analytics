"""PII-masking evasion regressions (adversarial audit 2026-07-06).

Companion to ``test_middleware.py::test_analyst_masking_blocks_safecol_evasion_filter``
(which pins the middleware *filter-key* lock). These unit tests pin the other
two boundaries of the same root cause — a mask_ips analyst appending a non-word
char to a field name ("ip.", "cookie_session ") so it slips a PII check that
uses the raw key while the SQL layer strips non-word chars and binds the REAL
column:

  * ``apply_pii_policy`` value-cell masking (field-values response surface), and
  * the shared ``normalize_filter_key`` ⊇ forbidden-column invariant that both
    the filter-key lock and the field-values dimension lock rely on.
"""

from __future__ import annotations

import copy

import pytest

from backend.core.share_db.validation import (
    IP_FAMILY_KEYS,
    SESSION_ID_KEYS,
    apply_pii_policy,
)
from backend.repositories.utils.filters import normalize_filter_key
from backend.utils.remote_access import _PII_FORBIDDEN_FILTER_COLS

_MASK = {"mask_ips": True}


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field", ["ip", "ip.", "ip ", "ip\t", "client_ip.", "remote_addr.", "IP", "Ip", "Client_IP", "IP."]
)
def test_apply_pii_policy_masks_ip_field_values_despite_junk_suffix(field):
    """field-values echoes the analyst's raw ``field`` verbatim; the query
    resolved the real ``ip`` column by stripping non-word chars. Masking MUST
    canonicalize the same way, else ``field="ip."`` leaks raw client IPs."""
    obj = {"field": field, "values": [{"value": "203.0.113.7", "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    got = out["values"][0]["value"]
    assert got != "203.0.113.7", (field, got)
    assert got.endswith("xxx"), (field, got)  # last octet zeroed by mask_ip


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "field",
    [
        "cookie_session",
        "cookie_session.",
        "cookie_session ",
        "cookie_session\t",
        "cookie_session%",
        "Cookie_Session",
        "COOKIE_SESSION",
        "Cookie_Session.",
    ],
)
def test_apply_pii_policy_redacts_session_field_values_despite_junk_suffix(field):
    """Same evasion on the session-id column — a raw SHA-256 session hash must
    never reach a mask_ips analyst regardless of the echoed field spelling."""
    obj = {"field": field, "values": [{"value": "a" * 64, "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    assert out["values"][0]["value"] == "[redacted]", (field, out)


@pytest.mark.security_regression
def test_apply_pii_policy_leaves_non_pii_field_values_verbatim():
    """A non-PII dimension (url) is untouched — the canonicalization must not
    over-mask legitimate columns."""
    obj = {"field": "url", "values": [{"value": "/index.html", "count": 9}]}
    out = apply_pii_policy(copy.deepcopy(obj), _MASK)
    assert out["values"][0]["value"] == "/index.html"


@pytest.mark.security_regression
def test_apply_pii_policy_noop_without_mask_ips():
    """No policy → no masking (a non-masking analyst / admin sees raw values)."""
    obj = {"field": "ip", "values": [{"value": "203.0.113.7", "count": 1}]}
    assert apply_pii_policy(copy.deepcopy(obj), {})["values"][0]["value"] == "203.0.113.7"


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "key",
    [
        "ip",
        "ip.",
        "ip ",
        "client_ip.",
        "filter_ip",
        "ip_2",
        "cookie_session",
        "cookie_session.",
        ".cookie_session",
        "filter_cookie_session.",
        # case variants — DuckDB resolves identifiers case-insensitively, so the
        # lock must too (else filters={"IP":...} / {"Cookie_Session":...} slips).
        "IP",
        "Ip",
        "Client_IP",
        "Cookie_Session",
        "COOKIE_SESSION",
        "filter_Cookie_Session.",
    ],
)
def test_normalize_filter_key_resolves_junk_pii_keys_into_forbidden_set(key):
    """The shared invariant behind BOTH middleware locks (filter keys and the
    field-values dimension): ``normalize_filter_key`` must resolve any junk
    variant of a PII column to the bare column so the forbidden-set membership
    check can't be evaded."""
    assert normalize_filter_key(key) in _PII_FORBIDDEN_FILTER_COLS, key


@pytest.mark.security_regression
@pytest.mark.parametrize("key", ["url", "status", "country", "oip", "waf_sig_ind", "_bot_name"])
def test_normalize_filter_key_leaves_non_pii_keys_allowed(key):
    """The strip is a no-op for legitimate word-only columns — it must not start
    blocking non-PII filters (waf_sig_ind / _bot_name are word-only)."""
    assert normalize_filter_key(key) not in _PII_FORBIDDEN_FILTER_COLS, key


def test_forbidden_set_covers_both_pii_families():
    """Sanity: the forbidden set the locks compare against is exactly the union
    of the two PII key families the masking policy protects."""
    assert _PII_FORBIDDEN_FILTER_COLS == IP_FAMILY_KEYS | SESSION_ID_KEYS
