"""Unit tests for the opaque AES-GCM session-reference token codec.

Pins the seal/open round-trip and — critically — that a tampered, wrong-service,
or wrong-key token is rejected (raises ``SessionTokenError``) rather than
silently decoding to attacker-chosen values. The detail endpoint relies on this:
an opaque token that an analyst can't forge or invert is what lets a PII-masking
analyst drill into a session without ever holding the real IP.
"""

from __future__ import annotations

import ast
import inspect
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from backend.core.session_token import SessionTokenError, open_session_token, seal_session_token
from backend.repositories.utils.filters import build_where_clause
from backend.utils.bounded_cache import CacheRegistry

from backend.core import session_token as st
from backend.models.common import FilterSpec


# ── Merged from tests/core/test_common_models.py ──
def test_build_where_clause_basic():
    """Test basic date range filtering and partition pruning."""
    start = "2024-01-01T00:00:00Z"
    end = "2024-01-02T00:00:00Z"
    filters = {}

    params, sql = build_where_clause(start, end, filters, inline_params=False, partition_pruning=True)

    assert "timestamp >=" in sql
    assert "timestamp <=" in sql
    # Params now includes original start/end times plus derived `dt` and `hour` partition values for start and end
    assert len(params) == 6
    assert params[0] == start
    assert params[3] == end


def test_build_where_clause_filters():
    """Test include and exclude filters."""
    filters = {
        "status": FilterSpec(mode="include", values=[200, 404]),
        "country": FilterSpec(mode="exclude", values=["US"]),
    }

    params, sql = build_where_clause(None, None, filters, inline_params=False)

    # IN-clause uses CAST(col AS VARCHAR) + stringified params defensively
    # so mixed-type filter values don't crash. See
    # test_aggregates_timeseries_properties for the original property-
    # test failure that surfaced this.
    assert "(CAST(status AS VARCHAR) IN (?, ?))" in sql
    # Numeric params get stringified for the IN-clause comparison
    assert "200" in params
    assert "404" in params

    # Check exclude parameter (country is already VARCHAR in the registry, so CAST is bypassed for performance)
    assert "(country NOT IN (?))" in sql
    assert "US" in params


def test_build_where_clause_inline():
    """Test inline parameterization (used for temporary tables)."""
    filters = {
        "status": FilterSpec(mode="include", values=[200, 404]),
        "country": FilterSpec(mode="exclude", values=["US"]),
    }

    params, sql = build_where_clause(None, None, filters, inline_params=True)

    assert len(params) == 0  # No parameterized query arguments
    # CAST AS VARCHAR + stringified literals matches the behaviour of
    # the wildcard branch and makes mixed-type filter values safe.
    assert "(CAST(status AS VARCHAR) IN ('200', '404'))" in sql
    assert "(country NOT IN ('US'))" in sql


def test_build_where_clause_excludes_rum_beacons():
    """Test that /rum-beacon is unconditionally excluded when url is present in actual_cols."""
    params, sql = build_where_clause(None, None, {}, actual_cols=["url"])
    assert "url != '/rum-beacon' AND url NOT LIKE '/rum-beacon' || chr(63) || '%'" in sql

    # If url is not present, no exclusion should be added
    params, sql = build_where_clause(None, None, {}, actual_cols=["ip"])
    assert "url != '/rum-beacon'" not in sql


class TestBuildWhereClauseExtended:
    def test_no_args_returns_passthrough(self):
        params, sql = build_where_clause(None, None, {})
        assert sql == "1=1"
        assert params == []

    def test_filter_prefix_stripped(self):
        # The frontend sometimes sends "filter_country" or "xfilter_country"
        filters = {"filter_country": FilterSpec(mode="include", values=["US"])}
        _, sql = build_where_clause(None, None, filters)
        assert "country" in sql
        assert "filter_country" not in sql

    def test_xfilter_prefix_stripped(self):
        filters = {"xfilter_country": FilterSpec(mode="exclude", values=["US"])}
        _, sql = build_where_clause(None, None, filters)
        # Since country is VARCHAR in the registry, it is NOT wrapped in CAST defensively
        assert "country NOT IN" in sql

    def test_numeric_suffix_stripped(self):
        # Frontend appends _2, _3 for duplicate filter keys
        filters = {"country_2": FilterSpec(mode="include", values=["DE"])}
        _, sql = build_where_clause(None, None, filters)
        assert "country" in sql
        assert "country_2" not in sql

    def test_wildcard_include(self):
        filters = {"url": FilterSpec(mode="include", values=["/api/*"])}
        _, sql = build_where_clause(None, None, filters, inline_params=True)
        assert "ILIKE '/api/%'" in sql

    def test_wildcard_exclude(self):
        filters = {"url": FilterSpec(mode="exclude", values=["/health*"])}
        _, sql = build_where_clause(None, None, filters, inline_params=True)
        assert "NOT ILIKE '/health%'" in sql

    def test_mixed_exact_and_wildcard(self):
        filters = {"url": FilterSpec(mode="include", values=["/api", "/static/*"])}
        _, sql = build_where_clause(None, None, filters, inline_params=True)
        assert "IN ('/api')" in sql
        assert "ILIKE '/static/%'" in sql

    def test_null_value_in_include(self):
        filters = {"country": FilterSpec(mode="include", values=[None])}
        _, sql = build_where_clause(None, None, filters)
        assert "IS NULL" in sql

    def test_null_value_in_exclude(self):
        filters = {"country": FilterSpec(mode="exclude", values=[None])}
        _, sql = build_where_clause(None, None, filters)
        assert "IS NOT NULL" in sql

    def test_null_mixed_with_values(self):
        filters = {"country": FilterSpec(mode="include", values=["US", None])}
        _, sql = build_where_clause(None, None, filters)
        assert "IN" in sql
        assert "IS NULL" in sql

    def test_empty_values_list_skipped(self):
        filters = {"country": FilterSpec(mode="include", values=[])}
        params, sql = build_where_clause(None, None, filters)
        assert sql == "1=1"

    def test_empty_string_values_stripped_defensively(self):
        """Filter values that are empty / whitespace-only must NOT
        reach the IN-clause builder. Pinned because losing this
        produced ``WHERE ("status" IN (''))`` against an INTEGER
        column and crashed with "Could not convert string '' to
        INT32" — surfaced by the hypothesis property tests for
        get_aggregates. The FE drops empty values before POSTing,
        but a regression on that side would 500 the dashboard."""
        filters = {"country": FilterSpec(mode="include", values=["", "   ", "US"])}
        params, sql = build_where_clause(None, None, filters)
        # Only "US" survives — placeholder count matches param count
        assert sql.count("?") == len(params) == 1
        assert "US" in params

    def test_all_empty_string_values_collapse_to_passthrough(self):
        """When every value in a filter is empty/whitespace, the
        whole filter is dropped (no IN clause, no syntactic error)."""
        filters = {"country": FilterSpec(mode="include", values=["", "   "])}
        params, sql = build_where_clause(None, None, filters)
        assert sql == "1=1"
        assert params == []

    def test_null_filter_still_works_after_empty_string_strip(self):
        """``values=[None]`` (explicit NULL filter) must NOT be
        affected by the empty-string strip. Pinned because the FE
        sends `[None]` to filter "rows where country IS NULL" — if
        we accidentally dropped None alongside empty strings, that
        feature would silently break."""
        filters = {"country": FilterSpec(mode="include", values=[None])}
        params, sql = build_where_clause(None, None, filters)
        assert "IS NULL" in sql

    def test_waf_sig_ind_uses_list_contains(self):
        filters = {"waf_sig_ind": FilterSpec(mode="include", values=["XSS"])}
        _, sql = build_where_clause(None, None, filters)
        assert "list_contains" in sql
        assert "waf_sig" in sql

    def test_waf_sig_ind_exclude(self):
        filters = {"waf_sig_ind": FilterSpec(mode="exclude", values=["SQLi"])}
        _, sql = build_where_clause(None, None, filters)
        assert "NOT list_contains" in sql

    def test_partition_pruning_skipped_when_dt_not_in_actual_cols(self):
        # When actual_cols is provided and "dt" is absent, no dt condition added.
        _, sql = build_where_clause(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", {}, actual_cols=["timestamp"], partition_pruning=True
        )
        assert "dt >=" not in sql
        assert "timestamp >=" in sql

    def test_partition_pruning_included_when_dt_present(self):
        _, sql = build_where_clause(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", {}, actual_cols=["timestamp", "dt"], partition_pruning=True
        )
        assert "dt >=" in sql
        assert "dt <=" in sql

    def test_timezone_aware_iso_parsed_correctly(self):
        # A timestamp with explicit +05:30 should be converted to UTC date correctly.
        _, sql = build_where_clause("2024-06-15T18:30:00+05:30", None, {}, inline_params=True, partition_pruning=True)
        # 18:30 IST == 13:00 UTC → date is still 2024-06-15
        assert "dt >= '2024-06-15'" in sql


class TestClampToFloatPreservesSubMinuteValues:
    def test_clamp_to_float_preserves_one_second_in_minutes(self):
        """``1/60`` (one second expressed as minutes) must NOT be
        rounded to 0 or to 1. Pinned because losing this would
        silently turn a "1s" granularity selector into "1m" — the
        original bug."""
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        out = clamp(1.0 / 60.0)
        assert 0 < out < 1, f"Sub-minute value collapsed to {out}"
        assert abs(out - 1.0 / 60.0) < 1e-9

    def test_clamp_to_float_uses_one_second_lower_bound(self):
        """Anything below ``1/3600`` (one second in minutes) is clamped
        UP to that floor — never to 0 (which would crash interval
        SQL) or to 1m (which would silently disable sub-minute
        granularity)."""
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        out = clamp(0.0)
        assert out == pytest.approx(1.0 / 3600.0)

    def test_clamp_to_float_clamps_to_upper_bound(self):
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        assert clamp(99999.0) == 1440.0

    def test_clamp_to_float_returns_value_unchanged_in_range(self):
        from backend.models.common import _clamp_to_float

        clamp = _clamp_to_float(1440.0)
        assert clamp(60.0) == 60.0
        assert clamp(1.5) == 1.5

    def test_limit1440_pydantic_annotation_validates_to_float(self):
        """The ``Limit1440`` Annotated type must surface as a float in
        the validated payload. Pinned because reverting to ``int``
        is exactly the regression we're guarding against — the
        validator silently coerces 0.0167 → 0 → 1."""
        import pydantic

        from backend.models.common import Limit1440

        class _M(pydantic.BaseModel):
            v: Limit1440 = 5.0

        out = _M(v=1.0 / 60.0)
        assert isinstance(out.v, float)
        # Crucially, NOT zero (would crash) and NOT 1.0 (the original bug)
        assert 0 < out.v < 1
        assert abs(out.v - 1.0 / 60.0) < 1e-9


# ── Merged from tests/core/test_session_token.py ──
@pytest.fixture
def fixed_key(monkeypatch):
    """Pin a deterministic key (SESSION_TOKEN_SECRET) and reset the module
    cache so the test controls key material. Resets again on teardown so the
    cached ephemeral key doesn't leak across tests."""
    monkeypatch.setenv("SESSION_TOKEN_SECRET", "unit-test-secret-A")
    st._key = None
    yield
    st._key = None


def _rekey(monkeypatch, secret: str) -> None:
    monkeypatch.setenv("SESSION_TOKEN_SECRET", secret)
    st._key = None


def test_round_trip(fixed_key):
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    ip, ja4, start, end = open_session_token(token, service_id="svcA")
    assert ip == "203.0.113.7"
    assert ja4 == "ja4abc"
    assert start == "2026-06-29T00:00:00+00:00"
    assert end == "2026-06-29T00:30:00+00:00"


def test_round_trip_null_ja4(fixed_key):
    """ja4 is often absent — it must round-trip as None, not the string 'None'
    or '' (the detail SQL treats None as 'don't filter on ja4')."""
    token = seal_session_token(
        "203.0.113.7", None, "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    ip, ja4, _start, _end = open_session_token(token, service_id="svcA")
    assert ip == "203.0.113.7"
    assert ja4 is None


def test_token_is_opaque_not_derivable_from_ip(fixed_key):
    """The token must not be a plain function of the IP — it must not contain
    the IP in cleartext (else masking is trivially reversible)."""
    ip = "198.51.100.23"
    token = seal_session_token(
        ip, "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    assert ip not in token
    # last octet (the masked part) must not be recoverable from the token text
    assert ".23" not in token


def test_tampered_token_rejected(fixed_key):
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    # Flip a character in the ciphertext body (last char is inside the GCM tag).
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    with pytest.raises(SessionTokenError):
        open_session_token(tampered, service_id="svcA")


def test_wrong_service_aad_rejected(fixed_key):
    """A token minted for svcA must not open under svcB (cross-service replay)."""
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    with pytest.raises(SessionTokenError):
        open_session_token(token, service_id="svcB")


def test_wrong_key_rejected(fixed_key, monkeypatch):
    """A token sealed under one key (e.g. before a restart) fails to open under
    a different key — the caller surfaces a 'reload the page' 400."""
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    _rekey(monkeypatch, "unit-test-secret-B")
    with pytest.raises(SessionTokenError):
        open_session_token(token, service_id="svcA")


def test_malformed_token_rejected(fixed_key):
    with pytest.raises(SessionTokenError):
        open_session_token("not-a-valid-token!!!", service_id="svcA")
    with pytest.raises(SessionTokenError):
        open_session_token("", service_id="svcA")


def test_concurrent_first_resolve_yields_single_key(monkeypatch):
    """Under the ephemeral default (no SESSION_TOKEN_SECRET), many threads all
    hitting the very first key resolution at once must converge on ONE key.

    Without the double-checked lock in ``_resolve_key`` each racing thread could
    generate its own random key and clobber the module global, so a token sealed
    by one thread would fail to open in another (a spurious SessionTokenError).
    We seal in every thread and confirm each token opens under the finally
    resolved key — i.e. all threads saw the same key.
    """
    import threading

    monkeypatch.delenv("SESSION_TOKEN_SECRET", raising=False)
    st._key = None

    barrier = threading.Barrier(16)
    tokens: list[str] = []
    tokens_lock = threading.Lock()

    def _worker() -> None:
        barrier.wait()  # maximise the odds all threads race the None check together
        tok = seal_session_token(
            "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
        )
        with tokens_lock:
            tokens.append(tok)

    threads = [threading.Thread(target=_worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every token — regardless of which thread minted it — must open under the
    # single surviving process key. A torn init would leave some unopenable.
    assert len(tokens) == 16
    for tok in tokens:
        ip, _ja4, _start, _end = open_session_token(tok, service_id="svcA")
        assert ip == "203.0.113.7"

    st._key = None


# ── Merged from tests/core/test_cid_salt.py ──
HEX64 = re.compile(r"[0-9a-f]{64}")


@pytest.fixture
def fake_store(monkeypatch):
    """Real config files in the conftest-sandboxed CONFIGS_DIR; ``saves`` records each disk write."""
    from backend import config as svcconfig

    saves: list[str] = []
    real_write = svcconfig._atomic_write_json

    def spy(path, data):
        time.sleep(0.005)  # widen the read-modify-write window so an unguarded race is reproducible
        saves.append(path.stem)
        real_write(path, data)

    monkeypatch.setattr(svcconfig, "_atomic_write_json", spy)

    class Store:
        def put(self, sid, cfg):
            real_write(svcconfig.config_path(sid), cfg)
            svcconfig._config_cache.pop(sid, None)

        def get(self, sid):
            return json.loads(svcconfig.config_path(sid).read_text())

        def exists(self, sid):
            return svcconfig.config_path(sid).exists()

    svcconfig._ensure_dirs()
    return Store(), saves


@pytest.mark.security_regression
def test_missing_config_never_uses_the_public_hardcoded_salt(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    salt = get_or_generate_cid_salt("svc-none")
    assert "default_test_salt" not in salt
    assert HEX64.fullmatch(salt)
    assert saves == []
    assert not store.exists("svc-none")


@pytest.mark.security_regression
def test_missing_config_logs_loudly(fake_store, caplog):
    from backend.core.duckdb import get_or_generate_cid_salt

    with caplog.at_level(logging.WARNING):
        get_or_generate_cid_salt("svc-none")
    assert "cid_salt_no_config" in caplog.text


@pytest.mark.security_regression
def test_missing_config_salt_is_stable_within_the_process(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    assert get_or_generate_cid_salt("svc-a") == get_or_generate_cid_salt("svc-b")


@pytest.mark.security_regression
def test_existing_salt_is_returned_unchanged_and_not_resaved(fake_store, caplog):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    store.put("svc", {"rum": {"cid_salt": "abc123"}})
    with caplog.at_level(logging.WARNING):
        assert get_or_generate_cid_salt("svc") == "abc123"
    assert saves == []
    assert "cid_salt_missing" not in caplog.text


@pytest.mark.security_regression
def test_first_use_generates_once_persists_and_logs(fake_store, caplog):
    from backend import config as svcconfig
    from backend.core.duckdb import get_or_generate_cid_salt

    store, saves = fake_store
    # Legacy config written before salts existed (bypasses save_config's first-save generation).
    store.put("svc", {"service_id": "svc", "rum": {}})
    with caplog.at_level(logging.WARNING):
        salt = get_or_generate_cid_salt("svc")
    assert HEX64.fullmatch(salt)
    assert store.get("svc")["rum"]["cid_salt"] == salt
    assert saves == ["svc"]
    assert "cid_salt_missing" in caplog.text
    assert svcconfig.load_config("svc")["rum"]["cid_salt"] == salt


@pytest.mark.security_regression
def test_lazy_generation_does_not_clobber_other_config_fields(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, _ = fake_store
    store.put("svc", {"service_id": "svc", "name": "keep", "rum": {"enabled": True}})
    get_or_generate_cid_salt("svc")
    cfg = store.get("svc")
    assert cfg["name"] == "keep"
    assert cfg["rum"]["enabled"] is True


@pytest.mark.security_regression
def test_concurrent_first_use_agrees_on_a_single_salt(fake_store):
    from backend.core.duckdb import get_or_generate_cid_salt

    store, _ = fake_store
    store.put("svc", {"service_id": "svc", "rum": {}})
    barrier = threading.Barrier(8)
    results: list[str] = []

    def worker():
        barrier.wait()
        results.append(get_or_generate_cid_salt("svc"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(results)) == 1
    assert store.get("svc")["rum"]["cid_salt"] == results[0]


_RACER = """
import sys, time
from pathlib import Path
import backend.config as c
c.CONFIGS_DIR = Path(sys.argv[1])
import backend.core.duckdb as d
orig = c._atomic_write_json
def slow(path, data):
    time.sleep(0.05)
    orig(path, data)
c._atomic_write_json = slow
go = Path(sys.argv[2])
while not go.exists():
    time.sleep(0.001)
print(d.get_or_generate_cid_salt("svc"))
"""


@pytest.mark.security_regression
def test_separate_processes_racing_first_use_agree_on_one_salt(tmp_path):
    cfg_dir = tmp_path / "cfgs"
    cfg_dir.mkdir()
    (cfg_dir / "svc.json").write_text(json.dumps({"service_id": "svc", "rum": {}}))
    go = tmp_path / "go"
    procs = [
        subprocess.Popen(
            [sys.executable, "-I", "-c", _RACER, str(cfg_dir), str(go)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONPATH": os.getcwd()},
        )
        for _ in range(4)
    ]
    time.sleep(4)  # let every child finish importing before releasing them together
    go.write_text("")
    outs = [p.communicate(timeout=60) for p in procs]
    for p, (_, err) in zip(procs, outs, strict=True):
        assert p.returncode == 0, err
    salts = {out.strip().splitlines()[-1] for out, _ in outs}
    assert len(salts) == 1
    assert json.loads((cfg_dir / "svc.json").read_text())["rum"]["cid_salt"] in salts


@pytest.mark.security_regression
def test_new_config_gets_salt_on_first_save(fake_store):
    from backend import config as svcconfig

    store, _ = fake_store
    svcconfig.save_config("fresh", {"service_id": "fresh"})
    assert HEX64.fullmatch(store.get("fresh")["rum"]["cid_salt"])


@pytest.mark.security_regression
def test_save_config_never_rekeys_an_existing_salt(fake_store):
    from backend import config as svcconfig

    store, _ = fake_store
    svcconfig.save_config("svc", {"service_id": "svc", "rum": {"enabled": True}})
    original = store.get("svc")["rum"]["cid_salt"]

    # Stale writers: replace rum wholesale, mint their own salt, or drop rum entirely.
    svcconfig.save_config("svc", {"service_id": "svc", "rum": {"enabled": False}})
    assert store.get("svc")["rum"]["cid_salt"] == original
    svcconfig.save_config("svc", {"service_id": "svc", "rum": {"cid_salt": "f" * 64}})
    assert store.get("svc")["rum"]["cid_salt"] == original
    svcconfig.save_config("svc", {"service_id": "svc"})
    assert store.get("svc")["rum"]["cid_salt"] == original


@pytest.mark.security_regression
def test_mutate_config_serializes_concurrent_partial_edits(fake_store):
    from backend import config as svcconfig

    store, _ = fake_store
    svcconfig.save_config("svc", {"service_id": "svc", "counters": {}})
    n = 12
    barrier = threading.Barrier(n)

    def worker(i):
        barrier.wait()
        svcconfig.mutate_config("svc", lambda c, i=i: c["counters"].__setitem__(str(i), i))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(store.get("svc")["counters"]) == sorted(str(i) for i in range(n))


@pytest.mark.security_regression
def test_mutate_config_missing_service_returns_none(fake_store):
    from backend import config as svcconfig

    assert svcconfig.mutate_config("nope", lambda c: None) is None


@pytest.mark.security_regression
def test_ensure_cid_salt_generates_when_absent_and_keeps_existing():
    from backend.core.duckdb import ensure_cid_salt

    cfg: dict = {}
    first = ensure_cid_salt(cfg)
    assert HEX64.fullmatch(first)
    assert cfg["cid_salt"] == first
    assert ensure_cid_salt(cfg) == first
    kept = {"cid_salt": "keep-me"}
    assert ensure_cid_salt(kept) == "keep-me"


@pytest.mark.security_regression
def test_services_router_does_not_mint_its_own_salt():
    from backend.routers.services import core as services_core

    tree = ast.parse(inspect.getsource(services_core))
    minted = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "token_hex"
    ]
    assert minted == []
    assert "ensure_cid_salt" in inspect.getsource(services_core)


# ── Merged from tests/core/test_ttl_cache_migration.py ──
@pytest.fixture
def _drain_caches():
    CacheRegistry.clear_all()
    yield
    CacheRegistry.clear_all()


def _describe_con(columns: list[str]) -> MagicMock:
    con = MagicMock()
    con.execute.return_value.fetchall.return_value = [(c,) for c in columns]
    return con


@pytest.mark.usefixtures("_drain_caches")
def test_pii_cols_cached_per_table_and_drained_by_registry():
    from backend.repositories import query

    con = _describe_con(["ip", "url", "status"])
    first = query._pii_redact_cols(con, "tbl_pii_a")
    second = query._pii_redact_cols(con, "tbl_pii_a")
    assert first == second
    assert "ip" in first and "url" not in first
    assert con.execute.call_count == 1

    query._pii_redact_cols(con, "tbl_pii_b")
    assert con.execute.call_count == 2

    CacheRegistry.clear("query._PII_COLS_CACHE")
    query._pii_redact_cols(con, "tbl_pii_a")
    assert con.execute.call_count == 3


@pytest.mark.usefixtures("_drain_caches")
def test_pii_cols_describe_failure_is_empty_and_not_cached():
    from backend.repositories import query

    con = MagicMock()
    con.execute.side_effect = duckdb.CatalogException("stale")
    assert query._pii_redact_cols(con, "tbl_pii_c") == frozenset()
    good = _describe_con(["ip"])
    assert "ip" in query._pii_redact_cols(good, "tbl_pii_c")


@pytest.mark.usefixtures("_drain_caches")
def test_compaction_stats_memoised_and_drained(tmp_path):
    from backend.core import local_compaction

    root = tmp_path / "cache"
    part = root / "data" / "timestamp_hour=2026-10-19-00"
    part.mkdir(parents=True)
    (part / "a.parquet").write_bytes(b"x")
    with patch("backend.core.duckdb._cache_dir", return_value=str(root)):
        first = local_compaction.compaction_stats({"name": "svc"})
        (part / "b.parquet").write_bytes(b"x")
        second = local_compaction.compaction_stats({"name": "svc"})
        assert first == second
        assert first["total_files"] == 1

        CacheRegistry.clear("core.local_compaction._COMPACTION_STATS_CACHE")
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 2


@pytest.mark.usefixtures("_drain_caches")
def test_compaction_stats_missing_dir_is_memoised(tmp_path):
    from backend.core import local_compaction

    root = tmp_path / "nothing"
    with patch("backend.core.duckdb._cache_dir", return_value=str(root)):
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 0
        (root / "data").mkdir(parents=True)
        assert local_compaction.compaction_stats({"name": "svc"})["total_files"] == 0


@pytest.mark.usefixtures("_drain_caches")
def test_storage_stats_cache_hit_force_and_invalidate():
    from backend.core.metadata import reconciliation as rec

    con = MagicMock()
    con.execute.return_value.fetchone.return_value = [3]
    rec._storage_stats_cache.clear()
    with (
        patch.object(rec, "get_con_readonly", return_value=con) as opener,
        patch.object(rec, "_open_usage_log", return_value=None),
    ):
        a = rec.get_metadata_storage_stats("svc-ttl")
        b = rec.get_metadata_storage_stats("svc-ttl")
        assert a is b
        assert opener.call_count == 1

        rec.get_metadata_storage_stats("svc-ttl", force=True)
        assert opener.call_count == 2

        rec.invalidate_storage_stats_cache("svc-ttl")
        rec.get_metadata_storage_stats("svc-ttl")
        assert opener.call_count == 3


# ── Merged from tests/core/test_config_lock_hygiene.py ──

"""config_lock hygiene: loud degradation, bounded wait, re-entrancy, salt safety, audited rotation."""


import fcntl

import pytest

from backend import config as svcconfig

# (scoped pytestmark) pytestmark = pytest.mark.security_regression

SID = "svc-lock"


@pytest.fixture()
def _reset_lock_state(monkeypatch):
    monkeypatch.setattr(svcconfig, "_lock_degraded", False)
    svcconfig._ensure_dirs()
    svcconfig.save_config(SID, {"service_id": SID})
    yield


def _salt() -> str:
    return json.loads(svcconfig.config_path(SID).read_text())["rum"]["cid_salt"]


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_flock_oserror_is_loud_once_and_flags_health(monkeypatch, caplog):
    def boom(fd, op):
        raise OSError(30, "Read-only file system")

    monkeypatch.setattr(svcconfig.fcntl, "flock", boom)
    with caplog.at_level(logging.ERROR, logger="backend.config"):
        with svcconfig.config_lock(SID):
            pass
        with svcconfig.config_lock(SID):
            pass
    errors = [r for r in caplog.records if "config_lock_degraded" in r.getMessage() and r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert svcconfig.config_lock_degraded() is True


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_health_snapshot_surfaces_config_lock_degraded(monkeypatch):
    from backend.routers.admin.health import health_snapshot

    assert health_snapshot()["config_lock_degraded"] is False
    monkeypatch.setattr(svcconfig, "_lock_degraded", True)
    assert health_snapshot()["config_lock_degraded"] is True


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_lock_wait_is_bounded_and_raises_a_clear_timeout(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        start = time.monotonic()
        with pytest.raises(svcconfig.ConfigLockTimeout, match=SID):
            with svcconfig.config_lock(SID):
                pass
        assert time.monotonic() - start < 3
    finally:
        fcntl.flock(holder, fcntl.LOCK_UN)
        os.close(holder)


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_lock_is_acquired_after_the_holder_releases(monkeypatch):
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    threading.Timer(0.2, lambda: (fcntl.flock(holder, fcntl.LOCK_UN), os.close(holder))).start()
    with svcconfig.config_lock(SID):
        pass


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_mutate_callback_saving_the_same_service_does_not_self_deadlock(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 1.0)

    def _mutate(cfg):
        cfg["a"] = 1
        inner = svcconfig.load_config(SID)
        inner["b"] = 2
        svcconfig.save_config(SID, inner)
        svcconfig.mutate_config(SID, lambda c: c.__setitem__("c", 3))

    seen: dict = {}

    def _outer(cfg):
        _mutate(cfg)
        seen.update(svcconfig.load_config(SID))

    svcconfig.mutate_config(SID, _outer)
    # nested writes were applied (visible inside the callback); the outer save then lands last
    assert seen["b"] == 2 and seen["c"] == 3
    assert svcconfig.load_config(SID)["a"] == 1


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_reentrancy_is_per_thread(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    result: list[str] = []

    def other():
        try:
            with svcconfig.config_lock(SID):
                result.append("acquired")
        except svcconfig.ConfigLockTimeout:
            result.append("timeout")

    with svcconfig.config_lock(SID):
        t = threading.Thread(target=other)
        t.start()
        t.join()
    assert result == ["timeout"]


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_update_status_goes_through_mutate_config(monkeypatch):
    calls = []
    real = svcconfig.mutate_config

    def spy(sid, fn, **kw):
        calls.append(sid)
        return real(sid, fn, **kw)

    monkeypatch.setattr(svcconfig, "mutate_config", spy)
    svcconfig.update_status(SID, {"x": 1})
    assert calls == [SID]
    assert svcconfig.get_status(SID)["x"] == 1


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_update_status_does_not_clobber_a_concurrent_writer():
    def _mutate(cfg):
        cfg["marker"] = "kept"

    def racing_update():
        svcconfig.update_status(SID, {"y": 2})

    # A concurrent update_status in another thread blocks on the lock and applies on a fresh read.
    with svcconfig.config_lock(SID):
        t = threading.Thread(target=racing_update)
        t.start()
        time.sleep(0.05)
        cfg = svcconfig._read_config_fresh(SID)
        _mutate(cfg)
        svcconfig._save_config_unlocked(SID, cfg)
    t.join()
    final = svcconfig.load_config(SID)
    assert final["marker"] == "kept" and final["status"]["y"] == 2


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_snapshot_config_takes_the_lock(monkeypatch):
    monkeypatch.setattr(svcconfig, "_LOCK_TIMEOUT_S", 0.3)
    holder = os.open(svcconfig.CONFIGS_DIR / f".{SID}.lock", os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        with pytest.raises(svcconfig.ConfigLockTimeout):
            svcconfig.snapshot_config(SID)
    finally:
        fcntl.flock(holder, fcntl.LOCK_UN)
        os.close(holder)
    assert svcconfig.snapshot_config(SID) is not None


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_transient_read_failure_aborts_the_save_instead_of_minting_a_new_salt(monkeypatch):
    before = _salt()

    def failing_read(sid):
        raise OSError(5, "Input/output error")

    with monkeypatch.context() as m:
        m.setattr(svcconfig, "_read_config_fresh", failing_read)
        with pytest.raises(svcconfig.CidSaltReadError):
            svcconfig.save_config(SID, {"service_id": SID, "rum": {}})
    assert _salt() == before


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_unreadable_missing_file_still_mints_a_salt_for_a_new_config(monkeypatch):
    svcconfig.save_config("svc-new", {"service_id": "svc-new"})
    assert json.loads(svcconfig.config_path("svc-new").read_text())["rum"]["cid_salt"]


@pytest.mark.security_regression
@pytest.mark.usefixtures("_reset_lock_state")
def test_save_config_cannot_rotate_the_salt():
    before = _salt()
    svcconfig.save_config(SID, {"service_id": SID, "rum": {"cid_salt": "attacker-chosen-salt-value"}})
    assert _salt() == before


# ── Merged from tests/core/test_reset.py ──

"""Tests for backend.core.reset.reset_service_logs — the Project Log Reset
("Delete Data") generator.

Focuses on the safety-critical guarantees called out in
local-docs/log_reset_design_plan.md: config preservation vs. operational
wipe, the per-service lock exclusion, the cron_busy guard, the
try/finally scheduler-resume-and-lock-release resilience under a mid-run
failure, and the iceberg/meta/ + raw/ cloud-purge scoping. Iceberg catalog
internals (init_iceberg_table / update_iceberg_view) are covered by their
own test modules, so they're stubbed here to keep this suite focused on
reset.py's own orchestration.
"""


import pytest

from backend.core import quarantine
from backend.core import reset as reset_mod
from backend.core.metadata.base import get_con
from tests.conftest import MOCK_SERVICE_ID


# ── Merged from tests/core/test_reset.py ──
def _save_config(**cron_sync_overrides):
    cron_sync = {"enabled": True, **cron_sync_overrides}
    svcconfig.save_config(
        MOCK_SERVICE_ID,
        {
            "service_id": MOCK_SERVICE_ID,
            "name": "test_service",
            "fos_bucket": "test-bucket",
            "fos_prefix": "",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test-key",
            "fos_secret_access_key": "test-secret",
            "access_level": "read_write",
            "provisioning": {"cron_sync": cron_sync},
        },
    )


def _seed_metadata_rows():
    """Insert one row into every WIPE table and every PRESERVE table."""
    con = get_con(MOCK_SERVICE_ID)
    con.execute(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, file_date) "
        "VALUES ('a.gz', ?, 10, 100, '2026-01-01')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO ingested_files_summary (source_name, file_count, total_rows, total_bytes) VALUES (?, 1, 10, 100)",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json) VALUES ('buf1', ?, '[]')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO quarantined_files (file_name, source_name, fos_key, error_key, meta_key) "
        "VALUES ('bad.gz', ?, 'raw/bad.gz', 'errors/bad.bad.jsonl', 'errors/bad.meta.json')",
        (MOCK_SERVICE_ID,),
    )

    con.execute("INSERT INTO sources (name, config, table_name) VALUES (?, '{}', 'logs')", (MOCK_SERVICE_ID,))
    con.execute(
        "INSERT INTO views (id, service_id, name, filters_json) VALUES ('v1', ?, 'My View', '{}')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO alerts (id, service_id, name, metric, operator, threshold, window_min) "
        "VALUES ('al1', ?, 'My Alert', 'error_rate', '>', 5.0, 10.0)",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO scoring_labels (id, service_id, sid, label) VALUES ('sl1', ?, 'session1', 'good')",
        (MOCK_SERVICE_ID,),
    )
    con.execute(
        "INSERT INTO scoring_audit (service_id, action, actor) VALUES (?, 'relabel', 'ui')",
        (MOCK_SERVICE_ID,),
    )
    con.execute("INSERT INTO asn_names (asn, name) VALUES (12345, 'Test ASN')")
    con.commit()


def _table_count(table: str) -> int:
    con = get_con(MOCK_SERVICE_ID)
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


@pytest.fixture
def _stub_iceberg_catalog(monkeypatch):
    """Stub the catalog-rebuild steps — exercised by test_iceberg.py, not here."""
    monkeypatch.setattr("backend.core.iceberg._core.init_iceberg_table", MagicMock())
    monkeypatch.setattr("backend.core.iceberg.view.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.get_connection", MagicMock())


@pytest.fixture
def fake_scheduler():
    """A stand-in for the ``reload_scheduler`` callback the router/CLI inject
    (real callers pass ``lambda: get_scheduler().reload()`` — see the
    layering note on ``reset_service_logs``)."""
    return MagicMock()


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_wipes_operational_preserves_config_and_meta(s3_mock, fos_source, fake_scheduler):
    _save_config()
    _seed_metadata_rows()
    evidence = quarantine.capture_evidence(
        MOCK_SERVICE_ID,
        "request",
        "raw/request/bad.gz",
        b'{"invalid":\x00}\n',
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid JSON",
    )
    evidence_path = quarantine._evidence_path(MOCK_SERVICE_ID, int(evidence["id"]))
    assert evidence_path.exists()
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/default/logs/data/foo.parquet", Body=b"x")
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/meta/admin_state.json", Body=b"{}")
    s3_mock.put_object(Bucket="test-bucket", Key="errors/bad.bad.jsonl", Body=b"bad")
    s3_mock.put_object(
        Bucket="test-bucket",
        Key="raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz",
        Body=b"raw",
    )

    events = list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler))

    assert events[-1]["type"] == "done"

    for table in (
        "ingested_files",
        "ingested_files_summary",
        "ingest_in_flight",
        "quarantined_files",
        "quarantine_evidence",
    ):
        assert _table_count(table) == 0, f"{table} should be wiped"
    assert not evidence_path.exists()

    for table in ("sources", "views", "alerts", "scoring_labels", "scoring_audit", "asn_names"):
        assert _table_count(table) == 1, f"{table} should be preserved"

    # record_audit("logs_reset") is written on top of the pre-seeded row.
    assert _table_count("audit_logs") == 1

    keys = {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}
    assert "iceberg/default/logs/data/foo.parquet" not in keys
    assert "iceberg/meta/admin_state.json" in keys, "iceberg/meta/ must survive the purge"
    assert "errors/bad.bad.jsonl" not in keys
    assert "raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz" in keys, (
        "raw/ must be left alone when delete_raw_logs=False (default)"
    )

    # Scheduler paused then resumed, and the config flag restored.
    assert fake_scheduler.call_count >= 2
    cfg = svcconfig.load_config(MOCK_SERVICE_ID)
    assert cfg["provisioning"]["cron_sync"]["enabled"] is True


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_delete_raw_logs_true_purges_raw_prefix(s3_mock, fos_source, fake_scheduler):
    _save_config()
    raw_key = "raw/request/year=2026/month=01/day=01/hour=00/minute=00/log.gz"
    s3_mock.put_object(Bucket="test-bucket", Key=raw_key, Body=b"raw")

    list(reset_mod.reset_service_logs(MOCK_SERVICE_ID, delete_raw_logs=True, reload_scheduler=fake_scheduler))

    keys = {o["Key"] for o in s3_mock.list_objects_v2(Bucket="test-bucket").get("Contents", [])}
    assert raw_key not in keys


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_cron_busy_blocks_reset(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    monkeypatch.setattr("backend.core.metadata.cron_busy", lambda service_id: True)

    gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
    with pytest.raises(RuntimeError, match="Active sync or commit"):
        next(gen)

    # The lock must not be left held after the guard raises.
    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1)
    lock.release()


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_concurrent_reset_is_excluded_by_service_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    """RLock is reentrant per-thread, so exclusion only shows up against a
    genuinely different thread holding the lock — the same-thread case
    would silently reacquire and defeat the point of this test."""
    import threading

    _save_config()
    monkeypatch.setattr(reset_mod, "_LOCK_TIMEOUT_S", 0.1)

    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    held = threading.Event()
    release = threading.Event()

    def _hold_lock():
        lock.acquire()
        held.set()
        release.wait(timeout=5)
        lock.release()

    holder = threading.Thread(target=_hold_lock, daemon=True)
    holder.start()
    held.wait(timeout=5)
    try:
        gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
        with pytest.raises(RuntimeError, match="Another operation is in progress"):
            next(gen)
    finally:
        release.set()
        holder.join(timeout=5)


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_error_mid_reset_still_resumes_scheduler_and_releases_lock(monkeypatch, s3_mock, fos_source, fake_scheduler):
    _save_config()
    # _purge_prefix only calls the delete_fn for pages that actually list
    # objects — seed one so the mocked failure is reached (an empty
    # bucket would otherwise never invoke it).
    s3_mock.put_object(Bucket="test-bucket", Key="iceberg/default/logs/data/foo.parquet", Body=b"x")
    monkeypatch.setattr(
        "backend.core.ingest._delete_objects_robust",
        MagicMock(side_effect=RuntimeError("FOS unreachable")),
    )

    gen = reset_mod.reset_service_logs(MOCK_SERVICE_ID, reload_scheduler=fake_scheduler)
    with pytest.raises(RuntimeError, match="FOS unreachable"):
        for _ in gen:
            pass

    cfg = svcconfig.load_config(MOCK_SERVICE_ID)
    assert cfg["provisioning"]["cron_sync"]["enabled"] is True, "must resume the paused sync even on failure"
    assert fake_scheduler.call_count >= 2

    from backend.core.iceberg.view import _get_service_lock

    lock = _get_service_lock(MOCK_SERVICE_ID)
    assert lock.acquire(timeout=0.1), "lock must be released even when the reset raises"
    lock.release()


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_no_service_raises_value_error(fake_scheduler):
    with pytest.raises(ValueError, match="No service found"):
        next(reset_mod.reset_service_logs("does-not-exist"))


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_purge_prefix_reports_progress_per_page_and_returns_total():
    """A large prefix (thousands of Iceberg data files) should show live
    progress per listed page instead of going silent until it's all done —
    the exact gap reported live during a real reset of a 100k+-file
    service. Uses a fake paginator so this doesn't depend on moto's
    default 1000-key page size."""

    class _FakePaginator:
        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": "iceberg/a.parquet"}, {"Key": "iceberg/b.parquet"}]}
            yield {"Contents": [{"Key": "iceberg/c.parquet"}]}

    class _FakeFos:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return _FakePaginator()

    delete_calls = []

    def _fake_delete(fos_client, bucket, keys):
        delete_calls.append(list(keys))
        return len(keys)

    gen = reset_mod._purge_prefix(_FakeFos(), "bucket", "iceberg/", _fake_delete, label="thing(s)")
    result = _drain(gen)

    assert result["return"] == 3
    assert delete_calls == [["iceberg/a.parquet", "iceberg/b.parquet"], ["iceberg/c.parquet"]]
    # One status event per non-empty page, each showing the running total.
    assert [e["message"] for e in result["yielded"]] == [
        "Deleted 2 thing(s) so far...",
        "Deleted 3 thing(s) so far...",
    ]


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_purge_prefix_excludes_prefix_and_skips_empty_pages():
    class _FakePaginator:
        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": "iceberg/meta/admin_state.json"}]}  # excluded, page has no deletable keys
            yield {"Contents": []}
            yield {"Contents": [{"Key": "iceberg/data/x.parquet"}]}

    class _FakeFos:
        def get_paginator(self, name):
            return _FakePaginator()

    def _fake_delete(fos_client, bucket, keys):
        return len(keys)

    gen = reset_mod._purge_prefix(
        _FakeFos(), "bucket", "iceberg/", _fake_delete, exclude_prefix="iceberg/meta/", label="thing(s)"
    )
    result = _drain(gen)
    assert result["return"] == 1
    assert [e["message"] for e in result["yielded"]] == ["Deleted 1 thing(s) so far..."]


def _drain(gen):
    yielded = []
    try:
        while True:
            yielded.append(next(gen))
    except StopIteration as stop:
        return {"yielded": yielded, "return": stop.value}


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_remove_tree_with_progress_reports_batches_and_removes_everything(tmp_path):
    for i in range(5):
        (tmp_path / f"f{i}.parquet").write_bytes(b"x")

    gen = reset_mod._remove_tree_with_progress(str(tmp_path), batch_size=2)
    result = _drain(gen)

    assert result["return"] == 5
    # One progress event every 2 files removed (5 files -> events at 2 and 4).
    assert [e["message"] for e in result["yielded"]] == [
        "Removed 2 local cache files so far...",
        "Removed 4 local cache files so far...",
    ]
    assert not os.path.exists(tmp_path)


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_rum_pause_and_restore_flags_round_trip_without_inventing_rum_enabled():
    """Restoring after a RUM reset must not create ``rum.enabled=False`` when the key never existed:
    ``is_rum_enabled`` gives ``rum.enabled`` precedence, so a fabricated False silently disables RUM."""
    from backend.config import is_rum_enabled
    from backend.core.reset import _pause_rum_flags, _restore_rum_flags

    cfg = {"rum_enabled": True, "rum": {"faro_version": "1.2.3"}}
    assert is_rum_enabled(cfg)

    paused = _pause_rum_flags(cfg)
    assert paused is not None
    assert not is_rum_enabled(cfg)

    _restore_rum_flags(cfg, paused)
    assert "enabled" not in cfg["rum"]
    assert cfg["rum_enabled"] is True
    assert is_rum_enabled(cfg)


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_rum_pause_restore_keeps_an_explicit_rum_enabled_value():
    from backend.core.reset import _pause_rum_flags, _restore_rum_flags

    cfg = {"rum_enabled": False, "rum": {"enabled": True}}
    paused = _pause_rum_flags(cfg)
    assert paused is not None
    _restore_rum_flags(cfg, paused)
    assert cfg["rum"]["enabled"] is True
    assert cfg["rum_enabled"] is False


@pytest.mark.usefixtures("_stub_iceberg_catalog")
def test_rum_pause_is_a_no_op_when_rum_is_not_enabled():
    from backend.core.reset import _pause_rum_flags

    cfg: dict = {"rum": {}}
    assert _pause_rum_flags(cfg) is None
    assert cfg == {"rum": {}}


# ── Merged from tests/services/test_service_manager.py ──
def _wait_for(condition_fn, timeout: float = 2.0, interval: float = 0.01) -> bool:
    """Spin until condition_fn() returns truthy or timeout. Avoids
    sleeping for fixed durations in tests that depend on a background
    thread completing."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition_fn():
            return True
        time.sleep(interval)
    return False


def test_get_dir_stats_returns_zero_for_missing_path(tmp_path, monkeypatch):
    """Nonexistent paths settle on (0, 0). The cache stores this so we
    don't re-walk on every subsequent call (a deleted cache dir would
    otherwise cost a background walk on every TTL expiry)."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    missing = str(tmp_path / "does-not-exist")
    # Cold call: (0, 0) placeholder immediately, real (missing-path) walk
    # result lands on the background thread — which also happens to be
    # (0, 0), so this assertion holds on the very first call too.
    assert sm._get_dir_stats(missing) == (0, 0)
    assert _wait_for(lambda: missing in sm._dir_stats_cache)


def test_get_dir_stats_counts_files_recursively(tmp_path, monkeypatch):
    """Walk visits subdirs and returns (total_bytes, file_count).
    Pinned because subdirectory recursion is the actual cost driver
    on cache/ which has 22k+ files across 22k dirs."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.parquet").write_bytes(b"y" * 200)
    (sub / "c.parquet").write_bytes(b"z" * 50)

    path = str(tmp_path)
    # Cold call returns the (0, 0) placeholder immediately; the real
    # count lands once the background walk finishes.
    assert _wait_for(lambda: sm._get_dir_stats(path) == (350, 3))


def test_get_dir_stats_returns_cached_value_within_ttl(tmp_path, monkeypatch):
    """Within TTL, cache hit returns the cached value without re-walking.
    This is the steady-state hot path."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    # First call is cold (returns (0, 0) immediately); wait for the
    # background walk to populate the real value before proceeding.
    sm._get_dir_stats(path)
    assert _wait_for(lambda: sm._get_dir_stats(path) == (100, 1))

    # Add a file AFTER caching. If the cache is used the new file should
    # NOT appear in the result.
    (tmp_path / "b.parquet").write_bytes(b"y" * 200)
    size, count = sm._get_dir_stats(path)
    assert count == 1, "cached value should ignore the newly-added file within TTL"
    assert size == 100


def test_get_dir_stats_returns_stale_value_and_refreshes_in_background(tmp_path, monkeypatch):
    """Stale-while-revalidate: when the cache entry is expired but
    present, return the stale value IMMEDIATELY and kick off a
    background refresh. The next call (after the bg refresh lands)
    sees the fresh value.

    This is the key cold-load mitigation. Without SWR the user pays
    the full ~700ms walk every time the cache expires (every 60s in
    the old code, every 5 min in the new code)."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    # Prime the cache with stale data.
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 999, 999)

    # Add real files post-priming so we can detect when the bg refresh runs.
    (tmp_path / "b.parquet").write_bytes(b"y" * 50)

    t0 = time.monotonic()
    size, count = sm._get_dir_stats(path)
    elapsed = time.monotonic() - t0

    # Critical: the call returned the STALE value (999/999), not the
    # fresh value (2 files / 150 bytes). The bg thread is still walking.
    assert (size, count) == (999, 999), (
        f"SWR must return STALE value on expired cache, not block on walk. Got ({size}, {count})."
    )
    # Returned essentially instantly (no walk on the foreground path).
    assert elapsed < 0.5, f"SWR foreground path must NOT block on the walk; took {elapsed:.3f}s"

    # The background refresh should land within the timeout, updating
    # the cache with the actual values (2 files / 150 bytes).
    assert _wait_for(lambda: sm._dir_stats_cache.get(path, (0, 0, 0))[2] == 2, timeout=2.0), (
        f"background refresh did not land within 2s. Cache state: {sm._dir_stats_cache.get(path)}"
    )

    # Next call sees the refreshed value.
    size2, count2 = sm._get_dir_stats(path)
    assert (size2, count2) == (150, 2), (
        f"after bg refresh, next call should return fresh value. Got ({size2}, {count2})."
    )


def test_get_dir_stats_first_ever_call_is_non_blocking(tmp_path, monkeypatch):
    """No cache entry yet → return the (0, 0) placeholder immediately
    (never block on the walk) and populate the real value on a
    background thread.

    This inverts the original contract on purpose: walking synchronously
    on first call was "fine" for a few thousand files, but for a service
    with 100k+ cache files it turned the very first post-restart
    /api/bootstrap or /api/services call into a multi-minute block —
    see the 2026-08-03 incident where a 162k-file cache dir made
    /admin unreachable for ~3 minutes after every deploy. The cost is
    now paid off the request path, every time, including the first."""
    import time as _time

    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 42)
    path = str(tmp_path)

    t0 = _time.monotonic()
    size, count = sm._get_dir_stats(path)
    elapsed = _time.monotonic() - t0

    assert (size, count) == (0, 0), "first-ever call must return the placeholder, not block on the walk"
    assert elapsed < 0.5, f"first-ever call must not block on the walk; took {elapsed:.3f}s"

    assert _wait_for(lambda: sm._get_dir_stats(path) == (42, 1)), (
        "background walk did not populate the real value within the timeout"
    )


def test_get_dir_stats_coalesces_concurrent_refreshes(tmp_path, monkeypatch):
    """When two threads both see a stale entry at the same instant,
    only ONE background refresh should fire — guarded by
    _dir_stats_refresh_in_flight. Otherwise on a 50-tenant fleet with
    50 concurrent /api/bootstrap calls we'd spawn 50 walk threads,
    saturating the filesystem with redundant work."""
    import threading

    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    # Patch the worker to count invocations and block briefly so
    # concurrent SWR calls overlap with the in-flight marker.
    refresh_count = {"n": 0}
    real_walk = sm._walk_dir_stats

    def _slow_walk(p):
        refresh_count["n"] += 1
        time.sleep(0.05)
        return real_walk(p)

    monkeypatch.setattr(sm, "_walk_dir_stats", _slow_walk)

    # Prime stale entry.
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 0, 0)

    # Fire 10 concurrent SWR-triggering calls.
    threads = [threading.Thread(target=lambda: sm._get_dir_stats(path)) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Wait for the (single) background refresh to land.
    assert _wait_for(lambda: len(sm._dir_stats_refresh_in_flight) == 0, timeout=2.0)

    assert refresh_count["n"] == 1, (
        f"coalescing failed: {refresh_count['n']} background walks fired for 10 concurrent calls "
        f"(expected exactly 1). The _dir_stats_refresh_in_flight guard is broken."
    )


def test_get_dir_stats_coalesces_cold_first_arrivals(tmp_path, monkeypatch):
    """When the cache is empty and N threads call _get_dir_stats(path)
    simultaneously, only ONE background walk should fire — the same
    `_dir_stats_refresh_in_flight` guard used for stale-refresh
    coalescing also covers this case, since the cold path no longer has
    a separate blocking code path of its own.

    Pinned because on a fleet cold-start (backend just rebooted, 50
    /api/bootstrap calls land in the first second), without this
    coalescing we'd fire 50 parallel walks for the same dir. Every
    caller gets the (0, 0) placeholder immediately now (none of them
    block), so this only asserts the walk count + eventual convergence,
    not that every caller's return value is already populated."""
    import threading

    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    (tmp_path / "a.parquet").write_bytes(b"x" * 100)
    path = str(tmp_path)

    walk_count = {"n": 0}
    real_walk = sm._walk_dir_stats

    walk_blocker = threading.Event()

    def _slow_walk(p):
        walk_count["n"] += 1
        walk_blocker.wait(timeout=5.0)  # Wait until all threads have made their call
        return real_walk(p)

    monkeypatch.setattr(sm, "_walk_dir_stats", _slow_walk)

    results = []
    results_lock = threading.Lock()

    call_barrier = threading.Barrier(11)  # 10 threads + main thread

    def _call():
        call_barrier.wait(timeout=5.0)
        r = sm._get_dir_stats(path)
        with results_lock:
            results.append(r)

    threads = [threading.Thread(target=_call) for _ in range(10)]
    for t in threads:
        t.start()

    # Wait for all threads to be ready to call
    call_barrier.wait(timeout=5.0)

    # Give them a tiny bit of time to actually make the call and get the placeholder
    time.sleep(0.1)

    # Unblock the background walk
    walk_blocker.set()

    for t in threads:
        t.join()

    assert walk_count["n"] == 1, (
        f"cold-path coalescing failed: {walk_count['n']} walks fired for 10 concurrent first-arrivals "
        f"(expected exactly 1). The _dir_stats_refresh_in_flight guard is broken."
    )
    # None of the concurrent callers blocked, so (0, 0) placeholders are
    # expected here — the real assertion is that the walk converges.
    assert all(r == (0, 0) for r in results), f"cold callers must get the placeholder, not block. Got {results}"
    assert _wait_for(lambda: sm._get_dir_stats(path) == (100, 1)), "background walk never converged on the real value"


def test_get_dir_stats_recovers_from_thread_start_failure(tmp_path, monkeypatch):
    """When Thread.start() raises (resource exhaustion, OS thread-limit),
    the in-flight marker must be released so the next reader can try
    again. Otherwise the cache becomes permanently stuck serving stale
    data until process restart — exactly the worst failure mode for a
    self-healing SWR design."""
    from backend.services import service_manager as sm

    monkeypatch.setattr(sm, "_dir_stats_cache", {})
    monkeypatch.setattr(sm, "_dir_stats_refresh_in_flight", set())

    path = str(tmp_path / "doesnt-matter")
    # Seed stale entry so the SWR branch fires.
    sm._dir_stats_cache[path] = (time.monotonic() - sm._DIR_STATS_TTL_SEC - 10, 7, 7)

    # Patch threading.Thread on the service_manager module so .start() raises.
    class _ExplodingThread:
        def __init__(self, *a, **kw):
            pass

        def start(self):
            raise RuntimeError("can't start new thread (simulated resource exhaustion)")

    monkeypatch.setattr(sm.threading, "Thread", _ExplodingThread)

    # First SWR call: returns stale, tries to schedule, fails to start.
    result = sm._get_dir_stats(path)
    assert result == (7, 7), "stale value must still be served when thread start fails"

    # The critical invariant: in_flight must NOT contain the path after
    # the failed start, so the next reader can try again.
    assert path not in sm._dir_stats_refresh_in_flight, (
        f"path stuck in _dir_stats_refresh_in_flight after Thread.start() failure — "
        f"cache is now permanently stuck serving stale. Set state: {sm._dir_stats_refresh_in_flight}"
    )


def test_dir_stats_ttl_is_long_enough_for_tab_idle(tmp_path, monkeypatch):
    """Pins _DIR_STATS_TTL_SEC ≥ 300 (5 minutes). The whole point of
    the bump from 60s → 300s is that a typical tab-idle (coffee break)
    no longer pays the cold-walk cost. Regressing this to 60s would
    silently undo half the SWR win."""
    from backend.services import service_manager as sm

    assert sm._DIR_STATS_TTL_SEC >= 300, (
        f"_DIR_STATS_TTL_SEC must be ≥ 300s so tab-idle doesn't pay the cold walk "
        f"on the next /api/bootstrap. Got {sm._DIR_STATS_TTL_SEC}s."
    )


def test_bust_dir_stats_cache_is_not_defined(monkeypatch):
    """The _bust_dir_stats_cache function was dead code (defined, never
    called). The SWR design makes manual invalidation unnecessary —
    every read serves stale + schedules a refresh, so the cache is
    self-healing within one TTL window. Pinned so a future copy-paste
    that re-introduces _bust_dir_stats_cache also re-introduces a
    code reviewer prompt to question whether it's actually needed."""
    from backend.services import service_manager as sm

    assert not hasattr(sm, "_bust_dir_stats_cache"), (
        "_bust_dir_stats_cache was removed as part of the SWR refactor — "
        "the cache is self-healing via stale-while-revalidate. If you genuinely "
        "need manual invalidation (e.g. for an immediate-update UX flow), "
        "document the use case in a comment AND check that the call site "
        "actually matters (the previous incarnation had zero callers across "
        "the entire codebase, including tests)."
    )
