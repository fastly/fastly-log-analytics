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
from backend.utils.bounded_cache import CacheRegistry

from backend.core import session_token as st
from backend.models.common import FilterSpec
from backend.repositories.utils.filters import build_where_clause


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
