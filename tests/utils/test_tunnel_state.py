"""Tests for :mod:`backend.utils.tunnel.state`.

Tiny module — three persistence helpers + a dataclass. Tests cover the
round-trip (persist → restore), the partial-state cases (missing file,
no endpoint), and the panic-cleanup case.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.utils.tunnel.state import (
    TunnelState,
    clear_persisted_state,
    persist_direct_state,
    restore_direct_state,
)


@pytest.fixture
def state_file(monkeypatch, tmp_path: Path) -> Path:
    """Redirect the persisted-state path to a per-test temp file."""
    # backend.config.DATA_DIR is what _state_file_path resolves against.
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path)
    return tmp_path / "tunnel_state.json"


def test_tunnel_state_default_values():
    s = TunnelState()
    assert s.public_endpoint is None
    assert s.forward_port == 3000
    assert s.direct_socket_addr is None


def test_persist_then_restore_round_trip(state_file: Path):
    s = TunnelState(public_endpoint="share.example.com", forward_port=8080)
    persist_direct_state(s)

    assert state_file.exists()
    data = json.loads(state_file.read_text())
    assert data == {
        "public_endpoint": "share.example.com",
        "forward_port": 8080,
    }

    restored = TunnelState()
    ok = restore_direct_state(restored)
    assert ok is True
    assert restored.public_endpoint == "share.example.com"
    assert restored.forward_port == 8080
    assert restored.direct_socket_addr == "0.0.0.0"
    # started_at is a recent ISO timestamp string.
    assert isinstance(restored.started_at, str)
    assert restored.started_at  # non-empty


def test_persist_swallows_io_errors(monkeypatch):
    """persist_direct_state is best-effort — an IO failure must not
    propagate up to the tunnel manager and break setup."""
    from backend.utils.tunnel import state as state_mod

    monkeypatch.setattr(state_mod, "_state_file_path", lambda: "/nonexistent/dir/x.json")
    # Must not raise — exception is logged + swallowed.
    persist_direct_state(TunnelState(public_endpoint="x"))


def test_clear_removes_existing_file(state_file: Path):
    state_file.write_text("{}")
    assert state_file.exists()
    clear_persisted_state()
    assert not state_file.exists()


def test_clear_is_noop_when_file_missing(state_file: Path):
    assert not state_file.exists()
    # Must not raise.
    clear_persisted_state()


def test_clear_swallows_io_errors(monkeypatch):
    from backend.utils.tunnel import state as state_mod

    # Make the path point to something that exists but can't be removed
    # (a directory, in this case — os.remove on a dir raises OSError).
    monkeypatch.setattr(state_mod, "_state_file_path", lambda: "/")
    # Must not raise — error is logged.
    clear_persisted_state()


def test_restore_returns_false_when_no_file(state_file: Path):
    s = TunnelState()
    assert restore_direct_state(s) is False
    # State unchanged.
    assert s.public_endpoint is None


def test_restore_returns_false_when_endpoint_empty(state_file: Path):
    state_file.write_text(json.dumps({"forward_port": 3000}))
    s = TunnelState()
    assert restore_direct_state(s) is False
    assert s.public_endpoint is None


def test_restore_returns_false_when_json_invalid(state_file: Path):
    state_file.write_text("{not-json")
    s = TunnelState()
    # JSON parse error swallowed → returns False.
    assert restore_direct_state(s) is False


def test_restore_defaults_forward_port_when_missing(state_file: Path):
    state_file.write_text(json.dumps({"public_endpoint": "a.b.c"}))
    s = TunnelState()
    assert restore_direct_state(s) is True
    assert s.forward_port == 3000  # default kicks in
