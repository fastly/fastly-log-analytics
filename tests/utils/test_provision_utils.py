"""Tests for ``backend.provision.utils`` — CLI-side helpers for the
interactive ``provision_cli.py`` tool.

The print helpers (``ok``, ``fail``, ``info``, ``warn``, ``banner``,
``step``, ``blank``) are trivial ANSI-coded ``print`` wrappers, so we
pin only their stream selection (stderr for ``fail``) and structural
shape — not the exact escape-code bytes, which would just relock the
colour palette without catching anything.

The real value here is the interactive prompts (``ask``, ``ask_yes``,
``ask_int``) — their retry / default / validation logic was previously
uncovered, and a regression would silently corrupt provisioning input.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from backend.provision import utils as pu

# ── _mask: secret-friendly truncation ────────────────────────────────────────


def test_mask_empty_returns_empty():
    assert pu._mask("") == ""


def test_mask_short_string_unchanged():
    """Strings shorter than ``visible`` are returned as-is — there's no
    secret to hide if the whole value would fit."""
    assert pu._mask("short", visible=8) == "short"
    # Exactly at the boundary: still unchanged
    assert pu._mask("12345678", visible=8) == "12345678"


def test_mask_long_string_truncated_with_ellipsis():
    assert pu._mask("supersecretvalue", visible=8) == "supersec..."


def test_mask_respects_custom_visible():
    assert pu._mask("supersecretvalue", visible=4) == "supe..."


# ── _highlight: regex-driven quote highlighting ──────────────────────────────


def test_highlight_wraps_single_quoted_substring_in_magenta():
    out = pu._highlight("created service 'svc-123'")
    assert pu.MAG in out  # magenta colour wraps the quoted name
    assert "svc-123" in out
    assert pu.RST in out


def test_highlight_leaves_already_coloured_strings_alone():
    """If the message already contains ANSI escapes, don't double-wrap.
    Otherwise the second pass would corrupt the colour reset codes."""
    pre_coloured = f"{pu.GRN}already coloured{pu.RST}"
    assert pu._highlight(pre_coloured) == pre_coloured


def test_highlight_handles_message_without_quotes():
    """No quotes → no change other than the wrapping the regex would
    have made. Pinned because the regex must be a no-op, not crash."""
    assert pu._highlight("no quotes here") == "no quotes here"


# ── ask: prompt + default handling ───────────────────────────────────────────


def test_ask_returns_user_input_when_provided():
    with patch("builtins.input", return_value="user-value"):
        assert pu.ask("what?") == "user-value"


def test_ask_returns_default_on_empty_input():
    """Bare Enter → fall back to default. Pinned because provisioning
    relies heavily on ``[default]`` prompts to skip past boilerplate."""
    with patch("builtins.input", return_value=""):
        assert pu.ask("region?", default="us-east-1") == "us-east-1"


def test_ask_returns_empty_string_when_no_default_and_no_input():
    """No default and no input → empty string (not None) so callers can
    safely call ``.strip()``, ``len()``, etc. without a None-check."""
    with patch("builtins.input", return_value=""):
        assert pu.ask("optional?") == ""


def test_ask_strips_whitespace_from_input():
    with patch("builtins.input", return_value="  value  "):
        assert pu.ask("what?") == "value"


# ── ask_yes: Y/n parsing ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "answer,expected",
    [
        ("y", True),
        ("Y", True),
        ("yes", True),
        ("YES", True),
        ("n", False),
        ("N", False),
        ("no", False),
        ("NO", False),
        ("", True),  # default=True (the default default)
    ],
)
def test_ask_yes_with_default_true(answer, expected):
    with patch("builtins.input", return_value=answer):
        assert pu.ask_yes("ok?") is expected


def test_ask_yes_default_false_returns_false_on_empty():
    with patch("builtins.input", return_value=""):
        assert pu.ask_yes("ok?", default=False) is False


def test_ask_yes_non_y_prefix_treated_as_no():
    """Anything not starting with 'y' is false. Pinned because a
    permissive interpretation here would let a typo (``q``, ``ok``)
    accidentally confirm a destructive action."""
    with patch("builtins.input", return_value="maybe"):
        assert pu.ask_yes("destroy?", default=False) is False


# ── ask_int: validation retry loop ───────────────────────────────────────────


def test_ask_int_returns_parsed_integer():
    with patch("builtins.input", return_value="42"):
        assert pu.ask_int("n?", default=10) == 42


def test_ask_int_returns_default_on_empty_input():
    with patch("builtins.input", return_value=""):
        assert pu.ask_int("n?", default=7) == 7


def test_ask_int_retries_on_non_integer():
    """First input is garbage → loop, second is valid → return. The
    operator should never get a Python traceback for a typo."""
    inputs = iter(["abc", "9"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0) == 9


def test_ask_int_rejects_below_min_then_accepts():
    inputs = iter(["3", "10"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0, min_val=5) == 10


def test_ask_int_rejects_above_max_then_accepts():
    inputs = iter(["999", "50"])
    with patch("builtins.input", side_effect=lambda _: next(inputs)):
        assert pu.ask_int("n?", default=0, max_val=100) == 50


# ── Print helpers: structural pinning (no escape-code lock-in) ──────────────


def test_fail_writes_to_stderr(capsys):
    """``fail`` goes to stderr so the CLI ``set -o pipefail`` users
    catch errors. The other helpers default to stdout."""
    pu.fail("boom")
    captured = capsys.readouterr()
    assert "boom" in captured.err
    assert captured.out == ""


def test_ok_writes_to_stdout(capsys):
    pu.ok("done")
    captured = capsys.readouterr()
    assert "done" in captured.out
    assert captured.err == ""


def test_blank_emits_newline(capsys):
    pu.blank()
    assert capsys.readouterr().out == "\n"


def test_step_includes_progress_and_title(capsys):
    pu.step(2, 5, "Configure Fastly")
    out = capsys.readouterr().out
    assert "[2/5]" in out
    assert "Configure Fastly" in out


def test_banner_draws_separator_around_title(capsys):
    pu.banner("Welcome")
    out = capsys.readouterr().out
    assert "Welcome" in out
    # Bar uses U+2501 BOX DRAWINGS HEAVY HORIZONTAL repeated 64 times
    assert "━" * 10 in out


# ── _c: trivial wrapper, exercised here so it's not "uncovered" ─────────────


def test_c_wraps_text_with_colour_and_reset():
    wrapped = pu._c(pu.GRN, "hello")
    assert wrapped.startswith(pu.GRN)
    assert wrapped.endswith(pu.RST)
    assert "hello" in wrapped
