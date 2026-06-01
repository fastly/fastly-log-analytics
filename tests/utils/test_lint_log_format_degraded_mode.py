"""Degraded-mode falco fallback contract.

``lint_log_format`` has two paths:

1. **JSON path** (always runs): rejects empty/newline/non-object/missing-
   timestamp.
2. **Falco path** (only when the ``falco`` binary is on ``PATH``): adds
   deeper VCL-syntax checking.

CI sets ``FALCO_REQUIRED=1`` so the falco path is always exercised
upstream. But analysts running the tool against a local checkout often
don't have falco installed — they hit the JSON-only path. This test
pins the asymmetry so it's clear which inputs are caught by which path,
and prevents a future refactor from quietly making the degraded path
*stricter* than the falco path (which would block legitimate inputs
from offline users) or *more permissive* in dangerous ways
(quietly accepting malformed VCL that CI flags).

The contract:

* Every input the JSON path REJECTS must also be rejected by the
  falco path. The two agree on the "definitely broken" set.
* The falco path may additionally reject things the JSON path accepts
  (deeper VCL semantics). Those are documented blind spots of the
  degraded path, not bugs.

Closes TESTING_PLAN_3 item 18.
"""

from __future__ import annotations

import os
import shutil

import pytest

from backend.utils.vcl_utils import lint_log_format

FALCO_INSTALLED = shutil.which("falco") is not None
FALCO_REQUIRED = os.environ.get("FALCO_REQUIRED") == "1"


# ── Fixtures: minimal set covering both paths ────────────────────────────────


VALID = '{"timestamp":"%{time.start}V","ip":"%{client.ip}V"}'
"""Well-formed format that BOTH paths should accept.

Notably we use ``%{time.start}V`` (a regular VCL expression macro) and
NOT ``%{...}t`` (Fastly-specific time formatting). The ``t`` macro
flavor is valid Fastly logging syntax but ``log_format_to_vcl_log``
emits it as a quoted literal that falco doesn't recognize as a
well-formed VCL log statement. The disagreement is between
``log_format_to_vcl_log`` and falco's grammar, not the linter — and
fixing it is out of scope here. Pinning the ``V``-macro path is
sufficient to lock the contract.
"""

JSON_REJECTS = [
    pytest.param("", id="empty"),
    pytest.param('{"timestamp":"x"}\n', id="newline"),
    pytest.param('"not_an_object"', id="root_not_object"),
    pytest.param('{"ip":"%{client.ip}V"}', id="missing_timestamp"),
]
"""Inputs the JSON path catches without Falco's help.

The contract: every one of these must ALSO be rejected by the falco
path. Falco rejecting these is a sanity check that the JSON path
isn't *over-rejecting* relative to CI.
"""


# ── Asymmetry contract ──────────────────────────────────────────────────────


def test_valid_format_accepted_by_json_path():
    """Sanity: the canonical-valid fixture is accepted by the JSON path.
    (The full lint_log_format may delegate to falco even when the binary
    is on PATH; mock it out so we only exercise the JSON branch here.)"""
    from unittest.mock import patch

    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, _ = lint_log_format(VALID)
    assert ok is True


@pytest.mark.parametrize("fmt", JSON_REJECTS)
def test_json_path_rejects(fmt):
    from unittest.mock import patch

    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, msg = lint_log_format(fmt)
    assert ok is False, (
        f"JSON path should reject {fmt!r} but accepted with msg={msg!r}. "
        f"If this rejection moved to the falco-only path, update the test "
        f"deliberately."
    )


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco not on PATH")
def test_valid_format_accepted_by_falco_path():
    """Same valid fixture; falco path also accepts. Pins the agreement
    on the 'definitely good' set."""
    ok, _ = lint_log_format(VALID)
    assert ok is True


@pytest.mark.skipif(not FALCO_INSTALLED, reason="falco not on PATH")
@pytest.mark.parametrize("fmt", JSON_REJECTS)
def test_falco_path_also_rejects_what_json_rejects(fmt):
    """**The core contract.** Every input the JSON path rejects must ALSO
    be rejected by the falco path. If this fails, it means the falco
    path is *more permissive* on a specific case — which would let bad
    input through CI when CI is the only gate. Investigate immediately.
    """
    ok, msg = lint_log_format(fmt)
    assert ok is False, (
        f"asymmetry: falco path ACCEPTED {fmt!r} but JSON path rejects it "
        f"(msg={msg!r}). The two paths disagree on the 'definitely broken' "
        f"set — degraded-mode users would be more strict than CI. Either "
        f"update the JSON path to allow this, or fix the falco path to "
        f"reject it."
    )


# ── Documented blind spot: things only falco catches ─────────────────────────


VCL_SYNTAX_BROKEN = '{"timestamp":"%{this_is_not_valid_vcl}V"}'
"""Looks like JSON; macro contents are not a valid VCL expression. The
JSON path can't catch this (it only looks at structure). The falco path
should catch it. This is the documented degraded-mode blind spot — we
pin it explicitly so reviewers understand what they lose without the
linter."""


def test_json_path_misses_invalid_vcl_macro_contents():
    """Documented blind spot of the degraded path: JSON-only validation
    can't reject macros whose contents would be a VCL syntax error."""
    from unittest.mock import patch

    with patch("backend.utils.vcl_utils.shutil.which", return_value=None):
        ok, _ = lint_log_format(VCL_SYNTAX_BROKEN)
    # Today's behavior: JSON path accepts. If a future refactor adds
    # deeper macro-content validation to the JSON path that closes this
    # blind spot, update the assertion deliberately.
    assert ok is True, (
        "JSON-only path used to accept syntactically-invalid VCL macros. "
        "If this changed (great!), update the test to pin the new contract."
    )
