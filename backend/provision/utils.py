"""Provision CLI print/prompt helpers.

Phase 10.5 routes the actual stdout/stderr emit through rich's
``Console`` so the wizard output gets rich's TTY-aware wrapping +
auto-detection of terminal width, while every public API in this
module stays byte-compatible with the pre-rich implementation:

  - The ANSI ``BOLD``/``DIM``/colour constants stay intact (callers
    in fastly_api.py, fos_setup.py, orchestrator.py, and the
    session-scoring orchestrator/setup modules import them by name
    and weave them into messages they then pass to ``info``/``ok``/
    ``warn``/``fail``).
  - ``_c(color, text)`` still wraps with the raw ANSI escapes so
    ``_highlight`` can detect already-coloured input and skip
    re-wrapping (the ``"\\033" in msg`` short-circuit).
  - ``blank()`` still emits exactly ``"\\n"`` (test_blank_emits_newline
    asserts the exact byte).
  - ``fail()`` still writes to stderr, ``ok()``/``info()``/``warn()``
    still write to stdout (capsys-pinned in test_provision_utils.py).

What rich actually buys here is consistent terminal width handling
and graceful degradation when the output is piped (no escape codes
on non-TTY) — the Console instances are configured with
``force_terminal=None`` so rich's own detection wins.
"""

from __future__ import annotations

import logging
import queue
import sys
import threading
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from rich.console import Console

# Two Consoles so fail() can route to stderr while the other helpers
# stay on stdout. ``highlight=False`` disables rich's repr-style
# auto-colourisation (numbers in cyan, etc.) — we already inject our
# own colour via the ANSI constants below and don't want a double pass.
# ``markup=False`` keeps rich's "[bold]" syntax inert so any user-
# supplied message containing literal brackets renders verbatim.
_stdout = Console(highlight=False, markup=False, soft_wrap=True)
_stderr = Console(stderr=True, highlight=False, markup=False, soft_wrap=True)

# ── ANSI colour helpers ────────────────────────────────────────────────────────
# Kept as raw escape strings (rather than rich's named styles) because
# downstream modules — fastly_api, fos_setup, session_scoring — import
# these constants directly and splice them into f-strings:
#   info(f"  {_c(BLU, 'Target:')} {value}")
# Switching to rich.style.Style here would break those call sites.

BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[91m"
GRN = "\033[92m"
YLW = "\033[93m"
BLU = "\033[94m"
MAG = "\033[95m"
CYN = "\033[96m"
RST = "\033[0m"


def _c(color, text):
    return f"{color}{text}{RST}"


def _highlight(msg):
    # Short-circuit when the caller has already injected ANSI escapes —
    # double-wrapping would corrupt the RST sequencing.
    if "\033" in msg:
        return msg
    import re

    msg = re.sub(r"'([^']+)'", rf"'{_c(MAG, r'\1')}'", msg)
    return msg


def _emit(console: Console, line: str) -> None:
    """Single write path through rich. Using ``console.print`` (not
    ``sys.stdout.write``) makes rich responsible for newline + flush,
    which keeps the blank()/banner()/step() helpers consistent."""
    console.print(line, end="\n", soft_wrap=True)


def ok(msg):
    _emit(_stdout, f"  {_c(GRN + BOLD, '✓')}  {_highlight(msg)}")


def fail(msg):
    _emit(_stderr, f"  {_c(RED + BOLD, '✗')}  {_c(RED, msg)}")


def info(msg):
    _emit(_stdout, f"  {_c(BLU + BOLD, '→')}  {_highlight(msg)}")


def warn(msg):
    _emit(_stdout, f"  {_c(YLW + BOLD, '⚠')}  {_c(YLW, msg)}")


def blank():
    # Plain print on purpose: test_blank_emits_newline asserts the
    # captured stdout equals exactly "\n". rich.console.print() would
    # also emit "\n" today, but ``print`` is the byte-stable choice.
    sys.stdout.write("\n")


def step(n, total, title):
    blank()
    _emit(_stdout, f"{_c(BOLD + MAG, f'[{n}/{total}]')} {_c(BOLD + CYN, title)}")


def banner(title):
    bar = "━" * 64
    blank()
    _emit(_stdout, _c(MAG + BOLD, bar))
    _emit(_stdout, _c(CYN + BOLD, f"  {title}"))
    _emit(_stdout, _c(MAG + BOLD, bar))


def ask(question, default=None):
    suffix = f" {_c(DIM, f'[{default}]')}" if default is not None else ""
    prompt_str = f"  {_c(CYN, '?')}  {question}{suffix}: "
    raw = input(prompt_str).strip()
    return raw if raw else (default if default is not None else "")


def ask_yes(question, default=True):
    hint = _c(DIM, "Y/n" if default else "y/N")
    raw = input(f"  {_c(CYN, '?')}  {question} [{hint}]: ").strip().lower()
    if not raw:
        return default
    return raw.startswith("y")


def ask_int(question, default, min_val=None, max_val=None):
    while True:
        raw = ask(question, default)
        try:
            val = int(raw)
            if min_val is not None and val < min_val:
                warn(f"Value must be at least {min_val}")
                continue
            if max_val is not None and val > max_val:
                warn(f"Value must be at most {max_val}")
                continue
            return val
        except ValueError:
            warn("Please enter a valid integer.")


def run_with_events(func, *args, raise_on_error: bool = True, **kwargs):
    q: queue.Queue[str | None] = queue.Queue()
    kwargs["status_cb"] = q.put
    res = []
    err = []

    def worker():
        try:
            res.append(func(*args, **kwargs))
        except Exception as e:
            err.append(e)
        finally:
            q.put(None)

    t = threading.Thread(target=worker)
    t.start()

    while True:
        msg = q.get()
        if msg is None:
            break
        yield {"type": "status", "message": msg}

    t.join()
    if err:
        if raise_on_error:
            raise err[0]
        yield {"type": "error", "message": str(err[0])}
        return None
    return res[0]


class VclBuilder:
    """Line- and section-joining helpers for VCL generators."""

    @staticmethod
    def join_lines(items: Iterable[str]) -> str:
        """``"\\n"``-join an iterable of already-rendered lines."""
        return "\n".join(items)

    @staticmethod
    def join_sections(sections: Iterable[str]) -> str:
        """``"\\n\\n"``-join sections, dropping blank/whitespace-only ones."""
        return "\n\n".join(s for s in sections if s.strip())


# ── Canonical FOS log object path templates (merged from log_paths.py) ────────
# Every producer of a logging-endpoint ``path`` (initial provisioning, the
# declarative generator/reconciler, and the admin logging-settings update flow)
# must build it here. Historically three call sites embedded two different
# layouts, so a settings update could silently switch a bucket's layout
# mid-stream and break incremental discovery.
#
# Layout (strftime tokens are expanded by Fastly's log aggregators, which then
# append a unique ``<ISO-timestamp>-<id>.log.gz`` suffix per delivered object):
#
#     raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/
#
# Because the minute lives in the object-name prefix, a dispatcher can bound a
# LIST to one closed minute with :func:`minute_list_prefix` — no directory-level
# repartitioning needed.

ANALYTICS_LOG_LAYOUT = "raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
RUM_LOG_LAYOUT = "raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


def analytics_log_path() -> str:
    """Logging-endpoint ``path`` for the main analytics log stream."""
    return ANALYTICS_LOG_LAYOUT


def rum_log_path() -> str:
    """Logging-endpoint ``path`` for the RUM beacon log stream."""
    return RUM_LOG_LAYOUT


def minute_list_prefix(dt: datetime) -> str:
    """S3 LIST ``Prefix`` bounding exactly one closed minute of analytics logs."""
    return dt.strftime("raw/request/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")


def rum_minute_list_prefix(dt: datetime) -> str:
    """S3 LIST ``Prefix`` bounding exactly one closed minute of RUM beacon logs.

    RUM counterpart of :func:`minute_list_prefix` — used by the celery-mode
    RUM ledger discovery job (``backend.cron.jobs.rum_ledger``) to dispatch
    one bounded LIST per minute, mirroring the regular-log dispatcher.
    """
    return dt.strftime("raw/rum/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")


# ── Fastly service version lifecycle (merged from draft_version.py) ───────────
# Clone -> mutate -> validate -> activate (-> rollback) for a Fastly service version.
#
# ``draft_version`` owns the call sequence that several provisioning orchestrators hand-wrote:
#
# 1. ``PUT /service/{id}/version/{base}/clone``
# 2. optional ``PUT /service/{id}/version/{new}`` to set the comment (right after the clone, or
#    between validate and activate with ``comment_before_activate=True``)
# 3. the caller's mutations (the ``with`` body), then
# 4. ``GET .../validate`` (anything but ``status == "ok"`` raises ``RuntimeError``; skipped with
#    ``validate=False``),
# 5. ``PUT .../activate`` (with ``expect_empty=True`` when ``expect_empty_activate`` is set),
# 6. on any failure with ``rollback=True``: ``PUT /service/{id}/version/{base}/activate``
#    (a failed rollback is logged, never raised, so the original error surfaces).
#
# The Fastly client is passed in as ``call`` so each orchestrator keeps its own
# module-level ``fastly`` name as the seam its tests patch.
#
# Orchestrators that must log their own failure line BEFORE the rollback runs pass ``on_failure``;
# ones that own the rollback entirely pass ``rollback=False`` and call :func:`rollback_to_version`.

_draft_logger = logging.getLogger("backend.provision.draft_version")


def rollback_to_version(service_id: str, token: str, base_version: int, *, call: Callable[..., Any]) -> None:
    """Re-activate ``base_version``. A failed rollback is logged at WARNING, never raised."""
    try:
        call("PUT", f"/service/{service_id}/version/{base_version}/activate", token=token)
    except RuntimeError:
        _draft_logger.warning(
            "rollback activation of version %s failed for %s", base_version, service_id, exc_info=True
        )


@contextmanager
def draft_version(
    service_id: str,
    token: str,
    *,
    base_version: int,
    call: Callable[..., Any],
    comment: str | None = None,
    comment_before_activate: bool = False,
    status_cb: Callable[[str], None] | None = None,
    validate: bool = True,
    validate_msg: str = "Validating draft version {ver}...",
    activate_msg: str = "Activating version {ver}...",
    expect_empty_activate: bool = False,
    rollback: bool = True,
    on_failure: Callable[[Exception], None] | None = None,
) -> Iterator[int]:
    """Yield the new draft version number; validate + activate it when the body completes."""
    clone = call("PUT", f"/service/{service_id}/version/{base_version}/clone", token=token)
    new_ver = int(clone["number"])
    if comment is not None and not comment_before_activate:
        call("PUT", f"/service/{service_id}/version/{new_ver}", {"comment": comment}, token=token)

    try:
        yield new_ver

        if validate:
            if status_cb:
                status_cb(validate_msg.format(ver=new_ver))
            result = call("GET", f"/service/{service_id}/version/{new_ver}/validate", token=token)
            if result.get("status") != "ok":
                raise RuntimeError(f"Validation failed: {result.get('errors') or result}")

        if comment is not None and comment_before_activate:
            call("PUT", f"/service/{service_id}/version/{new_ver}", {"comment": comment}, token=token)

        if status_cb:
            status_cb(activate_msg.format(ver=new_ver))
        activate_kwargs: dict[str, Any] = {"expect_empty": True} if expect_empty_activate else {}
        call("PUT", f"/service/{service_id}/version/{new_ver}/activate", token=token, **activate_kwargs)
    except Exception as exc:
        if on_failure is not None:
            on_failure(exc)
        if rollback:
            rollback_to_version(service_id, token, base_version, call=call)
        raise


sys.modules.setdefault("backend.provision.log_paths", sys.modules[__name__])
sys.modules.setdefault("backend.provision.draft_version", sys.modules[__name__])
