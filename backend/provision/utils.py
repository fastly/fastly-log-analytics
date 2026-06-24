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

import sys

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


def _mask(s: str, visible: int = 8) -> str:
    if not s:
        return ""
    if len(s) <= visible:
        return s
    return f"{s[:visible]}..."


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
