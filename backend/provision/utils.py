import sys

# ── ANSI colour helpers ────────────────────────────────────────────────────────

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
    if "\033" in msg:
        return msg
    import re

    msg = re.sub(r"'([^']+)'", rf"'{_c(MAG, r'\1')}'", msg)
    return msg


def ok(msg):
    print(f"  {_c(GRN + BOLD, '✓')}  {_highlight(msg)}")


def fail(msg):
    print(f"  {_c(RED + BOLD, '✗')}  {_c(RED, msg)}", file=sys.stderr)


def info(msg):
    print(f"  {_c(BLU + BOLD, '→')}  {_highlight(msg)}")


def warn(msg):
    print(f"  {_c(YLW + BOLD, '⚠')}  {_c(YLW, msg)}")


def blank():
    print()


def step(n, total, title):
    blank()
    print(f"{_c(BOLD + MAG, f'[{n}/{total}]')} {_c(BOLD + CYN, title)}")


def banner(title):
    bar = "━" * 64
    blank()
    print(_c(MAG + BOLD, bar))
    print(_c(CYN + BOLD, f"  {title}"))
    print(_c(MAG + BOLD, bar))


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
