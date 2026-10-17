"""Content Security policies: pure validation, normalization and resolution.

No I/O. Mirrors what the blocker (``fastly-test-player/compute``) does with a published
policy set, so values are stored in the form the edge compares against. See
``local-docs/content-security-policies-contract.md``.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from ipaddress import ip_address
from typing import Literal

# Categories a policy can block, pace or exempt.
RULE_CATEGORIES = ("country", "asn", "referer", "host", "proxy_type", "proxy_description")
# Categories that can only be blocked (never paced or exempted).
INDIVIDUAL_CATEGORIES = ("subscriber_id", "ip")
ALL_CATEGORIES = (*RULE_CATEGORIES, *INDIVIDUAL_CATEGORIES)

Action = Literal["block", "pace", "exempt"]

MAX_VALUE_LENGTH = 512
MAX_KBPS = 100_000_000
MAX_REGEX_LENGTH = 100
MAX_RULES_PER_CATEGORY = 500
MAX_INDIVIDUAL_PER_SERVICE = 1000
MAX_POLICIES_PER_SERVICE = 100
MAX_ASSIGNMENTS_PER_SERVICE = 200

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class PolicyValidationError(ValueError):
    """A policy, rule or assignment value is not acceptable. ``code`` is the API error code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def valid_id(value: str) -> bool:
    return bool(_ID_RE.match(value))


# ── Values ───────────────────────────────────────────────────────────────────

_REFERER_HOST_RE = re.compile(r"^(?:[a-zA-Z][a-zA-Z0-9+.-]*://)?(?:[^/@?#]*@)?([^/:?#]+).*$")


def normalize_value(category: str, value: str) -> str:
    """Canonical form of ``value`` for ``category`` (what the edge stores and compares)."""
    if category not in ALL_CATEGORIES:
        raise PolicyValidationError("invalid_category", f"Unknown category {category!r}")
    v = value.strip()
    if not v:
        raise PolicyValidationError("invalid_value", "Value must not be empty")
    if len(v) > MAX_VALUE_LENGTH:
        raise PolicyValidationError("invalid_value", f"Value longer than {MAX_VALUE_LENGTH} characters")
    if category == "country":
        if not re.fullmatch(r"[A-Za-z]{2}", v):
            raise PolicyValidationError("invalid_country", "Country must be a 2-letter code")
        return v.upper()
    if category == "asn":
        digits = v.upper().removeprefix("AS")
        if not (digits.isascii() and digits.isdigit()) or not 0 < int(digits) < 2**32:
            raise PolicyValidationError("invalid_asn", "Not a valid ASN")
        return str(int(digits))
    if category == "ip":
        try:
            return str(ip_address(v.strip("[]")))
        except ValueError as exc:
            raise PolicyValidationError("invalid_ip", "Not a valid IP address") from exc
    if category == "subscriber_id":
        return v
    if category == "referer":
        # The edge sends the referer's host only, so a block on a URL means its site.
        m = _REFERER_HOST_RE.match(v)
        return (m.group(1) if m else v).lower()
    return v.lower()


def normalize_rule(category: str, value: str, action: str, kbps: int | None = None) -> dict:
    """Validate one rule and return ``{category, value, action, kbps}`` (``kbps`` 0 unless pace)."""
    if action not in ("block", "pace", "exempt"):
        raise PolicyValidationError("invalid_action", "Action must be block, pace or exempt")
    if category in INDIVIDUAL_CATEGORIES and action != "block":
        raise PolicyValidationError("invalid_action", f"{category} can only be blocked")
    norm = normalize_value(category, value)
    if action == "pace":
        if kbps is None or not 0 < kbps <= MAX_KBPS:
            raise PolicyValidationError("invalid_kbps", f"Throttle rate must be 1 to {MAX_KBPS} kbps")
        return {"category": category, "value": norm, "action": "pace", "kbps": int(kbps)}
    return {"category": category, "value": norm, "action": action, "kbps": 0}


def normalize_rules(rules: list[dict]) -> list[dict]:
    """Normalize a rule list, dropping exact duplicates and rejecting conflicting ones.

    The last write for a (category, value) wins only when identical; two different
    actions for one value are a conflict, since the edge has one answer per value.
    """
    out: dict[tuple[str, str], dict] = {}
    for r in rules:
        rule = normalize_rule(r["category"], r["value"], r["action"], r.get("kbps"))
        key = (rule["category"], rule["value"])
        prior = out.get(key)
        if prior is not None and (prior["action"], prior["kbps"]) != (rule["action"], rule["kbps"]):
            raise PolicyValidationError(
                "conflicting_rules", f"{rule['category']} {rule['value']} has more than one action"
            )
        out[key] = rule
    per_cat: dict[str, int] = {}
    for category, _ in out:
        per_cat[category] = per_cat.get(category, 0) + 1
    for category, n in per_cat.items():
        if category in RULE_CATEGORIES and n > MAX_RULES_PER_CATEGORY:
            raise PolicyValidationError(
                "too_many_rules", f"At most {MAX_RULES_PER_CATEGORY} {category} rules per policy"
            )
    return sorted(out.values(), key=lambda r: (ALL_CATEGORIES.index(r["category"]), r["value"]))


# ── Country regex ────────────────────────────────────────────────────────────

_BACKREF_RE = re.compile(r"\\[1-9]|\\k<")
_LOOKAROUND_RE = re.compile(r"\(\?(?:=|!|<=|<!)")
_NESTED_QUANT_RE = re.compile(r"\((?:[^()\\]|\\.)*[+*](?:[^()\\]|\\.)*\)[+*{]")


def validate_country_regex(pattern: str | None) -> str | None:
    """Return the trimmed pattern (``None`` when empty) or raise.

    The edge runs it with JS ``RegExp`` against an uppercased 2-letter code only, but
    the same restrictions as the contract apply: no backreferences, lookaround or
    nested quantifiers, and Python-only group syntax is rejected so both engines agree.
    """
    if pattern is None:
        return None
    p = pattern.strip()
    if not p:
        return None
    if len(p) > MAX_REGEX_LENGTH:
        raise PolicyValidationError("invalid_regex", f"Regex longer than {MAX_REGEX_LENGTH} characters")
    if _BACKREF_RE.search(p) or _LOOKAROUND_RE.search(p) or _NESTED_QUANT_RE.search(p) or "(?P" in p:
        raise PolicyValidationError(
            "invalid_regex", "Backreferences, lookaround and nested quantifiers are not allowed"
        )
    try:
        re.compile(p)
    except re.error as exc:
        raise PolicyValidationError("invalid_regex", f"Invalid regex: {exc}") from exc
    return p


def regex_matches(pattern: str, codes: list[str]) -> list[str]:
    """The codes (already uppercased) the pattern matches; used for the editor's live preview."""
    rx = re.compile(pattern)
    return [c for c in codes if rx.search(c)]


# ── Times and assignments ────────────────────────────────────────────────────


def parse_time(value: str | None) -> datetime | None:
    """Parse an ISO-8601 string to an aware UTC datetime; ``None`` stays ``None``."""
    if value is None or not value.strip():
        return None
    s = value.strip()
    try:
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
    except ValueError as exc:
        raise PolicyValidationError("invalid_time", "Not a valid ISO-8601 date-time") from exc
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def format_time(dt: datetime | None) -> str | None:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def normalize_window(starts_at: str | None, ends_at: str | None) -> tuple[str | None, str | None]:
    start, end = parse_time(starts_at), parse_time(ends_at)
    if start and end and end <= start:
        raise PolicyValidationError("invalid_window", "End must be after start")
    return format_time(start), format_time(end)


def window_contains(starts_at: str | None, ends_at: str | None, now: datetime) -> bool:
    """Start inclusive, end exclusive, ``None`` unbounded. Same as the edge."""
    start, end = parse_time(starts_at), parse_time(ends_at)
    return (start is None or now >= start) and (end is None or now < end)


def windows_overlap(a: tuple[str | None, str | None], b: tuple[str | None, str | None]) -> bool:
    a_start, a_end, b_start, b_end = parse_time(a[0]), parse_time(a[1]), parse_time(b[0]), parse_time(b[1])
    return (a_start is None or b_end is None or a_start < b_end) and (
        b_start is None or a_end is None or b_start < a_end
    )


def assignment_status(
    starts_at: str | None, ends_at: str | None, now: datetime
) -> Literal["scheduled", "active", "ended"]:
    start, end = parse_time(starts_at), parse_time(ends_at)
    if start and now < start:
        return "scheduled"
    if end and now >= end:
        return "ended"
    return "active"


def pick_assignment(assignments: list[dict], content_id: str | None, now: datetime) -> dict | None:
    """The assignment in effect for ``content_id`` (D2): its own active one, else the default.

    ``assignments`` are non-deleted rows with ``scope_type`` (``content_id`` / ``default``),
    ``content_id``, ``starts_at`` and ``ends_at``. The overlap rule means at most one
    content-ID assignment is active for a content ID.
    """
    active = [a for a in assignments if window_contains(a["starts_at"], a["ends_at"], now)]
    if content_id:
        for a in active:
            if a["scope_type"] == "content_id" and a["content_id"] == content_id:
                return a
    for a in active:
        if a["scope_type"] == "default":
            return a
    return None


# ISO 3166-1 alpha-2 officially assigned codes, for the country-regex preview.
ISO_COUNTRY_CODES = tuple(
    (
        "AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ "
        "CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR "
        "GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP "
        "KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT "
        "MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW "
        "SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG "
        "UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW"
    ).split()
)
