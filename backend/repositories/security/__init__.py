"""Security repository package — pure-Python bot classification and shaping helpers."""

from __future__ import annotations

from backend.repositories.security import watchdog as _watchdog
from backend.repositories.security.watchdog import (
    _BOT_UA_CLASSIFY_LIMIT,
    _GENERIC_BOT_RE,
    _RDNS_ENQUEUE_LIMIT,
    _RDNS_LOOKUP_LIMIT,
    _VOWEL_TABLE,
    _stems_match,
    classify_ua_counts,
    summarize_bot_efficiency_rows,
    summarize_wellknown_bot_rows,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_CONN_REUSE_BUCKETS as _CONN_REUSE_BUCKETS,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_CONN_REUSE_LAST as _CONN_REUSE_LAST,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_EMPTY_CORE_SECTIONS as _EMPTY_CORE_SECTIONS,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_REQ_HEADER_BUCKETS as _REQ_HEADER_BUCKETS,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_REQ_HEADER_LAST as _REQ_HEADER_LAST,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_SECTION_DEFAULTS,
    empty_security_aggregates,
    empty_security_proxies,
)
from backend.repositories.utils.response_shaping import (
    finish_security_proxies as _finish_proxies,
)
from backend.repositories.utils.response_shaping import (
    shape_security_bucket_rows as _rows_to_buckets,
)
from backend.repositories.utils.response_shaping import (
    shape_security_fingerprint_row as _row_to_fingerprint,
)
from backend.repositories.utils.response_shaping import (
    shape_security_ngwaf_bot_row as _row_to_ngwaf_bot,
)
from backend.repositories.utils.response_shaping import (
    shape_security_ngwaf_top_bot_row as _row_to_ngwaf_top_bot,
)
from backend.repositories.utils.response_shaping import (
    shape_security_threat_intel_row as _row_to_threat_intel,
)
from backend.repositories.utils.response_shaping import (
    shape_security_top_ip_header_row as _row_to_top_ip_header,
)
from backend.utils.module_shim import install_shim_module

_SUBMODULES = (_watchdog,)

install_shim_module(__name__, _SUBMODULES)

__all__ = [
    "SECURITY_SECTION_DEFAULTS",
    "_BOT_UA_CLASSIFY_LIMIT",
    "_CONN_REUSE_BUCKETS",
    "_CONN_REUSE_LAST",
    "_EMPTY_CORE_SECTIONS",
    "_GENERIC_BOT_RE",
    "_RDNS_ENQUEUE_LIMIT",
    "_RDNS_LOOKUP_LIMIT",
    "_REQ_HEADER_BUCKETS",
    "_REQ_HEADER_LAST",
    "_VOWEL_TABLE",
    "_finish_proxies",
    "_row_to_fingerprint",
    "_row_to_ngwaf_bot",
    "_row_to_ngwaf_top_bot",
    "_row_to_threat_intel",
    "_row_to_top_ip_header",
    "_rows_to_buckets",
    "_stems_match",
    "classify_ua_counts",
    "empty_security_aggregates",
    "empty_security_proxies",
    "summarize_bot_efficiency_rows",
    "summarize_wellknown_bot_rows",
]
