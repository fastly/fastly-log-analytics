"""Canonical FOS log object path templates — the single source of truth.

Every producer of a logging-endpoint ``path`` (initial provisioning, the
declarative generator/reconciler, and the admin logging-settings update flow)
must build it here. Historically three call sites embedded two different
layouts, so a settings update could silently switch a bucket's layout
mid-stream and break incremental discovery.

Layout (strftime tokens are expanded by Fastly's log aggregators, which then
append a unique ``<ISO-timestamp>-<id>.log.gz`` suffix per delivered object):

    <prefix>/raw/%Y/%m/%d/%H/analytics_log_%M.json.gz

Because the minute lives in the object-name prefix, a dispatcher can bound a
LIST to one closed minute with :func:`minute_list_prefix` — no directory-level
repartitioning needed.
"""

from datetime import datetime

ANALYTICS_LOG_LAYOUT = "raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
RUM_LOG_LAYOUT = "rum/raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


def _join(prefix: str, layout: str) -> str:
    return f"{(prefix or '').strip('/')}/{layout}"


def analytics_log_path(prefix: str) -> str:
    """Logging-endpoint ``path`` for the main analytics log stream."""
    return _join(prefix, ANALYTICS_LOG_LAYOUT)


def rum_log_path(prefix: str) -> str:
    """Logging-endpoint ``path`` for the RUM beacon log stream."""
    return _join(prefix, RUM_LOG_LAYOUT)


def minute_list_prefix(prefix: str, dt: datetime) -> str:
    """S3 LIST ``Prefix`` bounding exactly one closed minute of analytics logs."""
    minute_part = dt.strftime("raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")
    return _join(prefix, minute_part).lstrip("/")


def rum_minute_list_prefix(prefix: str, dt: datetime) -> str:
    """S3 LIST ``Prefix`` bounding exactly one closed minute of RUM beacon logs.

    RUM counterpart of :func:`minute_list_prefix` — used by the celery-mode
    RUM ledger discovery job (``backend.cron.jobs.rum_ledger``) to dispatch
    one bounded LIST per minute, mirroring the regular-log dispatcher.
    """
    minute_part = dt.strftime("rum/raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/")
    return _join(prefix, minute_part).lstrip("/")
