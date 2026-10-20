"""Admin endpoint exposing the metric_snapshots time-series.

``GET /api/admin/metric-history/batch`` — every recorded series in one round
trip. Admin-only via :class:`RemoteAccessMiddleware`'s ``/api/admin/*`` prefix
gate. Used by the /admin/trends page on first paint and the per-stat
sparklines on the System Health card.
"""

from __future__ import annotations

import re

from fastapi import Depends, HTTPException, Query

from backend.core import metric_snapshots
from backend.deps import require_admin
from backend.models.admin import MetricHistoryBatchResponse, MetricHistoryPoint
from backend.utils.date_utils import parse_relative_time_window
from backend.utils.router_utils import bad_request

from ._router import router

_MAX_LOOKBACK_DAYS = 30
_VALID_WINDOW_RE = re.compile(r"^[1-9]\d*[mhd]\Z", re.IGNORECASE)


@router.get(
    "/admin/metric-history/batch",
    response_model=MetricHistoryBatchResponse,
    dependencies=[Depends(require_admin)],
)
def metric_history_batch(
    since: str = Query(default="1h", description="Lookback window: 30m, 1h, 6h, 24h, 7d, etc."),
) -> MetricHistoryBatchResponse:
    """Return every recorded series newer than ``since``.

    Response shape: ``{series: {key: [{ts, value}, ...], ...}}`` where
    ``key`` is ``"metric"`` for global, ``"metric|service_id"`` for
    per-service, ``"metric|service_id|task"`` for per-task.
    """
    if not since or not _VALID_WINDOW_RE.fullmatch(since.strip()):
        raise HTTPException(
            status_code=400,
            detail=bad_request(
                f"Invalid lookback window '{since}'. Expected <N>m, <N>h, or <N>d (e.g. 30m, 1h, 6h, 24h, 7d)."
            ),
        )
    cutoff = parse_relative_time_window(since, max_lookback_days=_MAX_LOOKBACK_DAYS)
    raw = metric_snapshots.get_batch(since=cutoff)
    return MetricHistoryBatchResponse(
        series={k: [MetricHistoryPoint(**r) for r in v] for k, v in raw.items()},
    )
