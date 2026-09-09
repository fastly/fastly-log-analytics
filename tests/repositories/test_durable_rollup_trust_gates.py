from datetime import UTC, datetime
from unittest.mock import patch

from backend.core import rollup_readiness as rr
from backend.repositories import _base


def test_collect_hourly_bundle_paths_trusts_durable_once_ready(tmp_path):
    rr.reset_rollup_coverage_ready()
    src = {"service_id": "svc-durable", "name": "svc-durable", "serving_mode": "durable"}

    with (
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.repositories._base._cached_listdir", return_value=[]) as mock_listdir,
    ):
        # Not ready yet: still refuses (returns None -> caller raw-falls-back)
        # without ever reaching the local-rollup walk.
        assert _base.collect_hourly_bundle_paths(src, None, None, str(tmp_path), "x.parquet") is None
        mock_listdir.assert_not_called()

    rr.mark_rollup_coverage_ready("svc-durable")

    with (
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.repositories._base._cached_listdir", return_value=[]) as mock_listdir,
    ):
        # Now ready: falls through past the unconditional durable early-return
        # into the normal walk logic, proven by reaching _cached_listdir.
        # (Needs a real [st, et) window — the walk logic dereferences it,
        # unlike the durable-gated early return which never touches st/et.)
        _base.collect_hourly_bundle_paths(
            src,
            datetime(2026, 9, 6, 12, tzinfo=UTC),
            datetime(2026, 9, 6, 13, tzinfo=UTC),
            str(tmp_path),
            "x.parquet",
        )
        mock_listdir.assert_called()

    rr.reset_rollup_coverage_ready("svc-durable")
