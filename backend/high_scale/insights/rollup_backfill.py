"""Rebuild ``request_insight_hourly`` from ``request_facts`` for hours the materialized view never saw.

Dry run by default: prints, per hour, how many batches already match, are missing, or disagree.
``--apply`` rewrites only the batches that do not match, one hour at a time, so an interrupted run
resumes by running it again (matched hours are skipped). Every batch is rebuilt whatever its
publication state; readers apply the publication fence to the rollup exactly as to the facts.

    uv run python -m backend.high_scale.insights.rollup_backfill --service-id SID          # dry run
    uv run python -m backend.high_scale.insights.rollup_backfill --service-id SID --apply
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from .rollup import ROLLUP_INSERT_COLUMNS, ROLLUP_SHAPES, ROLLUP_TABLE, rollup_select_sql

MAX_DAYS = 30
MAX_BATCH_SIZE = 500
_SERVICE_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


def _hour_range(column: str) -> str:
    return (
        f"service_id={{service_id:String}} AND {column} >= {{hour_start:DateTime64(3)}} "
        f"AND {column} < {{hour_end:DateTime64(3)}}"
    )


class _Client(Protocol):
    def execute(self, sql: str, params: dict | None = None) -> list[dict]: ...


@dataclass
class HourPlan:
    hour: datetime
    ok: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    mismatched: list[str] = field(default_factory=list)
    orphan: list[str] = field(default_factory=list)
    raw_rows: int = 0

    @property
    def to_delete(self) -> list[str]:
        return self.mismatched + self.orphan

    @property
    def to_insert(self) -> list[str]:
        return self.mismatched + self.missing

    def summary(self) -> str:
        return (
            f"{self.hour:%Y-%m-%dT%H:00Z} rows={self.raw_rows} ok={len(self.ok)} missing={len(self.missing)} "
            f"mismatched={len(self.mismatched)} orphan={len(self.orphan)}"
        )


def _uuid_list(batch_ids: Sequence[str]) -> str:
    return ", ".join(f"'{UUID(batch_id)}'" for batch_id in batch_ids)


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def plan_hour(client: _Client, service_id: str, hour: datetime) -> HourPlan:
    params = {"service_id": service_id, "hour_start": hour, "hour_end": hour + timedelta(hours=1)}
    raw_rows = client.execute(
        "SELECT toString(batch_id) AS batch_id, count() AS row_count FROM request_facts "
        f"WHERE {_hour_range('event_timestamp')} GROUP BY batch_id",
        params,
    )
    rollup_rows = client.execute(
        "SELECT toString(batch_id) AS batch_id, shape, sum(request_count) AS row_count "
        f"FROM {ROLLUP_TABLE} "
        f"WHERE {_hour_range('bucket_start')} GROUP BY batch_id, shape",
        params,
    )
    raw = {str(row["batch_id"]): int(str(row["row_count"])) for row in raw_rows}
    rolled: dict[str, dict[str, int]] = {}
    for row in rollup_rows:
        rolled.setdefault(str(row["batch_id"]), {})[str(row["shape"])] = int(str(row["row_count"]))
    plan = HourPlan(hour=hour, raw_rows=sum(raw.values()))
    for batch_id in sorted(raw.keys() | rolled.keys()):
        if batch_id not in raw:
            plan.orphan.append(batch_id)
        elif batch_id not in rolled:
            plan.missing.append(batch_id)
        elif rolled[batch_id] == {shape: raw[batch_id] for shape in ROLLUP_SHAPES}:
            plan.ok.append(batch_id)
        else:
            plan.mismatched.append(batch_id)
    return plan


def apply_hour(client: _Client, service_id: str, plan: HourPlan, batch_size: int) -> None:
    params = {"service_id": service_id, "hour_start": plan.hour, "hour_end": plan.hour + timedelta(hours=1)}
    for chunk in _chunks(plan.to_delete, batch_size):
        client.execute(
            f"ALTER TABLE {ROLLUP_TABLE} DELETE WHERE "
            f"{_hour_range('bucket_start')} "
            f"AND batch_id IN ({_uuid_list(chunk)}) SETTINGS mutations_sync=2",
            params,
        )
    for chunk in _chunks(plan.to_insert, batch_size):
        where = f"{_hour_range('event_timestamp')} AND batch_id IN ({_uuid_list(chunk)})"
        client.execute(f"INSERT INTO {ROLLUP_TABLE} ({ROLLUP_INSERT_COLUMNS}) {rollup_select_sql(where)}", params)


def hours_to_check(now: datetime, days: int, skip_recent_hours: int) -> list[datetime]:
    """Whole hours newest first, from ``skip_recent_hours`` before the current hour back ``days`` days."""
    current = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    newest = current - timedelta(hours=skip_recent_hours)
    return [newest - timedelta(hours=i) for i in range(days * 24 - skip_recent_hours)]


def run(
    client: _Client,
    service_id: str,
    *,
    apply: bool,
    days: int = 8,
    skip_recent_hours: int = 2,
    batch_size: int = 100,
    max_hours: int | None = None,
    now: datetime | None = None,
    emit: Callable[[str], object] = lambda line: sys.stdout.write(line + "\n"),
) -> list[HourPlan]:
    if not _SERVICE_ID.fullmatch(service_id):
        raise ValueError("invalid service id")
    if not 1 <= days <= MAX_DAYS:
        raise ValueError(f"--days must be between 1 and {MAX_DAYS}")
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError(f"--batch-size must be between 1 and {MAX_BATCH_SIZE}")
    if skip_recent_hours < 1:
        raise ValueError("--skip-recent-hours must be at least 1 (the current hour is still being written)")
    plans: list[HourPlan] = []
    changed = 0
    for hour in hours_to_check(now or datetime.now(UTC), days, skip_recent_hours):
        if max_hours is not None and changed >= max_hours:
            emit(f"stopping after {max_hours} rewritten hour(s); run again to continue")
            break
        plan = plan_hour(client, service_id, hour)
        plans.append(plan)
        if not plan.raw_rows and not plan.orphan:
            continue
        emit(plan.summary())
        if plan.to_delete or plan.to_insert:
            changed += 1
            if apply:
                apply_hour(client, service_id, plan, batch_size)
    pending = sum(1 for plan in plans if plan.to_delete or plan.to_insert)
    emit(f"{'rewrote' if apply else 'would rewrite'} {pending} hour(s){'' if apply else ' (dry run; add --apply)'}")
    return plans


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--service-id", required=True)
    parser.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    parser.add_argument("--days", type=int, default=8, help=f"hours to check, in days (1-{MAX_DAYS}, default 8)")
    parser.add_argument("--skip-recent-hours", type=int, default=2, help="newest hours to leave alone (default 2)")
    parser.add_argument("--batch-size", type=int, default=100, help="batches per statement (default 100)")
    parser.add_argument("--max-hours", type=int, default=None, help="stop after rewriting this many hours")
    args = parser.parse_args(argv)
    from backend.core.clickhouse import get_clickhouse_client

    client: Any = get_clickhouse_client()
    if client is None:
        sys.stderr.write("ClickHouse is not configured\n")
        return 2
    run(
        client,
        args.service_id,
        apply=args.apply,
        days=args.days,
        skip_recent_hours=args.skip_recent_hours,
        batch_size=args.batch_size,
        max_hours=args.max_hours,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
