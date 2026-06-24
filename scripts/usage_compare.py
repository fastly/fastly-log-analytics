"""Compare local usage_log accounting against Fastly's stats API.

Operational sanity check: for the last 24h of each configured service, print
local (usage_log) vs API (Fastly /stats) counts for FOS Class A/B ops and
CDN egress bytes. A large diff (>10% in either direction) usually means
either the operation_class classifier mislabelled rows (see
``usage_log_raw_http_verb_trap`` memory) or a backfill is mid-flight.

Run manually:
    .venv/bin/python scripts/usage_compare.py

Not in CI — depends on a live Fastly API key per service.
"""

import os
import sqlite3
import sys
from datetime import UTC, datetime, timedelta

# Ensure project root is in sys.path (scripts/ lives one level below root).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend import config
from backend.core import metadata as metadata_db
from backend.routers.usage import _extract_fos_ops, _fastly_api


def run_comparison():
    configs = config.list_configs()
    if not configs:
        print("No services configured.")
        return

    end = datetime.now(UTC)
    start = end - timedelta(hours=24)
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    from_ts = int(start.timestamp())
    to_ts = int(end.timestamp())

    print(f"\nComparing usage for the last 24 hours ({start_str} to {end_str})\n")
    print("-" * 115)
    print(f"{'Service':<25} | {'Metric':<18} | {'Local (Logged)':<18} | {'Fastly API (Stats)':<18} | {'Diff %':<10}")
    print("-" * 115)

    for cfg in configs:
        sid = cfg["service_id"]
        cdn_sid = cfg.get("cdn_service_id")
        api_key = cfg.get("fastly_api_key")
        name = cfg.get("name", sid)

        if not api_key:
            print(f"Missing API key for {name}")
            continue

        # 1. Local Data (usage_log)
        # usage_log.timestamp has three historical formats coexisting:
        #   - 'YYYY-MM-DDTHH:MM:SSZ'             (current iso_z_now())
        #   - 'YYYY-MM-DDTHH:MM:SS.uuuuuu+00:00' (pre-iso_z_now legacy)
        #   - 'YYYY-MM-DDTHH:MM:SS'              (fastly.edge synthetic, no tz)
        # SQLite's datetime() parses all three to canonical 'YYYY-MM-DD HH:MM:SS',
        # which sorts correctly. Lexicographic >= on the raw column drops legacy
        # rows because '.' < 'Z' in the encoded comparison.
        con = metadata_db.get_con(sid)
        con.row_factory = sqlite3.Row
        cur = con.execute(
            """
            SELECT
                operation_class,
                count(*) as count,
                sum(coalesce(bytes, 0)) as total_bytes
            FROM usage_log
            WHERE service_id = ?
              AND datetime(timestamp) >= datetime(?)
              AND datetime(timestamp) <= datetime(?)
            GROUP BY 1
        """,
            (sid, start_str, end_str),
        )
        local_usage = {row["operation_class"]: dict(row) for row in cur.fetchall()}

        # 2. Fastly Stats
        # FOS ops (account-wide via /stats/aggregate)
        fos_a_api = 0
        fos_b_api = 0
        try:
            payload = _fastly_api(f"/stats/aggregate?by=hour&from={from_ts}&to={to_ts}", api_key)
            for record in payload.get("data", []):
                a, b = _extract_fos_ops(record)
                fos_a_api += a
                fos_b_api += b
        except Exception as e:
            print(f"Error fetching aggregate stats for {name}: {e}")

        # CDN service for Egress
        cdn_egress_api = 0
        if cdn_sid:
            try:
                payload = _fastly_api(f"/stats/service/{cdn_sid}?by=hour&from={from_ts}&to={to_ts}", api_key)
                cdn_egress_api = sum(int(record.get("bandwidth", 0) or 0) for record in payload.get("data", []))
            except Exception as e:
                print(f"Error fetching CDN stats for {name}: {e}")

        # 3. Print Comparison
        metrics = [
            ("FOS Class A Ops", local_usage.get("A", {}).get("count", 0), fos_a_api, "ops"),
            ("FOS Class B Ops", local_usage.get("B", {}).get("count", 0), fos_b_api, "ops"),
            ("Tool CDN Egress", local_usage.get("CDN", {}).get("total_bytes", 0), cdn_egress_api, "bytes"),
        ]

        for label, local, api, unit in metrics:
            diff = 0
            if api > 0:
                diff = ((local - api) / api) * 100
            elif local > 0:
                diff = 100.0  # Infinity

            if unit == "ops":
                local_disp = f"{local:,}"
                api_disp = f"{api:,}"
            else:
                local_disp = f"{local / 1024 / 1024:.2f} MB"
                api_disp = f"{api / 1024 / 1024:.2f} MB"

            diff_str = f"{diff:>+8.1f}%" if api > 0 else "N/A"

            if api == 0 and local > 0 and "Ops" in label:
                api_disp = "0 (Missing?)"

            print(f"{name[:25]:<25} | {label:<18} | {local_disp:>18} | {api_disp:>18} | {diff_str:>10}")
        print("-" * 115)


if __name__ == "__main__":
    run_comparison()
