from datetime import UTC, datetime

st_iso = "2026-09-16T21:19:28Z"
et_iso = "2026-09-17T21:19:28Z"
from backend.utils.date_utils import parse_iso_utc

st = parse_iso_utc(st_iso)
et = parse_iso_utc(et_iso)
active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
active_hour_start = datetime.strptime(active_hour_str, "%Y-%m-%d-%H").replace(tzinfo=UTC)
print(f"active hour start: {active_hour_start}")
print(f"et: {et}")
print(f"crosses active: {et > active_hour_start}")
