from datetime import UTC, datetime, timedelta

st = datetime(2026, 9, 16, 21, 19, 28, 594000, tzinfo=UTC)
et = datetime(2026, 9, 17, 21, 19, 28, 594000, tzinfo=UTC)
print(f"et - st = {et - st}")
print(f"min_hours check (et - st) < 24h: {(et - st) < timedelta(hours=24)}")
