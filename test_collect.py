from datetime import UTC, datetime, timedelta

st = datetime(2026, 9, 16, 21, 19, 28, tzinfo=UTC)
et = datetime(2026, 9, 17, 21, 19, 28, tzinfo=UTC)
active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
print(f"active_hour_str = {active_hour_str}")

total_closed_hours = 0
cur = st.replace(minute=0, second=0, microsecond=0)
if cur < st:
    cur = cur + timedelta(hours=1)
while cur < et:
    hour_str = cur.strftime("%Y-%m-%d-%H")
    if hour_str >= active_hour_str:
        break
    total_closed_hours += 1
    cur += timedelta(hours=1)

print(f"total_closed_hours = {total_closed_hours}")
