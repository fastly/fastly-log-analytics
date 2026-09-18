import re

with open("backend/repositories/_base.py") as f:
    content = f.read()

# For each of the 5 methods, find the 'from datetime import UTC, datetime' block and inject base_table.
search = r"(from datetime import UTC, datetime\n\s*active_hour_start = datetime\.now\(UTC\)\.replace\(minute=0, second=0, microsecond=0\)\n)"
replace = (
    r"\1        from backend.core.rollups import _safe_table_for\n        base_table = _safe_table_for(self.src)\n"
)
content = re.sub(search, replace, content)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
