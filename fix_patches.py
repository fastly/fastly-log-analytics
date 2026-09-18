import re

with open("backend/repositories/_base.py") as f:
    content = f.read()

content = content.replace("FROM {self._table}", "FROM {base_table}")

content = re.sub(
    r"(active_hour_start = datetime.now\(UTC\).replace\(minute=0, second=0, microsecond=0\))",
    r"\1\n        from backend.core.rollups import _safe_table_for\n        base_table = _safe_table_for(self.src)",
    content,
)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
