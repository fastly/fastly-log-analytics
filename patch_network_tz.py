import re

with open("backend/repositories/network.py") as f:
    content = f.read()


# Make the generation of buckets explicitly use 'Z' or no tz depending on what the rollup expects,
# but it's simpler to just normalize the strings coming out of DuckDB.
def fix_isoformat(match):
    return r'bucket = r[{idx}].isoformat().replace("+00:00", "") if hasattr(r[{idx}], "isoformat") else str(r[{idx}])'.format(
        idx=match.group(1)
    )


content = re.sub(
    r'bucket = r\[(\d+)\].isoformat\(\) if hasattr\(r\[\1\], "isoformat"\) else str\(r\[\1\]\)', fix_isoformat, content
)

with open("backend/repositories/network.py", "w") as f:
    f.write(content)
