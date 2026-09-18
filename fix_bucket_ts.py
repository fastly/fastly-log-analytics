with open("backend/repositories/_base.py") as f:
    content = f.read()

# Replace `TIMESTAMPTZ '{ah_iso}' AS bucket_ts` with `CAST(TIMESTAMPTZ '{ah_iso}' AS TIMESTAMP) AS bucket_ts`
content = content.replace(
    "TIMESTAMPTZ '{ah_iso}' AS bucket_ts", "CAST(TIMESTAMPTZ '{ah_iso}' AS TIMESTAMP) AS bucket_ts"
)

with open("backend/repositories/_base.py", "w") as f:
    f.write(content)
