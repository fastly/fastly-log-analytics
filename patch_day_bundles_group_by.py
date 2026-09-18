with open("backend/core/rollups/day_bundles.py") as f:
    content = f.read()

content = content.replace("GROUP BY pop", "GROUP BY 1")

with open("backend/core/rollups/day_bundles.py", "w") as f:
    f.write(content)
