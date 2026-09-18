def _patch():
    with open("backend/repositories/_base.py") as f:
        content = f.read()

    # Find the block:
    #                 "countries": [
    #                     r[0]
    #                     for r in self.execute(
    #                         f"SELECT DISTINCT dim_val FROM read_parquet([{', '.join(f'''{p}''' for p in self._collect_rollup_paths(st, et, NETWORK_QUALITY_COUNTRY_FILENAME))}])"
    #                     ).fetchall()
    #                 ],
    import re

    content = re.sub(
        r'"countries": \[\s*r\[0\]\s*for r in self.execute\(\s*f"SELECT DISTINCT dim_val FROM read_parquet\(\[\{\', \'\.join\(f\'\'\'\{p\}\'\'\' for p in self\._collect_rollup_paths\(st, et, NETWORK_QUALITY_COUNTRY_FILENAME\)\)\}\]\)"\s*\)\.fetchall\(\)\s*\],',
        '"countries": [c["value"] for c in by_country],',
        content,
    )

    with open("backend/repositories/_base.py", "w") as f:
        f.write(content)


_patch()
