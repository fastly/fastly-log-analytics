def _patch():
    with open("backend/repositories/_base.py") as f:
        content = f.read()

    # Find the line that has SyntaxError
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if '"countries": [r[0] for r in self.execute' in line:
            lines[i] = (
                """                "countries": [r[0] for r in self.execute(f"SELECT DISTINCT dim_val FROM read_parquet([{', '.join(f"""
                + "'''{p}'''"
                + """ for p in self._collect_rollup_paths(st, et, NETWORK_QUALITY_COUNTRY_FILENAME))}])").fetchall()],"""
            )
            break

    with open("backend/repositories/_base.py", "w") as f:
        f.write("\n".join(lines))


_patch()
