def _patch_base():
    with open("backend/repositories/_base.py") as f:
        content = f.read()

    # Find the _approx: True line and add countries fetching
    target = '                "countries": [], # Frontend gets countries from network-health map anyway!'
    replacement = (
        """                "countries": [r[0] for r in self.execute(f"SELECT DISTINCT dim_val FROM read_parquet([{', '.join(f"""
        "{p}"
        """ for p in self._collect_rollup_paths(st, et, NETWORK_QUALITY_COUNTRY_FILENAME))}])").fetchall()],"""
    )

    content = content.replace(target, replacement)

    with open("backend/repositories/_base.py", "w") as f:
        f.write(content)


_patch_base()
