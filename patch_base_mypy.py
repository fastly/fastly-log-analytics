def _patch():
    with open("backend/repositories/_base.py") as f:
        content = f.read()

    # Fix _run_dim return type
    content = content.replace(
        "filter_params: list = None) -> list[dict]:", "filter_params: list = None) -> list[dict] | None:"
    )

    # Fix countries list
    lines = content.split("\n")
    for i, line in enumerate(lines):
        if '"countries": [r[0] for r in self.execute' in line:
            lines[i] = '                "countries": [c["value"] for c in by_country],'
            break

    with open("backend/repositories/_base.py", "w") as f:
        f.write("\n".join(lines))


_patch()
