def _patch():
    with open("backend/models/network.py") as f:
        content = f.read()

    content = content.replace(
        "    countries: list[str] = []", "    countries: list[str] = []\n    _approx: bool = False"
    )

    with open("backend/models/network.py", "w") as f:
        f.write(content)


_patch()
