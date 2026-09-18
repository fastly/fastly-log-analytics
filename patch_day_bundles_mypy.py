def _patch():
    with open("backend/core/rollups/day_bundles.py") as f:
        content = f.read()

    content = content.replace(
        "def _build_sql(dim_col: str, include_country: bool):",
        "def _build_sql(dim_col: str, include_country: bool) -> typing.Callable[[str, str], str]:",
    )
    content = content.replace(
        "def compact_network_quality_closed_days_to_daily(service_id: str, source: dict) -> int:",
        "import typing\n\ndef compact_network_quality_closed_days_to_daily(service_id: str, source: dict) -> int:",
    )

    with open("backend/core/rollups/day_bundles.py", "w") as f:
        f.write(content)


_patch()
