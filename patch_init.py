def _patch():
    with open("backend/core/rollups/__init__.py") as f:
        lines = f.readlines()

    out = []
    for line in lines:
        if "from .network_summary" in line:
            out.append("from .network_quality import backfill_network_quality_bundles, build_network_quality_bundles\n")
        if '"backfill_network_speed_bundles",' in line:
            out.append('    "build_network_quality_bundles",\n')
            out.append('    "backfill_network_quality_bundles",\n')
        out.append(line)

    with open("backend/core/rollups/__init__.py", "w") as f:
        f.writelines(out)


_patch()
