def _patch():
    with open("backend/core/rollups/recompute.py") as f:
        content = f.read()

    # import
    content = content.replace(
        "from .network_summary import build_network_summary_bundles",
        "from .network_summary import build_network_summary_bundles\nfrom .network_quality import build_network_quality_bundles",
    )

    # call
    target = """    try:
        build_network_summary_bundles(service_id, source, hours)
    except Exception as e:
        logger.warning("[%s] network_summary bundle failed (raw scan will serve): %s", service_id, e)"""

    replacement = (
        target
        + """

    try:
        build_network_quality_bundles(service_id, source, hours)
    except Exception as e:
        logger.warning("[%s] network_quality bundle failed (raw scan will serve): %s", service_id, e)"""
    )

    content = content.replace(target, replacement)

    with open("backend/core/rollups/recompute.py", "w") as f:
        f.write(content)


_patch()
