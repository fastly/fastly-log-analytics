from backend.repositories.insights.registry import InsightCategory, registry


def test_every_insight_has_valid_category():
    """Drift guard: every registered insight declares a category in the
    known enum. ``InsightDefinition.category`` is a required, enum-typed
    field, so a forgotten/invalid category already fails at import — this
    test documents the valid set and pins the invariant explicitly (design
    plan §6). A new insight added without a category can't reach this test."""
    valid = {c.value for c in InsightCategory}
    for d in registry.get_all():
        assert str(d.category) in valid, f"{d.id} has bad category {d.category!r}"


def test_availability_catalog_categories_match_registry():
    """The runtime registry (definitions.py) and the legacy availability
    catalog (``INSIGHT_DEFINITIONS`` in _log_fields_data.py) are SEPARATE
    subsystems with no data flow — category is set in both independently.
    For every id they share, the category MUST agree so the loaded cards
    and the loading skeletons group into the same sections. Legacy-only
    stubs (not computed by the registry) are exempt."""
    from backend.core.field_registry import INSIGHT_DEFINITIONS

    reg = {d.id: str(d.category) for d in registry.get_all()}
    legacy = {d["id"]: d.get("category") for d in INSIGHT_DEFINITIONS}

    # Every computed insight must be represented in the availability catalog
    # (18-vs-30 drift resolution — design plan §5.1 step 5).
    missing = sorted(set(reg) - set(legacy))
    assert not missing, f"computed insights missing from availability catalog: {missing}"

    # Every legacy entry (including stubs) must carry a category.
    no_cat = sorted(i for i, c in legacy.items() if not c)
    assert not no_cat, f"availability catalog entries without a category: {no_cat}"

    # Shared ids must agree on category.
    mismatch = {i: (reg[i], legacy[i]) for i in reg if i in legacy and reg[i] != legacy[i]}
    assert not mismatch, f"category mismatch between registry and availability catalog: {mismatch}"


def test_registry_initialization():
    """Verify that the registry is initialized and contains definitions."""
    all_defs = registry.get_all()
    assert len(all_defs) >= 2

    ids = [d.id for d in all_defs]
    assert "error_spikes" in ids
    assert "botnet_grouping" in ids


def test_insight_definition_structure():
    """Verify the structure of a specific insight definition."""
    definition = registry.get("error_spikes")
    assert definition is not None
    assert definition.title == "Error Spikes"
    assert "url" in definition.required_fields
    assert "status" in definition.required_fields
    assert "{table_name}" in definition.sql_template
    assert definition.row_processor is not None


def test_row_processor_execution():
    """Verify that a row processor can be executed."""
    definition = registry.get("error_spikes")
    assert definition.row_processor is not None

    # Mock row: [url, w_rate, b_rate, w_errors, w_total, b_total]
    mock_row = ("/test", 0.1, 0.02, 10, 100, 500)
    item = definition.row_processor(mock_row, definition, {})

    assert item["label"] == "/test"
    assert item["current_val"] == 10.0  # 0.1 * 100
    assert item["baseline_val"] == 2.0  # 0.02 * 100
    assert item["unit"] == "% 5xx"
    assert item["severity"] == "warning"  # 0.1 < 0.5

    mock_row_crit = ("/crit", 0.6, 0.02, 60, 100, 500)
    item_crit = definition.row_processor(mock_row_crit, definition, {})
    assert item_crit["severity"] == "critical"
