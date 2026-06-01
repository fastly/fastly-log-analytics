from backend.repositories.insights.registry import registry


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
