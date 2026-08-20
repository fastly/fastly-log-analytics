from backend.core.metadata import usage_log, usage_log_db


def test_telemetry_schema_and_helpers(monkeypatch, tmp_path):
    # Setup temporary directory for metadata DB
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))

    # Initialize connection which runs schemas
    con = usage_log_db.get_con("test_service")

    # Verify tables exist
    tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    assert "telemetry_queries" in tables
    assert "telemetry_sections" in tables

    # Test logging queries
    usage_log.log_telemetry_query(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        engine="duckdb",
        sql="SELECT 1",
        duration_ms=45.2,
    )

    # Test logging sections
    usage_log.log_telemetry_section(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        section_name="dashboard.aggregates",
        duration_ms=12.5,
    )

    # Test retrieval
    telemetry = usage_log.get_page_telemetry("test_service", "page-123")
    assert len(telemetry["queries"]) == 1
    assert telemetry["queries"][0]["sql_query"] == "SELECT 1"
    assert telemetry["queries"][0]["engine"] == "duckdb"
    assert telemetry["queries"][0]["duration_ms"] == 45.2

    assert len(telemetry["sections"]) == 1
    assert telemetry["sections"][0]["section_name"] == "dashboard.aggregates"
    assert telemetry["sections"][0]["duration_ms"] == 12.5
