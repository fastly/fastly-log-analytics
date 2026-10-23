from backend.core import metadata as metadata_db
from backend.provision.declarative.diff import DiffResult, LoggingEndpoint
from backend.provision.declarative.reconciler import _record_reconcile_audit


def _endpoint(fmt: str) -> LoggingEndpoint:
    return LoggingEndpoint(
        name="Fastly Log Analytics - FOS",
        endpoint_type="s3",
        path="/",
        period=5,
        response_condition="",
        format_string=fmt,
        placement=None,
        response_object_name="",
    )


def test_reconcile_records_added_and_removed_fields():
    before = [_endpoint('{"ip":"%{client.ip}V","ua":"x","old_field":"y"}')]
    after = [_endpoint('{"ip":"%{client.ip}V","ua":"x","new_field":"z"}')]

    _record_reconcile_audit("svc_audit_test", DiffResult(), before, after, 42)

    _, rows = metadata_db.get_audit_logs("svc_audit_test", event_type="log_format_change", page=1, per_page=10)
    assert len(rows) == 1
    d = rows[0]["details"]
    assert d["fields_added"] == ["new_field"]
    assert d["fields_removed"] == ["old_field"]
    assert d["version"] == 42
    assert rows[0]["actor"] == "reconciler"
