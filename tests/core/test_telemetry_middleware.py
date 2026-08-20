from fastapi.testclient import TestClient

from backend.main import app
from backend.utils.telemetry import get_page_load_id


def test_page_load_id_propagation_middleware():
    client = TestClient(app)
    # Fire request with X-Page-Load-ID
    response = client.get("/api/health", headers={"X-Page-Load-ID": "test-page-999"})
    assert response.status_code == 200
    # Clean check outside request context should be None
    assert get_page_load_id() is None
