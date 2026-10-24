from backend.utils.service_ids import service_id_from_source


def test_service_id_from_source_prefers_explicit_override():
    assert service_id_from_source({"name": "name", "service_id": "id"}, "override") == "override"


def test_service_id_from_source_uses_service_id_by_default():
    assert service_id_from_source({"name": "name", "service_id": "id"}) == "id"


def test_service_id_from_source_can_prefer_name():
    assert service_id_from_source({"name": "name", "service_id": "id"}, prefer_name=True) == "name"


def test_service_id_from_source_falls_back_to_available_value():
    assert service_id_from_source({"name": "name"}) == "name"
    assert service_id_from_source({}) == ""
