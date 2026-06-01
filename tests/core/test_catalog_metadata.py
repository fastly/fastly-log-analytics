from backend.core.log_fields import get_catalog_for_api


def test_catalog_enrichment():
    """Verify that the catalog API output includes UI metadata like formatter and unit."""
    catalog = get_catalog_for_api()

    # Check a bytes field
    resp_bytes = next(f for f in catalog if f["id"] == "resp_bytes")
    assert resp_bytes.get("formatter") == "bytes"

    # Check a country field
    country = next(f for f in catalog if f["id"] == "country")
    assert country.get("formatter") == "country"

    # Check a field with units
    elapsed = next(f for f in catalog if f["id"] == "elapsed")
    assert elapsed.get("unit") == "µs"

    # Check a field with precision
    tls = next(f for f in catalog if f["id"] == "tls")
    assert tls.get("formatter") == "number"
    assert tls.get("precision") == 1
