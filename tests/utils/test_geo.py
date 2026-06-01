"""Tests for backend/utils/geo.py — format_city_label."""

from backend.utils.geo import format_city_label


class TestFormatCityLabel:
    def test_full_label(self):
        result = format_city_label("Portland", "US", "OR")
        assert result == "Portland, OR, United States"

    def test_no_region(self):
        result = format_city_label("London", "GB")
        assert result in ("London, United Kingdom", "London, GB")

    def test_country_only(self):
        result = format_city_label("", "US")
        assert result in ("United States", "US")

    def test_unknown_country_code_uppercased(self):
        result = format_city_label("Metropolis", "ZZ")
        assert "Metropolis" in result
        assert "ZZ" in result

    def test_all_empty_returns_unknown(self):
        assert format_city_label("", "", "") == "Unknown"

    def test_city_title_cased(self):
        result = format_city_label("new york", "US")
        assert result.startswith("New York")

    def test_region_uppercased(self):
        result = format_city_label("Lyon", "FR", "ara")
        assert ", ARA" in result

    def test_none_like_empty_strings(self):
        # The function coerces falsy to "" via `city = city or ""`
        result = format_city_label("", "", "")
        assert result == "Unknown"

    def test_country_without_city_or_region(self):
        result = format_city_label("", "DE", "")
        assert result in ("Germany", "DE")

    def test_country_code_case_insensitive(self):
        lower = format_city_label("Berlin", "de")
        upper = format_city_label("Berlin", "DE")
        assert lower == upper
