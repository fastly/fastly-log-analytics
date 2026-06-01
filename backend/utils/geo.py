"""Geographic label formatting utilities."""

from __future__ import annotations

from backend.utils.countries import COUNTRY_MAP


def format_city_label(city: str, country: str, region: str = "") -> str:
    city = city or ""
    country = country or ""
    region = region or ""
    c_name = COUNTRY_MAP.get(country.upper(), country.upper())

    parts = []
    if city:
        parts.append(city.title())
    if region:
        parts.append(region.upper())
    if c_name:
        parts.append(c_name)
    elif country:
        parts.append(country.upper())

    return ", ".join(parts) if parts else "Unknown"
