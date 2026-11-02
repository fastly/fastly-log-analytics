"""Service management package."""

from __future__ import annotations

from backend.services import service_manager
from backend.services.service_manager import _get_dir_stats, get_enriched_services

__all__ = [
    "_get_dir_stats",
    "get_enriched_services",
    "service_manager",
]
