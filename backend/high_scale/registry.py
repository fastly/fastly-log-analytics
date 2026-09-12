"""Explicit service bindings for the isolated high-scale query surface."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.query_service import QueryClient


@dataclass(frozen=True)
class HighScaleService:
    service_id: str
    client: QueryClient
    cursor_secret: bytes
    request_watermark: ServingWatermark | Callable[[], ServingWatermark]

    def watermark(self) -> ServingWatermark:
        value = self.request_watermark() if callable(self.request_watermark) else self.request_watermark
        value.validate()
        return value


class HighScaleServiceRegistryProtocol(Protocol):
    def resolve(self, service_id: str) -> HighScaleService | None: ...


class HighScaleServiceRegistry:
    """Small injectable registry; services opt in by explicit registration."""

    def __init__(self, services: Mapping[str, HighScaleService] | None = None) -> None:
        self._services = dict(services or {})

    def register(self, service: HighScaleService) -> None:
        if not service.service_id:
            raise ValueError("high-scale service id is required")
        if not service.cursor_secret:
            raise ValueError("high-scale cursor secret is required")
        self._services[service.service_id] = service

    def resolve(self, service_id: str) -> HighScaleService | None:
        service = self._services.get(service_id)
        if service is None or service.service_id != service_id:
            return None
        return service


_registry = HighScaleServiceRegistry()


def get_high_scale_service_registry() -> HighScaleServiceRegistryProtocol:
    """FastAPI dependency for explicit high-scale service bindings."""
    return _registry
