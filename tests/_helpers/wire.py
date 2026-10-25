"""Byte-for-byte wire comparison between a producer's dict output and a response model."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Iterable
from typing import Any

from fastapi.encoders import jsonable_encoder

_TELEMETRY_KEYS = frozenset(
    {
        "debug_queries",
        "debug_calls",
        "debug_sqlite",
        "debug_postgres",
        "section_timings",
        "is_cached",
        "_debug_queries",
        "_debug_calls",
        "_debug_sqlite",
        "_debug_postgres",
        "_section_timings",
        "_is_cached",
    }
)


def wire(value: Any) -> str:
    return json.dumps(jsonable_encoder(value), separators=(",", ":"), ensure_ascii=False)


def _wire_key(model_cls: type, key: str) -> str:
    """The serialized key for a producer key (a field name or an alias such as ``_approx``)."""
    fields = model_cls.model_fields
    info = fields.get(key)
    if info is None:
        info = next((f for f in fields.values() if key in (f.alias, f.serialization_alias)), None)
    assert info is not None, f"{model_cls.__name__} does not declare producer key {key!r}"
    return info.serialization_alias or info.alias or key


def assert_sections_wire_identical(model_cls: type, produced: dict[str, Any], *, non_empty: Iterable[str] = ()) -> None:
    """Every non-telemetry key the producer emitted must serialize to the same bytes
    through ``model_cls`` as it does through ``jsonable_encoder`` (the untyped path)."""
    for key in non_empty:
        assert produced.get(key), f"fixture did not exercise {key!r}; the comparison would be vacuous"
    model = model_cls.model_validate({k: v for k, v in produced.items() if k not in _TELEMETRY_KEYS})
    dumped = json.loads(model.model_dump_json(by_alias=True))
    for key, value in produced.items():
        if key in _TELEMETRY_KEYS:
            continue
        out_key = _wire_key(model_cls, key)
        assert out_key in dumped, f"{key!r} dropped from the wire"
        assert wire(dumped[out_key]) == wire(value), f"{key!r} wire bytes differ from the producer output"


def assert_rows_wire_identical(row_cls: type, rows: list[dict[str, Any]]) -> None:
    """Row-level variant for models used directly as ``list[Row]`` in a response."""
    assert rows, "no rows to compare"
    dumped = [json.loads(row_cls.model_validate(r).model_dump_json(by_alias=True, exclude_unset=True)) for r in rows]
    assert wire(dumped) == wire(rows)


def parse_sse_events(text: str) -> list[dict]:
    """Decode every ``data:`` event in an SSE body into a dict, skipping non-JSON payloads."""
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        chunk = chunk.strip()
        if chunk.startswith("data:"):
            try:
                out.append(json.loads(chunk[len("data:") :].strip()))
            except json.JSONDecodeError:
                pass
    return out


def wait_until(
    check_fn: Callable[[], bool],
    timeout: float = 1.0,
    initial_interval: float = 0.001,
    backoff_factor: float = 2.0,
    max_interval: float = 0.05,
    message: str = "",
) -> None:
    """Poll ``check_fn`` until it returns truthy or ``timeout`` elapses."""
    start = time.perf_counter()
    interval = initial_interval
    while True:
        if check_fn():
            return
        if time.perf_counter() - start >= timeout:
            suffix = f": {message}" if message else ""
            raise AssertionError(f"Timed out after {timeout}s waiting for condition{suffix}")
        time.sleep(interval)
        interval = min(interval * backoff_factor, max_interval)


async def await_until(
    check_fn: Callable[[], bool],
    timeout: float = 1.0,
    initial_interval: float = 0.001,
    backoff_factor: float = 2.0,
    max_interval: float = 0.05,
    message: str = "",
) -> None:
    """Async variant of :func:`wait_until`."""
    start = time.perf_counter()
    interval = initial_interval
    while True:
        if check_fn():
            return
        if time.perf_counter() - start >= timeout:
            suffix = f": {message}" if message else ""
            raise AssertionError(f"Timed out after {timeout}s waiting for condition{suffix}")
        await asyncio.sleep(interval)
        interval = min(interval * backoff_factor, max_interval)
