from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

from backend.utils.bounded_cache import CacheRegistry

_active_requests_logger = logging.getLogger("backend.utils.active_requests")
_lock = threading.Lock()
_active_count = 0
_defer_started_at: dict[tuple[str, str], float] = {}


def increment_active_requests() -> None:
    global _active_count
    with _lock:
        _active_count += 1


def decrement_active_requests() -> None:
    global _active_count
    with _lock:
        if _active_count > 0:
            _active_count -= 1


def active_request_count() -> int:
    with _lock:
        return _active_count


def should_defer_cron(job_name: str, service_id: str, *, max_defer_secs: float = 30.0) -> bool:
    key = (job_name, service_id)
    with _lock:
        if _active_count == 0:
            _defer_started_at.pop(key, None)
            return False
        now = time.monotonic()
        started = _defer_started_at.get(key)
        if started is None:
            _defer_started_at[key] = now
            return True
        if (now - started) >= max_defer_secs:
            _defer_started_at.pop(key, None)
            _active_requests_logger.warning(
                "[active-request-gate] %s/%s ran after %.1fs of active-request deferrals — "
                "sustained traffic on this service; consider raising max_defer_secs.",
                job_name,
                service_id,
                now - started,
            )
            return False
        return True


def yield_to_api(
    *,
    max_wait_secs: float = 1.0,
    poll_interval: float = 0.1,
    sleep_fn: Callable[[float], None] | None = None,
) -> float:
    if active_request_count() == 0:
        return 0.0
    sleep = sleep_fn or time.sleep
    deadline = time.monotonic() + max_wait_secs
    slept = 0.0
    while time.monotonic() < deadline:
        sleep(poll_interval)
        slept += poll_interval
        if active_request_count() == 0:
            return slept
    return slept


def _reset_active_requests_for_tests() -> None:
    global _active_count
    with _lock:
        _active_count = 0
        _defer_started_at.clear()


class _ActiveRequestsResetAdapter:
    @staticmethod
    def clear() -> None:
        _reset_active_requests_for_tests()


CacheRegistry.register("utils.active_requests", _ActiveRequestsResetAdapter())


def _mkstemp_sibling(target: str | Path, prefix: str | None, suffix: str) -> tuple[Path, int, str]:
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=prefix if prefix is not None else f".{dst.name}.",
        suffix=suffix,
        dir=str(dst.parent),
    )
    return dst, fd, tmp_name


def _write_stream(f: Any, data: bytes, fsync: bool) -> None:
    f.write(data)
    if fsync:
        f.flush()
        os.fsync(f.fileno())


@contextmanager
def atomic_temp_path(
    target: str | Path,
    *,
    prefix: str | None = None,
    suffix: str = ".tmp",
    mode: int | None = None,
) -> Iterator[Path]:
    dst, fd, tmp_name = _mkstemp_sibling(target, prefix, suffix)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        if mode is not None:
            os.chmod(tmp_path, mode)
        yield tmp_path
        if tmp_path.exists():
            os.replace(tmp_path, dst)
    finally:
        with suppress(OSError):
            if tmp_path.exists():
                tmp_path.unlink()


def atomic_write_bytes(
    target: str | Path,
    data: bytes,
    *,
    fsync: bool = False,
    mode: int | None = None,
    prefix: str | None = None,
    suffix: str = ".tmp",
    fallback_in_place_errnos: set[int] | None = None,
) -> None:
    dst, fd, tmp_name = _mkstemp_sibling(target, prefix, suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            _write_stream(f, data, fsync)
        if mode is not None:
            os.chmod(tmp_name, mode)
        os.replace(tmp_name, dst)
    except OSError as e:
        with suppress(OSError):
            os.unlink(tmp_name)
        err_msg = str(e).lower()
        if fallback_in_place_errnos and (
            e.errno in fallback_in_place_errnos or "busy" in err_msg or "permission" in err_msg
        ):
            with open(dst, "wb") as f:
                _write_stream(f, data, fsync)
            return
        raise
    except BaseException:
        with suppress(OSError):
            os.unlink(tmp_name)
        raise


def atomic_write_json(
    target: str | Path,
    data: Any,
    *,
    indent: int | None = 2,
    sort_keys: bool = False,
    separators: tuple[str, str] | None = None,
    default: Callable[[Any], Any] | None = None,
    fsync: bool = False,
    prefix: str | None = None,
    suffix: str = ".tmp",
    fallback_in_place_errnos: set[int] | None = None,
) -> None:
    payload = json.dumps(data, indent=indent, sort_keys=sort_keys, separators=separators, default=default).encode(
        "utf-8"
    )
    atomic_write_bytes(
        target,
        payload,
        fsync=fsync,
        prefix=prefix,
        suffix=suffix,
        fallback_in_place_errnos=fallback_in_place_errnos,
    )
