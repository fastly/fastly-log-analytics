"""Atomic file-write utilities using temporary sibling files and ``os.replace``."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any


@contextmanager
def atomic_temp_path(
    target: str | Path,
    *,
    prefix: str | None = None,
    suffix: str = ".tmp",
    mode: int | None = None,
) -> Iterator[Path]:
    """Yield a sibling temporary file path that is atomically replaced onto ``target`` on exit.

    If the caller raises an exception or removes the temporary file before exiting,
    the temporary file is cleaned up and ``target`` is left untouched.
    """
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=prefix if prefix is not None else f".{dst.name}.",
        suffix=suffix,
        dir=str(dst.parent),
    )
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
    """Atomically write ``data`` to ``target`` via a sibling temporary file and ``os.replace``."""
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=prefix if prefix is not None else f".{dst.name}.",
        suffix=suffix,
        dir=str(dst.parent),
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            if fsync:
                f.flush()
                os.fsync(f.fileno())
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
                f.write(data)
                if fsync:
                    f.flush()
                    os.fsync(f.fileno())
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
    """Serialize ``data`` as UTF-8 JSON and atomically write it to ``target``."""
    payload = json.dumps(
        data,
        indent=indent,
        sort_keys=sort_keys,
        separators=separators,
        default=default,
    ).encode("utf-8")
    atomic_write_bytes(
        target,
        payload,
        fsync=fsync,
        prefix=prefix,
        suffix=suffix,
        fallback_in_place_errnos=fallback_in_place_errnos,
    )
