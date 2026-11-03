"""Ingest chunks are bounded by compressed bytes as well as file count, so a high-RPS backlog cannot build a multi-GB batch."""

from __future__ import annotations

import pytest

from backend.core.ingest.writer import _plan_chunks

MIB = 1024 * 1024


def _files(n: int) -> list[str]:
    return [f"s3://b/raw/{i:05d}.log.gz" for i in range(n)]


def test_small_files_keep_the_file_count_cap():
    files = _files(1200)
    sizes = {f: 10_000 for f in files}  # 12 MB total, far under the byte cap
    chunks = _plan_chunks(files, sizes, max_files=500, max_bytes=128 * MIB)
    assert [len(c) for c in chunks] == [500, 500, 200]


def test_large_files_split_on_the_byte_cap():
    files = _files(10)
    sizes = {f: 40 * MIB for f in files}  # 400 MiB total
    chunks = _plan_chunks(files, sizes, max_files=500, max_bytes=128 * MIB)
    assert [len(c) for c in chunks] == [3, 3, 3, 1]
    assert all(sum(sizes[f] for f in c) <= 128 * MIB for c in chunks)


def test_a_single_oversized_file_is_processed_alone_not_dropped():
    files = _files(3)
    sizes = {files[0]: 10 * MIB, files[1]: 500 * MIB, files[2]: 10 * MIB}
    chunks = _plan_chunks(files, sizes, max_files=500, max_bytes=128 * MIB)
    assert chunks == [[files[0]], [files[1]], [files[2]]]


def test_unknown_sizes_fall_back_to_the_count_cap():
    files = _files(7)
    chunks = _plan_chunks(files, {f: None for f in files}, max_files=3, max_bytes=128 * MIB)
    assert [len(c) for c in chunks] == [3, 3, 1]
    assert [len(c) for c in _plan_chunks(files, {}, max_files=3, max_bytes=128 * MIB)] == [3, 3, 1]


@pytest.mark.parametrize("n", [0, 1, 499, 500, 501, 2_345])
def test_every_file_appears_exactly_once_in_order(n):
    files = _files(n)
    sizes = {f: (i % 7 + 1) * 5 * MIB for i, f in enumerate(files)}
    chunks = _plan_chunks(files, sizes, max_files=500, max_bytes=128 * MIB)
    assert [f for c in chunks for f in c] == files
    assert all(c for c in chunks)
