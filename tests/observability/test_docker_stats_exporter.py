from observability.docker_stats_exporter import _block_io_bytes


def test_block_io_bytes_sums_read_and_write_operations():
    stats = {
        "blkio_stats": {
            "io_service_bytes_recursive": [
                {"op": "Read", "value": 1024},
                {"op": "Write", "value": 2048},
                {"op": "Read", "value": 512},
                {"op": "Sync", "value": 99},
            ]
        }
    }

    assert _block_io_bytes(stats) == (1536, 2048)


def test_block_io_bytes_handles_missing_or_invalid_values():
    assert _block_io_bytes({}) == (0, 0)
    assert _block_io_bytes(
        {"blkio_stats": {"io_service_bytes_recursive": [{"op": "Read"}, {"op": "Write", "value": "bad"}]}}
    ) == (0, 0)
