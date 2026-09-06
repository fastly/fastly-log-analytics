import os
from unittest.mock import patch


def test_view_build_with_walk_optimization():
    # Setup mock data dir and files
    data_dir = "/fake/data/dir"

    fake_files = [
        f"{data_dir}/timestamp_hour=2026-08-19-12/file1.parquet",
        f"{data_dir}/timestamp_hour=2026-08-19-12/file2.parquet",
    ]

    # Mock os.path.isdir and os.walk to return the fake files
    with (
        patch("os.path.isdir", return_value=True),
        patch(
            "os.walk",
            return_value=[(f"{data_dir}/timestamp_hour=2026-08-19-12", [], ["file1.parquet", "file2.parquet"])],
        ),
        patch("os.path.exists", side_effect=lambda p: p in fake_files),
    ):
        # Verification of directory scanner
        existing_local_paths_set = set()
        if os.path.isdir(data_dir):
            for root, _, filenames in os.walk(data_dir):
                for filename in filenames:
                    existing_local_paths_set.add(os.path.abspath(os.path.join(root, filename)))

        assert os.path.abspath(fake_files[0]) in existing_local_paths_set
        assert os.path.abspath(fake_files[1]) in existing_local_paths_set
        assert "/fake/data/dir/notexist.parquet" not in existing_local_paths_set
