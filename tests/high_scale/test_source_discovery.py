import pytest

from backend.high_scale.source_discovery import (
    MAX_SOURCE_PAGE_SIZE,
    S3SourceObjectLister,
    SourceObjectDescriptor,
    SourceObjectPage,
)


class _DummyPaginator:
    def paginate(self, **kwargs):
        return [
            {
                "Contents": [
                    {"Key": "raw/request/a.gz", "ETag": '"etag-a"', "Size": 10},
                ],
                "NextToken": "cursor-1",
            }
        ]


class _DummyS3Client:
    def get_paginator(self, _name: str):
        return _DummyPaginator()


def test_s3_source_object_lister_lists_bounded_page() -> None:
    lister = S3SourceObjectLister(_DummyS3Client(), bucket="fos-bucket")
    page = lister.list_source_objects("svc", "request", page_size=10, cursor=None)
    assert page == SourceObjectPage(
        (SourceObjectDescriptor("raw/request/a.gz", "etag:etag-a", 10, '"etag-a"'),),
        "cursor-1",
    )


def test_s3_source_object_lister_rejects_unbounded_page_size() -> None:
    lister = S3SourceObjectLister(_DummyS3Client(), bucket="fos-bucket")
    with pytest.raises(ValueError, match="page_size"):
        lister.list_source_objects("svc", "request", page_size=MAX_SOURCE_PAGE_SIZE + 1, cursor=None)
