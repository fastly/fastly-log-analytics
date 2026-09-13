from __future__ import annotations

from backend.high_scale.source_discovery import (
    INITIAL_SOURCE_CURSOR,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    S3SourceObjectLister,
    S3SourceObjectReader,
)


class _Body:
    def read(self) -> bytes:
        return b"payload"


class _Paginator:
    def __init__(self) -> None:
        self.requests: list[dict] = []

    def paginate(self, **request):
        self.requests.append(request)
        return iter(
            [
                {
                    "Contents": [
                        {
                            "Key": "tenant/raw/request/one.gz",
                            "ETag": '"source-etag"',
                            "Size": 7,
                        }
                    ],
                    "NextToken": "next-page",
                }
            ]
        )


class _Client:
    def __init__(self) -> None:
        self.paginator = _Paginator()
        self.get_object_calls: list[dict] = []

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self.paginator

    def get_object(self, **request):
        self.get_object_calls.append(request)
        return {"Body": _Body()}


def test_s3_lister_uses_bounded_paginated_listing_and_source_identity() -> None:
    client = _Client()
    lister = S3SourceObjectLister(client, bucket="archive-bucket", prefix="tenant")

    page = lister.list_source_objects("svc", "request", page_size=25, cursor="cursor-1")

    assert page.next_cursor == "next-page"
    assert page.objects[0].object_key == "tenant/raw/request/one.gz"
    assert page.objects[0].checksum == "etag:source-etag"
    assert page.objects[0].size_bytes == 7
    request = client.paginator.requests[0]
    assert request["Bucket"] == "archive-bucket"
    assert request["Prefix"] == "tenant/raw/request/"
    assert request["PaginationConfig"] == {"PageSize": 25, "StartingToken": "cursor-1"}


def test_s3_reader_reads_the_exact_listed_key() -> None:
    client = _Client()
    reader = S3SourceObjectReader(client, bucket="archive-bucket")

    assert reader.read_source_object("svc", "request", "tenant/raw/request/one.gz") == b"payload"
    assert client.get_object_calls == [{"Bucket": "archive-bucket", "Key": "tenant/raw/request/one.gz"}]


def test_s3_lister_treats_initial_cursor_as_start_of_bucket() -> None:
    client = _Client()
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    lister.list_source_objects("svc", "request", page_size=25, cursor=INITIAL_SOURCE_CURSOR)

    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25}


def test_s3_lister_uses_start_after_for_terminal_cursor() -> None:
    client = _Client()
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    lister.list_source_objects(
        "svc",
        "request",
        page_size=25,
        cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/last.gz",
    )

    request = client.paginator.requests[0]
    assert request["PaginationConfig"] == {"PageSize": 25}
    assert request["StartAfter"] == "raw/request/last.gz"
