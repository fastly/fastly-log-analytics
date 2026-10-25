"""Regression coverage for bounded late-arrival RUM source discovery."""

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from botocore.stub import Stubber
from psycopg_pool import ConnectionPool

from backend.high_scale.orchestration import HighScaleWorkerCoordinator
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.source_discovery import (
    INITIAL_SOURCE_CURSOR,
    RUM_REINSPECTION_INTERVAL,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    S3SourceObjectLister,
    SourceObjectDescriptor,
    SourceObjectMissingError,
    SourceObjectPage,
)


class _Paginator:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.responses = responses
        self.requests: list[dict[str, Any]] = []

    def paginate(self, **request: Any) -> list[dict[str, Any]]:
        self.requests.append(request)
        return [self.responses.pop(0)]


class _Client:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self.paginator = _Paginator(responses)

    def get_paginator(self, name: str) -> _Paginator:
        assert name == "list_objects_v2"
        return self.paginator


def _page(
    *,
    token: str | None = None,
    continuation_token: str | None = None,
    key: str | None = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {"Contents": []}
    if key is not None:
        response["Contents"] = [{"Key": key, "ETag": '"etag"', "Size": 1}]
    if token is not None:
        response["NextToken"] = token
    if continuation_token is not None:
        response["IsTruncated"] = True
        response["NextContinuationToken"] = continuation_token
    return response


def test_rum_cursor_schedules_a_bounded_reinspection_without_rewinding_forward_cursor() -> None:
    """One reinspection page is inserted after each ten forward pages."""
    pages = [_page(token=f"forward-{i}") for i in range(1, 11)]
    pages.append(_page(continuation_token="reinspect-1", key="raw/rum/page-1.gz"))
    client = _Client(pages)
    lister = S3SourceObjectLister(client, bucket="archive-bucket")
    cursor: str | None = INITIAL_SOURCE_CURSOR
    reinspection_cursor = INITIAL_SOURCE_CURSOR
    forward_pages = 0

    for _ in range(RUM_REINSPECTION_INTERVAL):
        page = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=25,
            cursor=cursor,
            reinspection_cursor=reinspection_cursor,
            forward_pages_since_reinspection=forward_pages,
        )
        cursor = page.next_cursor
        reinspection_cursor = page.next_reinspection_cursor or reinspection_cursor
        forward_pages = page.forward_pages_since_reinspection or 0
    assert cursor == "forward-10"
    assert reinspection_cursor == INITIAL_SOURCE_CURSOR
    assert forward_pages == RUM_REINSPECTION_INTERVAL

    page = lister.list_source_objects(
        "svc",
        "rum_vitals",
        page_size=25,
        cursor=cursor,
        reinspection_cursor=reinspection_cursor,
        reinspect=True,
        forward_pages_since_reinspection=forward_pages,
    )
    assert page.next_cursor == cursor
    assert page.next_reinspection_cursor == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-1.gz"
    assert page.forward_pages_since_reinspection == 0
    # Reinspection uses its own page token and leaves the forward scan intact.
    request = client.paginator.requests[-1]
    assert request["Prefix"] == "raw/rum/"
    assert request["PaginationConfig"] == {"PageSize": 25}


def test_rum_late_objects_are_seen_when_reinspection_wraps_to_prefix_start() -> None:
    """A new key behind the forward cursor is eligible during a bounded sweep."""
    client = _Client(
        [
            _page(key="raw/rum/late-arrival.gz"),
        ]
    )
    lister = S3SourceObjectLister(client, bucket="archive-bucket")
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/z.gz"

    page = lister.list_source_objects(
        "svc",
        "rum_errors",
        page_size=25,
        cursor=forward_cursor,
        reinspection_cursor=INITIAL_SOURCE_CURSOR,
        reinspect=True,
        forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
    )

    assert [obj.object_key for obj in page.objects] == ["raw/rum/late-arrival.gz"]
    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25}
    assert page.next_cursor == forward_cursor
    assert page.next_reinspection_cursor == INITIAL_SOURCE_CURSOR
    assert page.forward_pages_since_reinspection == 0


def test_rum_reinspection_continues_one_real_s3_page_at_a_time() -> None:
    import boto3

    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(client)
    stubber.add_response(
        "list_objects_v2",
        {
            "IsTruncated": True,
            "Contents": [
                {"Key": "raw/rum/a.gz", "ETag": '"a"', "Size": 1},
                {"Key": "raw/rum/b.gz", "ETag": '"b"', "Size": 1},
            ],
            "NextContinuationToken": "opaque-continuation-token",
        },
        {"Bucket": "bucket", "Prefix": "raw/rum/", "MaxKeys": 2},
    )
    stubber.add_response(
        "list_objects_v2",
        {
            "IsTruncated": False,
            "Contents": [{"Key": "raw/rum/c.gz", "ETag": '"c"', "Size": 1}],
        },
        {
            "Bucket": "bucket",
            "Prefix": "raw/rum/",
            "MaxKeys": 2,
            "StartAfter": "raw/rum/b.gz",
        },
    )
    lister = S3SourceObjectLister(client, bucket="bucket")

    with stubber:
        first = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=2,
            cursor="forward-cursor",
            reinspection_cursor=INITIAL_SOURCE_CURSOR,
            reinspect=True,
        )
        second = lister.list_source_objects(
            "svc",
            "rum_vitals",
            page_size=2,
            cursor="forward-cursor",
            reinspection_cursor=first.next_reinspection_cursor,
            reinspect=True,
        )

    assert [obj.object_key for obj in first.objects] == ["raw/rum/a.gz", "raw/rum/b.gz"]
    assert first.next_reinspection_cursor == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/b.gz"
    assert [obj.object_key for obj in second.objects] == ["raw/rum/c.gz"]
    assert second.next_reinspection_cursor == INITIAL_SOURCE_CURSOR
    assert first.next_cursor == second.next_cursor == "forward-cursor"


def test_legacy_rum_terminal_cursor_keeps_start_after_semantics() -> None:
    client = _Client([_page(key="raw/rum/new.gz")])
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    page = lister.list_source_objects(
        "svc",
        "rum_vitals",
        page_size=25,
        cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/previous.gz",
    )

    assert client.paginator.requests[0]["StartAfter"] == "raw/rum/previous.gz"
    assert page.objects[0].object_key == "raw/rum/new.gz"
    assert page.next_cursor is None


def test_legacy_rum_paginator_token_keeps_starting_token_semantics() -> None:
    client = _Client([_page(token="next-token")])
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    page = lister.list_source_objects("svc", "rum_errors", page_size=25, cursor="legacy-page-token")

    assert client.paginator.requests[0]["PaginationConfig"] == {
        "PageSize": 25,
        "StartingToken": "legacy-page-token",
    }
    assert page.next_cursor == "next-token"


def test_request_cursor_path_remains_unchanged() -> None:
    client = _Client([_page(key="raw/request/new.gz")])
    lister = S3SourceObjectLister(client, bucket="archive-bucket")

    lister.list_source_objects(
        "svc",
        "request",
        page_size=25,
        cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/request/previous.gz",
    )

    assert client.paginator.requests[0]["PaginationConfig"] == {"PageSize": 25}
    assert client.paginator.requests[0]["StartAfter"] == "raw/request/previous.gz"


def test_coordinator_persists_reinspection_state_without_advancing_forward_cursor() -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(
        current_owner="high_scale",
        owner_epoch=3,
        source_cursor="owner-cursor",
    )
    control.source_cursor_for.return_value = forward_cursor
    control.source_reinspection_state_for.return_value = (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL)
    control.claim_sources_batch.return_value = ()
    control.expired_claims.return_value = ()
    control.advance_source_cursor = MagicMock()

    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(
        (),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=MagicMock(),
        controller=MagicMock(),
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="rum-reinspection-test",
        control_plane=control,
    )

    run = coordinator.run_page(service_id="svc", domain="rum_vitals")

    assert run.next_cursor == forward_cursor
    lister.list_source_objects.assert_called_once_with(
        "svc",
        "rum_vitals",
        page_size=100,
        cursor=forward_cursor,
        reinspection_cursor=INITIAL_SOURCE_CURSOR,
        reinspect=True,
        forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
    )
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_called_once_with(
        "svc",
        "rum_vitals",
        INITIAL_SOURCE_CURSOR,
        0,
        expected_owner="high_scale",
        expected_owner_epoch=3,
    )


def test_coordinator_advances_forward_and_reinspection_cursors_together() -> None:
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(
        current_owner="high_scale",
        owner_epoch=3,
        source_cursor="owner-cursor",
    )
    control.source_cursor_for.return_value = "forward-page-1"
    control.source_reinspection_state_for.return_value = ("reinspection-page-2", 2)
    control.claim_sources_batch.return_value = ()
    control.expired_claims.return_value = ()

    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(
        (),
        "forward-page-2",
        next_reinspection_cursor="reinspection-page-2",
        forward_pages_since_reinspection=3,
    )
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=MagicMock(),
        controller=MagicMock(),
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="rum-forward-test",
        control_plane=control,
    )

    coordinator.run_page(service_id="svc", domain="rum_errors")

    control.advance_source_cursor.assert_called_once_with(
        "svc",
        "forward-page-2",
        expected_owner="high_scale",
        expected_owner_epoch=3,
        domain="rum_errors",
        reinspection_cursor="reinspection-page-2",
        forward_pages_since_reinspection=3,
    )


def test_postgres_reinspection_state_defaults_and_reads_domain_cursor_fields() -> None:
    pool = MagicMock()
    connection = MagicMock()
    pool.connection.return_value.__enter__.return_value = connection
    connection.transaction.return_value.__enter__.return_value = connection
    selected = MagicMock()
    selected.fetchone.return_value = ("__terminal__:raw/rum/a.gz", 4)
    connection.execute.side_effect = [MagicMock(), selected]
    control = PostgresControlPlane(pool=pool)

    assert control.source_reinspection_state_for("svc", "rum_vitals") == (
        "__terminal__:raw/rum/a.gz",
        4,
    )
    sql = connection.execute.call_args_list[1].args[0]
    assert "reinspection_cursor" in sql
    assert "forward_pages_since_reinspection" in sql


def test_postgres_reinspection_state_defaults_for_a_new_domain() -> None:
    pool = MagicMock()
    connection = MagicMock()
    pool.connection.return_value.__enter__.return_value = connection
    connection.transaction.return_value.__enter__.return_value = connection
    selected = MagicMock()
    selected.fetchone.return_value = None
    connection.execute.side_effect = [MagicMock(), selected]
    control = PostgresControlPlane(pool=pool)

    assert control.source_reinspection_state_for("svc", "rum_errors") == (INITIAL_SOURCE_CURSOR, 0)


def test_postgres_reinspection_advance_does_not_update_forward_source_cursor() -> None:
    pool = MagicMock()
    connection = MagicMock()
    pool.connection.return_value.__enter__.return_value = connection
    connection.transaction.return_value.__enter__.return_value = connection
    owner = MagicMock()
    owner.fetchone.return_value = {
        "service_id": "svc",
        "owner_epoch": 3,
        "current_owner": "high_scale",
        "previous_owner": None,
        "source_cursor": "owner-cursor",
        "drain_complete": False,
        "cutover_committed": False,
        "rollback_allowed": False,
    }
    connection.execute.side_effect = [owner, MagicMock()]
    control = PostgresControlPlane(pool=pool)

    control.advance_reinspection_state(
        "svc",
        "rum_vitals",
        "reinspection-page-2",
        0,
        expected_owner="high_scale",
        expected_owner_epoch=3,
    )

    update_sql = connection.execute.call_args_list[1].args[0]
    assert "SET reinspection_cursor=%s" in update_sql
    assert "forward_pages_since_reinspection=%s" in update_sql
    assert "source_cursor=" not in update_sql
    assert connection.execute.call_args_list[1].args[1] == (
        "reinspection-page-2",
        0,
        "svc",
        "rum_vitals",
    )


@pytest.mark.parametrize("failure_stage", ["read", "publication"])
def test_reinspection_failure_preserves_forward_and_reinspection_cursors(failure_stage: str) -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    descriptor = SourceObjectDescriptor("raw/rum/late.gz", "etag:late", 12, "v1")
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(current_owner="high_scale", owner_epoch=5, source_cursor=None)
    control.source_cursor_for.return_value = forward_cursor
    control.source_reinspection_state_for.return_value = (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL)
    control.discover_source.return_value = SimpleNamespace(status="discovered")
    control.claim_sources_batch.return_value = (SimpleNamespace(object_key=descriptor.object_key),)
    control.expired_claims.return_value = ()
    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(
        (descriptor,),
        forward_cursor,
        next_reinspection_cursor=f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/late.gz",
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    reader = MagicMock()
    controller = MagicMock()
    if failure_stage == "read":
        reader.read_source_object.side_effect = RuntimeError("temporary FOS read failure")
    else:
        reader.read_source_object.return_value = b"source payload"
        controller.ingest_batch.side_effect = RuntimeError("temporary publication failure")
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=controller,
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="rum-reinspection-failure-test",
        control_plane=control,
    )

    run = coordinator.run_page(service_id="svc", domain="rum_vitals")

    assert run.failed == 1
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_not_called()
    if failure_stage == "publication":
        assert controller.ingest_batch.call_args.kwargs["next_cursor"] is None


def test_confirmed_missing_reinspection_object_advances_only_reinspection_position() -> None:
    forward_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
    descriptor = SourceObjectDescriptor("raw/rum/late-missing.gz", "etag:late", 12, "v1")
    control = MagicMock()
    control.owner.return_value = SimpleNamespace(current_owner="high_scale", owner_epoch=5, source_cursor=None)
    control.source_cursor_for.return_value = forward_cursor
    control.source_reinspection_state_for.return_value = (INITIAL_SOURCE_CURSOR, RUM_REINSPECTION_INTERVAL)
    control.discover_source.return_value = SimpleNamespace(status="discovered")
    control.claim_sources_batch.return_value = (SimpleNamespace(object_key=descriptor.object_key),)
    control.expired_claims.return_value = ()
    lister = MagicMock()
    lister.list_source_objects.return_value = SourceObjectPage(
        (descriptor,),
        forward_cursor,
        next_reinspection_cursor=INITIAL_SOURCE_CURSOR,
        forward_pages_since_reinspection=0,
        reinspection_page=True,
    )
    reader = MagicMock()
    reader.read_source_object.side_effect = SourceObjectMissingError("source disappeared")
    coordinator = HighScaleWorkerCoordinator(
        lister=lister,
        reader=reader,
        controller=MagicMock(),
        ownership=MagicMock(),
        ledger=MagicMock(),
        worker_id="rum-reinspection-missing-test",
        control_plane=control,
    )

    run = coordinator.run_page(service_id="svc", domain="rum_vitals")

    assert run.missing == 1 and run.failed == 0
    control.mark_source_missing.assert_called_once()
    control.advance_source_cursor.assert_not_called()
    control.advance_reinspection_state.assert_called_once_with(
        "svc",
        "rum_vitals",
        INITIAL_SOURCE_CURSOR,
        0,
        expected_owner="high_scale",
        expected_owner_epoch=5,
    )


@pytest.mark.postgres_integration
def test_postgres_rum_reinspection_state_is_schema_scoped_and_owner_fenced() -> None:
    dsn = os.getenv("HIGH_SCALE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("HIGH_SCALE_POSTGRES_TEST_DSN is not set")

    schema = f"hs_reinspect_{uuid4().hex}"
    setup_pool = ConnectionPool(
        conninfo=dsn,
        kwargs={"autocommit": False},
        min_size=1,
        max_size=1,
        timeout=5,
    )
    scoped_pool: ConnectionPool | None = None
    try:
        setup_pool.open(wait=True)
        with setup_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(f'CREATE SCHEMA "{schema}"')
        scoped_pool = ConnectionPool(
            conninfo=dsn,
            kwargs={"autocommit": False, "options": f"-csearch_path={schema}"},
            min_size=1,
            max_size=2,
            timeout=5,
        )
        scoped_pool.open(wait=True)
        control = PostgresControlPlane(pool=scoped_pool, pool_timeout=5)
        control.create_schema()
        owner = control.initialize_owner("svc", owner="high_scale", source_cursor="owner")
        control.advance_source_cursor(
            "svc",
            f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz",
            expected_owner="high_scale",
            expected_owner_epoch=owner.owner_epoch,
            domain="rum_vitals",
            reinspection_cursor=INITIAL_SOURCE_CURSOR,
            forward_pages_since_reinspection=RUM_REINSPECTION_INTERVAL,
        )

        # Simulate an existing schema and row from a worker predating the
        # additive state. Bootstrap must preserve its forward wire cursor.
        with scoped_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(
                "ALTER TABLE high_scale_source_cursors "
                "DROP COLUMN reinspection_cursor, DROP COLUMN forward_pages_since_reinspection"
            )
        control.create_schema()
        assert control.source_cursor_for("svc", "rum_vitals") == f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz"
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (INITIAL_SOURCE_CURSOR, 0)

        control.advance_reinspection_state(
            "svc",
            "rum_vitals",
            f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-2.gz",
            0,
            expected_owner="high_scale",
            expected_owner_epoch=owner.owner_epoch,
        )
        assert control.source_cursor_for("svc", "rum_vitals") == (f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz")
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (
            f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-2.gz",
            0,
        )
        # A rolling older worker updates just the forward column and must not
        # erase a newer worker's independently persisted sweep position.
        control.advance_source_cursor(
            "svc",
            f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz",
            expected_owner="high_scale",
            expected_owner_epoch=owner.owner_epoch,
            domain="rum_vitals",
        )
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (
            f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-2.gz",
            0,
        )

        with pytest.raises(ValueError, match="owner epoch"):
            control.advance_reinspection_state(
                "svc",
                "rum_vitals",
                INITIAL_SOURCE_CURSOR,
                0,
                expected_owner="high_scale",
                expected_owner_epoch=owner.owner_epoch + 1,
            )
        assert control.source_cursor_for("svc", "rum_vitals") == (f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/latest.gz")
        assert control.source_reinspection_state_for("svc", "rum_vitals") == (
            f"{TERMINAL_SOURCE_CURSOR_PREFIX}raw/rum/page-2.gz",
            0,
        )
    finally:
        if scoped_pool is not None:
            scoped_pool.close()
        if setup_pool.closed:
            setup_pool.open(wait=True)
        with setup_pool.connection(timeout=5) as connection, connection.transaction():
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        setup_pool.close()
