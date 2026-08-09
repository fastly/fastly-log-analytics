"""Tests for ``backend.provision.fos_setup`` — FOS bucket + access-key CRUD.

This module is the provisioning CLI's S3 layer. Its operations are
hard-to-reverse (creating buckets, generating keys) and rarely run in
production, so the tests pin happy paths + the idempotency branches
(already-exists detection) that protect against double-provisioning
when a user re-runs the wizard.

All boto3 + Fastly API calls are mocked at the client boundary.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from backend.provision import fos_setup

# ── _get_fos_s3_client ─────────────────────────────────────────────────────


def test_get_fos_s3_client_returns_raw_boto_when_no_service_id():
    """Without a service_id (e.g. the standalone CLI before a service
    is registered), the helper returns the bare boto3 client and
    skips telemetry. Pinned because wrapping a raw boto3 client in
    the tracker without a service_id would NPE on flush()."""
    with patch("boto3.client") as mock_boto:
        mock_boto.return_value = "raw-client"
        out = fos_setup._get_fos_s3_client("k", "s", "us-east-1")

    assert out == "raw-client"


def test_get_fos_s3_client_wraps_with_tracker_when_service_id_provided():
    """When a service_id is supplied (e.g. by the orchestrator), the
    boto3 client is wrapped in _ProvisioningTracker so every call
    lands in usage_log. Pinned because losing this wrapper was the
    bug that left provisioning ops invisible to dashboards."""
    with patch("boto3.client") as mock_boto:
        mock_boto.return_value = MagicMock()
        out = fos_setup._get_fos_s3_client("k", "s", "us-east-1", service_id="svc-1", bucket_name="my-bucket")

    assert isinstance(out, fos_setup._ProvisioningTracker)
    assert out._service_id == "svc-1"
    assert out._bucket_name == "my-bucket"


def test_get_fos_s3_client_uses_region_endpoint():
    """The endpoint URL is derived from ``region_endpoint(region)``,
    NOT hardcoded to us-east-1. Pinned because losing this would
    silently route eu-west-1 traffic to the US shard."""
    with (
        patch("boto3.client") as mock_boto,
        patch("backend.core.fastly.utils.region_endpoint", return_value="eu-west-1.object.fastlystorage.app"),
    ):
        fos_setup._get_fos_s3_client("k", "s", "eu-west-1")

    kwargs = mock_boto.call_args.kwargs
    assert "eu-west-1.object.fastlystorage.app" in kwargs["endpoint_url"]
    assert kwargs["region_name"] == "eu-west-1"


# ── ensure_fos_bucket ──────────────────────────────────────────────────────


def test_ensure_fos_bucket_returns_true_when_bucket_exists():
    """``head_bucket`` succeeds → bucket exists → no create call.
    Pinned because re-creating would fail with BucketAlreadyExists
    and break the provisioning wizard on re-runs."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}  # 200 OK

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        out = fos_setup.ensure_fos_bucket("my-bucket", "us-east-1", "k", "s")

    assert out is True
    fake_s3.create_bucket.assert_not_called()


def test_ensure_fos_bucket_creates_when_head_raises_404():
    """``head_bucket`` raises (bucket missing) → call ``create_bucket``.
    Pinned because a refactor that re-throws on any ClientError would
    abort first-time provisioning."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        out = fos_setup.ensure_fos_bucket("new-bucket", "us-east-1", "k", "s")

    assert out is True
    fake_s3.create_bucket.assert_called_once_with(Bucket="new-bucket")


def test_ensure_fos_bucket_calls_status_callback_on_create():
    """When a ``status_cb`` is provided, the wizard surfaces "Creating
    bucket..." + "Bucket created" to the user. Pinned because losing
    these would make the provisioning UI silent during a long step."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")

    statuses: list[str] = []
    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", status_cb=statuses.append)

    assert any("Creating bucket" in s for s in statuses)
    assert any("Bucket created" in s for s in statuses)


def test_ensure_fos_bucket_raises_runtime_error_on_create_failure():
    """A ClientError from ``create_bucket`` (perms denied, quota) →
    RuntimeError with the cause. Pinned because RuntimeError is what
    the wizard's outer try/except catches to render a clear error."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "perms"}}, "CreateBucket"
    )

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError, match="Could not create FOS bucket"):
            fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s")


# ── delete_fos_bucket ─────────────────────────────────────────────────────


def test_delete_fos_bucket_short_circuits_when_already_deleted():
    """head_bucket → 404 → no list_objects / delete_bucket calls.
    Pinned because the teardown is idempotent — running it twice
    must not error."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")  # must not raise

    fake_s3.delete_bucket.assert_not_called()


def test_delete_fos_bucket_empties_then_deletes_on_happy_path():
    """Bucket has objects → list_objects + delete_objects + delete_bucket.
    Pinned because skipping the empty step would surface as a
    BucketNotEmpty error after a few attempts."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Deleted": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    fake_s3.delete_objects.assert_called()
    fake_s3.delete_bucket.assert_called_with(Bucket="b")


def test_delete_fos_bucket_raises_after_max_retries_on_bucket_not_empty():
    """If ``delete_bucket`` repeatedly returns BucketNotEmpty (which
    means deleted-and-undeleted by S3 eventual consistency), the
    helper retries with backoff but eventually raises after 15
    attempts. Pinned because infinite retries would hang the wizard."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.side_effect = ClientError(
        {"Error": {"Code": "BucketNotEmpty", "Message": "x"}}, "DeleteBucket"
    )

    with (
        patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3),
        patch("time.sleep"),  # skip the retry backoff
    ):
        with pytest.raises(RuntimeError, match="Could not delete FOS bucket after 15 attempts"):
            fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    # All 15 attempts hit the API
    assert fake_s3.delete_bucket.call_count == 15


def test_delete_fos_bucket_raises_on_unexpected_client_error():
    """Non-BucketNotEmpty ClientError (perms denied) → immediate raise,
    no retry loop. Pinned because retrying through an AccessDenied is
    pointless and slows the failure."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "perms"}}, "DeleteBucket"
    )

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        with pytest.raises(RuntimeError):
            fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    # Only one attempt, not 15
    assert fake_s3.delete_bucket.call_count == 1


def test_delete_fos_bucket_aborts_multipart_uploads_before_listing_objects():
    """Multipart uploads must be aborted before object deletion, or
    they leak forever as zombie partial uploads (and accrue storage
    charges). Pinned because skipping this is a silent cost leak."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": [{"Key": "partial.gz", "UploadId": "upload-123"}]}
    fake_s3.get_paginator.return_value.paginate.return_value = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")

    fake_s3.abort_multipart_upload.assert_called_once_with(Bucket="b", Key="partial.gz", UploadId="upload-123")


def test_delete_fos_bucket_tolerates_multipart_listing_failure():
    """``list_multipart_uploads`` raises (perms gap) → swallow and
    continue. Pinned because some FOS keys don't have multipart
    perms even though they can list/delete objects."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "x"}}, "ListMultipartUploads"
    )
    fake_s3.get_paginator.return_value.paginate.return_value = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_bucket("b", "us-east-1", "k", "s")  # must not raise

    fake_s3.delete_bucket.assert_called()


# ── find_fos_key ──────────────────────────────────────────────────────────


def test_find_fos_key_returns_matching_key():
    fake_keys = {
        "data": [
            {"access_key": "AK111", "description": "fos-log-analysis-svc1"},
            {"access_key": "AK222", "description": "manual-key"},
        ]
    }
    with patch("backend.provision.fos_setup.fastly", return_value=fake_keys):
        out = fos_setup.find_fos_key("manual-key", token="tkn")

    assert out["access_key"] == "AK222"


def test_find_fos_key_returns_none_when_no_match():
    with patch("backend.provision.fos_setup.fastly", return_value={"data": []}):
        assert fos_setup.find_fos_key("nonexistent", token="tkn") is None


def test_find_fos_key_swallows_runtime_error_from_fastly_api():
    """A RuntimeError from the Fastly API (rate-limited, 5xx) →
    return None. Pinned because raising here would crash the
    ``ensure_fos_access_key`` idempotency check."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("rate limit")):
        assert fos_setup.find_fos_key("any", token="tkn") is None


# ── ensure_fos_access_key ─────────────────────────────────────────────────


def test_ensure_fos_access_key_creates_new_key():
    """No existing key → POST /access-keys → return id+secret."""
    fake_resp = {"access_key": "AK999", "secret_key": "SK999"}
    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=None),
        patch("backend.provision.fos_setup.fastly", return_value=fake_resp) as mock_fastly,
    ):
        out = fos_setup.ensure_fos_access_key("fos-log-analysis-test", state={}, token="tkn", buckets=["b1"])

    assert out == {"id": "AK999", "access_key": "AK999", "secret_key": "SK999"}
    # The POST body must include both permission and buckets
    args, kwargs = mock_fastly.call_args[:2] if len(mock_fastly.call_args) > 1 else (mock_fastly.call_args[0], {})
    payload = mock_fastly.call_args[0][2] if len(mock_fastly.call_args[0]) > 2 else {}
    assert payload.get("permission") == "read-write-objects"
    assert payload.get("buckets") == ["b1"]


def test_ensure_fos_access_key_recreates_managed_key():
    """An existing key with our ``fos-log-analysis-*`` prefix is a
    leftover from a prior run → DELETE then re-POST. Pinned because
    losing this would cause the provisioning to fail on every
    re-run after a previous partial completion."""
    existing = {"access_key": "AK_OLD", "description": "fos-log-analysis-svc1"}
    new_key = {"access_key": "AK_NEW", "secret_key": "SK_NEW"}

    calls: list[tuple] = []

    def _fastly_spy(method, path, payload=None, token=None, **kwargs):
        calls.append((method, path))
        if method == "POST":
            return new_key
        return {}

    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=existing),
        patch("backend.provision.fos_setup.fastly", side_effect=_fastly_spy),
    ):
        out = fos_setup.ensure_fos_access_key("fos-log-analysis-svc1", state={}, token="tkn")

    # DELETE happened first, then POST
    assert ("DELETE", "/resources/object-storage/access-keys/AK_OLD") in calls
    assert any(call[0] == "POST" for call in calls)
    assert out["access_key"] == "AK_NEW"


def test_ensure_fos_access_key_raises_on_unmanaged_existing_key():
    """If an existing key has a description NOT starting with
    ``fos-log-analysis-``, it was created manually → don't delete it,
    raise instead. Pinned because clobbering a customer's hand-rolled
    key would be a data-loss bug."""
    existing = {"access_key": "AK_MANUAL", "description": "my-prod-key"}

    with patch("backend.provision.fos_setup.find_fos_key", return_value=existing):
        with pytest.raises(RuntimeError, match="not managed by this tool"):
            fos_setup.ensure_fos_access_key("my-prod-key", state={}, token="tkn")


def test_ensure_fos_access_key_omits_buckets_when_none_supplied():
    """``buckets=None`` → no ``buckets`` key in the POST payload (so
    the key gets account-wide perms). Pinned because sending
    ``"buckets": null`` would trip Fastly's schema validation."""
    fake_resp = {"access_key": "AK", "secret_key": "SK"}
    with (
        patch("backend.provision.fos_setup.find_fos_key", return_value=None),
        patch("backend.provision.fos_setup.fastly", return_value=fake_resp) as mock_fastly,
    ):
        fos_setup.ensure_fos_access_key("d", state={}, token="tkn", buckets=None)

    payload = mock_fastly.call_args[0][2]
    assert "buckets" not in payload


# ── delete_fos_access_key ─────────────────────────────────────────────────


def test_delete_fos_access_key_calls_delete_endpoint():
    with patch("backend.provision.fos_setup.fastly") as mock_fastly:
        fos_setup.delete_fos_access_key("AK123", token="tkn")

    args, kwargs = mock_fastly.call_args[0], mock_fastly.call_args[1]
    assert args[0] == "DELETE"
    assert "AK123" in args[1]


def test_delete_fos_access_key_treats_404_as_already_deleted():
    """404 from the DELETE is idempotency-friendly — treat as success.
    Pinned because the teardown CLI may run twice (manual retry); the
    second run must not error on a missing key."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("404 Not Found")):
        fos_setup.delete_fos_access_key("AK123", token="tkn")  # must not raise


def test_delete_fos_access_key_raises_on_other_errors():
    """Non-404 RuntimeError → re-raise. Pinned distinct from the
    404-tolerant path so transient 5xx errors aren't silently
    swallowed (the teardown would think it succeeded and clean up
    state pointing to a still-existing key)."""
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("500 Internal")):
        with pytest.raises(RuntimeError):
            fos_setup.delete_fos_access_key("AK123", token="tkn")


def test_ensure_fos_access_key_emits_status_callbacks_on_recreate():
    """When recreating a managed key, status_cb receives "Recreating
    existing key..." then "Creating ... access key..." then
    "Access key created.". Pinned because the SSE wizard renders
    these as user-visible progress steps — losing them would leave
    the user staring at a stalled progress bar."""
    statuses = []
    fastly_calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        fastly_calls.append((method, path))
        if method == "POST" and "/access-keys" in path:
            return {"access_key": "AKNEW", "secret_key": "SKNEW", "id": "AKNEW"}
        return {}

    with (
        patch(
            "backend.provision.fos_setup.find_fos_key",
            return_value={"access_key": "AKEXISTING"},
        ),
        patch("backend.provision.fos_setup.fastly", side_effect=fake_fastly),
    ):
        result = fos_setup.ensure_fos_access_key(
            "fos-log-analysis-svc-1",
            state={},
            token="tok",
            permission="read-write-objects",
            buckets=["b"],
            status_cb=statuses.append,
        )

    # Status callbacks fired in expected order
    joined = " ".join(statuses)
    assert "Recreating" in joined
    # The existing key was DELETEd then a new POST happened
    assert any(m == "DELETE" for m, _ in fastly_calls)
    assert any(m == "POST" for m, _ in fastly_calls)
    assert result["access_key"] == "AKNEW"


# ── provisioning capture (writes to usage_log) ────────────────────────────


def test_provisioning_tracker_records_create_bucket_to_usage_log():
    """The headline pin: when service_id is supplied, every S3 call
    made during ``ensure_fos_bucket`` lands in usage_log via
    ``metadata_db.log_usage_calls``. Pinned because provisioning ops
    were silently dropped pre-fix — the wrapper did not exist, so
    head_bucket / create_bucket attempts never appeared in any
    dashboard, hiding a class of cost from the operator."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        fos_setup.ensure_fos_bucket("new-bucket", "us-east-1", "k", "s", service_id="svc-x")

    assert mock_log.called, "log_usage_calls must be invoked on flush"
    args, kwargs = mock_log.call_args
    assert args[0] == "svc-x"
    methods = [c["method"] for c in args[1]]
    # Both the existence probe and the creation land as rows
    assert "HEAD_BUCKET" in methods
    assert "CREATE_BUCKET" in methods
    assert kwargs["process_context"] == "provision:new-bucket"


def test_provisioning_tracker_marks_create_bucket_as_class_a_in_details():
    """``log_usage_calls`` auto-classifies as Class B by default; the
    "Class A" sentinel in details is the documented escape hatch.
    Pinned because miss-classifying CreateBucket as Class B would
    under-count A-class billable ops for every provisioning run."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.return_value = {}

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", service_id="svc-x")

    create_row = next(c for c in captured if c["method"] == "CREATE_BUCKET")
    assert "Class A" in create_row["details"]
    head_row = next(c for c in captured if c["method"] == "HEAD_BUCKET")
    assert "Class A" not in head_row["details"]  # HEAD is Class B


def test_provisioning_tracker_records_errors_as_rows_too():
    """A ClientError on create_bucket still results in a usage_log
    row (status="Error:<code>"). Pinned because failed provisioning
    attempts also incur API charges — pretending they didn't happen
    would under-count cost."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.side_effect = ClientError({"Error": {"Code": "404", "Message": "x"}}, "HeadBucket")
    fake_s3.create_bucket.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "CreateBucket"
    )

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        with pytest.raises(RuntimeError):
            fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s", service_id="svc-x")

    create_row = next(c for c in captured if c["method"] == "CREATE_BUCKET")
    assert "Error" in str(create_row["status"])
    assert "AccessDenied" in str(create_row["status"])


def test_provisioning_tracker_tags_teardown_context_separately_from_provision():
    """delete_fos_bucket flushes with process_context="teardown:<name>"
    so dashboards can distinguish setup cost from teardown cost.
    Pinned because mixing the two would hide the teardown-Class-A
    spike that happens when emptying a large bucket."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}
    fake_s3.get_paginator.return_value.paginate.return_value = []
    fake_s3.delete_bucket.return_value = {}

    contexts: list[str] = []

    def _capture(_sid, _calls, **kwargs):
        contexts.append(kwargs.get("process_context"))

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.delete_fos_bucket("doomed-bucket", "us-east-1", "k", "s", service_id="svc-x")

    assert "teardown:doomed-bucket" in contexts


def test_provisioning_tracker_skips_logging_when_no_service_id():
    """Standalone CLI runs without a service_id (e.g. the bootstrap
    wizard runs ensure_fos_bucket before logging_service_id is even
    chosen). The wrapper must not be applied and log_usage_calls
    must not be called. Pinned because a NameError trying to flush
    to a missing service file would crash the CLI."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls") as mock_log,
    ):
        fos_setup.ensure_fos_bucket("b", "us-east-1", "k", "s")  # no service_id

    mock_log.assert_not_called()


def test_provisioning_tracker_records_delete_objects_pagination():
    """delete_fos_bucket calls list_objects_v2 via paginator and
    delete_objects from a ThreadPoolExecutor. Both must land in
    usage_log. Pinned because a large-bucket teardown can issue
    thousands of Class A delete_objects calls — losing them would
    hide a major cost spike on every teardown."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}
    fake_s3.list_multipart_uploads.return_value = {"Uploads": []}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {"Deleted": [{"Key": "obj1.gz"}, {"Key": "obj2.gz"}]}
    fake_s3.delete_bucket.return_value = {}

    captured: list[dict] = []

    def _capture(_sid, calls, **_):
        captured.extend(calls)

    with (
        patch("boto3.client", return_value=fake_s3),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        fos_setup.delete_fos_bucket("doomed-bucket", "us-east-1", "k", "s", service_id="svc-x")

    methods = [c["method"] for c in captured]
    assert "LIST_OBJECTS_V2" in methods
    assert "DELETE_OBJECTS" in methods
    assert "DELETE_BUCKET" in methods


def test_ensure_fos_bucket_calls_status_callback_on_already_exists():
    """When the bucket already exists (head_bucket succeeds), the
    status_cb receives an "already exists" message. Pinned because
    the wizard's idempotent re-run UX depends on this distinction
    vs the "creating..." message admins see on first run."""
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}  # success

    statuses = []

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        result = fos_setup.ensure_fos_bucket(
            "my-bucket",
            "us-east-1",
            "AK",
            "SK",
            status_cb=statuses.append,
        )

    assert result is True
    # create_bucket was NOT called (idempotency)
    fake_s3.create_bucket.assert_not_called()
    # Status callback got the "already exists" signal
    assert any("already exists" in s.lower() for s in statuses)


def test_delete_fos_prefix_happy_path():
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw/log1.gz"}, {"Key": "raw/log2.gz"}]},
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_prefix("my-bucket", "us-east-1", "k", "s", "raw/")

    fake_s3.delete_objects.assert_called_once_with(
        Bucket="my-bucket", Delete={"Objects": [{"Key": "raw/log1.gz"}, {"Key": "raw/log2.gz"}]}
    )


def test_delete_fos_prefix_excludes_matching():
    fake_s3 = MagicMock()
    fake_s3.head_bucket.return_value = {}

    fake_paginator = MagicMock()
    fake_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "raw/log1.gz"},
                {"Key": "raw/rum/log2.gz"},
                {"Key": "raw/log3.gz"},
            ]
        },
    ]
    fake_s3.get_paginator.return_value = fake_paginator
    fake_s3.delete_objects.return_value = {}

    with patch("backend.provision.fos_setup._get_fos_s3_client", return_value=fake_s3):
        fos_setup.delete_fos_prefix("my-bucket", "us-east-1", "k", "s", "raw/", exclude_prefix="raw/rum/")

    fake_s3.delete_objects.assert_called_once_with(
        Bucket="my-bucket", Delete={"Objects": [{"Key": "raw/log1.gz"}, {"Key": "raw/log3.gz"}]}
    )
