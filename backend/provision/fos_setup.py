import threading
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from backend.core.fastly.client import fastly
from backend.core.fastly.utils import region_endpoint
from backend.provision.utils import BOLD, _c, info, ok, warn

# Methods we want billed as Class A regardless of name-based auto-classification
# in metadata_db.log_usage_calls. The auto-classifier only knows about
# PUT_OBJECT/POST_OBJECT/COPY_OBJECT/LIST_OBJECTS_V2; the rest of the
# bucket-lifecycle ops fall through to Class B by default, so we tag them
# explicitly via the "Class A" sentinel in details (which the classifier
# honors as an override).
_PROVISION_CLASS_A: set[str] = {
    "CREATE_BUCKET",
    "DELETE_BUCKET",
    "DELETE_OBJECT",
    "DELETE_OBJECTS",
    "LIST_MULTIPART_UPLOADS",
    "ABORT_MULTIPART_UPLOAD",
}


class _ProvisioningTracker:
    """Wraps a boto3 S3 client so every call lands in usage_log.

    Why this exists: provisioning runs outside any request handler
    context, so the ContextVar-backed `record_call` mechanism that
    powers dashboards drops every op silently — and the local
    telemetry proxy can't help either, since the service_id config
    (which carries the SigV4 credentials the proxy needs) does not
    exist yet when the bucket is being created. We write directly to
    usage_log via `metadata_db.log_usage_calls`, which is the same
    persistence layer the proxy writes to — keeping a single source
    of truth for the Usage Log dashboard.

    Each tracked call accumulates in `_calls`; `flush()` drains them
    to SQLite. Thread-safe (delete_fos_bucket uses a ThreadPoolExecutor
    for parallel delete_objects fan-out).
    """

    def __init__(self, client, service_id: str, bucket_name: str, context: str):
        self._client = client
        self._service_id = service_id
        self._bucket_name = bucket_name
        self._context = context  # e.g. "provision:bucket-name" or "teardown:bucket-name"
        self._calls: list[dict] = []
        self._lock = threading.Lock()

    def get_paginator(self, operation_name):
        # boto3 paginators internally call list_objects_v2 — wrap the page
        # iterator so we capture each underlying API call, not just the
        # surface .paginate() invocation.
        return _ProvisioningPaginator(self._client.get_paginator(operation_name), operation_name, self)

    def __getattr__(self, name):
        attr = getattr(self._client, name)
        if not callable(attr) or name.startswith("_"):
            return attr

        op = name.upper()

        def tracked(*args: Any, **kwargs: Any) -> Any:
            t0 = time.time()
            status: str | int = "OK"
            exc_raised = None
            try:
                response = attr(*args, **kwargs)
                if isinstance(response, dict):
                    meta = response.get("ResponseMetadata") or {}
                    code = meta.get("HTTPStatusCode")
                    if isinstance(code, int):
                        status = code
                return response
            except ClientError as exc:
                err_code = exc.response.get("Error", {}).get("Code", "Error")
                status = f"Error:{err_code}"
                exc_raised = exc
                raise
            except Exception as exc:
                status = f"Error:{type(exc).__name__}"
                exc_raised = exc
                raise
            finally:
                elapsed_ms = round((time.time() - t0) * 1000, 2)
                self._record(op, args, kwargs, elapsed_ms, status, exception=exc_raised)

        return tracked

    def _record(self, op: str, args, kwargs, elapsed_ms: float, status, exception=None):
        bucket = kwargs.get("Bucket") or (args[0] if args else "") or self._bucket_name
        key = kwargs.get("Key", "")
        path = f"{bucket}/{key}" if key else bucket
        details = "Class A · provisioning" if op in _PROVISION_CLASS_A else "Class B · provisioning"
        with self._lock:
            self._calls.append(
                {
                    "service": "FOS",
                    "method": op,
                    "path": path or "FOS Operation",
                    "time_ms": elapsed_ms,
                    "status": status,
                    "details": details,
                    "caller": f"provision.{op.lower()}",
                }
            )

    def flush(self) -> int:
        """Persist accumulated calls. Returns rows written."""
        with self._lock:
            if not self._calls:
                return 0
            batch = self._calls[:]
            self._calls.clear()
        try:
            from backend.core import metadata as metadata_db

            metadata_db.log_usage_calls(self._service_id, batch, process_context=self._context)
            return len(batch)
        except Exception:
            # Provisioning must never fail because telemetry persistence failed.
            return 0


class _ProvisioningPaginator:
    """Page iterator that records each underlying list_objects_v2 call."""

    def __init__(self, paginator, operation_name: str, tracker: _ProvisioningTracker):
        self._paginator = paginator
        self._operation_name = operation_name
        self._tracker = tracker

    def paginate(self, **kwargs):
        op = self._operation_name.upper()
        underlying = self._paginator.paginate(**kwargs)
        bucket = kwargs.get("Bucket") or self._tracker._bucket_name
        prefix = kwargs.get("Prefix", "")
        path = f"{bucket}/{prefix}" if prefix else bucket

        for page in underlying:
            t0 = time.time()
            # The actual API call already happened by the time iteration yields
            # the page; we can't time it precisely. Record it as 0ms — counts
            # matter more than per-page latency for Class A pagination billing.
            self._tracker._record(op, (), {"Bucket": bucket}, round((time.time() - t0) * 1000, 2), "OK")
            # Use 0.0 explicitly so totals don't get polluted by tiny iteration overhead.
            if self._tracker._calls and self._tracker._calls[-1]["method"] == op:
                self._tracker._calls[-1]["time_ms"] = 0.0
                self._tracker._calls[-1]["path"] = path
            yield page


class _MockS3ClientForProvisioning:
    """Mock S3 client used during FASTLY_MOCK_MODE=1 to bypass real FOS network calls."""

    def __init__(self):
        # boto3 clients have a meta property that some listeners might access
        self.meta = type("Meta", (), {"events": type("Events", (), {"register": lambda *a, **kw: None})()})()

    def head_bucket(self, Bucket):
        # Always raise 404 to indicate the bucket doesn't exist yet, forcing the create_bucket flow.
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket")

    def create_bucket(self, Bucket, CreateBucketConfiguration=None):
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def delete_bucket(self, Bucket):
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def list_multipart_uploads(self, Bucket):
        return {"Uploads": []}

    def get_paginator(self, operation_name):
        class _MockPaginator:
            def paginate(self, **kwargs):
                return []

        return _MockPaginator()

    def delete_objects(self, Bucket, Delete):
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


def _get_fos_s3_client(access_key, secret_key, region, *, service_id=None, bucket_name="", context=""):
    from backend.core.fastly.mock_fixtures import is_mock_mode

    if is_mock_mode():
        client = _MockS3ClientForProvisioning()
    else:
        endpoint = region_endpoint(region)
        boto_config = Config(
            signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}
        )
        client = boto3.client(
            "s3",
            endpoint_url=f"https://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=boto_config,
        )
    if service_id:
        return _ProvisioningTracker(client, service_id, bucket_name, context or f"provision:{bucket_name}")
    return client


def ensure_fos_bucket(
    name: str,
    region: str,
    access_key: str,
    secret_key: str,
    status_cb=None,
    *,
    service_id: str | None = None,
) -> bool:
    """Create the bucket via Boto3 (S3 API). Returns True on success."""
    s3 = _get_fos_s3_client(
        access_key, secret_key, region, service_id=service_id, bucket_name=name, context=f"provision:{name}"
    )

    try:
        try:
            s3.head_bucket(Bucket=name)
            ok(f"FOS bucket already exists: {_c(BOLD, name)}")
            if status_cb:
                status_cb(f"✅ Bucket '{name}' already exists.")
            return True
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                pass
            elif error_code in ("403", "Forbidden", "AccessDenied"):
                raise RuntimeError(
                    f"Bucket {_c(BOLD, name)} exists but access denied. "
                    f"Verify FOS credentials have permission to HeadBucket: {exc}"
                )
            else:
                raise RuntimeError(f"Could not check FOS bucket status: {exc}")

        info(f"Creating FOS bucket {_c(BOLD, name)} in {_c(BOLD, region)}…")
        if status_cb:
            status_cb(f"⏳ Creating bucket '{name}' in {region}...")
        try:
            if region == "us-east-1":
                s3.create_bucket(Bucket=name)
            else:
                s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": region})
            ok("FOS bucket created")
            if status_cb:
                status_cb("✅ Bucket created.")
            return True
        except ClientError as exc:
            raise RuntimeError(f"Could not create FOS bucket via S3 API: {exc}")
    finally:
        if isinstance(s3, _ProvisioningTracker):
            s3.flush()


def delete_fos_bucket(
    name: str,
    region: str,
    access_key: str,
    secret_key: str,
    status_cb=None,
    *,
    service_id: str | None = None,
):
    """Delete the bucket via Boto3, emptying it first if necessary."""
    s3 = _get_fos_s3_client(
        access_key, secret_key, region, service_id=service_id, bucket_name=name, context=f"teardown:{name}"
    )

    try:
        try:
            s3.head_bucket(Bucket=name)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                ok(f"FOS bucket {_c(BOLD, name)} already deleted")
                return
            raise RuntimeError(f"Could not check FOS bucket status: {exc}")

        import concurrent.futures

        def empty_bucket():
            deleted_count = 0

            def _report():
                if status_cb and deleted_count > 0:
                    status_cb(f"🧹 Emptying bucket '{name}'... (deleted {deleted_count:,} objects so far)")

            try:
                uploads = s3.list_multipart_uploads(Bucket=name).get("Uploads", [])
                for u in uploads:
                    s3.abort_multipart_upload(Bucket=name, Key=u["Key"], UploadId=u["UploadId"])
                    deleted_count += 1
            except Exception:
                pass

            found_any = False
            paginator = s3.get_paginator("list_objects_v2")

            def delete_batch(batch):
                s3.delete_objects(Bucket=name, Delete={"Objects": batch})
                return len(batch)

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = set()
                for page in paginator.paginate(Bucket=name):
                    objects = page.get("Contents", [])
                    if objects:
                        found_any = True
                        delete_list = [{"Key": obj["Key"]} for obj in objects]
                        for j in range(0, len(delete_list), 1000):
                            batch = delete_list[j : j + 1000]
                            futures.add(executor.submit(delete_batch, batch))

                    done = {f for f in futures if f.done()}
                    for f in done:
                        try:
                            deleted_count += f.result()
                        except Exception:
                            pass
                    futures -= done
                    if done:
                        _report()

                for f in concurrent.futures.as_completed(futures):
                    try:
                        deleted_count += f.result()
                    except Exception:
                        pass
                    _report()

            return found_any

        if status_cb:
            status_cb(f"🧹 Emptying bucket '{name}'...")
        empty_bucket()

        info(f"Deleting FOS bucket {_c(BOLD, name)}…")
        if status_cb:
            status_cb(f"⏳ Deleting bucket '{name}'...")

        max_attempts = 15
        for attempt in range(max_attempts):
            try:
                s3.delete_bucket(Bucket=name)
                ok("FOS bucket deleted")
                if status_cb:
                    status_cb("✅ Bucket deleted.")
                return
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "BucketNotEmpty" and attempt < max_attempts - 1:
                    wait_time = 2 if attempt <= 5 else 5
                    if status_cb:
                        status_cb(
                            f"⏳ Bucket not empty (attempt {attempt + 1}/{max_attempts}), retrying in {wait_time}s..."
                        )
                    time.sleep(wait_time)
                    empty_bucket()
                    continue
                raise RuntimeError(f"Could not delete FOS bucket after {max_attempts} attempts: {exc}")
    finally:
        if isinstance(s3, _ProvisioningTracker):
            s3.flush()


def product_enabled(token: str, product_id: str) -> bool:
    """Return True if the Fastly account behind ``token`` has ``product_id``
    enabled, per Fastly's product-enablement endpoint
    (``GET /enabled-products/v1/{product_id}``).

    Only a subset of Fastly products expose this status endpoint (Object
    Storage and KV Store are two; Compute and Config Store are NOT — those have
    to be detected by attempting the create). For the ones that do, we treat:

      * 200            -> True  (enabled)
      * 4xx            -> False (not enabled for this account)
      * 5xx / network  -> True  (inconclusive — don't block; the reactive
                                 create/access failure downstream still guards)

    Callers should validate the token first (e.g. list services) so a 401/403
    here is only seen for a known-good token and reads as "not enabled".
    """
    try:
        fastly("GET", f"/enabled-products/v1/{product_id}", token=token)
        return True
    except RuntimeError as exc:
        # backend.core.fastly.client.fastly raises "HTTP <code> ..." on HTTP
        # errors; a 4xx means the product isn't enabled for this account.
        return "HTTP 4" not in str(exc)


def object_storage_enabled(token: str) -> bool:
    """Return True if the account has Object Storage enabled. Object Storage is
    REQUIRED for log storage; see ``product_enabled`` for the 200/4xx/5xx
    semantics."""
    return product_enabled(token, "object_storage")


def find_fos_key(description: str, token: str) -> dict | None:
    try:
        resp = fastly("GET", "/resources/object-storage/access-keys", token=token)
        for key in resp.get("data", []):
            if key.get("description") == description:
                return key
    except RuntimeError:
        pass
    return None


def ensure_fos_access_key(
    description: str,
    state: dict,
    token: str,
    *,
    permission: str = "read-write-objects",
    buckets: list[str] | None = None,
    status_cb=None,
) -> dict:
    """Create an FOS access key and return {id, access_key, secret_key}."""
    existing = find_fos_key(description, token)
    if existing:
        if description.startswith("fos-log-analysis-"):
            warn(f"An FOS key with description '{description}' already exists. Recreating...")
            if status_cb:
                status_cb(f"🔄 Recreating existing key '{description}'...")
            fastly(
                "DELETE",
                f"/resources/object-storage/access-keys/{existing['access_key']}",
                token=token,
                expect_empty=True,
            )
        else:
            raise RuntimeError("FOS access key exists and is not managed by this tool.")

    info(f"Creating FOS access key ({permission})…")
    if status_cb:
        status_cb(f"⏳ Creating {permission} access key...")
    payload: dict[str, Any] = {"permission": permission, "description": description}
    if buckets:
        payload["buckets"] = buckets

    key = fastly("POST", "/resources/object-storage/access-keys", payload, token=token)
    ok(f"FOS access key created  (key: {key['access_key']})")
    if status_cb:
        status_cb("✅ Access key created.")

    return {
        "id": key["access_key"],
        "access_key": key["access_key"],
        "secret_key": key["secret_key"],
    }


def delete_fos_access_key(key_id: str, token: str, status_cb=None):
    info(f"Deleting FOS access key {key_id}…")
    if status_cb:
        status_cb(f"⏳ Deleting access key {key_id}...")
    try:
        fastly("DELETE", f"/resources/object-storage/access-keys/{key_id}", token=token, expect_empty=True)
        ok("FOS access key deleted")
        if status_cb:
            status_cb("✅ Access key deleted.")
    except RuntimeError as exc:
        if "404" in str(exc):
            ok("FOS access key already deleted")
        else:
            raise exc


def delete_fos_tokens_for_service(service_id: str, token: str, status_cb=None):
    """List all FOS access keys and delete ones matching this service's patterns.

    Searches for keys with descriptions matching:
    - fos-log-analysis-{service_id}
    - fos-log-analysis-temp-admin-{service_id}
    - temp-teardown-{service_id}
    """
    try:
        resp = fastly("GET", "/resources/object-storage/access-keys", token=token)
        keys = resp.get("data", [])
        patterns = [
            f"fos-log-analysis-{service_id}",
            f"fos-log-analysis-temp-admin-{service_id}",
            f"temp-teardown-{service_id}",
        ]

        deleted_count = 0
        for key in keys:
            desc = key.get("description", "")
            if any(desc.startswith(p) for p in patterns):
                try:
                    key_id = key.get("access_key")
                    fastly("DELETE", f"/resources/object-storage/access-keys/{key_id}", token=token, expect_empty=True)
                    info(f"Deleted FOS key: {desc}")
                    deleted_count += 1
                except RuntimeError as exc:
                    if "404" not in str(exc):
                        warn(f"Failed to delete FOS key {desc}: {exc}")

        if deleted_count > 0:
            ok(f"Deleted {deleted_count} FOS access key(s)")
        if status_cb:
            status_cb(f"✅ Deleted {deleted_count} FOS access key(s).")
    except RuntimeError as exc:
        warn(f"Failed to list FOS keys: {exc}")
        if status_cb:
            status_cb(f"⚠️ Could not clean up FOS keys: {exc}")


def delete_fos_prefix(
    bucket_name: str,
    region: str,
    access_key: str,
    secret_key: str,
    prefix: str,
    exclude_prefix: str = None,
    status_cb=None,
    *,
    service_id: str | None = None,
):
    """Delete all objects with prefix, optionally excluding those starting with exclude_prefix."""
    from botocore.exceptions import ClientError

    s3 = _get_fos_s3_client(
        access_key,
        secret_key,
        region,
        service_id=service_id,
        bucket_name=bucket_name,
        context=f"delete_prefix:{prefix}",
    )

    try:
        try:
            s3.head_bucket(Bucket=bucket_name)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return
            raise RuntimeError(f"Could not check FOS bucket status: {exc}")

        deleted_count = 0
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            objects = page.get("Contents", [])
            if objects:
                delete_list = []
                for obj in objects:
                    key = obj["Key"]
                    if exclude_prefix and key.startswith(exclude_prefix):
                        continue
                    delete_list.append({"Key": key})

                if delete_list:
                    for j in range(0, len(delete_list), 1000):
                        batch = delete_list[j : j + 1000]
                        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": batch})
                        deleted_count += len(batch)
                        if status_cb:
                            status_cb(
                                f"🧹 Deleting cloud files under '{prefix}'... (deleted {deleted_count:,} objects so far)"
                            )

        if status_cb:
            status_cb(f"✅ Deleted {deleted_count:,} cloud files.")
    except Exception as e:
        if status_cb:
            status_cb(f"⚠️ Warning: Failed to delete cloud files under prefix '{prefix}': {e}")
