from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
from typing import Any

logger = logging.getLogger(__name__)
# ── CMCD custom field definitions & VCL helpers (folded from cmcd_fields.py) ──
_CMCD_CUSTOM_FIELDS: list[dict[str, Any]] = [
    {
        "name": "cmcd_sid",
        "label": "CMCD Session ID",
        "vcl_log_expression": "req.http.x-cmcd:sid",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 40,
        "byte_limit": 288,
        "enabled": True,
    },
    {
        "name": "cmcd_cid",
        "label": "CMCD Content ID",
        "vcl_log_expression": "req.http.x-cmcd:cid",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 30,
        "byte_limit": 288,
        "enabled": True,
    },
    {
        "name": "cmcd_br",
        "label": "Encoded Bitrate (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:br",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_bl",
        "label": "Buffer Length (ms)",
        "vcl_log_expression": "req.http.x-cmcd:bl",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_bs",
        "label": "Buffer Starvation",
        "vcl_log_expression": "req.http.x-cmcd:bs",
        "collection_stage": "edge",
        "duckdb_type": "BOOLEAN",
        "value_type": "boolean",
        "bytes_estimate": 1,
        "enabled": True,
    },
    {
        "name": "cmcd_d",
        "label": "Object Duration (ms)",
        "vcl_log_expression": "req.http.x-cmcd:d",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_dl",
        "label": "Deadline (ms)",
        "vcl_log_expression": "req.http.x-cmcd:dl",
        "vcl_log_expression_note": "ms",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_mtp",
        "label": "Measured Throughput (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:mtp",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_ot",
        "label": "Object Type",
        "vcl_log_expression": "req.http.x-cmcd:ot",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 2,
        "enabled": True,
    },
    {
        "name": "cmcd_sf",
        "label": "Streaming Format",
        "vcl_log_expression": "req.http.x-cmcd:sf",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 2,
        "enabled": True,
    },
    {
        "name": "cmcd_st",
        "label": "Stream Type",
        "vcl_log_expression": "req.http.x-cmcd:st",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 2,
        "enabled": True,
    },
    {
        "name": "cmcd_su",
        "label": "Startup",
        "vcl_log_expression": "req.http.x-cmcd:su",
        "collection_stage": "edge",
        "duckdb_type": "BOOLEAN",
        "value_type": "boolean",
        "bytes_estimate": 1,
        "enabled": True,
    },
    {
        "name": "cmcd_tb",
        "label": "Top Bitrate (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:tb",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
    {
        "name": "cmcd_rtp",
        "label": "Requested Max Throughput (kbps)",
        "vcl_log_expression": "req.http.x-cmcd:rtp",
        "collection_stage": "edge",
        "duckdb_type": "INTEGER",
        "value_type": "numeric",
        "bytes_estimate": 5,
        "enabled": True,
    },
]
for _cf in _CMCD_CUSTOM_FIELDS:
    _cf.pop("vcl_log_expression_note", None)
_CMCD_FIELD_NAMES = {cf["name"] for cf in _CMCD_CUSTOM_FIELDS}


def get_cmcd_fields(enabled: bool) -> list[dict]:
    """Return CMCD custom fields if enabled, else empty list."""
    if enabled:
        return [dict(cf) for cf in _CMCD_CUSTOM_FIELDS]
    return []


def reconcile_cmcd_custom_fields(custom_fields: list[dict] | None, *, enabled: bool) -> list[dict]:
    """Return ``custom_fields`` with the canonical CMCD fields applied or stripped."""
    kept = [cf for cf in (custom_fields or []) if cf.get("name") not in _CMCD_FIELD_NAMES]
    return kept + get_cmcd_fields(enabled)


def _remove_cmcd_custom_fields(cfg: dict) -> None:
    lf_dict = cfg.get("log_fields") or {}
    cfs = lf_dict.get("custom_fields")
    if cfs:
        lf_dict["custom_fields"] = [cf for cf in cfs if cf.get("name") not in _CMCD_FIELD_NAMES]


CMCD_SNIPPET_NAME = "CMCD Extraction"


def _bool_v1(padded_source: str, key: str) -> str:
    """v1 bare-key-presence: match ``,key,`` against a pre-padded source var."""
    return f'if({padded_source} ~ ",{key},", "1", "")'


def _bool_v2(source: str, key: str) -> str:
    """v2 RFC 8941 boolean: ``subfield()`` returns ``?1``."""
    return f'if(subfield({source}, "{key}", ",") == "?1", "1", "")'


def _cmcd_field_assignments_vcl(
    obj_src: str, req_src: str, sess_src: str, status_src: str, su_expr: str, bs_expr: str
) -> str:
    return f"""\
  # CMCD-Object fields
  set req.http.x-cmcd:br = subfield({obj_src}, "br", ",");
  set req.http.x-cmcd:d = subfield({obj_src}, "d", ",");
  set req.http.x-cmcd:ot = subfield({obj_src}, "ot", ",");
  set req.http.x-cmcd:tb = subfield({obj_src}, "tb", ",");

  # CMCD-Request fields
  set req.http.x-cmcd:bl = subfield({req_src}, "bl", ",");
  set req.http.x-cmcd:dl = subfield({req_src}, "dl", ",");
  set req.http.x-cmcd:mtp = subfield({req_src}, "mtp", ",");
  set req.http.x-cmcd:su = {su_expr};

  # CMCD-Session fields — strip quotes from string values.
  set req.http.x-cmcd:sid = regsuball(subfield({sess_src}, "sid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:cid = regsuball(subfield({sess_src}, "cid", ","), {{"^"|"$"}}, "");
  set req.http.x-cmcd:sf = subfield({sess_src}, "sf", ",");
  set req.http.x-cmcd:st = subfield({sess_src}, "st", ",");

  # CMCD-Status fields
  set req.http.x-cmcd:bs = {bs_expr};
  set req.http.x-cmcd:rtp = subfield({status_src}, "rtp", ",");"""


def _header_mode_vcl(version: int) -> str:
    bf = _bool_v1 if version == 1 else _bool_v2
    tag = f"v{version}"
    if version == 1:
        su = bf("var.cmcd_req_pad", "su")
        bs = bf("var.cmcd_status_pad", "bs")
        pad_decl = """\

  # Pad header values with commas for bare-key boolean matching.
  declare local var.cmcd_req_pad STRING;
  set var.cmcd_req_pad = "," req.http.CMCD-Request ",";
  declare local var.cmcd_status_pad STRING;
  set var.cmcd_status_pad = "," req.http.CMCD-Status ",";
"""
    else:
        su = bf("req.http.CMCD-Request", "su")
        bs = bf("req.http.CMCD-Status", "bs")
        pad_decl = ""
    assignments = _cmcd_field_assignments_vcl(
        "req.http.CMCD-Object", "req.http.CMCD-Request", "req.http.CMCD-Session", "req.http.CMCD-Status", su, bs
    )
    return f"""\
# CMCD {tag} header-mode extraction.
if (req.http.CMCD-Object != "" || req.http.CMCD-Request != "" || req.http.CMCD-Session != "" || req.http.CMCD-Status != "") {{
{pad_decl}
{assignments}

  # Strip CMCD headers before forwarding to origin.
  unset req.http.CMCD-Object;
  unset req.http.CMCD-Request;
  unset req.http.CMCD-Session;
  unset req.http.CMCD-Status;
}}"""


def _query_string_mode_vcl(version: int) -> str:
    bf = _bool_v1 if version == 1 else _bool_v2
    tag = f"v{version}"
    if version == 1:
        su = bf("var.cmcd_pad", "su")
        bs = bf("var.cmcd_pad", "bs")
    else:
        su = bf("var.cmcd", "su")
        bs = bf("var.cmcd", "bs")
    pad_block = (
        """
  declare local var.cmcd_pad STRING;
  set var.cmcd_pad = "," var.cmcd ",";
"""
        if version == 1
        else ""
    )
    assignments = _cmcd_field_assignments_vcl("var.cmcd", "var.cmcd", "var.cmcd", "var.cmcd", su, bs)
    return f"""\
# CMCD {tag} query-string-mode extraction.
declare local var.cmcd STRING;
set var.cmcd = urldecode(querystring.get(req.url, "CMCD"));

if (var.cmcd != "") {{
{pad_block}
{assignments}

  # Strip CMCD from URL to prevent cache key pollution.
  set req.url = querystring.filter(req.url, "CMCD");
}}"""


def generate_cmcd_vcl(mode: str = "query_string", version: int = 1) -> dict[str, str]:
    """Return ``{CMCD_SNIPPET_NAME: vcl_body}`` for the configured mode + version."""
    if version not in (1, 2):
        raise ValueError(f"Unknown CMCD version: {version!r} (expected 1 or 2)")
    if mode == "headers":
        body = _header_mode_vcl(version)
    elif mode == "query_string":
        body = _query_string_mode_vcl(version)
    else:
        raise ValueError(f"Unknown CMCD mode: {mode!r} (expected 'headers' or 'query_string')")
    return {CMCD_SNIPPET_NAME: body}


def cmcd_snippet_names() -> list[str]:
    """Names of the CMCD snippets we install."""
    return [CMCD_SNIPPET_NAME]


def system_feature_flags(cfg: dict[str, Any]) -> tuple[bool, bool]:
    """Return ``(scoring_enabled, cmcd_enabled)`` for a service config."""
    scoring_enabled = bool((cfg.get("scoring") or {}).get("enabled"))
    cmcd_enabled = bool((cfg.get("cmcd") or {}).get("enabled"))
    return scoring_enabled, cmcd_enabled


def reconcile_system_custom_fields(
    custom_fields: list[dict] | None, *, scoring_enabled: bool, cmcd_enabled: bool
) -> list[dict]:
    """Return ``custom_fields`` with system-managed entries re-asserted."""
    from backend.provision.session_scoring_orchestrator import _SCORING_CUSTOM_FIELDS, _SCORING_FIELD_NAMES

    out = [cf for cf in (custom_fields or []) if cf.get("name") not in _SCORING_FIELD_NAMES]
    if scoring_enabled:
        out.extend(dict(cf) for cf in _SCORING_CUSTOM_FIELDS)
    return reconcile_cmcd_custom_fields(out, enabled=cmcd_enabled)


def reconcile_cfg_system_custom_fields(cfg: dict[str, Any]) -> list[dict]:
    """Re-assert system fields on ``cfg['log_fields']['custom_fields']`` in place."""
    scoring_enabled, cmcd_enabled = system_feature_flags(cfg)
    lf_dict = cfg.get("log_fields")
    if not isinstance(lf_dict, dict):
        lf_dict = {"schema_version": 2, "custom_fields": []}
        cfg["log_fields"] = lf_dict
    reconciled = reconcile_system_custom_fields(
        lf_dict.get("custom_fields"), scoring_enabled=scoring_enabled, cmcd_enabled=cmcd_enabled
    )
    lf_dict["custom_fields"] = reconciled
    return reconciled


sys.modules["backend.provision.cmcd_fields"] = sys.modules[__name__]
from backend.core import field_registry as lf  # noqa: E402
from backend.core.duckdb import rum_duckdb_path  # noqa: E402
from backend.core.faro_versions import DEFAULT_FARO_VERSION  # noqa: E402
from backend.core.fastly.client import fastly, is_mock_mode, region_endpoint  # noqa: E402
from backend.provision.cli import BOLD, _c, info, ok, run_with_events, step, warn  # noqa: E402

sys.modules["backend.provision.fos_setup"] = sys.modules[__name__]
sys.modules["backend.utils.terraform_gen"] = sys.modules[__name__]
import threading

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

_PROVISION_CLASS_A: set[str] = {
    "CREATE_BUCKET",
    "DELETE_BUCKET",
    "DELETE_OBJECT",
    "DELETE_OBJECTS",
    "LIST_MULTIPART_UPLOADS",
    "ABORT_MULTIPART_UPLOAD",
}


class _ProvisioningTracker:
    """Wraps a boto3 S3 client so every call lands in usage_log."""

    def __init__(self, client, service_id: str, bucket_name: str, context: str):
        self._client = client
        self._service_id = service_id
        self._bucket_name = bucket_name
        self._context = context  # e.g. "provision:bucket-name" or "teardown:bucket-name"
        self._calls: list[dict] = []
        self._lock = threading.Lock()

    def get_paginator(self, operation_name):
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
            self._tracker._record(op, (), {"Bucket": bucket}, round((time.time() - t0) * 1000, 2), "OK")
            if self._tracker._calls and self._tracker._calls[-1]["method"] == op:
                self._tracker._calls[-1]["time_ms"] = 0.0
                self._tracker._calls[-1]["path"] = path
            yield page


class _MockS3ClientForProvisioning:
    """Mock S3 client used during FASTLY_MOCK_MODE=1 to bypass real FOS network calls."""

    def __init__(self):
        self.meta = type("Meta", (), {"events": type("Events", (), {"register": lambda *a, **kw: None})()})()

    def head_bucket(self, Bucket):
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
    from backend.core.fastly.client import is_mock_mode

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
    name: str, region: str, access_key: str, secret_key: str, status_cb=None, *, service_id: str | None = None
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
    name: str, region: str, access_key: str, secret_key: str, status_cb=None, *, service_id: str | None = None
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
        from tenacity import Retrying, retry_if_exception, stop_after_attempt

        max_attempts = 15

        def _is_bucket_not_empty(exc: BaseException) -> bool:
            return isinstance(exc, ClientError) and exc.response.get("Error", {}).get("Code") == "BucketNotEmpty"

        def _before_sleep(retry_state) -> None:
            attempt = retry_state.attempt_number
            wait_time = 2 if attempt <= 6 else 5
            if status_cb:
                status_cb(f"⏳ Bucket not empty (attempt {attempt}/{max_attempts}), retrying in {wait_time}s...")
            time.sleep(wait_time)
            empty_bucket()

        try:
            for attempt in Retrying(
                stop=stop_after_attempt(max_attempts),
                retry=retry_if_exception(_is_bucket_not_empty),
                before_sleep=_before_sleep,
                reraise=True,
            ):
                with attempt:
                    s3.delete_bucket(Bucket=name)
                    ok("FOS bucket deleted")
                    if status_cb:
                        status_cb("✅ Bucket deleted.")
                    return
        except ClientError as exc:
            raise RuntimeError(f"Could not delete FOS bucket after {max_attempts} attempts: {exc}") from exc
    finally:
        if isinstance(s3, _ProvisioningTracker):
            s3.flush()


def product_enabled(token: str, product_id: str) -> bool:
    """Return True if the Fastly account behind ``token`` has ``product_id``     enabled, per Fastly's product-enablement endpoint     (``GET /enabled-products/v1/{product_id}``)."""
    try:
        fastly("GET", f"/enabled-products/v1/{product_id}", token=token)
        return True
    except RuntimeError as exc:
        return "HTTP 4" not in str(exc)


def object_storage_enabled(token: str) -> bool:
    """Return True if the account has Object Storage enabled. Object Storage is     REQUIRED for log storage; see ``product_enabled`` for the 200/4xx/5xx     semantics."""
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
    return {"id": key["access_key"], "access_key": key["access_key"], "secret_key": key["secret_key"]}


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
    """List all FOS access keys and delete ones matching this service's patterns."""
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


from backend.provision.declarative.reconciler import FeatureState, reconcile_vcl_state  # noqa: E402
from backend.provision.fastly_api import (  # noqa: E402
    delete_cdn_service,
    ensure_cdn_service,
    remove_logging_endpoint,
    validate_log_format,
)


def _sync_crontab():
    """Refresh the background scheduler if running within the web app."""
    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception:
        pass


def _upload_faro_bundle_sync(service_id: str, version: str, token: str, *, status_cb=None):
    """Sync wrapper around the async ``download_and_upload_faro`` for use with ``run_with_events``."""
    import asyncio

    from backend.provision.rum_assets import download_and_upload_faro

    return asyncio.run(download_and_upload_faro(service_id, version, token, status_cb=status_cb))


def _reject_unsafe_fos_component(field: str, value: str, *, allow_slash: bool) -> None:
    """Reject path-traversal tokens in an FOS path component before persisting."""
    if not value:
        return
    bad = ("\\", "..", "\x00") if allow_slash else ("/", "\\", "..", "\x00")
    for tok in bad:
        if tok in value:
            raise ValueError(f"{field} contains an illegal path token {tok!r}")


def write_service_config(state: dict):
    """Write a service config JSON file to configs/{service_id}.json."""
    from backend import config as svcconfig

    service_id = state.get("logging_service_id") or state.get("service_id")
    if not service_id:
        raise ValueError("ingest state missing logging_service_id / service_id")
    db_path = svcconfig.duckdb_path(service_id)

    def _build_cfg(existing_cfg: dict) -> dict:
        fos_key = state.get("fos_access_key_id") or state.get("fos_access_key", "")
        fos_secret = state.get("fos_secret_access_key") or state.get("fos_secret_key", "")
        bucket = state.get("fos_bucket") or state.get("fos_bucket_name", "")
        fos_prefix = ""
        _reject_unsafe_fos_component("fos_bucket", bucket, allow_slash=False)
        region = state.get("fos_region", "us-east-1")
        cdn_url = state.get("cdn_url", "")
        incoming_lf = dict(state.get("log_fields") or {})
        incoming_custom = incoming_lf.get("custom_fields")
        existing_custom = list((existing_cfg.get("log_fields") or {}).get("custom_fields") or [])
        if not incoming_custom and existing_custom:
            incoming_lf["custom_fields"] = lf._filter_user_custom_fields(existing_custom)
        else:
            incoming_lf["custom_fields"] = lf._filter_user_custom_fields(incoming_lf.get("custom_fields", []))
        import datetime as _dt

        created_at = existing_cfg.get("created_at") or _dt.datetime.now(_dt.UTC).isoformat()
        cfg = {
            "service_id": service_id,
            "name": state.get("name") or state.get("service_name") or service_id,
            "created_at": created_at,
            "access_level": state.get("access_level", "read_write"),
            "storage_mode": state.get("storage_mode", "cloud"),
            "fos_endpoint": region_endpoint(region),
            "fos_access_key_id": fos_key,
            "fos_secret_access_key": fos_secret,
            "fos_bucket": bucket,
            "fos_prefix": fos_prefix,
            "raw_layout_version": 3,
            "fos_region": region,
            "cdn_url": cdn_url,
            "cdn_secret": state.get("cdn_secret", ""),
            "cdn_service_id": state.get("cdn_service_id", ""),
            "fastly_api_key": state.get("fastly_api_key") or state.get("admin_token", ""),
            "log_retention_days": int(state.get("log_retention_days", 30)),
            "duckdb_path": db_path,
            "log_fields": incoming_lf,
        }
        for preserved_key in ("scoring", "cmcd", "ngwaf_workspace_id", "rum"):
            if preserved_key not in state and preserved_key in existing_cfg:
                cfg[preserved_key] = existing_cfg[preserved_key]
            elif preserved_key in state:
                cfg[preserved_key] = state[preserved_key]
        logging_enabled = state.get("logging_enabled")
        if logging_enabled is None:
            logging_enabled = existing_cfg.get("logging_enabled", True)
        rum_enabled = state.get("rum_enabled")
        if rum_enabled is None:
            rum_enabled = existing_cfg.get("rum_enabled", False)
        cfg["logging_enabled"] = bool(logging_enabled)
        cfg["rum_enabled"] = bool(rum_enabled)
        if cfg["rum_enabled"] and not cfg.get("rum"):
            import datetime as _dt

            cfg["rum"] = {"enabled": True, "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")}
        if "log_period" in state:
            cfg["log_period"] = state["log_period"]
        elif "log_period" in state.get("provisioning", {}):
            cfg["log_period"] = state["provisioning"]["log_period"]
        log_period_secs = int(cfg.get("log_period", 120))
        sync_interval_seconds = max(5, log_period_secs)
        commit_interval_mins = max(max(1, sync_interval_seconds // 60), int(state.get("commit_interval_mins", 5)))
        _rate_limiting = state.get("rate_limiting")
        if _rate_limiting is None:
            _rate_limiting = state.get("provisioning", {}).get("rate_limiting")
        if _rate_limiting is None:
            _rate_limiting = (existing_cfg.get("provisioning") or {}).get("rate_limiting")
        rate_limiting_val = True if _rate_limiting is None else bool(_rate_limiting)
        prov_in = state.get("provisioning", {})
        cron_sync_in = prov_in.get("cron_sync", {})
        cron_compact_in = prov_in.get("cron_compact", {})

        def _int_opt(key: str, default: int, *, sub: dict = cron_sync_in) -> int:
            return int(state.get(key, sub.get(key, default)))

        cfg["provisioning"] = {
            "endpoint_name": prov_in.get("endpoint_name", "Fastly Object Storage Logs"),
            "cdn_service_id": state.get("cdn_service_id", prov_in.get("cdn_service_id", "")),
            "cdn_url": cdn_url,
            "cdn_shield": state.get("cdn_shield", prov_in.get("cdn_shield", "")),
            "sample_rate": state.get("sample_rate", prov_in.get("sample_rate", "100")),
            "edge_only": state.get("edge_only", prov_in.get("edge_only", True)),
            "custom_condition": state.get("custom_condition", prov_in.get("custom_condition", "")),
            "rate_limiting": rate_limiting_val,
            "cron_sync": {
                "enabled": state.get("enable_cron_sync", cron_sync_in.get("enabled", True)),
                "delete_after": state.get("delete_after", cron_sync_in.get("delete_after", True)),
                "interval_seconds": sync_interval_seconds,
                "commit_interval_mins": commit_interval_mins,
                "log_enabled": True,
                "log_retention_days": int(state.get("log_retention_days", 30)),
                "data_retention_days": _int_opt("data_retention_days", 30),
                "rum_retention_days": _int_opt("rum_retention_days", 30),
                "cache_retention_days": _int_opt("cache_retention_days", 90),
                "rollup_retention_months": _int_opt("rollup_retention_months", 12),
            },
            "cron_compact": {
                "enabled": state.get("enable_cron_compact", cron_compact_in.get("enabled", True)),
                "interval_mins": 1440,
                "log_enabled": True,
                "log_retention_days": int(state.get("log_retention_days", 30)),
            },
            "temp_admin_key_id": state.get("temp_admin_key_id", prov_in.get("temp_admin_key_id")),
        }
        return cfg

    def _apply(target: dict) -> None:
        target.update(_build_cfg(target))

    with svcconfig.config_lock(service_id):
        if svcconfig.mutate_config(service_id, _apply) is None:
            first_cfg: dict = {}
            _apply(first_cfg)
            svcconfig.save_config(service_id, first_cfg)
    ok(f"Service config written → configs/{service_id}.json")


class _PreflightError(Exception):
    pass


def _state_file_path() -> str:
    """Absolute path to the provisioning resume-state file."""
    from backend import config as svcconfig

    svcconfig._ensure_dirs()
    return str(svcconfig.SYSTEM_DATA_DIR / "setup-state.json")


def save_state(state: dict):
    try:
        with open(_state_file_path(), "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.debug("Failed to save provisioning state: %s", e)


def load_state() -> dict:
    try:
        path = _state_file_path()
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    except Exception as e:
        logger.debug("Failed to load provisioning state: %s", e)
    return {}


def ensure_logging_via_reconciler(state: dict, token: str, status_cb=None) -> int:
    """Apply logging and VCL configuration via declarative reconciliation."""
    from backend.core.fastly.client import get_active_version
    from backend.provision.cli import info, ok

    service_id = state.get("logging_service_id")
    if not service_id:
        raise ValueError("state missing logging_service_id")
    info("Reconciling VCL state via declarative model…")
    if status_cb:
        status_cb(f"🔄 Reconciling VCL state for {service_id}...")
    state_for_feature = dict(state)
    state_for_feature["service_id"] = service_id
    try:
        FeatureState.from_config(state_for_feature)
    except (KeyError, ValueError) as e:
        raise RuntimeError(f"Invalid feature state configuration: {e}") from e
    try:
        result = reconcile_vcl_state(service_id, token, dry_run=False, status_cb=status_cb)
    except Exception as e:
        raise RuntimeError(f"VCL reconciliation failed: {e}") from e
    if result.activated_version is None:
        active_version = get_active_version(service_id, token)
        if active_version is None:
            raise RuntimeError(f"Service {service_id} has no active version")
        ok(f"Logging configuration already in desired state (version {active_version})")
        if status_cb:
            status_cb("✅ Logging configuration already in desired state.")
        return active_version
    else:
        ok(f"Logging and VCL configuration deployed (version {result.activated_version})")
        if status_cb:
            status_cb("✅ Logging and VCL configuration deployed.")
        return result.activated_version


def provision(cfg: dict, _resume_from_state: bool = False):
    token = cfg["admin_token"]
    state = load_state() if _resume_from_state else {}
    state.update(cfg)
    config_existed = False
    _check_service_id = cfg.get("logging_service_id")
    if _check_service_id:
        try:
            from backend import config as svcconfig

            if os.path.exists(svcconfig.config_path(_check_service_id)):
                config_existed = True
        except Exception:
            pass
    if cfg.get("fos_access_key_id") and cfg.get("fos_secret_access_key"):
        state.setdefault("fos_access_key", cfg["fos_access_key_id"])
        state.setdefault("fos_secret_key", cfg["fos_secret_access_key"])
        state.setdefault("fos_key_id", cfg["fos_access_key_id"])
    total = 8
    try:
        yield {"type": "progress", "current": 0, "total": total}
        using_falco = bool(shutil.which("falco"))
        validator = "falco" if using_falco else "built-in checks"
        yield {"type": "status", "message": f"✈️ Step 1/8: Pre-flight validation ({validator})..."}
        step(1, total, f"Pre-flight validation ({validator})")
        fmt_errors = validate_log_format(cfg.get("log_fields"))
        if fmt_errors:
            for err in fmt_errors:
                yield {"type": "status", "message": f"  ERROR: {err}"}
            raise _PreflightError(f"Log format validation failed ({len(fmt_errors)} error(s)).")
        yield {"type": "status", "message": "✅ Pre-flight: Log format OK."}
        yield {"type": "progress", "current": 1, "total": total}
        yield {"type": "status", "message": "🔑 Step 2/8: Creating temporary Admin Fastly Object Storage key..."}
        step(2, total, "Creating temporary Admin Fastly Object Storage key")
        temp_desc = f"fos-log-analysis-temp-admin-{cfg['logging_service_id']}"
        temp_key = yield from run_with_events(
            ensure_fos_access_key, temp_desc, state, token, permission="read-write-admin"
        )
        state["temp_admin_access_key"] = temp_key["access_key"]
        state["temp_admin_secret_key"] = temp_key["secret_key"]
        state["temp_admin_key_id"] = temp_key["id"]
        save_state(state)
        yield {"type": "progress", "current": 2, "total": total}
        yield {"type": "status", "message": "🪣 Step 3/8: Creating Fastly Object Storage bucket..."}
        step(3, total, "Creating Fastly Object Storage bucket")
        yield from run_with_events(
            ensure_fos_bucket,
            cfg["fos_bucket_name"],
            cfg["fos_region"],
            state["temp_admin_access_key"],
            state["temp_admin_secret_key"],
            service_id=cfg["logging_service_id"],
        )
        save_state(state)
        yield {"type": "progress", "current": 3, "total": total}
        yield {"type": "status", "message": "🔐 Step 4/8: Creating permanent Scoped Fastly Object Storage key..."}
        step(4, total, "Creating permanent Scoped Fastly Object Storage key")
        key_desc = f"fos-log-analysis-{cfg['logging_service_id']}"
        key = yield from run_with_events(
            ensure_fos_access_key,
            key_desc,
            state,
            token,
            permission="read-write-objects",
            buckets=[cfg["fos_bucket_name"]],
        )
        state["fos_access_key"] = key["access_key"]
        state["fos_secret_key"] = key["secret_key"]
        state["fos_key_id"] = key["id"]
        state["fos_key_description"] = key_desc
        save_state(state)
        try:
            write_service_config(state)
            ok("Service config persisted early (FOS ready) — survives stream interruption")
        except Exception as early_cfg_exc:  # noqa: BLE001
            warn(f"Early service-config write failed (non-fatal, retried at finalize): {early_cfg_exc}")
        yield {"type": "progress", "current": 4, "total": total}
        yield {"type": "status", "message": "🧹 Step 5/8: Cleaning up temporary admin key..."}
        step(5, total, "Cleaning up temporary admin key")
        if state.get("temp_admin_key_id"):
            yield from run_with_events(delete_fos_access_key, state["temp_admin_key_id"], token)
            for k in ["temp_admin_access_key", "temp_admin_secret_key", "temp_admin_key_id"]:
                state.pop(k, None)
            save_state(state)
        else:
            ok("Temporary key already removed")
        yield {"type": "progress", "current": 5, "total": total}
        yield {
            "type": "status",
            "message": "🌐 Step 6/8: Creating Fastly CDN service to front Fastly Object Storage...",
        }
        step(6, total, "Creating Fastly CDN service to front Fastly Object Storage")

        def _record_cdn_service_id(svc_id):
            state["cdn_service_id"] = svc_id
            save_state(state)

        cdn_svc = yield from run_with_events(
            ensure_cdn_service,
            cfg,
            state["fos_access_key"],
            state["fos_secret_key"],
            token,
            on_created=_record_cdn_service_id,
        )
        state["cdn_service_id"] = cdn_svc["id"]
        if cdn_svc.get("rate_limiting") is not None:
            state["rate_limiting"] = cdn_svc["rate_limiting"]
        save_state(state)
        yield {"type": "progress", "current": 6, "total": total}
        yield {"type": "status", "message": "📡 Step 7/8: Reconciling logging configuration via declarative model..."}
        step(7, total, "Reconciling logging configuration")
        write_service_config(state)
        ok("Service config written (for reconciler)")
        if state.get("rum_enabled"):
            rum_state_raw = state.get("rum")
            rum_state_cfg = rum_state_raw if isinstance(rum_state_raw, dict) else {}
            resolved_faro_version = rum_state_cfg.get("faro_version") or DEFAULT_FARO_VERSION
            yield {"type": "status", "message": f"⏳ Uploading Faro Web SDK v{resolved_faro_version}..."}
            try:
                yield from run_with_events(
                    _upload_faro_bundle_sync, state["logging_service_id"], resolved_faro_version, token
                )
            except Exception as faro_exc:  # noqa: BLE001
                logger.warning(
                    "[provision] Faro bundle upload failed for %s (non-fatal — "
                    "the RUM sync cron self-heals it on its next tick): %s",
                    state.get("logging_service_id"),
                    faro_exc,
                    exc_info=True,
                )
                warn(f"Faro Web SDK upload failed (non-fatal, cron will restore it): {faro_exc}")
                yield {
                    "type": "status",
                    "message": (
                        f"⚠ Faro Web SDK upload failed (non-fatal): {faro_exc}. "
                        "The RUM sync cron will restore the bundle automatically."
                    ),
                }
            else:
                from backend import config as svcconfig

                refreshed_cfg = svcconfig.load_config(state["logging_service_id"]) or {}
                refreshed_rum = refreshed_cfg.get("rum")
                if isinstance(refreshed_rum, dict):
                    state["rum"] = refreshed_rum
                else:
                    rum_state_cfg = dict(rum_state_cfg)
                    rum_state_cfg["faro_version"] = resolved_faro_version
                    state["rum"] = rum_state_cfg
                write_service_config(state)
                ok(f"Faro Web SDK v{resolved_faro_version} uploaded (for reconciler)")
        new_ver = yield from run_with_events(ensure_logging_via_reconciler, state, token)
        state["activated_logging_version"] = new_ver
        save_state(state)
        yield {"type": "progress", "current": 7, "total": total}
        yield {"type": "status", "message": "⚙️ Step 8/8: Finalizing service configuration..."}
        step(8, total, "Finalizing configuration")
        write_service_config(state)
        if is_mock_mode():
            yield {"type": "status", "message": "⚠ Skipping Iceberg initialization in mock mode."}
            yield {"type": "progress", "current": 8, "total": total}
            yield {"type": "done", "message": "🎉 Provisioning complete!"}
            return
        try:
            from backend.core import duckdb as db
            from backend.core import iceberg as db_iceberg

            src = db.get_source_for_service(state["logging_service_id"])
            if not src:
                logger.warning(
                    "[provision] %s: no source resolved after config write — "
                    "Iceberg table will be created lazily on the first commit",
                    state.get("logging_service_id"),
                )
                warn("Iceberg table not initialized yet (no source resolved) — created on first commit")
                yield {
                    "type": "status",
                    "message": "⚠ Iceberg table not initialized yet (no source resolved) — "
                    "it will be created on the first commit.",
                }
            else:
                yield {"type": "status", "message": "🧊 Initializing Iceberg table in Fastly Object Storage..."}
                from tenacity import Retrying, retry_if_exception_type, stop_after_attempt

                _ICE_INIT_ATTEMPTS = 3
                ice_exc: Exception | None = None
                try:
                    for attempt in Retrying(
                        stop=stop_after_attempt(_ICE_INIT_ATTEMPTS),
                        retry=retry_if_exception_type(OSError),
                        reraise=True,
                    ):
                        with attempt:
                            idx = attempt.retry_state.attempt_number
                            if idx > 1:
                                yield {
                                    "type": "status",
                                    "message": (
                                        f"… Object Storage not ready yet "
                                        f"(attempt {idx - 1}/{_ICE_INIT_ATTEMPTS}); retrying…"
                                    ),
                                }
                                time.sleep(1.5 * (idx - 1))
                            db_iceberg.init_iceberg_table(src)
                except Exception as e:
                    ice_exc = e
                if ice_exc is None:
                    ok("Iceberg table ready")
                    yield {"type": "status", "message": "✓ Iceberg table ready."}
                else:
                    logger.error(
                        "[provision] Iceberg table init failed after %d attempt(s) (deferred to first commit)",
                        _ICE_INIT_ATTEMPTS,
                        exc_info=ice_exc,
                    )
                    warn(f"Iceberg table init deferred to first commit: {ice_exc}")
                    yield {"type": "status", "message": f"⚠ Iceberg table init deferred to first commit: {ice_exc}"}
                c = db.get_connection(source=src)
                initial_lf = state.get("log_fields", {})
                if initial_lf:
                    details = {
                        "preset": initial_lf.get("preset"),
                        "groups_before": [],
                        "groups_after": sorted(initial_lf.get("groups", [])),
                        "fields_added": [],
                        "fields_removed": [],
                        "format_hash": lf.format_hash(initial_lf),
                    }
                    from backend.core import metadata as metadata_db

                    metadata_db.record_audit(src["name"], "log_format_change", details, actor="provisioning")
                c.close()
        except Exception:
            logger.exception("[provision] post-provision Iceberg/audit step failed (non-fatal)")
        yield {"type": "progress", "current": 8, "total": total}
        yield {"type": "done", "message": "🎉 Provisioning complete!"}
    except _PreflightError as e:
        yield {"type": "error", "message": str(e)}
        return
    except Exception as e:
        yield {"type": "status", "message": f"Provisioning failed: {str(e)}. Starting full cleanup rollback..."}
        yield from perform_teardown(state, token)
        service_id = state.get("logging_service_id")
        if service_id and not config_existed:
            from backend import config as svcconfig

            cfg_path = svcconfig.config_path(service_id)
            if os.path.exists(cfg_path):
                os.remove(cfg_path)
        yield {"type": "error", "message": str(e)}
        raise e


def perform_teardown(state: dict, token: str, opts: dict | None = None):
    if opts is None:
        opts = {
            "remove_logging": True,
            "remove_rum": True,
            "remove_cdn": True,
            "remove_bucket": True,
            "remove_scoring": True,
            "remove_fos_tokens": True,
        }
    total_steps = 5
    yield {"type": "progress", "current": 1, "total": total_steps}
    scoring_meta = state.get("scoring") or {}
    do_scoring = opts.get("remove_scoring", True) and (
        scoring_meta.get("enabled") or state.get("scoring_enabled") or bool(scoring_meta.get("scoring_service_id"))
    )
    step(1, total_steps, "Tearing down session scoring" if do_scoring else "Skipping session scoring teardown")
    if do_scoring:
        try:
            from backend.provision.session_scoring_orchestrator import teardown_scoring_resources

            failed = yield from run_with_events(
                teardown_scoring_resources, state["logging_service_id"], scoring_meta, token
            )
            for label, store_id in failed or []:
                yield {
                    "type": "status",
                    "message": f"Warning: scoring {label} store {store_id} not deleted — remove it manually.",
                }
        except Exception as exc:
            yield {
                "type": "status",
                "message": (
                    f"❌ Scoring teardown ABORTED (Compute service left in place on purpose): {exc} "
                    "Re-run teardown after resolving; continuing with the remaining steps."
                ),
            }
    yield {"type": "progress", "current": 2, "total": total_steps}
    step(
        2,
        total_steps,
        "Removing logging endpoint" if opts.get("remove_logging") else "Skipping logging endpoint removal",
    )
    endpoint_name = state.get("provisioning", {}).get("endpoint_name") or state.get("endpoint_name", "")
    if (opts.get("remove_logging") or opts.get("remove_rum")) and state.get("logging_service_id") and endpoint_name:
        try:
            yield from run_with_events(
                remove_logging_endpoint,
                state["logging_service_id"],
                endpoint_name,
                token,
                remove_logging=opts.get("remove_logging", True),
                remove_rum=opts.get("remove_rum", True),
            )
        except Exception as exc:
            yield {"type": "status", "message": f"Warning: {exc}"}
    yield {"type": "progress", "current": 3, "total": total_steps}
    step(
        3,
        total_steps,
        "Deleting FOS access keys" if opts.get("remove_fos_tokens") else "Skipping FOS access keys deletion",
    )
    if opts.get("remove_fos_tokens"):
        try:
            yield from run_with_events(delete_fos_tokens_for_service, state["logging_service_id"], token)
        except Exception:
            pass
    yield {"type": "progress", "current": 4, "total": total_steps}
    step(4, total_steps, "Deleting FOS bucket" if opts.get("remove_bucket") else "Skipping FOS bucket deletion")
    remove_bucket = bool(opts.get("remove_bucket"))
    remove_cloud_files = not remove_bucket and bool(opts.get("remove_cloud_files"))
    if (remove_bucket or remove_cloud_files) and state.get("fos_bucket_name"):
        try:
            temp_desc = f"fos-log-analysis-temp-teardown-{state.get('logging_service_id', 'unknown')}"
            temp_key = yield from run_with_events(
                ensure_fos_access_key, temp_desc, {}, token, permission="read-write-admin"
            )
            try:
                if remove_bucket:
                    yield from run_with_events(
                        delete_fos_bucket,
                        state["fos_bucket_name"],
                        state["fos_region"],
                        temp_key["access_key"],
                        temp_key["secret_key"],
                        service_id=state.get("logging_service_id"),
                    )
                else:
                    prefix = state.get("fos_prefix", "")
                    yield from run_with_events(
                        delete_fos_prefix,
                        state["fos_bucket_name"],
                        state["fos_region"],
                        temp_key["access_key"],
                        temp_key["secret_key"],
                        prefix,
                        service_id=state.get("logging_service_id"),
                    )
            finally:
                yield from run_with_events(delete_fos_access_key, temp_key["id"], token)
        except Exception as exc:
            if remove_cloud_files:
                yield {"type": "status", "message": f"Warning: failed to delete cloud files: {exc}"}
    yield {"type": "progress", "current": 5, "total": total_steps}
    step(5, total_steps, "Deleting CDN service" if opts.get("remove_cdn") else "Skipping CDN service deletion")
    if (
        opts.get("remove_cdn")
        and state.get("cdn_service_id")
        and state.get("cdn_service_name")
        and state.get("cdn_service_id") != state.get("logging_service_id")
    ):
        try:
            yield from run_with_events(delete_cdn_service, state["cdn_service_id"], state["cdn_service_name"], token)
        except Exception:
            pass
    elif opts.get("remove_cdn") and state.get("cdn_service_id") == state.get("logging_service_id"):
        logger.error(
            "[provision] refusing to delete CDN service %s because it is the logging service",
            state.get("cdn_service_id"),
        )
        yield {
            "type": "status",
            "message": (
                f"Refusing to delete CDN service {state.get('cdn_service_id')}: it matches the logging service."
            ),
        }


def cleanup_local_data(service_id: str, bucket: str = None, remove_data: bool = False):
    """Remove local config files, database, and cache associated with a service."""
    from backend import config as svcconfig

    if service_id:
        cfg_path = svcconfig.config_path(service_id)
        if os.path.exists(cfg_path):
            os.remove(cfg_path)
            ok(f"Removed service config: {cfg_path}")
    if remove_data:
        db_path = (
            svcconfig.duckdb_path(service_id)
            if service_id
            else os.path.join(os.path.dirname(__file__), "..", "data", "logs.duckdb")
        )
        rum_db_path = rum_duckdb_path(db_path)
        for target_db, label in ((db_path, "local database"), (rum_db_path, "local RUM database")):
            if os.path.exists(target_db):
                os.remove(target_db)
                wal = target_db + ".wal"
                if os.path.exists(wal):
                    os.remove(wal)
                ok(f"Removed {label}: {target_db}")
        if service_id:
            try:
                from backend.core import metadata as metadata_db

                metadata_db.teardown(service_id)
                ok(f"Cleaned up metadata for {service_id}")
            except Exception:
                pass
        if bucket:
            if any(c in bucket for c in ("/", "\\", "..", "\x00")):
                logger.warning("[teardown] refusing to remove cache for bucket=%r with path-shape characters", bucket)
            else:
                for base in [os.getcwd(), os.path.join(os.path.dirname(__file__), "..", "..")]:
                    cache_root = os.path.realpath(os.path.join(base, "cache"))
                    svc_cache_dir = os.path.realpath(os.path.join(cache_root, bucket))
                    try:
                        common = os.path.commonpath([cache_root, svc_cache_dir])
                    except ValueError:
                        continue
                    if common != cache_root:
                        logger.warning(
                            "[teardown] refusing to remove cache: resolved path %s escapes %s",
                            svc_cache_dir,
                            cache_root,
                        )
                        continue
                    if os.path.exists(svc_cache_dir):
                        shutil.rmtree(svc_cache_dir)
                        ok(f"Removed local cache: {svc_cache_dir}")
    _sync_crontab()


def analyst_path_a_supported(source: dict | None = None) -> bool:
    """Return whether independent FOS-only analyst invites are usable."""
    from backend import config as svcconfig

    return not svcconfig.is_high_scale_mode(source)


def generate_analyst_invite(service_id: str) -> dict:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise RuntimeError(f"Service {service_id} not found")
    if cfg.get("access_level") != "read_write":
        raise RuntimeError("Invite generation requires a read_write service configuration")
    if not analyst_path_a_supported(cfg):
        raise RuntimeError(
            "Independent analyst invites are unavailable when DEPLOYMENT_MODE=high_scale: "
            "analytical data resides in ClickHouse and is not included in the FOS-only invite. "
            "Use live shared-instance analyst access (Path B) instead."
        )
    api_token = cfg.get("fastly_api_key", "").strip()
    if not api_token:
        raise RuntimeError(
            f"Service {service_id} has no stored fastly_api_key. Rotate the credential before generating a viewer key."
        )
    bucket = cfg.get("fos_bucket", "")
    region = cfg.get("fos_region", "us-east-1")
    key = fastly(
        "POST",
        "/resources/object-storage/access-keys",
        {
            "permission": "read-only-objects",
            "description": f"fos-log-analysis-analyst-{service_id}-{int(time.time())}",
            "buckets": [bucket],
        },
        token=api_token,
    )
    if not isinstance(key, dict) or "access_key" not in key or "secret_key" not in key:
        raise RuntimeError(
            f"Fastly access-key API returned unexpected shape (keys={list(key.keys()) if isinstance(key, dict) else type(key).__name__}); "
            "cannot generate analyst invite."
        )
    return {
        "name": cfg.get("name", service_id),
        "service_id": service_id,
        "fos_bucket": bucket,
        "fos_region": region,
        "fos_endpoint": cfg.get("fos_endpoint", f"{region}.object.fastlystorage.app"),
        "fos_prefix": cfg.get("fos_prefix", ""),
        "access_key_id": key["access_key"],
        "secret_key": key["secret_key"],
        "iceberg_metadata_location": None,
        "cdn_url": cfg.get("cdn_url", ""),
        "cdn_service_id": cfg.get("cdn_service_id") or "",
        "cdn_secret": cfg.get("cdn_secret") or "",
    }


def _build_log_fields_config(args) -> dict:
    preset_name = getattr(args, "preset", None) or "standard"
    preset = lf.PRESETS.get(preset_name)
    if preset is None:
        raise ValueError(f"Unknown log-fields preset: {preset_name!r}")
    groups = list(preset["groups"])
    for g in getattr(args, "enable_group", None) or []:
        if g not in groups:
            groups.append(g)
    for g in getattr(args, "disable_group", None) or []:
        if g in groups:
            groups.remove(g)
    field_overrides = dict(preset.get("field_overrides") or {})
    field_overrides.update({fid: True for fid in (getattr(args, "enable_field", None) or [])})
    field_overrides.update({fid: False for fid in (getattr(args, "disable_field", None) or [])})
    return {"schema_version": 2, "preset": preset_name, "groups": sorted(groups), "field_overrides": field_overrides}


from backend.config import is_rum_enabled
from backend.core.fastly.utils import load_vcl
from backend.provision.declarative.reconciler import (
    desired_backends,
    desired_logging_endpoints,
    desired_snippets,
    logging_service_snippets,
)
from backend.provision.fastly_api import _CDN_SNIPPETS


def _terraform_template_escape(value: object) -> str:
    """Escape ``${`` / ``%{`` inside a string so Terraform treats them as     literal characters rather than template-syntax openers."""
    s = "" if value is None else str(value)
    return s.replace("${", "$${").replace("%{", "%%{")


def _dump(obj: dict) -> str:
    """Canonical .tf.json serialisation: 2-space indent, keys sorted for     determinism, trailing newline so ``terraform fmt -check`` is happy."""
    return json.dumps(obj, indent=2, sort_keys=True) + "\n"


def generate_terraform(cfg: dict[str, Any], fos_access_key: str, fos_secret_key: str) -> dict[str, str]:
    """Generate Terraform .tf.json for the given provisioning configuration."""
    service_id = str(cfg.get("logging_service_id", "YOUR_SERVICE_ID")).replace("\r", "").replace("\n", "")
    endpoint_name = cfg.get("provisioning", {}).get("endpoint_name", "fastly_log_analysis")
    region = cfg.get("fos_region", "us-east-1")
    bucket = cfg.get("fos_bucket_name", "your-bucket-name")
    prefix = cfg.get("fos_prefix", "").strip("/")
    try:
        period = int(cfg.get("log_period", 3600))
    except (TypeError, ValueError):
        period = 3600
    try:
        rum_period_val = cfg.get("rum_log_period") or (cfg.get("rum") or {}).get("log_period")
        rum_period = int(rum_period_val) if rum_period_val is not None else None
    except (TypeError, ValueError):
        rum_period = None
    cdn_service_name = cfg.get("cdn_service_name", "Fastly Log Analysis CDN Proxy")
    cdn_prefix = cfg.get("cdn_prefix", bucket)
    cdn_domain = f"{cdn_prefix}.global.ssl.fastly.net"
    cdn_shield = cfg.get("cdn_shield", "iad-va-us")
    cdn_secret = cfg.get("cdn_secret", "replace-with-secure-secret")
    prov = cfg.get("provisioning", {})
    sample_rate = prov.get("sample_rate") if "sample_rate" in prov else cfg.get("sample_rate", 100)
    sample_rate = int(sample_rate) if sample_rate is not None else 100
    edge_only = prov.get("edge_only") if "edge_only" in prov else cfg.get("edge_only", False)
    edge_only = bool(edge_only)
    custom_condition = prov.get("custom_condition") if "custom_condition" in prov else cfg.get("custom_condition", "")
    custom_condition = (custom_condition or "").strip()
    cmcd_cfg = cfg.get("cmcd", {})
    scoring_cfg = cfg.get("scoring", {})
    fs_cfg = {
        "service_id": service_id,
        "log_period": period,
        "rum_log_period": rum_period,
        "sample_rate": sample_rate,
        "edge_only": edge_only,
        "custom_condition": custom_condition,
        "fos_prefix": prefix,
        "logging_enabled": bool(cfg.get("logging_enabled", True)),
        "rum_enabled": is_rum_enabled(cfg),
        "cmcd": {
            "enabled": bool(cmcd_cfg.get("enabled", False)),
            "mode": cmcd_cfg.get("mode", "query_string"),
            "version": int(cmcd_cfg.get("version", 1)),
        },
        "scoring": {
            "enabled": bool(scoring_cfg.get("enabled", False)),
            "domain": scoring_cfg.get("domain", ""),
            "request_secret": scoring_cfg.get("request_secret", ""),
            "exclude_url_regex": scoring_cfg.get("exclude_url_regex", ""),
            "enforce_status_code": int(scoring_cfg.get("enforce_status_code", 429)),
        },
        "log_fields": cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []},
    }
    state = FeatureState.from_config(fs_cfg)
    desired_snippets_list = desired_snippets(state)
    desired_endpoints = desired_logging_endpoints(state)
    desired_backends_list = desired_backends(state)
    cdn_vcl = load_vcl(rate_limiting=True)
    cond_parts = ["!segmented_caching.is_inner_req"]
    if edge_only:
        if scoring_cfg.get("enabled", False):
            cond_parts.append("fastly.ff.visits_this_service == 0")
        else:
            cond_parts.append("(req.restarts == 0 && fastly.ff.visits_this_service == 0)")
    if sample_rate < 100:
        cond_parts.append(f"randombool({sample_rate}, 100)")
    if custom_condition:
        cond_parts.append(f"({custom_condition})")
    cond_stmt = " && ".join(cond_parts)
    fos_host = f"{region}.object.fastlystorage.app"
    files: dict[str, str] = {}
    # ── 1. Companion VCL files (not Terraform config) ──────────────────────────
    files["cdn_proxy.vcl"] = cdn_vcl
    # ── 2. versions.tf.json — provider pinning ─────────────────────────────────
    files["versions.tf.json"] = _dump(
        {
            "terraform": {
                "required_version": ">= 1.6",
                "required_providers": {
                    "aws": {"source": "hashicorp/aws", "version": "~> 5.0"},
                    "fastly": {"source": "fastly/fastly", "version": "~> 5.0"},
                },
            },
        }
    )
    # ── 3. fos.tf.json — Object Storage bucket ─────────────────────────────────
    files["fos.tf.json"] = _dump(
        {"resource": {"aws_s3_bucket": {"fos_bucket": {"bucket": _terraform_template_escape(bucket)}}}}
    )
    # ── 4. cdn_proxy.tf.json — CDN proxy service + dictionaries ────────────────
    cdn_snippets_blocks: list[dict] = []
    for name, type_, content, priority in _CDN_SNIPPETS:
        snip_filename = f"cdn_snippets/{name.replace('-', '_')}.vcl"
        files[snip_filename] = content
        cdn_snippets_blocks.append(
            {
                "name": name,
                "type": type_,
                "priority": priority,
                "content": f'${{file("${{path.module}}/{snip_filename}")}}',
            }
        )
    cdn_proxy_block: dict[str, Any] = {
        "name": _terraform_template_escape(cdn_service_name),
        "domain": [{"name": _terraform_template_escape(cdn_domain)}],
        "backend": [
            {
                "name": "fos_origin",
                "address": _terraform_template_escape(fos_host),
                "port": 443,
                "use_ssl": True,
                "ssl_cert_hostname": _terraform_template_escape(fos_host),
                "ssl_sni_hostname": _terraform_template_escape(fos_host),
                "connect_timeout": 5000,
                "first_byte_timeout": 60000,
                "between_bytes_timeout": 30000,
            }
        ],
        "vcl": [{"name": "main", "content": '${file("${path.module}/cdn_proxy.vcl")}', "main": True}],
        "dictionary": [{"name": "fos_credentials", "write_only": True}, {"name": "cdn_auth", "write_only": True}],
        "snippet": cdn_snippets_blocks,
    }
    if cdn_shield and cdn_shield.lower() != "none":
        cdn_proxy_block["backend"][0]["shield"] = _terraform_template_escape(cdn_shield)
    files["cdn_proxy.tf.json"] = _dump(
        {
            "resource": {
                "fastly_service_vcl": {"cdn_proxy": cdn_proxy_block},
                "fastly_service_dictionary_items": {
                    "fos_credentials": {
                        "service_id": "${fastly_service_vcl.cdn_proxy.id}",
                        "dictionary_id": '${{ for d in fastly_service_vcl.cdn_proxy.dictionary : d.name => d.dictionary_id }["fos_credentials"]}',
                        "items": {
                            "access_key": _terraform_template_escape(fos_access_key),
                            "secret_key": _terraform_template_escape(fos_secret_key),
                            "bucket": "${aws_s3_bucket.fos_bucket.bucket}",
                            "region": _terraform_template_escape(region),
                        },
                    },
                    "cdn_auth": {
                        "service_id": "${fastly_service_vcl.cdn_proxy.id}",
                        "dictionary_id": '${{ for d in fastly_service_vcl.cdn_proxy.dictionary : d.name => d.dictionary_id }["cdn_auth"]}',
                        "items": {"secret": _terraform_template_escape(cdn_secret)},
                    },
                },
            },
        }
    )
    # ── 5. logging_service.tf.json — Logging endpoint on existing service ──────
    logging_service_snips = logging_service_snippets(state)
    logging_snippet_blocks: list[dict] = []
    seen_filenames: set[str] = set()
    for snippet in logging_service_snips:
        subroutine = snippet.subroutine.replace("vcl_", "")
        if snippet.name == "Fastly Log Analysis Reset Client IP":
            snip_filename = "capture_snippets/recv_reset.vcl"
        else:
            snip_filename = f"capture_snippets/{subroutine}.vcl"
        if snip_filename not in seen_filenames:
            files[snip_filename] = snippet.body
            seen_filenames.add(snip_filename)
        logging_snippet_blocks.append(
            {
                "name": snippet.name,
                "type": subroutine,
                "priority": snippet.priority,
                "content": f'${{file("${{path.module}}/{snip_filename}")}}',
            }
        )
    logging_s3_blocks: list[dict] = []
    condition_blocks: list[dict] = []
    for endpoint in desired_endpoints:
        if endpoint.name == "Fastly Log Analytics":
            cond_name = f"Log Sampling - {_terraform_template_escape(endpoint_name)}"
            condition_blocks.append(
                {"name": cond_name, "statement": _terraform_template_escape(cond_stmt), "type": "RESPONSE"}
            )
            resp_cond = cond_name
            format_filename = "log_format.vcl"
        else:
            resp_cond = ""
            format_filename = f"{endpoint.name.lower().replace(' ', '_')}_format.vcl"
        files[format_filename] = endpoint.format_string
        logging_s3_blocks.append(
            {
                "name": _terraform_template_escape(endpoint.name),
                "bucket_name": "${aws_s3_bucket.fos_bucket.bucket}",
                "domain": _terraform_template_escape(fos_host),
                "path": _terraform_template_escape(endpoint.path),
                "period": endpoint.period,
                "gzip_level": 9,
                "message_type": "blank",
                "timestamp_format": "%Y-%m-%dT%H:%M:%S.000",
                "response_condition": resp_cond,
                "format_version": 2,
                "format": f'${{file("${{path.module}}/{format_filename}")}}',
                "s3_access_key": _terraform_template_escape(fos_access_key),
                "s3_secret_key": _terraform_template_escape(fos_secret_key),
            }
        )
    logging_backends: list[dict] = []
    for backend in desired_backends_list:
        address = backend.address
        if address == "fos.example.com":
            address = fos_host
        logging_backends.append(
            {
                "name": backend.name,
                "address": _terraform_template_escape(address),
                "port": backend.port,
                "use_ssl": True,
                "ssl_cert_hostname": _terraform_template_escape(address),
                "ssl_sni_hostname": _terraform_template_escape(address),
                "connect_timeout": backend.connect_timeout,
                "first_byte_timeout": backend.first_byte_timeout,
                "between_bytes_timeout": backend.between_bytes_timeout,
                "auto_loadbalance": backend.auto_loadbalance,
            }
        )
    logging_service_block: dict[str, Any] = {
        "name": _terraform_template_escape(cfg.get("service_name", "Logging Service")),
        "domain": [{"name": "example.com"}],
        "logging_s3": logging_s3_blocks,
        "snippet": logging_snippet_blocks,
    }
    if condition_blocks:
        logging_service_block["condition"] = condition_blocks
    if logging_backends:
        logging_service_block["backend"] = logging_backends
    files["logging_service.tf.json"] = _dump(
        {"resource": {"fastly_service_vcl": {"logging_service": logging_service_block}}}
    )
    # ── 6. instructions — companion README explaining the layout ──────────────
    files["instructions"] = f"""\
# Fastly Log Analysis Terraform Export

This directory contains the Terraform configuration to set up Fastly Object Storage logging and a CDN proxy for the Fastly Log Analysis tool.

Configuration is emitted as Terraform's JSON syntax (`.tf.json`). All Terraform commands (`init`, `fmt`, `validate`, `plan`, `apply`) accept it interchangeably with HCL.

## Files
- `versions.tf.json`: Terraform CLI floor + pinned major versions for the `aws` and `fastly` providers.
- `fos.tf.json`: The Fastly Object Storage bucket resource (created via the AWS S3-compatible provider).
- `cdn_proxy.tf.json`: A NEW Fastly Delivery service that fronts the FOS bucket for fast dashboard access.
- `logging_service.tf.json`: The logging endpoint and capture snippets for your existing service.
- `cdn_proxy.vcl`: The main VCL for the CDN proxy service (loaded by `cdn_proxy.tf.json`).
- `log_format.vcl`: The JSON log format string (loaded by `logging_service.tf.json`).
- `cdn_snippets/`: VCL snippets for the CDN proxy service (loaded by `cdn_proxy.tf.json`).
- `capture_snippets/`: VCL snippets for your existing logging service (loaded by `logging_service.tf.json`).

## AWS provider configuration

`fos.tf.json` declares an `aws_s3_bucket` resource. Fastly Object Storage is S3-compatible, so the AWS provider works against the Fastly FOS endpoint. Configure it in your root module (or alongside these files) like this:

```hcl
provider "aws" {{
  region                      = "{region}"
  access_key                  = "<your fos access key>"
  secret_key                  = "<your fos secret key>"
  endpoints {{ s3 = "https://{fos_host}" }}
  skip_credentials_validation = true
  skip_region_validation      = true
  skip_requesting_account_id  = true
}}
```

## Instructions
1. Review `fos.tf.json` and ensure the `aws` provider above is correctly configured.
2. `cdn_proxy.tf.json` creates a NEW Fastly service to accelerate your log reads.
3. `logging_service.tf.json` contains the configuration for your ACTIVE service. You should copy the `logging_s3`, `condition`, and `snippet` blocks into your existing `fastly_service_vcl` resource for service ID `{service_id}`.
4. Run `terraform init` and `terraform apply` to deploy the changes.
"""
    return files


_self_mod = sys.modules[__name__]
for _alias in ("backend.provision.cmcd_fields", "backend.provision.fos_setup", "backend.utils.terraform_gen"):
    sys.modules[_alias] = _self_mod
_prov_pkg = sys.modules.get("backend.provision")
if _prov_pkg is not None:
    _prov_pkg.cmcd_fields = _self_mod  # type: ignore[attr-defined]
    _prov_pkg.fos_setup = _self_mod  # type: ignore[attr-defined]
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.terraform_gen = _self_mod  # type: ignore[attr-defined]
