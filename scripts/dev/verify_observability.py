"""Verify OTLP delivery from a container SDK or a correlated application HTTP request."""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from backend.core import request_telemetry as telemetry

_HTTP_DURATION_COUNTS = ("http_server_duration_milliseconds_count", "http_server_request_duration_seconds_count")


class VerificationError(RuntimeError):
    pass


class ProbeReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    probe_id: UUID
    service_name: Literal["fastly-log-analytics"] = "fastly-log-analytics"
    service_instance_id: UUID
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    emitted_at: AwareDatetime


class ApplicationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    started_at: AwareDatetime
    path: Literal["/api/health", "/api/bootstrap"]


def emit_probe(probe_id: UUID) -> ProbeReceipt:
    if telemetry.effective_exporter() != "otlp":
        raise VerificationError("probe requires the deployed OTLP exporter configuration")
    emitted_at = datetime.now(UTC)
    tracer = telemetry.get_tracer()
    meter = telemetry.get_meter()
    tracer_provider = trace.get_tracer_provider()
    meter_provider = metrics.get_meter_provider()
    if not isinstance(tracer_provider, TracerProvider) or not isinstance(meter_provider, MeterProvider):
        raise VerificationError("application SDK is disabled")
    resource = tracer_provider.resource.attributes
    if resource.get("service.name") != "fastly-log-analytics":
        raise VerificationError("unexpected application SDK resource")
    instance = resource.get("service.instance.id")
    if not isinstance(instance, str):
        raise VerificationError("application SDK has no instance identity")
    with tracer.start_as_current_span("app.observability.probe", attributes={"fla.probe.id": str(probe_id)}) as span:
        context = span.get_span_context()
        if not context.is_valid or not context.trace_flags.sampled:
            raise VerificationError("probe span was not sampled")
        meter.create_counter("app.observability.probe").add(1)
        receipt = ProbeReceipt(
            probe_id=probe_id,
            service_instance_id=UUID(instance),
            trace_id=f"{context.trace_id:032x}",
            span_id=f"{context.span_id:016x}",
            emitted_at=emitted_at,
        )
    for provider in (tracer_provider, meter_provider):
        if not provider.force_flush(timeout_millis=5000):
            raise VerificationError("application SDK flush did not complete")
    return receipt


def _query_origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise VerificationError("receiver query URL must be a credential-free origin")
    return value.rstrip("/")


def _receiver_json(
    client: httpx.Client,
    url: str,
    *,
    remaining: float,
    params: dict[str, str] | None = None,
    allow_missing: bool = False,
) -> dict[str, Any] | None:
    timeout = max(0.1, min(3.0, remaining))
    deadline = time.monotonic() + timeout
    try:
        with client.stream(
            "GET", url, params=params, timeout=timeout, follow_redirects=False, headers={"Accept": "application/json"}
        ) as response:
            if response.status_code == 404 and allow_missing:
                return None
            if response.status_code != 200:
                raise VerificationError(f"receiver returned HTTP {response.status_code}")
            body = bytearray()
            for chunk in response.iter_bytes():
                if time.monotonic() > deadline:
                    raise VerificationError("receiver response exceeded the probe deadline")
                body.extend(chunk)
                if len(body) > 1_048_576:
                    raise VerificationError("receiver response exceeds the probe limit")
        data = json.loads(body)
    except httpx.HTTPError as exc:
        raise VerificationError("receiver transport failed") from exc
    except (ValueError, UnicodeError) as exc:
        raise VerificationError("receiver returned invalid JSON") from exc
    if not isinstance(data, dict):
        raise VerificationError("receiver returned a non-object response")
    return data


def _metric_matches(
    payload: dict[str, Any], *, job: str, instance: str, emitted_at: datetime, http_only: bool = False
) -> bool:
    try:
        if payload["status"] != "success" or payload["data"]["resultType"] != "vector":
            raise VerificationError("Prometheus query failed or returned an unexpected result type")
        rows = payload["data"]["result"]
        if not isinstance(rows, list):
            raise VerificationError("Prometheus returned an invalid result")
        for row in rows:
            labels = row["metric"]
            timestamp, value = map(float, row["value"])
            if (
                labels.get("job") == job
                and labels.get("instance") == instance
                and math.isfinite(timestamp)
                and emitted_at.timestamp() - 2 <= timestamp <= time.time() + 2
                and math.isfinite(value)
                and (labels.get("__name__") in _HTTP_DURATION_COUNTS and value >= 1 if http_only else value == 1)
            ):
                return True
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise VerificationError("Prometheus returned invalid metric data") from exc
    return False


def _attributes(values: list[dict[str, Any]]) -> dict[str, Any]:
    return {item["key"]: item["value"].get("stringValue") for item in values}


def _id_matches(value: str, expected: str) -> bool:
    if value.lower() == expected:
        return True
    try:
        return base64.b64decode(value, validate=True).hex() == expected
    except ValueError:
        return False


def _trace_matches(payload: dict[str, Any], receipt: ProbeReceipt) -> bool:
    try:
        batches = payload.get("batches", payload.get("resourceSpans"))
        if not isinstance(batches, list):
            raise VerificationError("Tempo returned invalid trace data")
        for batch in batches:
            resource = _attributes(batch["resource"]["attributes"])
            if resource.get("service.name") != receipt.service_name or resource.get("service.instance.id") != str(
                receipt.service_instance_id
            ):
                continue
            for scope in batch.get("scopeSpans", batch.get("instrumentationLibrarySpans", [])):
                for span in scope["spans"]:
                    if (
                        span["name"] == "app.observability.probe"
                        and _id_matches(span["traceId"], receipt.trace_id)
                        and _id_matches(span["spanId"], receipt.span_id)
                        and _attributes(span.get("attributes", [])).get("fla.probe.id") == str(receipt.probe_id)
                    ):
                        return True
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise VerificationError("Tempo returned invalid trace data") from exc
    return False


def verify_delivery(
    receipt: ProbeReceipt,
    *,
    client: httpx.Client,
    prometheus_url: str,
    tempo_url: str,
    timeout_seconds: float = 90,
) -> dict[str, Any]:
    if not math.isfinite(timeout_seconds) or not 0 <= timeout_seconds <= 300:
        raise VerificationError("probe timeout must be between 0 and 300 seconds")
    age = time.time() - receipt.emitted_at.timestamp()
    if not -5 <= age <= 300 or not int(receipt.trace_id, 16) or not int(receipt.span_id, 16):
        raise VerificationError("probe receipt is stale or invalid")
    prometheus_url, tempo_url = _query_origin(prometheus_url), _query_origin(tempo_url)
    query = f'app_observability_probe_total{{job="{receipt.service_name}",instance="{receipt.service_instance_id}"}}'
    deadline = time.monotonic() + timeout_seconds
    metric_observed = trace_observed = False
    while True:
        if not metric_observed:
            payload = _receiver_json(
                client, f"{prometheus_url}/api/v1/query", params={"query": query}, remaining=deadline - time.monotonic()
            )
            if payload is not None:
                metric_observed = _metric_matches(
                    payload,
                    job=receipt.service_name,
                    instance=str(receipt.service_instance_id),
                    emitted_at=receipt.emitted_at,
                )
        if not trace_observed:
            payload = _receiver_json(
                client,
                f"{tempo_url}/api/traces/{receipt.trace_id}",
                allow_missing=True,
                remaining=deadline - time.monotonic(),
            )
            if payload is not None:
                trace_observed = _trace_matches(payload, receipt)
        if metric_observed and trace_observed:
            return {
                "scope": "container_sdk_delivery",
                "probe_id": str(receipt.probe_id),
                "metric_observed": True,
                "trace_observed": True,
                "verified_at": datetime.now(UTC).isoformat(),
            }
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise VerificationError(f"delivery not observed: metric={metric_observed}, trace={trace_observed}")
        time.sleep(min(5.0, remaining))


def _application_instance(payload: dict[str, Any], receipt: ApplicationReceipt) -> tuple[str, str] | None:
    try:
        batches = payload.get("batches", payload.get("resourceSpans"))
        if not isinstance(batches, list):
            raise VerificationError("Tempo returned invalid trace data")
        for batch in batches:
            resource = _attributes(batch["resource"]["attributes"])
            if resource.get("service.name") != "fastly-log-analytics":
                continue
            for scope in batch.get("scopeSpans", batch.get("instrumentationLibrarySpans", [])):
                for span in scope["spans"]:
                    if (
                        span["name"] == f"GET {receipt.path}"
                        and span.get("kind") in (2, "SPAN_KIND_SERVER")
                        and _id_matches(span["traceId"], receipt.trace_id)
                        and receipt.started_at.timestamp() - 5
                        <= int(span["startTimeUnixNano"]) / 1_000_000_000
                        <= time.time() + 5
                    ):
                        namespace = resource.get("service.namespace")
                        job = f"{namespace}/fastly-log-analytics" if namespace else "fastly-log-analytics"
                        return job, str(UUID(resource["service.instance.id"]))
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise VerificationError("Tempo returned invalid application trace data") from exc
    return None


def verify_application_delivery(
    receipt: ApplicationReceipt,
    *,
    client: httpx.Client,
    prometheus_url: str,
    tempo_url: str,
    timeout_seconds: float = 90,
) -> dict[str, Any]:
    if not math.isfinite(timeout_seconds) or not 0 <= timeout_seconds <= 300:
        raise VerificationError("probe timeout must be between 0 and 300 seconds")
    if not -5 <= time.time() - receipt.started_at.timestamp() <= 300 or not int(receipt.trace_id, 16):
        raise VerificationError("application receipt is stale or invalid")
    prometheus_url, tempo_url = _query_origin(prometheus_url), _query_origin(tempo_url)
    deadline = time.monotonic() + timeout_seconds
    instance = None
    metric_observed = False
    while True:
        if instance is None:
            payload = _receiver_json(
                client,
                f"{tempo_url}/api/traces/{receipt.trace_id}",
                allow_missing=True,
                remaining=deadline - time.monotonic(),
            )
            if payload is not None:
                instance = _application_instance(payload, receipt)
        if instance is not None:
            job, instance_id = instance
            query = (
                f'{{__name__=~"{"|".join(_HTTP_DURATION_COUNTS)}",'
                f"job={json.dumps(job)},instance={json.dumps(instance_id)}}}"
            )
            payload = _receiver_json(
                client, f"{prometheus_url}/api/v1/query", params={"query": query}, remaining=deadline - time.monotonic()
            )
            if payload is not None:
                metric_observed = _metric_matches(
                    payload, job=job, instance=instance_id, emitted_at=receipt.started_at, http_only=True
                )
        if metric_observed:
            return {
                "scope": "application_http_delivery",
                "trace_id": receipt.trace_id,
                "path": receipt.path,
                "metric_observed": True,
                "trace_observed": True,
                "verified_at": datetime.now(UTC).isoformat(),
            }
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise VerificationError(
                f"application delivery not observed: metric={metric_observed}, trace={instance is not None}"
            )
        time.sleep(min(5.0, remaining))


def _write_private(path: Path, data: dict[str, Any]) -> None:
    encoded = json.dumps(data, indent=2) + "\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(encoded)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    emit = commands.add_parser("emit", help="run inside the deployed backend container")
    emit.add_argument("--probe-id", type=UUID, required=True)
    emit.add_argument("--output", type=Path, required=True)
    for command in ("verify", "verify-request"):
        verify = commands.add_parser(command, help="query private Prometheus and Tempo APIs")
        verify.add_argument("--receipt", type=Path, required=True)
        verify.add_argument("--prometheus-url", required=True)
        verify.add_argument("--tempo-url", required=True)
        verify.add_argument("--output", type=Path, required=True)
        verify.add_argument("--timeout-seconds", type=float, default=90)
    args = parser.parse_args()
    try:
        if args.command == "emit":
            result = emit_probe(args.probe_id).model_dump(mode="json")
        else:
            with httpx.Client(trust_env=False, follow_redirects=False) as client:
                if args.command == "verify-request":
                    result = verify_application_delivery(
                        ApplicationReceipt.model_validate_json(args.receipt.read_text()),
                        client=client,
                        prometheus_url=args.prometheus_url,
                        tempo_url=args.tempo_url,
                        timeout_seconds=args.timeout_seconds,
                    )
                else:
                    result = verify_delivery(
                        ProbeReceipt.model_validate_json(args.receipt.read_text()),
                        client=client,
                        prometheus_url=args.prometheus_url,
                        tempo_url=args.tempo_url,
                        timeout_seconds=args.timeout_seconds,
                    )
        _write_private(args.output, result)
    except VerificationError as exc:
        print(f"Observability verification failed: {exc}", file=sys.stderr)
        return 1
    except (OSError, ValidationError) as exc:
        print(f"Observability verification failed: {type(exc).__name__}", file=sys.stderr)
        return 1
    print(json.dumps({"command": args.command, "completed": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
