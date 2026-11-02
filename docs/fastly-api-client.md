# Fastly API client

All Fastly REST API traffic from the backend goes through one wrapper,
[`backend/core/fastly/client.py`](../backend/core/fastly/client.py) (`fastly()`
for JSON calls, plus an upload variant for large or multipart bodies). It pins
the API host, sets the `Fastly-Key` header, emits telemetry, and retries
transient failures. `scripts/check_no_raw_urllib.sh` keeps new code from
bypassing it.

## Retry behavior

- Retried: HTTP 429, 500, 502, 503, 504, plus timeouts and connection errors.
- Attempts: `max_retries + 1` (default `max_retries=3`), with exponential
  backoff and jitter.
- The policy is **method-blind**: POST, PUT and DELETE are retried exactly like
  GET.

## Duplicate-resource risk on POST

If Fastly accepts a create call (service, version, domain, backend, ...) but the
response is lost to a timeout or a 5xx, the retry repeats the create. The result
can be a duplicate resource, or a spurious 409 even though the first attempt
succeeded.

This is accepted and unchanged. Code that creates resources must tolerate it:
on a 409 or an ambiguous failure, list or read the resource to check whether the
earlier attempt landed before treating the call as failed, and prefer
idempotent updates (PUT with a stable identifier) where the API offers them.
