# Design Specification: Real-Time Active Query Monitoring (DuckDB & SQLite)

This document outlines the architecture, data-flow, and implementation strategy for introducing a real-time active query process list ("SHOW PROCESSLIST") to the dashboard.

Admins will be able to see every executing SQLite and DuckDB query, trace its exact origin (such as which Analyst ran it or which specific Cron Job is executing), monitor active runtimes, and terminate slow-running or runaway queries with a **Kill Button**.

---

## 1. Architectural Overview & Goals

This system is a **small extension to the existing instrumentation in [sqlite_profiler.py](backend/utils/sqlite_profiler.py)**, not a new parallel stack. The profiler already wraps every SQLite execute and writes to a ring buffer of *historical* queries; we add an in-memory dict of *active* queries (registered at execute-start, removed at execute-end) and mirror the same hot path for DuckDB via the pool checkout layer. One ContextVar carries attribution; one stack walk captures the call site.

Goals:

- **Low-overhead instrumentation.** Register/deregister cost stays within the same order of magnitude as the existing profiler (measured at ~5μs). No global lock on the hot path — rely on CPython dict atomicity, the same approach `sqlite_profiler` uses ([sqlite_profiler.py:14-19](backend/utils/sqlite_profiler.py#L14-L19)).
- **Complete coverage.** Capture every query across DuckDB and SQLite — analyst requests, admin endpoints, cron jobs, pool warmers, schema migrations. The two entrypoints that already exist (`InstrumentedCursor`, `checkout_connection`) are the only seams we touch.
- **Structured attribution (see §4).** Each row carries `kind`, principal id/name, cron job + run id, and `caller_file:line` — enough for an admin to identify both *who* triggered the query and *what code* is running it.
- **Safe cancellation.** Both DuckDB and SQLite support `con.interrupt()`; both are exposed. The kill path verifies under a lock that the targeted query is still the one bound to that connection before interrupting — pooled connections get reused and we will not cancel an innocent caller.
- **Cheap, incremental updates.** TanStack Query poll at 2s, default to 1s only while there are active queries. Responses use the `since_seq` incremental pattern already established in [sqlite_profiler.py:160](backend/utils/sqlite_profiler.py#L160) so a steady-state empty list returns ~0 bytes.

---

## 2. Backend Registry (`backend/core/query_registry.py`)

A process-global in-memory registry of currently-executing queries. Designed to mirror the lock-free hot path of [sqlite_profiler.py](backend/utils/sqlite_profiler.py) — CPython dict `__setitem__`/`pop` are atomic under the GIL for our single-key operations, so no lock is needed on register/deregister. The cancel path takes a short lock because it reads multiple fields and validates against per-connection state.

Key design choices, with rationale:

- **`itertools.count(1)` for query IDs.** The doc previously used `f"sqlite-{thread_id}-{time.monotonic()}"`, which can collide at coarse monotonic clock resolution. The existing profiler already uses an atomic counter ([sqlite_profiler.py:44](backend/utils/sqlite_profiler.py#L44)); we use the same scheme.
- **`weakref` to the connection.** DuckDB connections checked out from the pool are closed/discarded on error ([duckdb_pool.py:338](backend/core/duckdb_pool.py#L338)). A strong reference in `ActiveQuery` would resurrect them or, worse, keep a discarded connection's file descriptor alive. A weakref that derefs to `None` means "already done, nothing to cancel".
- **Per-connection `current_query_id`.** Pooled connections execute many queries over their lifetime. To cancel safely we must verify, atomically, that the query we want to kill is *still* the one bound to that connection. We stamp the connection's id() → query_id in a small protected dict and the cancel handler refuses to interrupt if the stamp has moved on.
- **SQL truncation.** Cap at 4KB to mirror `sqlite_profiler._summarize_sql` ([sqlite_profiler.py:73-78](backend/utils/sqlite_profiler.py#L73-L78)). Cancellable for both engines (`sqlite3.Connection.interrupt()` exists too).
- **Sequence number per registration** so the API can support `since_seq` incremental fetch (mirrors the profiler's `get_recent(since_seq=…)` pattern).
- **Completed-history ring buffer (v1).** The most-investigated case is "wait, what just ran?" or "that query failed — why?". On deregister we push the row into a bounded `deque(maxlen=200)` with `ended_at`, `outcome ∈ {ok, error, cancelled}`, `error_type`, `error_message` (truncated 512 chars). Cheap, mirrors the pattern of `sqlite_profiler._buffer` for SQLite history.
- **Pool-slot in v1 attribution.** First question ops will ask when they see contention is "which pool slot is this on?". Two lines: `_get_conn_state(con, "service_key")` already exists in [duckdb_pool.py:121](backend/core/duckdb_pool.py#L121).
- **OTel metric emission (v1).** Mirrors the existing `app.thread_wait_ms` histogram pattern ([duckdb_pool.py:221](backend/core/duckdb_pool.py#L221)). Free observability via the dashboard that already exists.
- **structlog correlation (v1).** `structlog.contextvars.merge_contextvars` is already configured ([structlog_config.py:56](backend/utils/structlog_config.py#L56)), so `bind_contextvars(query_id=qid)` during the wrapped execute means every log line emitted *during* a query carries its id. Two lines to add, huge debugging payoff.

```python
from __future__ import annotations

import collections
import itertools
import logging
import threading
import time
import weakref
from dataclasses import dataclass, field
from typing import Any, Callable

from backend.core.query_attribution import Attribution, current_attribution

logger = logging.getLogger(__name__)

_SQL_TRUNCATE = 4096
_ERR_TRUNCATE = 512
_HISTORY_CAP = 200
_seq = itertools.count(1)

# Identity → query_id map. Validates that interrupt() targets the right query.
# id(con) is stable for the connection's lifetime and we clear the entry on
# deregister, so a reused pool slot can't be confused with a prior query.
_conn_to_query: dict[int, int] = {}
_conn_to_query_lock = threading.Lock()


@dataclass(slots=True)
class ActiveQuery:
    query_id: int                      # monotonic sequence — also the row's `seq`
    db_type: str                       # "DuckDB" | "SQLite"
    sql: str                           # truncated to _SQL_TRUNCATE
    attribution: Attribution           # who triggered + caller_file:line + pool_slot (see §4)
    service_id: str | None
    started_at_mono: float
    started_at_utc: float
    # weakref so a closed connection auto-clears; None for SQLite where the
    # cursor (not connection) is the cancellable handle — we hold the cursor.
    _con_ref: Callable[[], Any] | None
    _con_id: int | None                # cached id(con) for validation on cancel
    cancelled_at: float | None = None  # set by cancel_query; kept ~1s for UI feedback


@dataclass(slots=True)
class CompletedQuery:
    """Snapshot pushed into the history ring buffer on deregister."""
    query_id: int
    db_type: str
    sql: str
    attribution: Attribution
    service_id: str | None
    started_at_utc: float
    ended_at_utc: float
    duration_ms: float
    outcome: str                       # "ok" | "error" | "cancelled"
    error_type: str | None = None      # e.g. "OutOfMemoryException"
    error_message: str | None = None   # truncated to _ERR_TRUNCATE


class QueryRegistry:
    def __init__(self) -> None:
        self._queries: dict[int, ActiveQuery] = {}
        # Bounded ring; thread-safe for append/iterate under CPython.
        self._history: collections.deque[CompletedQuery] = collections.deque(maxlen=_HISTORY_CAP)

    # ── hot path: no lock; relies on dict op atomicity ───────────────────────

    def register(
        self,
        db_type: str,
        sql: str,
        service_id: str | None,
        con: Any | None,
    ) -> int:
        try:
            qid = next(_seq)
            con_id = id(con) if con is not None else None
            sql_trunc = sql if len(sql) <= _SQL_TRUNCATE else sql[:_SQL_TRUNCATE] + f"… [+{len(sql)-_SQL_TRUNCATE}]"
            active = ActiveQuery(
                query_id=qid,
                db_type=db_type,
                sql=sql_trunc,
                attribution=current_attribution.get() or Attribution.system(),
                service_id=service_id,
                started_at_mono=time.monotonic(),
                started_at_utc=time.time(),
                _con_ref=weakref.ref(con) if con is not None else None,
                _con_id=con_id,
            )
            self._queries[qid] = active
            if con_id is not None:
                with _conn_to_query_lock:
                    _conn_to_query[con_id] = qid
            _metric_active_count.add(1, {"db": db_type, "kind": active.attribution.kind})
            return qid
        except Exception:
            # Instrumentation is observability, not control flow. Never propagate.
            logger.debug("query_registry.register failed", exc_info=True)
            return -1  # sentinel — deregister/cancel are no-ops on -1

    def deregister(self, qid: int, *, error: BaseException | None = None) -> None:
        if qid < 0:
            return
        try:
            active = self._queries.pop(qid, None)
            if active is None:
                return
            if active._con_id is not None:
                with _conn_to_query_lock:
                    if _conn_to_query.get(active._con_id) == qid:
                        del _conn_to_query[active._con_id]

            ended = time.time()
            duration_ms = round((time.monotonic() - active.started_at_mono) * 1000, 2)
            if active.cancelled_at is not None:
                outcome = "cancelled"
            elif error is not None:
                outcome = "error"
            else:
                outcome = "ok"
            err_type = type(error).__name__ if error is not None else None
            err_msg = None
            if error is not None:
                msg = str(error)
                err_msg = msg if len(msg) <= _ERR_TRUNCATE else msg[:_ERR_TRUNCATE] + f"… [+{len(msg)-_ERR_TRUNCATE}]"

            self._history.append(CompletedQuery(
                query_id=active.query_id,
                db_type=active.db_type,
                sql=active.sql,
                attribution=active.attribution,
                service_id=active.service_id,
                started_at_utc=active.started_at_utc,
                ended_at_utc=ended,
                duration_ms=duration_ms,
                outcome=outcome,
                error_type=err_type,
                error_message=err_msg,
            ))

            _metric_active_count.add(-1, {"db": active.db_type, "kind": active.attribution.kind})
            _metric_duration_ms.record(duration_ms, {
                "db": active.db_type,
                "kind": active.attribution.kind,
                "outcome": outcome,
            })
        except Exception:
            logger.debug("query_registry.deregister failed", exc_info=True)

    # ── cancel: short lock to validate before interrupt() ────────────────────

    def cancel_query(self, qid: int, *, admin_id: str | None = None) -> str:
        """Returns one of: "cancelled" | "not_found" | "already_finished" |
        "connection_gone". Idempotent — admins re-click. Writes one audit
        line on every "cancelled" outcome (admin_id, target attribution, age)."""
        active = self._queries.get(qid)
        if active is None:
            return "not_found"
        if active._con_ref is None:
            return "already_finished"
        with _conn_to_query_lock:
            if active._con_id is None or _conn_to_query.get(active._con_id) != qid:
                return "already_finished"
            con = active._con_ref()
            if con is None:
                return "connection_gone"
            try:
                con.interrupt()  # supported by both duckdb and sqlite3
            except Exception:
                return "connection_gone"
            active.cancelled_at = time.time()

        # Audit log — structlog, outside the lock.
        try:
            from backend.utils.structlog_config import audit_log
            audit_log.warning(
                "query_cancel",
                admin_id=admin_id,
                query_id=qid,
                target_kind=active.attribution.kind,
                target_principal=active.attribution.principal_id(),
                target_caller=active.attribution.caller_file,
                target_service=active.service_id,
                target_db=active.db_type,
                target_duration_ms=round((time.monotonic() - active.started_at_mono) * 1000, 2),
            )
            _metric_cancelled_total.add(1, {"db": active.db_type, "kind": active.attribution.kind})
        except Exception:
            logger.debug("query_cancel audit log failed", exc_info=True)
        return "cancelled"

    # ── read: snapshot for the admin endpoint ────────────────────────────────

    def snapshot(self, since_seq: int = 0, *, full_sql: bool = False,
                 include_completed: bool = False) -> dict:
        now_mono = time.monotonic()
        active_rows: list[dict] = []
        for q in list(self._queries.values()):  # list() snapshots — safe under concurrent writes
            if q.query_id <= since_seq:
                continue
            active_rows.append(_row_for_active(q, now_mono, full_sql))

        completed_rows: list[dict] = []
        if include_completed:
            for c in list(self._history):
                if c.query_id <= since_seq:
                    continue
                completed_rows.append(_row_for_completed(c, full_sql))

        last_seq_active = max((r["query_id"] for r in active_rows), default=since_seq)
        last_seq_completed = max((r["query_id"] for r in completed_rows), default=since_seq)
        return {
            "last_seq": max(last_seq_active, last_seq_completed, since_seq),
            "active": active_rows,
            "completed": completed_rows,
        }


# OTel hooks — created lazily, mirror the pattern at duckdb_pool.py:221.
def _meter():
    from opentelemetry import metrics
    return metrics.get_meter("backend.query_registry")

_metric_active_count = _meter().create_up_down_counter("app.active_queries.count")
_metric_duration_ms = _meter().create_histogram("app.query_duration_ms", unit="ms")
_metric_cancelled_total = _meter().create_counter("app.queries_cancelled_total")


query_registry = QueryRegistry()
```

`_row_for_active` and `_row_for_completed` produce the shapes documented in §6. `audit_log` is a small structlog logger added to [structlog_config.py](backend/utils/structlog_config.py) so admin-action lines route to a dedicated stream rather than mixing into request logs.

**On the "< 1μs" claim from earlier drafts:** dropped. A realistic register call is dict insert + ContextVar `.get()` + frame walk + weakref creation — ~5–10μs, same order as the existing profiler. Accurate is better than aspirational.

---

## 3. Query Instrumentation Strategy (No Query Missed)

Two seams, both already exist. We do not touch business-logic files.

### A. SQLite — extend the existing `InstrumentedCursor`

[sqlite_profiler.py:94-154](backend/utils/sqlite_profiler.py#L94-L154) already intercepts every SQLite execute. We add register/deregister calls alongside the existing `_record` call so both the active registry and the historical ring buffer see the same statements:

```python
class InstrumentedCursor(sqlite3.Cursor):
    def execute(self, sql, parameters=(), /):
        t0 = time.perf_counter()
        qid = query_registry.register("SQLite", sql, service_id=None, con=self.connection)
        # Bind to structlog so any log emitted during this query carries the id.
        structlog.contextvars.bind_contextvars(query_id=qid)
        err: BaseException | None = None
        try:
            return super().execute(sql, parameters)
        except BaseException as e:
            err = e
            raise
        finally:
            query_registry.deregister(qid, error=err)
            structlog.contextvars.unbind_contextvars("query_id")
            _record(sql, parameters, (time.perf_counter() - t0) * 1000.0, self.rowcount, "execute")
```

Register/deregister are wrapped in best-effort error handling at the registry layer — the SQL path itself must never see an exception from instrumentation (this is the explicit contract documented at [sqlite_profiler.py:50-51](backend/utils/sqlite_profiler.py#L50-L51) and we honor it). Same treatment for `executemany`/`executescript`. `structlog.contextvars.bind_contextvars` works because `merge_contextvars` is already in the processor chain ([structlog_config.py:56](backend/utils/structlog_config.py#L56)).

### B. DuckDB — wrap inside `checkout_connection`, NOT in the pool internals

[duckdb_pool.py:121](backend/core/duckdb_pool.py#L121) keys per-connection state on `id(raw_con)` and [duckdb_pool.py:307](backend/core/duckdb_pool.py#L307) expects `pool.release(raw_con, ...)`. A proxy returned from `pool.acquire` would break both. The clean fix is to wrap **only at the `checkout_connection` boundary** and unwrap before release:

```python
@contextmanager
def checkout_connection(src: dict, max_wait: float = 10.0):
    # ... existing pool.acquire path ...
    raw_con = pool.acquire(src, max_wait=max_wait)
    wrapped = InstrumentedDuckDBConnection(raw_con, service_key)
    errored = False
    try:
        yield wrapped
    except Exception:
        errored = True
        raise
    finally:
        pool.release(raw_con, errored=errored)   # unwrap — pool sees raw only
```

This keeps `_conn_state`, `_idle`, and the `_holder` (which currently stores the raw con on `RequestContext._holder`, see [request_context.py:65](backend/core/request_context.py#L65)) untouched. The proxy lives one request long and never enters the pool.

### Wrapping every execution path — and the result object

DuckDB has several execute entrypoints we must cover: `execute`, `executemany`, `sql`, `query`, and the relational-API methods (`from_query`, `table`, `view`, `read_csv`, `read_parquet`). They all return a result object (`DuckDBPyRelation` / cursor) and **the actual heavy work happens during fetch** — `.fetchall()`, `.fetchdf()`, `.fetchnumpy()`, `.arrow()`, iteration. If we deregister after `.execute()` returns, the monitor will show queries for milliseconds even when the user is waiting 30s on `.fetchdf()`.

The proxy therefore wraps the *result* too and defers deregistration to the result's terminal call or its destructor:

```python
class InstrumentedDuckDBConnection:
    _EXEC_METHODS = ("execute", "executemany", "sql", "query")

    def __init__(self, raw_con, service_id: str):
        self._con = raw_con
        self._service_id = service_id

    def execute(self, query, *args, **kwargs):
        qid = query_registry.register("DuckDB", query, self._service_id, self._con)
        structlog.contextvars.bind_contextvars(query_id=qid)
        try:
            result = self._con.execute(query, *args, **kwargs)
        except BaseException as e:
            query_registry.deregister(qid, error=e)
            structlog.contextvars.unbind_contextvars("query_id")
            raise
        return _InstrumentedResult(result, qid)

    # Same wrapper for sql/query/executemany — kept short here.

    def __getattr__(self, name):
        # Delegate everything else (close, register, unregister, list_filesystems, …)
        return getattr(self._con, name)


class _InstrumentedResult:
    """Thin proxy over DuckDBPyRelation that deregisters on terminal fetch
    or on garbage collection — whichever comes first. Captures exceptions
    from the terminal call so the registry's completed-history records
    `outcome="error"` with the exception type/message."""

    _TERMINAL_METHODS = ("fetchall", "fetchone", "fetchmany", "fetchnumpy",
                         "fetchdf", "fetch_df", "df", "arrow", "fetch_arrow_table",
                         "pl", "fetch_record_batch", "close")

    def __init__(self, raw, qid: int):
        self._raw = raw
        self._qid = qid
        self._done = False

    def _finish(self, error: BaseException | None = None) -> None:
        if not self._done:
            self._done = True
            query_registry.deregister(self._qid, error=error)
            structlog.contextvars.unbind_contextvars("query_id")

    def __getattr__(self, name):
        attr = getattr(self._raw, name)
        if name in self._TERMINAL_METHODS and callable(attr):
            def wrapper(*a, **kw):
                err: BaseException | None = None
                try:
                    return attr(*a, **kw)
                except BaseException as e:
                    err = e
                    raise
                finally:
                    self._finish(err)
            return wrapper
        return attr

    def __iter__(self):
        err: BaseException | None = None
        try:
            yield from iter(self._raw)
        except BaseException as e:
            err = e
            raise
        finally:
            self._finish(err)

    def __del__(self):
        # Safety net for callers that never call a terminal method.
        self._finish()
```

**`.arrow()` caveat from this codebase.** [iceberg/buffer.py:647](backend/core/iceberg/buffer.py#L647) notes that DuckDB 1.5's `.arrow()` "returns a RecordBatchReader" and the comment recommends `fetch_arrow_table()` for materialization. Our `_TERMINAL_METHODS` includes both; on `.arrow()` we deregister at the call boundary, which may be slightly early if the caller iterates the reader lazily. Mitigation: a v1 test against the real `iceberg/buffer.py` call path that asserts the registry's `duration_ms` for that query is within an order of magnitude of measured wall-clock. If it's not, we wrap the returned reader too.

**Caveats called out explicitly:**

1. `__getattr__` proxies break `isinstance(con, duckdb.DuckDBPyConnection)` checks. We grep the codebase for these before merging; if any exist, switch them to duck-typing or expose a `.raw` accessor.
2. `__del__` is best-effort but reliable here because the wrapper lives only inside the request's `with checkout_connection(...) as con:` scope — CPython reference counting will fire `__del__` deterministically when the result goes out of scope.
3. DuckDB's `con.register("name", df)` / `con.unregister("name")` and the relational-API chains pass through `__getattr__` unchanged. They don't execute SQL until materialized, so we only track when materialization hits the terminal methods.

---

## 4. Query Attribution Discovery

Every active query row must answer two questions: **who triggered it** (principal) and **what code is running it** (call site). Both are captured at register time and stored as structured fields on `ActiveQuery` (not a single pre-formatted `source` string — the UI composes the display label from the parts).

### Structured attribution schema

```python
@dataclass(slots=True)
class Attribution:
    # WHO — exactly one of these is populated
    kind: str               # "analyst" | "admin" | "cron" | "system"
    analyst_id: str | None = None      # passcode hash / session id
    analyst_name: str | None = None    # display name or "Guest"
    admin_id: str | None = None        # admin identity from RemoteAccessMiddleware
    cron_job: str | None = None        # e.g. "sync_svc1", "local_compact", "alerts_sweep"
    cron_run_id: str | None = None     # one cron tick may execute many queries — group them

    # WHAT — always populated
    caller_qualname: str    # e.g. "iceberg.compaction.run_local_compact"
    caller_file: str        # e.g. "backend/core/iceberg/compaction.py:142"
    request_path: str | None = None    # FastAPI route for analyst/admin queries, None for cron
    request_id: str | None = None      # correlation id — pairs with telemetry / logs
```

### Resolution priority (first match wins)

1. **Analyst request** — `RequestContext.analyst_session` is set:
   - `kind="analyst"`, `analyst_id` from session, `analyst_name` from session display name (falls back to `"Guest (passcode …{last4})"` if no name).
   - `request_path` from `RequestContext.request_path`, `request_id` from telemetry root span.
2. **Admin request** — request is present but `analyst_session is None` (the admin auth path through `RemoteAccessMiddleware`):
   - `kind="admin"`, `admin_id` from the middleware-stamped identity (IP + auth method if no identity is available).
   - `request_path`, `request_id` as above.
3. **Cron** — `process_context` ContextVar is set by `process_context_scope` ([main.py:82](backend/main.py#L82)):
   - `kind="cron"`, `cron_job` from the context tag (e.g. `"sync_svc1"`, `"local_compact:fastly_prod"`, `"alerts:eval"`), `cron_run_id` is the scope's unique id so an admin can see "all 47 queries from this sync tick" grouped.
4. **System fallback** — none of the above (pool warmer, schema migration, boot-time work):
   - `kind="system"`, `cron_job=None`. The thread name (e.g. `"pool-warmer"`) is folded into `caller_qualname`.

### Capturing the call site

The "what code is running it" part is the new requirement. At register time, walk the Python stack to find the first frame **outside the instrumentation layer and outside the DB driver**:

```python
import sys

_INSTRUMENTATION_PREFIXES = (
    "backend/core/query_registry",
    "backend/utils/sqlite_profiler",
    "backend/core/duckdb_pool",   # InstrumentedDuckDBConnection wrapper lives here
)

def _capture_caller() -> tuple[str, str]:
    """Walk up the stack and return (qualname, 'file:line') of the first
    application frame outside the instrumentation/driver layer."""
    frame = sys._getframe(2)  # skip _capture_caller + register()
    while frame is not None:
        path = frame.f_code.co_filename
        if not any(p in path for p in _INSTRUMENTATION_PREFIXES):
            qual = frame.f_code.co_qualname  # 3.11+: "ClassName.method"
            # Trim absolute project prefix for display
            rel = path.split("backend/", 1)
            display_path = ("backend/" + rel[1]) if len(rel) == 2 else path
            return qual, f"{display_path}:{frame.f_lineno}"
        frame = frame.f_back
    return "<unknown>", "<unknown>"
```

Cost: ~5–10μs per query (Python frame walks are cheap; we stop at the first match). Acceptable because the registry-write path is already in the same order of magnitude.

### Examples of what an admin sees

| `kind` | Display label | Example |
| --- | --- | --- |
| `analyst` | `Analyst: <name> — <route>` | `Analyst: Drew Michael — POST /api/query` |
| `analyst` | `Analyst: Guest (…a3f1) — <route>` | `Analyst: Guest (…a3f1) — GET /api/dashboard/summary` |
| `admin` | `Admin: <id> — <route>` | `Admin: ops@anthropic.com — POST /api/admin/ingest-logs` |
| `cron` | `Cron: <job> (run <run_id>)` | `Cron: sync_svc1 (run 7f3a)` |
| `system` | `System: <qualname>` | `System: duckdb_pool._Pool.warm_idle` |

In every case the **caller line** (`caller_file`) is shown as a subtitle and as a clickable `file:line` reference in the row's expanded drawer — so an admin who sees a runaway query knows exactly which function to investigate without having to grep for the SQL text.

### One ContextVar, set at every entrypoint

Rather than the registry hunting through three separate context sources at register time (a hot-path cost paid on every query), define a single `current_attribution: ContextVar[Attribution | None]` and set it at the entrypoints that already exist:

- `RequestContext` construction in [request_context.py:140](backend/core/request_context.py#L140) sets the analyst/admin attribution.
- `process_context_scope` in [main.py:82](backend/main.py#L82) sets the cron attribution.
- Boot-time and pool-warmer paths leave it `None`; the registry then synthesizes a `system` attribution from the thread name + caller frame.

Registration becomes a single ContextVar `.get()` plus the stack walk — no branching on "is there a request? is there a cron context?". This matches the pattern already used by `backend.utils.telemetry` and avoids a second source of truth.

---

## 5. Live Communication & Update Mechanism

### Polling via TanStack Query with adaptive interval and incremental fetch

We use HTTP polling, not SSE/WebSockets. The reason is operational simplicity and that the `since_seq` incremental pattern already works in this codebase ([sqlite_profiler.py:160](backend/utils/sqlite_profiler.py#L160)) — not because SSE leaks sockets (modern uvicorn handles SSE fine via `StreamingResponse`).

**Interval policy:**

| State | Interval |
| --- | --- |
| Tab visible, queries active | **1000 ms** |
| Tab visible, no active queries | **2000 ms** |
| Tab hidden / minimized | paused (TanStack Query default `refetchIntervalInBackground: false`) |

The "no active queries" backoff matters: with N admin tabs the load multiplies. 500ms polling (the earlier draft) is too aggressive once you have two ops people looking at the page.

```tsx
const visible = useDocumentVisibility();   // small hook around `document.visibilityState`
const { data } = useQuery({
  queryKey: ["active-queries", sinceSeq],
  queryFn:  () => fetch(`/api/admin/queries?since_seq=${sinceSeq}`).then(r => r.json()),
  enabled:  visible,
  refetchInterval: (q) =>
    (q.state.data?.queries?.length ?? 0) > 0 ? 1000 : 2000,
  refetchIntervalInBackground: false,
});
```

**Incremental responses:** the endpoint accepts `?since_seq=N` and returns only rows with a higher `query_id` than the client's last-seen sequence. Steady-state empty responses are ~30 bytes. Full SQL text is fetched on demand via the per-query endpoint (see §6) so the poll payload stays small.

**When polling is the wrong choice:** if multiple admins regularly leave the page open and we see the endpoint dominating ops CPU, switch to SSE via `StreamingResponse`. That decision is data-driven, not architectural.

---

## 6. Admin API Specifications

**Authorization.** All endpoints below sit behind `RemoteAccessMiddleware`, which structurally enforces admin-only access on `/api/admin/*` and returns 403 to any analyst session ([remote_access.py:735](backend/utils/remote_access.py#L735)). A new router `backend/routers/admin_queries.py` registered under `/api/admin` inherits this gating — no per-route auth code needed. The earlier draft's `access_level == "read_write"` check was wrong; that's the per-service *source* access level, not user authorization.

**Feature flag.** Endpoints return `404` (not `503`) when `QUERY_MONITOR_ENABLED=0` so the frontend's nav-gating call (`/api/admin/app-config`) sees the feature as absent. Default is `true`. Pattern follows [settings.py:79](backend/core/settings.py#L79):

```python
query_monitor_enabled: bool = Field(default=True, alias="QUERY_MONITOR_ENABLED")
```

**Rate limit.** The cancel endpoint is capped at 10/sec per admin (token-bucket via [bounded_cache](backend/utils/bounded_cache.py)). Above the cap → `429` with `Retry-After`. Logged at WARN.

### A. Fetch active queries (summary, incremental)

- **Endpoint:** `GET /api/admin/queries?since_seq={int}`
- **Response:**
  ```json
  {
    "last_seq": 18421,
    "queries": [
      {
        "query_id": 18420,
        "db_type": "DuckDB",
        "sql_preview": "SELECT country, count(*) FROM logs WHERE …",
        "sql_len": 312,
        "attribution": {
          "kind": "analyst",
          "label": "Analyst: Drew Michael — POST /api/query",
          "principal_id": "passcode_a3f1",
          "caller_qualname": "routers.query.run_query",
          "caller_file": "backend/routers/query.py:88",
          "request_path": "/api/query",
          "request_id": "req_01HV…",
          "cron_job": null,
          "cron_run_id": null
        },
        "service_id": "fastly_prod",
        "started_at_utc": 1718134700.0,
        "duration_ms": 1420.5,
        "cancellable": true,
        "cancelled_at": null
      }
    ]
  }
  ```
- Notes: `sql_preview` is the first 200 chars; `sql_len` lets the UI show "…(312 chars, click to expand)". A cancelled row stays in the response for ~1s with `cancelled_at` set so the UI can animate it out without losing the row mid-render.

### B. Fetch full SQL for one query

- **Endpoint:** `GET /api/admin/queries/{query_id}`
- **Response:** the same row shape as above, plus `"sql": "<full text>"`.
- Reason this is a separate call: full SQL can be multi-KB and we don't want it on every 1s poll.

### C. Cancel a query (idempotent, structured result)

- **Endpoint:** `POST /api/admin/queries/{query_id}/cancel`
- **Response:**
  ```json
  { "state": "cancelled", "query_id": 18420 }
  ```
- `state` is one of `"cancelled"`, `"not_found"`, `"already_finished"`, `"connection_gone"`. Admins re-click; the endpoint always returns 200 with a state field rather than throwing 404 on second click.

### D. Pool / counts summary (cheap header)

- **Endpoint:** `GET /api/admin/queries/summary`
- **Response:**
  ```json
  { "active_total": 3, "by_db_type": {"DuckDB": 2, "SQLite": 1}, "longest_ms": 1420.5 }
  ```
- The tab badge polls this independently at 2s so the badge updates even if the user is on a different tab inside the page. Costs ~50μs to compute.

---

## 7. Frontend Layout Design (Admin Page)

**Lives under `/admin`, NOT `/query`.** The `/query` page is shared between analysts and admins. The Live Query Monitor is admin-only — placing the tab on a shared route would either expose forbidden UI to analysts (an empty tab they can't open) or leak the existence of the feature. The monitor mounts as a new section on the existing admin page (`/frontend/app/admin/page.tsx`) or a dedicated `/admin/queries` route — whichever fits the existing admin nav. The component itself is conditionally rendered behind:

```tsx
const { isAdmin } = useAuthContext();   // analyst_session === null
const { data: cfg } = useQuery({ queryKey: ["app-config"], queryFn: getAppConfig });
if (!isAdmin || !cfg?.query_monitor_enabled) return null;
```

So the page won't even appear in the nav for analysts, and the kill-switch env var hides it cleanly in environments where it's disabled.

```
┌─────────────────────────────────────────────────────────────┐
│  [ Query Editor ]   [ Live Query Monitor (Active: 2) ]       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  DuckDB Running Queries          SQLite Running Queries     │
│  ┌─────────────────────────────┐ ┌────────────────────────┐ │
│  │ Source | Duration | Actions │ │ Source | Duration      │ │
│  ├────────┼──────────┼─────────┤ ├────────┼────────────────┤ │
│  │ Drew M.│ 1.4s     │ [ Kill ]│ │ Sync   │ 0.1s           │ │
│  │ Cron   │ 0.4s     │ [ Kill ]│ │ Alerts │ 0.05s          │ │
│  └────────┴──────────┴─────────┘ └────────┴────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### UI elements

- **Tab badge.** Count of running queries (e.g. `Live Query Monitor (3)`). Pulse animation when any query exceeds the slow threshold — **configurable per env** (default 2s prod, 10s dev, because the local sandbox routinely runs longer queries against fresh buffers).
- **Row columns:** `Source` (composed `attribution.label`), `Caller` (`caller_file:line` as a clickable VS Code link), `Service`, `Pool slot`, `Duration`, `Actions`.
- **Duration color coding:** green `< 500 ms`, yellow `< 2 s`, orange `< 10 s`, red `≥ 10 s`. Quick visual scan replaces sorting in most cases.
- **Expanded drawer.** Clicking a row opens a drawer with the formatted SQL (fetched lazily via `GET /api/admin/queries/{id}`), the full attribution block, the `request_id` for log correlation, and a copy-as-curl helper.
- **Kill button — kind-aware confirmation.**
  - `analyst` / `admin`: 1-click (re-runnable; no side effects).
  - `cron`: 2-click confirm with the row's identity shown ("Cancel Cron sync_svc1 — running 1.4s? Next tick will reconcile.").
  - `system`: hidden behind an "Advanced" toggle; default not killable.
- **Optimistic UI on cancel.** Row dims immediately, action label flips to "Cancelling…", next poll surfaces `cancelled_at` and the row animates out after ~1s. `state: "already_finished"` → silent disappear, no error toast.
- **Grouping toggle.** Group by `attribution.kind` (Analyst / Admin / Cron / System) or by `service_id`. Cron rows additionally group by `cron_run_id` so an admin can see "all 47 queries from sync tick 7f3a" as one collapsible block — keeps a hot cron from drowning out the analyst query you're investigating.
- **Completed-history strip.** A collapsed strip below the active list shows the last 30s of completed queries (from `?include=completed`). Errored rows in red with the exception type inline; cancelled rows in gray. Auto-collapses to "Show last 200" when empty.
- **Filter + search.** Filter chips per `kind`; one-line search box that filters on SQL substring OR `caller_qualname`. Filters persist in URL params so an ops link "look at all cron rows" is shareable.
- **Empty state.** "No active queries. Long-running queries will appear here in real time." Plus a tiny pulse dot proving the poller is alive.
- **Stable row keys** (`query_id`) so reorder animations don't tear during fast polls.
- **Auto-scroll suppression.** If the user has scrolled away from the top, new rows don't yank the viewport.
- **Polling state indicator.** Tiny corner status: `Live` (green dot), `Paused (tab hidden)`, `Error — retrying`.
- **Keyboard shortcuts.** `/` focuses the search box, `Esc` closes the drawer, `k` opens the kill confirm on the focused row. Shortcuts list in a `?` overlay.
- **ARIA live region** on the active count so screen readers announce changes without polling chatter.
- **Sound notification (off by default).** Optional, opt-in toggle for ops who leave the page open while monitoring an incident.

---

## 8. Data Hygiene and Security

- **SQL truncation.** 4KB cap mirrors [sqlite_profiler.py:73-78](backend/utils/sqlite_profiler.py#L73-L78). Defends against pathological `IN(...)` expansions and analyst-pasted multi-megabyte queries.
- **PII in SQL text.** The analyst query editor submits literal SQL (not bound params), so column values can appear in the text — emails, tokens, IP addresses. This is acceptable because the endpoint is admin-only, but call out in the UI: a small "contains literal values" notice on the drawer.
- **Passcode handling in attribution.** `Attribution.analyst_id` stores the passcode hash, never the raw passcode. The display label uses `…{last4}` of the hash for unnamed guests.
- **No parameter values captured.** Mirrors the existing profiler's contract ([sqlite_profiler.py:81-91](backend/utils/sqlite_profiler.py#L81-L91)) — we record only param *shape* (`seq[5]`, `map[3]`), not values.
- **Best-effort isolation.** Registry exceptions must never propagate into the SQL path. Every register/deregister/cancel call is wrapped at the registry boundary with `try/except Exception: logger.debug(..., exc_info=True)`. Instrumentation is observability, not control flow.

---

## 9. Test Plan

Tests live under `backend/tests/test_query_registry.py` and `backend/tests/test_query_instrumentation.py`.

### Registry unit tests

- `register` returns monotonic sequence; `deregister` is idempotent.
- Concurrent `register`/`deregister` from 32 threads (1k iterations each) leaves the registry empty and `_conn_to_query` empty.
- `cancel_query` returns `"not_found"` for unknown id, `"already_finished"` after deregister, `"cancelled"` on success.
- `cancel_query` refuses to interrupt when `_conn_to_query[con_id] != qid` — simulated by overwriting the stamp mid-test. This is the "do not kill the wrong query" invariant.
- Weakref behavior: closing the connection between register and cancel returns `"connection_gone"`, not a crash.

### Attribution

- ContextVar set by `RequestContext` constructor flows into a registered query made on the same thread.
- ContextVar set by `process_context_scope` flows into a registered query made on a thread spawned via `contextvars.copy_context().run(...)` (matches the existing pattern in [admin.py:73-79](backend/routers/admin.py#L73-L79)).
- Stack walk skips `query_registry`, `sqlite_profiler`, and `duckdb_pool` frames and returns the first business-logic frame.

### Instrumentation integration

- SQLite: a query executed through `metadata_db.get_con()` appears in `query_registry.snapshot()` between `execute` start and `fetchall` return.
- DuckDB: a query executed through `checkout_connection` appears in the registry until `.fetchdf()` returns (this is the *result-wrapper deregistration* regression test — without the wrapper, this test would show ~0ms duration for a long-running fetch).
- Discarded pool connection: a query that raises mid-execute removes its registry entry AND its `_conn_to_query` stamp.
- `pool.release(raw_con, ...)` is called with the **raw** connection, not the proxy — `id(raw_con)` lookups in [duckdb_pool.py:121](backend/core/duckdb_pool.py#L121) still hit.

### End-to-end

- Admin endpoint returns the live query while a long DuckDB query runs in another thread; `POST .../cancel` causes `con.interrupt()` and the query thread sees a `duckdb.InterruptedError`.
- Two-admin race: both call cancel; first returns `"cancelled"`, second returns `"already_finished"`. No double-interrupt on the connection.

---

## 10. Implementation Order

Backend first, end-to-end verified on local dev, then frontend.

1. **Settings:** add `QUERY_MONITOR_ENABLED: bool = Field(default=True, alias="QUERY_MONITOR_ENABLED")` in [settings.py](backend/core/settings.py).
2. **Audit logger:** add a small `audit_log = structlog.get_logger("audit")` export in [structlog_config.py](backend/utils/structlog_config.py).
3. `backend/core/query_attribution.py` — `Attribution` dataclass (incl. `pool_slot`), `current_attribution` ContextVar, `_capture_caller`, `display_label()` / `principal_id()` helpers, `Attribution.system()` factory.
4. `backend/core/query_registry.py` — `ActiveQuery`, `CompletedQuery`, `QueryRegistry` with completed-history deque, `_conn_to_query` stamp, error capture, OTel hooks.
5. Wire `current_attribution.set(...)` in `RequestContext` ([request_context.py:140](backend/core/request_context.py#L140)) and `process_context_scope` ([main.py:82](backend/main.py#L82) and friends in [scheduler.py](backend/scheduler.py)).
6. Extend `InstrumentedCursor` in [sqlite_profiler.py](backend/utils/sqlite_profiler.py) — register/deregister + structlog bind + error capture.
7. Add `InstrumentedDuckDBConnection` + `_InstrumentedResult` and wire into `checkout_connection` ([duckdb_pool.py:557](backend/core/duckdb_pool.py#L557)). Confirm zero `isinstance(.., DuckDBPyConnection)` call sites (already verified — only `IOException` is grepped).
8. New router `backend/routers/admin_queries.py` registered under `/api/admin`, with all four endpoints from §6 + rate limit. Surface `query_monitor_enabled` from `/api/admin/app-config` so the frontend can hide the tab when the flag is off.
9. Tests at each step (don't batch — the result-wrapper is the part most likely to regress silently). The pool-reuse race test is mandatory.
10. **Local-dev verification gate** (per CLAUDE.md): start dev (13002/18002), exercise an analyst query + a cron tick + a manual kill, confirm rows + cancellation + audit log + OTel counters before touching the frontend.
11. Frontend tab + adaptive polling + drawer + kind-aware confirm + completed strip + search/filter + keyboard shortcuts.
12. Final local pass: full UX walkthrough (empty state → active row → drawer → kill → completed strip → error row).
13. Commit, push, deploy via `~/restart.sh`, verify on prod.

---

## 11. Open Questions

- **Async / `asyncio.to_thread` boundaries.** Cron jobs that hop threads via `to_thread` copy the ContextVar correctly (Python 3.11+ guarantees this), but verify with a focused test before relying on it for attribution. Same for the FastAPI thread pool.
- **Long-tail iterator paths.** `_InstrumentedResult.__iter__` covers `for row in result:`. If any caller stores the relation, returns it, and iterates later — across the request boundary — `__del__` will fire at GC time, which is fine for the registry but may show stale rows for a few seconds. Grep for places that return a `DuckDBPyRelation` out of `with checkout_connection`.
- **System-attribution `caller_qualname` for `__init__`-time work.** Module import-time queries (schema init) have no useful qualname. We fall back to the module path and a `boot` tag; revisit if the noise dominates.
- **PyPy / non-CPython.** Design relies on CPython's deterministic refcount for `__del__` and on GIL-protected dict atomicity. We're CPython-only. If that ever changes, add an explicit `close()` call in the FastAPI dependency teardown that walks any live `_InstrumentedResult` for the request.

---

## 12. Verified Implementation Facts

Empirically confirmed against the dependencies in this repo before finalizing the plan:

| Assumption | Verified |
| --- | --- |
| `sqlite3.Connection.interrupt()` exists | ✓ (cpython stdlib) |
| `duckdb.DuckDBPyConnection` supports `weakref.ref()` | ✓ (duckdb 1.5.3) |
| `execute()` returns ~instantly; heavy work is in `fetchdf/fetchall/arrow` | ✓ (0.3 ms execute → 1147 ms fetchdf on a 50M-row SELECT) |
| `__getattr__` proxy breaks `isinstance(con, DuckDBPyConnection)` | ✓ — but only one call site uses `isinstance(.., duckdb.*)` in the backend ([duckdb.py:619](backend/core/duckdb.py#L619)) and it tests `IOException`, not the connection class, so the proxy ships unblocked |
| DuckDB 1.5 exposes `current_query()` and `duckdb_memory()` for in-flight introspection | ✓ — both work on a `:memory:` connection |
| Single ContextVar seam exists in `backend.utils.telemetry` and is wired into `process_context_scope` | ✓ ([main.py:88,250,535](backend/main.py)) |

---

## 13. Beyond v1 — What a "Live Debugger" View Should Eventually Include

The plan above is correct for "see + kill". A true live debugger needs more, called out here so it's on the record and so the v1 data shapes don't lock us out of v2 features.

### 13.1 ~~Recently-completed history~~ — **promoted to v1** (see §2)

### 13.2 ~~Errored queries first-class~~ — **promoted to v1** (see §2)

### 13.3 Auto-kill / runaway protection (v1.2, single-screw lever)

Once the registry exists, a single background thread iterating `snapshot()` once per second and calling `cancel_query(qid)` past a per-attribution-kind threshold is ~30 lines. Defaults: `analyst > 5 min`, `admin > 10 min`, `cron > 30 min` (or `None`), `system > 0` (never auto-kill — there's a reason it's running). Settings via env vars to start; per-service overrides later. This is often the *real* reason a "process list" gets built.

### 13.4 DuckDB progress + memory per row

DuckDB 1.5 has `pragma progress_bar_time = 500` and `duckdb_memory()`. For any DuckDB row whose `duration_ms > 2000`, the snapshot endpoint can opportunistically read `duckdb_memory()` on the same connection (cheap, no lock contention) and surface `memory_mb` and `progress_pct`. This is the single biggest UX upgrade for the live view — turns "DuckDB query running for 47s, who knows what it's doing" into "DuckDB query at 64% complete, holding 1.2 GB". Implementation requires a side-channel call on the connection while the main query is mid-execute — DuckDB supports this (different statement on a different cursor) but we test it explicitly before relying on it.

### 13.5 ~~Audit log of cancellations~~ — **promoted to v1** (see §2, §6)

### 13.6 ~~OTel metrics~~ — **promoted to v1** (see §2)

### 13.7 ~~Kind-aware kill confirmation~~ — **promoted to v1** (see §7)

### 13.8 ~~Structlog correlation~~ — **promoted to v1** (see §3)

### 13.9 ~~Pool-slot in attribution~~ — **promoted to v1** (see §4)

### 13.10 The pool-reuse-race test, explicitly

The single most-important invariant — "do not kill the wrong query when a connection is reused" — needs a literal-scenario test, not just the stamp-overwrite unit test in §9:

```python
# pseudocode
conn = pool.acquire(src)
qid1 = registry.register("DuckDB", "SELECT 1", svc, conn)
registry.deregister(qid1)
qid2 = registry.register("DuckDB", "SELECT 2", svc, conn)  # same physical conn
# Admin (slow click) targets qid1:
assert registry.cancel_query(qid1) == "already_finished"
# Confirm qid2 is untouched:
assert qid2 in registry._queries
```

This is the regression test that pays for the entire `_conn_to_query` stamp design.

### 13.11 Streaming Arrow / `fetch_record_batch` iteration

`_InstrumentedResult.__iter__` covers `for row in rel`. The Arrow batch iterator (`for batch in rel.fetch_record_batch():`) returns its own iterator object — verify `__del__` on the outer result still fires when the batch iterator is exhausted, or wrap the batch iterator too. Test before merge.

### 13.12 Persistence across restart

The registry dies with the process — a long DuckDB query running during a deploy disappears from history. Acceptable for v1 (the query itself is killed by SIGTERM anyway). If we add the completed-history deque, periodically flush to a small SQLite table so post-restart investigation works. Defer to v2.

### 13.13 ~~Rate-limit cancel endpoint~~ — **promoted to v1** (see §6)

### 13.14 ~~Peer-count grouping~~ — **promoted to v1** (see §7)

---

## 14. Best-Practice Checklist — v1 SHIPPED 2026-06-11

All items below shipped in commits `a0419db` (backend), `42262ea` (frontend),
`bd4e290` (pydantic-settings dep), `a834d99` (300ms polling + just-finished
promotion), `919fea9` (active row visuals + slow-queries panel), `80c78a0`
(view-mode tabs + registry kill switch).

- [x] Result-wrapper covers `fetchall/fetchone/fetchmany/fetchdf/fetch_df/df/arrow/fetch_arrow_table/pl/fetch_record_batch/close` AND `__iter__` AND `__del__`. (`backend/core/query_instrumentation.py`)
- [x] `_conn_to_query` stamp validated under lock before every `interrupt()`. (`backend/core/query_registry.py` — pool-reuse race test in `tests/core/test_query_registry.py::TestCancel::test_pool_reuse_race_does_not_kill_wrong_query`)
- [x] Pool sees raw connections only — proxy unwrap happens inside `checkout_connection`. (`backend/core/duckdb_pool.py:557`)
- [x] Instrumentation exceptions never propagate into the SQL path.
- [x] SQL truncated to 4KB at register time; full text fetched lazily via `GET /api/admin/queries/{qid}`.
- [x] Both DuckDB AND SQLite reported as `cancellable: true` (sqlite3.Connection.interrupt() does exist).
- [x] Cancel returns structured state, never throws.
- [x] Admin endpoints behind `RemoteAccessMiddleware` on the `/api/admin/*` prefix.
- [x] Errored queries captured in the completed-history deque with `error_type` + truncated `error_message`.
- [x] Audit log line on every successful cancel (`audit_log` in `backend/utils/structlog_config.py`).
- [x] OTel counters/histograms emitted: `app.active_queries.count`, `app.query_duration_ms`, `app.queries_cancelled_total`.
- [x] Pool-reuse race test landed green.
- [x] No new third-party dependency surfaced to the runtime — `pydantic-settings` was already transitive; we promoted it to a direct dep when the live-monitor router made the import load-bearing (commit `bd4e290`).

---

## 15. What ALSO shipped beyond the original v1 spec

Polishing the page in front of a real operator surfaced things the spec
didn't anticipate. All of these landed today:

- **View-mode tabs** (commit `80c78a0`). Top-level `All / Live only / Past only` toggle. "Live only" hides the past sections for a focused real-time view; "Past only" hides the active section for post-mortem work.
- **Notable Slow Queries panel** (commit `919fea9`). Filters the completed-history ring buffer by a chosen threshold (100ms / 500ms / 1s / 2s / 5s), sorted slowest first. Answers "what was running when the dashboard got slow a minute ago" without scrolling the full completed list.
- **Active rows visually distinct** (commit `919fea9`). Tinted background + left accent border + pulsing dot next to duration. Faded just-finished rows for clear contrast.
- **Adaptive polling at 300 ms** (commit `a834d99`). The original 1-2s polling was useless for typical traffic — empirical measurement on prod showed p50 query duration is 0.2ms, max 29ms across 200 samples. Dropped to 300ms so real activity appears.
- **"Just-finished" promotion into Active** (commit `a834d99`, window bumped to 10s in `919fea9`). Anything that completed in the last 10s appears in the Active section as a faded row with its outcome badge. Without this the Active list reads empty on typical traffic regardless of polling rate.
- **Registry kill switch** (commit `80c78a0`). `QUERY_REGISTRY_DISABLED=1` env makes `register()` return immediately for zero hot-path overhead. Defensive lever for future incidents (measured overhead is ~21µs/query, but the switch costs nothing to ship).

What's STILL deferred (genuine v2 work, see §13):
- Auto-kill / runaway protection per attribution kind
- DuckDB per-row progress + memory via `duckdb_memory()` and `pragma progress_bar_time`
- Persistence across restart for the completed-history deque
- Verify `.arrow()` lazy-reader semantics against `iceberg/buffer.py:647` (works in tests but the call-site comment hinted at quirks)
- Explicit `cron_run_id` grouping in the UI (today we expose the field and the filter chips work, but there's no collapsible group-by render)
- Keyboard shortcuts, ARIA live regions, sound notifications, URL-persisted filters — UX polish from the original §7 list that wasn't material for the operator workflow today

---

## 16. v2 SHIPPED 2026-06-12 — closing the deferred-list

Everything from the §15 "STILL deferred" list except the three items below is
now shipped in a single commit (`613605c` —
`feat(live-monitor): peak memory column, keyboard shortcuts, URL-persisted filters`).

### Backend

- **Peak memory at completion** ([backend/core/query_instrumentation.py](backend/core/query_instrumentation.py),
  [backend/core/query_registry.py](backend/core/query_registry.py)).
  At deregister time, `_probe_duckdb_memory()` opens a fresh cursor on the
  now-idle connection and runs
  `SELECT sum(memory_usage_bytes) + sum(temporary_storage_bytes) FROM duckdb_memory()`.
  Result lands as `CompletedQuery.peak_memory_mb` and surfaces in the
  snapshot endpoint and the Completed table. SQLite rows stay `None`.
  Wired in the `_InstrumentedResult._finish` path AND the
  `InstrumentedDuckDBConnection.execute` error path.
- **`.arrow()` / `fetch_record_batch` lazy-reader wrapper**
  ([backend/core/query_instrumentation.py](backend/core/query_instrumentation.py)).
  New `_InstrumentedRecordReader` proxies `pyarrow.RecordBatchReader` so
  deregistration waits for iteration to complete instead of firing at the
  call site. Closes §13.11 — verified by a new regression test that drives
  `.arrow()` through a 500k-row stream with sleeps between batches and
  asserts the row stays active throughout. The §15 "verify `.arrow()`"
  follow-up is therefore both verified AND defensively wrapped.
- **Tests added** ([tests/core/test_query_registry.py](tests/core/test_query_registry.py)):
  `_parse_memory_mb` parser (ints, binary + decimal suffixes, garbage),
  peak-memory probe success + error swallowing, completed row carries
  `peak_memory_mb`, SQLite stays null, reader-iteration holds registration,
  `to_arrow_table` materialises immediately, `reader.close()` completes,
  reader schema pass-through. 11 new tests, 37 total green.

### Frontend (all in [frontend/app/admin/queries/](frontend/app/admin/queries/))

- **`cron_run_id` collapsible grouping** ([_sections/ActiveTable.tsx](frontend/app/admin/queries/_sections/ActiveTable.tsx)).
  New "Group runs" toggle next to the kind chips. When on, cron rows
  bucket by `cron_run_id` (null → "Ungrouped cron"); each bucket is a
  collapsible block headed by `Cron: {job} (run {short_id}) — N queries,
  oldest {duration}`. Non-cron rows stay inline.
- **Keyboard shortcuts** ([_hooks/useKeyboardShortcuts.ts](frontend/app/admin/queries/_hooks/useKeyboardShortcuts.ts),
  [_sections/ShortcutsHelp.tsx](frontend/app/admin/queries/_sections/ShortcutsHelp.tsx)).
  `/` focus search, `j`/`k` row nav, `Enter` expand, `x` cancel focused,
  `Esc` close (drawer → confirm dialog → help → blur), `?` help overlay.
  Focused row gets a visible ring. Help dialog accessible via the
  keyboard icon in the page header for discoverability.
- **ARIA live region** ([_sections/SummaryStrip.tsx](frontend/app/admin/queries/_sections/SummaryStrip.tsx)).
  `<div role="status" aria-live="polite" class="sr-only">` announces
  the active count to screen readers. Memoised on the count itself so
  the 300ms poll doesn't chatter announcements every tick.
- **Opt-in sound notification** ([page.tsx](frontend/app/admin/queries/page.tsx)).
  Visible speaker toggle in the page header; `localStorage` persists the
  preference. Fires on the first poll where a new `outcome === 'error'`
  appears in completed, via Web Audio (~200ms two-tone ping). No audio
  asset shipped. Doesn't beep retroactively for errors that existed
  before the user enabled sound.
- **Memory column** ([_sections/CompletedTable.tsx](frontend/app/admin/queries/_sections/CompletedTable.tsx)).
  Renders only when at least one visible row has `peak_memory_mb !== null`,
  so an all-SQLite view collapses the column out entirely.
- **URL-persisted filter state** ([page.tsx](frontend/app/admin/queries/page.tsx)).
  `search → ?q`, `kindFilter → ?kind`, `viewMode → ?view`,
  `slowThresholdMs → ?slow`, `groupByRun → ?group=run`. Hydrate-once
  pattern with a `hydratedRef` guard; writes via `window.history.replaceState`
  to avoid Next's router refresh (mirrors [useFilterUrlSync.ts](frontend/hooks/useFilterUrlSync.ts)).
  Default values omitted from the URL so clean views stay clean.

### Surprise during verification

- **`?` shortcut layout-quirk.** During the browser smoke-test the `?`
  shortcut didn't fire. Cause: Playwright (and some non-US keyboard
  layouts on older Chromium) report Shift+/ as `event.key === '/'` with
  `shiftKey === true`, NOT as `event.key === '?'`. Real macOS Chrome on
  US QWERTY reports `'?'` directly, which is why the bug didn't surface
  in earlier manual testing. Fix: `logicalKey()` normalizer in
  [useKeyboardShortcuts.ts](frontend/app/admin/queries/_hooks/useKeyboardShortcuts.ts)
  promotes Shift + `/` (or `event.code === 'Slash'`) to `'?'` before
  binding lookup. Regression-tested via a vitest case that fires
  `KeyboardEvent({ key: '/', shiftKey: true, code: 'Slash' })` and
  asserts the `?` handler runs.

### What stays deferred (still v2.5 / v3 work)

These three items are explicitly out of scope per user decision (see
plan `/Users/drew.michael/.claude/plans/goofy-questing-ember.md` — context
section):

- **Auto-kill / runaway protection.** Admins manually cancel; no
  automated killing.
- **Disk persistence of completed-history.** The live query page must
  not require a database; the in-memory ring buffer is the contract.
- **In-flight DuckDB progress probing.** Too much concurrency risk for
  the marginal UX win when the deregister-time probe gives a usable
  number on most queries.

These are documented here so a future reader sees they were considered
and consciously left out, not forgotten.
