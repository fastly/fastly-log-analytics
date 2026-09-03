# Slow-Query Flush Resource-Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Return PostgreSQL metadata connections acquired by short-lived slow-query flush workers before those workers exit.

**Architecture:** Keep `get_con()`'s existing long-lived thread-local contract for request and reused cron threads. Add a `finally` release at the short-lived `_flush_all()` worker boundary, using the existing `release_thread_connection()` helper so only the current worker's connection is returned; preserve best-effort flush behavior and SQLite no-op semantics.

**Tech Stack:** Python 3.12+, FastAPI backend, SQLite/PostgreSQL metadata backends, pytest, `uv`, ruff, mypy.

## Global Constraints

- Do not modify or revert the user's existing uncommitted changes in `backend/core/metadata/base.py`, `backend/core/metadata/pg_connection.py`, `backend/cron/decorators.py`, or their tests.
- Do not change `get_con()` or the long-lived connection behavior used by normal request and cron worker threads.
- Preserve `_flush_all()`'s current best-effort behavior: database failures remain swallowed after the release cleanup runs.
- `release_thread_connection()` must be called from the same short-lived worker thread that acquired the connection.
- SQLite behavior must remain unchanged because release is a no-op when `METADATA_DSN` is unset.
- Use `uv run pytest`, not bare `pytest`.
- Run `make ci` after the implementation, as required by `AGENTS.md`.

---

## File Map

- Modify: `backend/core/metadata/slow_queries.py` — release the calling thread's metadata connection after each service flush.
- Modify: `tests/core/test_slow_queries_optimizations.py` — regression coverage for cleanup on success, failure, short-lived worker execution, and SQLite mode.
- Do not modify: existing PostgreSQL connection or cron heartbeat changes; they provide the release helper this plan consumes.

### Task 1: Add failing lifecycle regression tests

**Files:**
- Modify: `tests/core/test_slow_queries_optimizations.py` (add `threading`,
  `MagicMock`, and `backend.core.metadata.slow_queries` imports alongside the
  existing imports)

**Interfaces:**
- Consumes: `backend.core.metadata.slow_queries._flush_all`, `_buffer`, and `_buffer_lock`.
- Produces: tests that require `_flush_all()` to call `backend.core.metadata.base.release_thread_connection()` once per service flush, including failure paths.

- [ ] **Step 1: Inspect the existing slow-query test fixtures and imports**

Run:

```bash
sed -n '1,260p' tests/core/test_slow_queries_optimizations.py
```

Use the existing test reset/fixture conventions in that file. Do not add a second global buffer-reset mechanism if one already exists.

- [ ] **Step 2: Add the success-path release test**

Patch the slow-query module's `get_con` with a fake connection and patch
`release_thread_connection`. Seed one service in `_buffer`, call `_flush_all()`,
and assert the connection executed `executemany()` and `commit()`, while the
release helper was called exactly once.

The test should follow this shape:

```python
def test_flush_releases_postgres_thread_connection_after_success(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release)

    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-flush"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    con.executemany.assert_called_once()
    con.commit.assert_called_once_with()
    release.assert_called_once_with()
```

- [ ] **Step 3: Add the failure-path release test**

Configure `executemany()` to raise a `RuntimeError`, call `_flush_all()`, and
assert the exception remains swallowed while `release_thread_connection()` is
still called once. This pins cleanup independently from persistence success.

```python
def test_flush_releases_connection_when_write_fails(monkeypatch):
    con = MagicMock()
    con.executemany.side_effect = RuntimeError("metadata unavailable")
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release)

    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-flush-error"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()
```

- [ ] **Step 4: Add the short-lived worker test**

Run `_flush_all()` inside a new `threading.Thread`, patch the release helper to
record the calling thread, join the thread, and assert release happened on the
worker rather than the main test thread. This guards against placing cleanup
outside the worker boundary.

```python
def test_flush_releases_on_the_short_lived_worker_thread(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    released_on = []

    def release():
        released_on.append(threading.current_thread())

    monkeypatch.setattr(slow_queries, "release_thread_connection", release)
    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-thread"] = [{"query_id": "q1"}]

    worker = threading.Thread(target=slow_queries._flush_all)
    worker.start()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert released_on == [worker]
```

- [ ] **Step 5: Add the no-connection compatibility test**

Patch `slow_queries.get_con` to raise before acquiring a connection and patch
the release helper. Call `_flush_all()` and assert the best-effort path does not
raise while the cleanup helper is still safely invoked. The helper itself is
responsible for determining that there is no PostgreSQL connection to return.

```python
def test_flush_without_connection_keeps_best_effort_behavior(monkeypatch):
    monkeypatch.setattr(
        slow_queries,
        "get_con",
        MagicMock(side_effect=RuntimeError("metadata unavailable")),
    )
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release)
    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-no-connection"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()
```

The helper is intentionally safe/no-op when no connection exists. This test
asserts `_flush_all()` preserves its current swallowed-error behavior and
always reaches the cleanup seam. The existing
`test_release_thread_connection_noop_when_dsn_unset` test in
`tests/core/test_metadata_base_pg_routing.py` covers the helper's SQLite
behavior directly.

- [ ] **Step 6: Run the new tests and confirm they fail before implementation**

Run:

```bash
uv run pytest tests/core/test_slow_queries_optimizations.py -q
```

Expected: the new release assertions fail because `_flush_all()` currently has
no `release_thread_connection()` call; unrelated existing tests must remain
green.

- [ ] **Step 7: Commit the failing tests**

```bash
git add tests/core/test_slow_queries_optimizations.py
git commit -m "test: pin slow-query flush connection cleanup"
```

### Task 2: Implement short-lived flush cleanup

**Files:**
- Modify: `backend/core/metadata/slow_queries.py`

**Interfaces:**
- Consumes: `get_con(service_id)` and `release_thread_connection()` from `backend.core.metadata.base`.
- Produces: `_flush_all()` that invokes `release_thread_connection()` after every attempted per-service flush.

- [ ] **Step 1: Import the existing release helper at module scope**

Change the existing import:

```python
from backend.core.metadata.base import get_con, release_thread_connection
```

Keep imports at module scope to avoid the documented conditional-import
`UnboundLocalError` trap.

- [ ] **Step 2: Wrap each service flush in `try/finally`**

Change only the per-service block to this shape:

```python
    for service_id, rows in pending.items():
        try:
            con = get_con(service_id)
            con.executemany(_INSERT_SQL, rows)
            con.commit()
        except Exception:
            pass
        finally:
            release_thread_connection()
```

The helper must be in `finally`, outside the existing `except`, so cleanup
also runs when `get_con()` succeeds but `executemany()` or `commit()` fails.
Do not broaden the exception behavior, retry failed writes, or requeue rows in
this change.

- [ ] **Step 3: Run the focused tests**

Run:

```bash
uv run pytest tests/core/test_slow_queries_optimizations.py -q
```

Expected: all slow-query optimization tests pass, including the new lifecycle
regressions.

- [ ] **Step 4: Commit the implementation**

```bash
git add backend/core/metadata/slow_queries.py
git commit -m "fix: release slow-query flush connections"
```

### Task 3: Validate the lifecycle fix and repository gates

**Files:**
- No additional source files.

**Interfaces:**
- Consumes: the implementation and regression tests from Tasks 1–2.
- Produces: verified behavior without changing unrelated worktree files.

- [ ] **Step 1: Run adjacent metadata and pool tests**

Run:

```bash
uv run pytest \
  tests/core/test_slow_queries_optimizations.py \
  tests/core/test_metadata_base_pg_routing.py \
  tests/core/test_pg_connection.py \
  tests/test_scheduler_watchdog.py -q
```

Expected: all selected tests pass. Any failure in the existing uncommitted
PostgreSQL/cron tests must be diagnosed without reverting or rewriting those
changes.

- [ ] **Step 2: Run formatting and static checks for changed files**

Run:

```bash
uv run ruff format backend/core/metadata/slow_queries.py tests/core/test_slow_queries_optimizations.py
uv run ruff check backend/core/metadata/slow_queries.py tests/core/test_slow_queries_optimizations.py
git diff --check HEAD~2..HEAD
```

Expected: formatting and ruff checks pass with no whitespace errors.

- [ ] **Step 3: Run the required full CI gate**

Run:

```bash
make ci
```

Expected: the full repository gate passes. If it reports failures caused by
the pre-existing uncommitted changes, report those separately and do not
alter unrelated files to conceal them.

- [ ] **Step 4: Confirm the final diff is scoped**

Run:

```bash
git status --short
git diff --stat HEAD~2..HEAD
```

Expected: the two implementation commits contain only the slow-query source,
its tests, and their commit metadata. Existing user changes and `.agents/`
remain present and untouched.
