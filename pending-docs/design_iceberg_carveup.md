# iceberg.py Carve-Up Design

## 1. Status / Decision

**Status:** Proposed (refactor/cleanup branch, Phase 4a)
**Decision:** Adopt **Phase 4a: minimal-fs-only carve** as the winning strategy.
**Date:** 2026-06-09

### Rationale

`backend/core/iceberg.py` is 4,232 LOC and carries six load-bearing monkeypatches plus the F3 view-cache wedge contract, the FosSqlCatalog test-fixture identity invariant, the Trap #21 sync_data orphan-cleanup contract, and 275 import sites across `backend/` and `tests/`. Four-lens judge scores (blast radius / monkeypatch fidelity / loc discipline / testability):

| Strategy | Blast | Fidelity | LOC | Testability |
|---|---|---|---|---|
| aggressive-5-modules | 4 | 3 | 6 | 7 |
| staged-2-then-3 | 7 | 9 | 2 | 8 |
| **Phase 4a: minimal-fs-only carve** | **9** | **10** | **1** | **6** |

Phase 4a wins three of four lenses. It is the only proposal that ships zero behavior change: a pure relocation of the self-contained `try: from s3fs import S3FileSystem ... except ImportError` block (current `iceberg.py:46-515`, ~470 LOC) into a new `backend/core/iceberg/fs.py`, with every other concern (catalog, view, cache, manifest, buffer, sync_data) untouched. This matches `MONKEYPATCHES.md`'s explicit 2026-05-21 conclusion that the five s3fs patches are "structurally optimal until pyiceberg upstream adds a supply-your-own-FileSystem hook." The aggressive subclass path was investigated and rejected because pyiceberg's `_s3()` factory constructs `S3FileSystem` directly; displacing it requires *adding* a patch instead of removing one. The loc-discipline penalty (estimated reduction = 0) is the deliberate trade: organizational restructure without behavior drift, deferring any subclass conversion to a hypothetical Phase 4b.

The F3 wedge code, sync_data (Trap #21), FosSqlCatalog identity check, view cache, and catalog cache are **literally untouched** in `_core.py`. Single-commit revert restores the monolith with no data migrations and no on-disk format implications.

---

## 2. Module Shape

| Path | Owns | Est. LOC |
|---|---|---|
| `backend/core/iceberg/__init__.py` | Package shim. Imports `fs` FIRST (so monkeypatches install before any pyiceberg/s3fs import) then `from backend.core.iceberg._core import *`. Explicit `__all__` listing every symbol referenced by routers/repositories/tests, including the test-touched private helpers (`_get_catalog`, `_table_identifier`, `_DUCKDB_TO_ICEBERG`, `_get_service_lock`, `_buffer_dir`, `_orig_s3fs_init`, `_orig_s3fs_set_session`, `_read_metadata_pointer`, `_get_cached_table`, `_set_cached_table`, `_invalidate_cached_table`, `_load_table_cached`, `_write_metadata_pointer`, `_sql_load_table_real_calls`, `_table_object_cache`, `_snapshot_files_cache`, `_view_cache`, `_manifest_metadata_cache`, `_manifest_bytes_cache`, `_PENDING_FS_SOURCE`, `_LAST_FS_SOURCE`, `_catalog_cache`, `_catalog_db_path`, `_catalog_lock`, `_get_fos_catalog_class`, `_get_cache_file`). | ~60 |
| `backend/core/iceberg/fs.py` | All five s3fs monkeypatches (#1-#5) plus the `ThreadPoolExecutor.submit` patch (#6) and their support: `_PENDING_FS_SOURCE`, `_LAST_FS_SOURCE`, `_patched_submit`, `_proxy_targets_from_endpoint`, `_register_proxy_event_hook`, `_orig_s3fs_init`, `_orig_s3fs_set_session`, `_orig_cat_file`, `_orig_info`, `_orig_open`, `_orig_submit`, `_patched_s3fs_init`, `_patched_s3fs_set_session`, immutable-bytes LRU (`_MANIFEST_CACHE_MAX_BYTES`, `_manifest_bytes_cache`, `_manifest_cache_size`, `_manifest_cache_lock`, `_is_immutable_path`, `_canonical_cache_key`, `_cache_get`, `_cache_put`, `_inflight_async`, `_get_or_fetch_immutable_async`), `_patched_cat_file`, `_patched_info`, `_ImmutableWriteCacheTee`, `_patched_open`. The `try/except ImportError` envelope is preserved verbatim. | ~470 |
| `backend/core/iceberg/_core.py` | Renamed from current `backend/core/iceberg.py`. Keeps every other symbol untouched (schema, catalog lifecycle, FosSqlCatalog, view, persistent cache, manifest reading, buffer, commit, sync_data, optimize_table, run_cloud_maintenance, configure_duckdb_s3, `_get_service_lock`, `clear_source_caches`, `update_iceberg_view`, etc.). The vacated header block (lines 46-515) becomes a single `from backend.core.iceberg.fs import *  # noqa: F401,F403` plus an explicit re-import shim for the names callers reference (`_PENDING_FS_SOURCE`, `_LAST_FS_SOURCE`, `_manifest_bytes_cache`, etc.). | ~3,760 |

**Module loc sum:** 60 + 470 + 3,760 = 4,290 (vs 4,232 monolith). The ~58 LOC overhead is the package `__init__.py` shim and explicit re-export accounting.

---

## 3. Per-Monkeypatch Disposition

| # | Target | Action | Target file | Source line cite | Notes |
|---|---|---|---|---|---|
| 1 | `s3fs.S3FileSystem.__init__` | **keep (move verbatim)** | `fs.py` | current `iceberg.py:194` install; `iceberg.py:195-294` body | Cannot subclass: pyiceberg's `_s3()` constructs `S3FileSystem` directly. `_orig_s3fs_init` MUST be re-exported (used by `tests/test_e2e_pyiceberg_s3.py:146` moto E2E). |
| 2 | `s3fs.S3FileSystem.set_session` + `_connect` | **keep (move verbatim)** | `fs.py` | current `iceberg.py:511-513` install; `iceberg.py:296-414` body | Structurally coupled to #1 via `_fos_proxy_source` attribute stashing. `_orig_s3fs_set_session` re-exported for the same moto test. |
| 3 | `s3fs.S3FileSystem._cat_file` | **keep (move verbatim)** | `fs.py` | current `iceberg.py:507` install; `iceberg.py:301-375` body | Marked `replaceable_via_subclass: true` in the inventory but Phase 4a defers per MONKEYPATCHES.md's "structurally optimal until upstream hook" guidance. Preserves 1.00× telemetry ratio and 2.4 GB/day CDN saving. |
| 4 | `s3fs.S3FileSystem._info` | **keep (move verbatim)** | `fs.py` | current `iceberg.py:508` install; `iceberg.py:385-403` body | Synthesize-on-cache-hit / HEAD-on-miss shape preserved verbatim. Avoids the 89% mid-stream disconnect documented 2026-05-21. |
| 5 | `s3fs.S3FileSystem._open` | **keep (move verbatim)** | `fs.py` | current `iceberg.py:509` install; `iceberg.py:467-505` body; `_ImmutableWriteCacheTee` at `iceberg.py:405-449` | LRU hit-path read + immutable-write-tee. Tee class moves with it. Seed-on-close failure-mode safety preserved. |
| 6 | `concurrent.futures.ThreadPoolExecutor.submit` | **keep (move verbatim)** | `fs.py` | current `iceberg.py:90-95` install; `iceberg.py:80-95` body | Co-located with #1/#2 because it shares `_PENDING_FS_SOURCE` / `_LAST_FS_SOURCE` ContextVars. Hard constraint per audit finding 003 (2026-06-06): cross-tenant safety requires native ContextVar propagation, not a registry fallback. |

**Net:** All six patches preserved verbatim. Zero patches added. Zero patches dropped. Zero subclass conversions in this phase.

---

## 4. Cutover Steps

Each step is independently revertable. The test sweep per step is a gate — if it fails, revert that step before continuing.

**Step 1 — Package skeleton (pure rename).**
Create `backend/core/iceberg/` directory. Move existing `backend/core/iceberg.py` to `backend/core/iceberg/_core.py`. Add `backend/core/iceberg/__init__.py` with:
```
from backend.core.iceberg import fs as _fs  # noqa: F401  -- install monkeypatches FIRST
from backend.core.iceberg._core import *  # noqa: F401,F403
```
plus an explicit `__all__` enumerating every name from the inventory's `public_symbols` and the test-imported privates. At this point `fs.py` is an empty file; the import is a no-op.
**Test sweep:** `pytest tests/core/test_iceberg.py tests/core/test_iceberg_helpers.py tests/test_e2e_pyiceberg_s3.py -x`. Full suite must pass with zero changes outside `iceberg/`.

**Step 2 — Carve `fs.py`.**
Cut `_core.py:46-515` (everything from the first `import contextvars` through the `except ImportError: pass` block) verbatim into `backend/core/iceberg/fs.py`. Preserve the `try: import botocore as _botocore; from s3fs import S3FileSystem ... except ImportError: pass` envelope. At the top of `_core.py`, replace the removed block with:
```
from backend.core.iceberg.fs import (
    _PENDING_FS_SOURCE, _LAST_FS_SOURCE, _patched_submit, _orig_submit,
    _proxy_targets_from_endpoint, _register_proxy_event_hook,
    _manifest_bytes_cache, _manifest_cache_size, _manifest_cache_lock,
    _MANIFEST_CACHE_MAX_BYTES, _is_immutable_path, _canonical_cache_key,
    _cache_get, _cache_put, _inflight_async, _get_or_fetch_immutable_async,
    _ImmutableWriteCacheTee, _orig_s3fs_init, _orig_s3fs_set_session,
    _orig_cat_file, _orig_info, _orig_open,
)
```
Keep `_contextvars` / `_threading` aliases at `_core.py` module level (used elsewhere — `_get_service_lock` etc.).
**Test sweep:** `pytest tests/core/test_iceberg.py tests/core/test_iceberg_helpers.py -x` plus the import-order assertion `python -c 'import backend.core.iceberg; from s3fs import S3FileSystem; assert S3FileSystem.__init__.__name__ == "_patched_s3fs_init"'`.

**Step 3 — Add monkeypatch-contract guards.**
Add to the TOP of `fs.py`, immediately inside the `try` block and before any `_orig_* = S3FileSystem.X` capture:
```
_REQUIRED_S3FS_SLOTS = ('__init__', 'set_session', '_connect', '_cat_file', '_info', '_open')
_missing = [a for a in _REQUIRED_S3FS_SLOTS if not hasattr(S3FileSystem, a)]
if _missing:
    raise RuntimeError(
        f'pyiceberg/s3fs upgrade broke monkeypatch contract: missing {_missing}. '
        'Pin pyiceberg or update backend/core/iceberg/fs.py per MONKEYPATCHES.md.'
    )
```
Add `tests/core/test_iceberg_monkeypatch_contract.py` asserting `S3FileSystem._cat_file is fs._patched_cat_file` etc. for each of the six slots after package import. Add `pyiceberg[sql,s3fs,duckdb]>=0.8,<0.9` upper bound in `pyproject.toml`.
**Test sweep:** new contract test green; `uv sync` succeeds; existing tests still pass.

**Step 4 — Preemptive import-order hotfix.**
Add `from backend.core.iceberg import fs as _install_patches  # noqa: F401` to the top of `backend/main.py` AND `backend/scheduler.py`. These are independent process entry points; co-locating the patch install at startup eliminates the only realistic Phase-4a-specific failure mode (a future change importing s3fs at scheduler module-top before importing iceberg).
**Test sweep:** `pytest -x` end-to-end; start backend locally on 18002 and confirm telemetry-proxy receives `X-Fos-Target` on 100% of S3 requests during a 60-second smoke.

**Step 5 — Telemetry-proxy ContextVar validation.**
Run `pytest tests/utils/test_telemetry_proxy_phase3b.py -x`. This exercises `_ic._PENDING_FS_SOURCE.set(...)` (lines 39, 42, 121, 183, 244) and `_register_proxy_event_hook` (line 280). Both must resolve via the package re-export.

**Step 6 — FosSqlCatalog composition validation.**
Run `pytest tests/test_e2e_pyiceberg_s3.py -x`. Confirms `_orig_s3fs_init`, `_orig_s3fs_set_session`, `_PENDING_FS_SOURCE`, `_catalog_cache`, `_catalog_db_path`, `_catalog_lock`, `_get_fos_catalog_class` all resolve. Critical: the FosSqlCatalog identity-by-base check at `_core.py:871` (`if SqlCatalog in _FOS_CATALOG_CLASS.__bases__`) is untouched — the `from pyiceberg.catalog.sql import SqlCatalog` import stays LOCAL to `_get_fos_catalog_class` so test fixtures that monkeypatch `pyiceberg.catalog.sql.SqlCatalog` still resolve to their stub.

**Step 7 — F3 wedge validation.**
Run `pytest tests/repositories -k 'stale_view or self_heal or wedge' -x`. Smoke: `python -c 'from backend.core import iceberg as I; assert callable(I.clear_source_caches) and callable(I.update_iceberg_view)'`. The wedge call sequence in `backend/repositories/_base.py:323-324` and `:367-368` (`db_iceberg.clear_source_caches(...) → db_iceberg.update_iceberg_view(..., force=True)`) is a package attribute lookup; both resolve to the same function objects via `from _core import *`.

**Step 8 — Trap #21 validation.**
Run `pytest tests/core/test_local_compaction.py::test_compaction_outputs_survive_iceberg_sync_orphan_cleanup -x`. `sync_data`, `commit_buffer`, `run_cloud_maintenance` are all unmoved in `_core.py`; this is a guard test, not a behavior test.

**Step 9 — Full grep audit + docs.**
Run `grep -rn 'from backend.core.iceberg' backend tests` and diff against the pre-carve list (46 sites) — every import must still resolve. Update `MONKEYPATCHES.md` line-number citations (24 of them) to point at the new `fs.py` lines. Add `pending-docs/adr/06-iceberg-package-layout.md` if ADRs are being kept current.

**Step 10 — Squash + ship.**
Single squashed commit on `refactor/cleanup`: `core/iceberg: extract s3fs monkeypatch block into iceberg/fs.py`. Body enumerates: zero behavior change, `fs.py` contents, public import path preserved via package shim, contract guards added, upper version pin set, MONKEYPATCHES.md citations updated.

**Step 11 — verify-dev-first (per memory).**
Start backend on 18002 / frontend on 13002. Exercise dashboard read paths, a fresh `sync_data` cycle, an `optimize_table` run, and a deliberate stale-view self-heal. Confirm monkeypatch count via `grep -c 'monkeypatch\|MONKEYPATCH' backend/core/iceberg/fs.py backend/core/iceberg/_core.py` (expect 6).

**Step 12 — Deploy + verify prod.**
On GCE: pre-flight `git fetch && git reset --hard origin/refactor/cleanup` per `gce-deploy-rebuild` memory, then `~/restart.sh`. Hard-refresh browser. Monitor pool wait p95, RSS, Iceberg catalog query latency, and telemetry-proxy `X-Fos-Target` coverage for 15 min.

---

## 5. Rollback Plan

**Per-step rollback (Steps 1-9):** Each step is a separate working-tree state; `git checkout HEAD -- backend/core/iceberg/ backend/core/iceberg.py` restores the prior state. Test sweep gates prevent advancing past a failing step.

**Per-PR rollback (post-Step 10):** Single `git revert <carve-commit>` restores `backend/core/iceberg.py` as a single file and removes `backend/core/iceberg/` directory. Because Phase 4a is pure relocation with no semantic change, the revert is mechanically safe — no data migrations, no on-disk format changes, no API contract shifts.

**Pre-cutover safety net:** Tag the pre-carve commit as `pre-phase-4a-fs-carve` so reviewers/operators can `git diff pre-phase-4a-fs-carve..HEAD -- backend/core/iceberg` to verify only the package boundary moved. Also snapshot per-service DuckDB files, `iceberg_catalog.db`, `backend.db` to `/mnt/app-data/snapshots/phase-4a-<timestamp>/` though no on-disk format changes — this is belt-and-suspenders against an unforeseen mid-cron-cycle revert.

**Production hotfix recipe.** If a downstream entry point imports `s3fs` before `backend.core.iceberg` and bypasses the patches in prod (the only realistic Phase-4a-specific failure mode), the one-line fix `from backend.core.iceberg import fs as _install_patches  # noqa: F401` at the top of the offending module restores patch installation. Step 4 already places this line in `backend/main.py` and `backend/scheduler.py` preemptively; if a third entry point surfaces, the same one-line fix applies.

**GCE rollback procedure** (per `gce-deploy-rebuild` memory): `git reset --hard <pre-carve-sha> && ~/restart.sh` reverts in under 2 min; hard-refresh browser after frontend rebuild.

---

## 6. Compatibility

**Re-exported import paths (all preserved, zero caller changes required):**

Public symbols (per inventory): `get_iceberg_schema`, `get_arrow_schema`, `get_schema_field_names`, `init_iceberg_table`, `table_location`, `tombstone_buffer_files`, `sweep_tombstoned_buffer_files`, `buffer_files`, `buffer_backlog_stats`, `write_to_buffer`, `commit_buffer`, `optimize_table`, `run_cloud_maintenance`, `sync_data`, `configure_duckdb_s3`, `clear_source_caches`, `get_last_view_stats`, `inject_view_debug`, `update_iceberg_view`, `get_table_info`, `get_snapshot_calendar`.

Test-touched privates: `_get_catalog`, `_table_identifier`, `_DUCKDB_TO_ICEBERG`, `_get_service_lock`, `_buffer_dir`, `_orig_s3fs_init`, `_orig_s3fs_set_session`, `_orig_cat_file`, `_orig_info`, `_orig_open`, `_read_metadata_pointer`, `_write_metadata_pointer`, `_get_cached_table`, `_set_cached_table`, `_invalidate_cached_table`, `_load_table_cached`, `_sql_load_table_real_calls`, `_table_object_cache`, `_snapshot_files_cache`, `_view_cache`, `_manifest_metadata_cache`, `_manifest_bytes_cache`, `_PENDING_FS_SOURCE`, `_LAST_FS_SOURCE`, `_catalog_cache`, `_catalog_db_path`, `_catalog_lock`, `_get_fos_catalog_class`, `_get_cache_file`, `_pointer_cache_invalidate`, `_write_table_summary_async`, `_quarantine_buffer_file`.

**Deprecated paths:** None.

**Breaking changes:** None at import-path level.

**Sharp edge — mutable vs. immutable name binding:** Tests that mutate cache state via `iceberg._manifest_bytes_cache.clear()` (mutable dict, mutated through the re-export — works fine). Tests that REASSIGN immutable scalars via `iceberg._manifest_cache_size = 0` (e.g., `tests/core/test_iceberg.py:1519-1520`) re-bind the NAME in the package namespace but DO NOT mutate the int that `fs.py`'s patched methods read. **Fix:** rewrite the small handful of cache-size-reassignment call sites in `tests/core/test_iceberg.py` to use `iceberg.fs._manifest_cache_size = 0` directly. Grep `_manifest_cache_size =` under `tests/` to enumerate (estimated 3-5 sites). Mutable dict mutations are unaffected.

**`__file__` consumers:** Any tool keying on `backend.core.iceberg.__file__` now resolves to the package `__init__.py` path, not the monolith file path. Grep `pyproject.toml` `[tool.coverage]` config and `tests/conftest.py` for explicit references; update if needed.

---

## 7. F3 Wedge Preservation

**The F3 wedge (commit dc5b37d-era, surfaced 2026-06-05 prod incident) lives in `backend/core/duckdb_pool.py:178-213`, NOT in `iceberg.py`.** It moves the `update_iceberg_view` call OUTSIDE the per-pool `Condition` lock so a slow rebuild does not block all pool checkouts. The iceberg-side contract the wedge depends on is the pair invoked from `backend/repositories/_base.py:322-324` and `:366-368` and from `backend/repositories/insights/repository.py:378`:

```
db_iceberg.clear_source_caches(source_name, keep_snapshot_cache=True)
db_iceberg.update_iceberg_view(con, src, force=True)
```

**Phase 4a preserves this contract by construction:**

1. **Neither function moves.** `clear_source_caches` (current `iceberg.py:2827`), `update_iceberg_view` (current `iceberg.py:3253`), `_view_cache` (current `iceberg.py:2815`), `_snapshot_files_cache`, `_service_locks`, `_rebuild_signals`, `_update_iceberg_view_locked`, `_try_fast_path_view`, `_rebuild_locked`, `_persistent_view_exists` — all stay in `_core.py` at their existing line numbers, byte-for-byte.

2. **The package shim re-exports both as identical function objects.** `db_iceberg.clear_source_caches` and `db_iceberg.update_iceberg_view` (where `db_iceberg = from backend.core import iceberg`) resolve via `from backend.core.iceberg._core import *` to the same function objects as today. A re-export is a name binding, not a wrapper.

3. **Lock domain is unchanged.** Both functions touch the module-level `_view_cache` dict in `_core.py`. The bust-then-rebuild sequence runs entirely within `_core.py`'s lock domain — no cross-module ordering hazard, no GIL-yield window where another module could re-populate the cache between the pop and the rebuild.

4. **`force=True` semantics intact.** The lock-acquire-timeout fallback at current `iceberg.py:3306-3312` (the path the wedge defends against re-executing `_view_cache[source_key][3]` stale SQL) lives in `_core.py` untouched.

5. **`keep_snapshot_cache=True` intact.** The knob that preserves `_snapshot_files_cache` to avoid the empty-view downgrade documented at current `iceberg.py:2832-2841` is unchanged.

**Validation:** Step 7 of cutover runs `pytest tests/repositories -k 'stale_view or self_heal or wedge' -x` plus a callable-presence smoke. Step 11 (verify-dev-first) exercises a deliberate stale-view self-heal under local dev load.

---

## 8. Test Impact (per Phase 0 test_audit.md)

| Disposition | Count est. | Tests | Reason |
|---|---|---|---|
| **Keep unchanged** | ~40 | All tests using `from backend.core import iceberg as _ice` or `from backend.core.iceberg import X` followed by `_ice.X` / `X` reads, dict mutations (`_ice._manifest_bytes_cache.clear()`, `_ice._view_cache.pop(...)`), `@patch('backend.core.iceberg._get_catalog')` and similar `unittest.mock.patch` decorators against any public or private symbol. Includes `tests/core/test_iceberg.py` (majority), `tests/core/test_iceberg_helpers.py`, `tests/test_e2e_pyiceberg_s3.py` (after Step 6 validation), `tests/test_e2e_pipeline.py`, `tests/core/test_local_compaction.py`, `tests/repositories/*`, `tests/utils/test_telemetry_proxy_phase3b.py`. The package shim makes every import path and attribute access resolve identically. |
| **Rewrite (mechanical)** | 3-5 | Sites in `tests/core/test_iceberg.py` around lines 1519-1520 (and any other `_manifest_cache_size =` reassignment). Change `_ice._manifest_cache_size = 0` → `_ice.fs._manifest_cache_size = 0` to mutate the actual int in `fs.py` rather than re-binding the package namespace name. One-line per site. |
| **Add (new)** | 1 | `tests/core/test_iceberg_monkeypatch_contract.py` — asserts after `import backend.core.iceberg` that each of the six slots resolves to our `_patched_*` function (e.g., `S3FileSystem._cat_file is iceberg.fs._patched_cat_file`). Catches both upstream rename and silent-bypass attack variants in CI before any deploy. Added in Step 3. |
| **Delete** | 0 | No tests deleted. |

**Coverage delta:** No coverage targets shift because no behavior changes. The new contract test adds a small amount of coverage on the monkeypatch installation surface that today is implicitly covered by E2E tests only.

---

## 9. Adversarial Mitigations

Three adversarial attacks were considered. Two survive as-is; one requires landing four explicit mitigations as part of the carve.

### Attack 1 — Upstream pyiceberg/s3fs API rename (NOT SURVIVED without mitigations)

**Scenario:** s3fs minor upgrade renames `_cat_file → _async_cat_file` (or any of the six patched slots). At `fs.py` module-import time, `_orig_cat_file = S3FileSystem._cat_file` raises `AttributeError`, aborting the entire `backend.core.iceberg` package import and crashing every router/repository on app startup. Silent-bypass variant: upstream renames the slot AND adds a new method with the old name that pyiceberg's `_s3()` factory bypasses; capture succeeds but pyiceberg's new code path never hits our LRU/proxy, breaking the 1:1 telemetry ratio invisibly until a 2.4 GB/day CDN spike. Reachable via routine `uv sync` because `pyproject.toml` currently has no upper bound on `pyiceberg`.

**Mitigations (all four land in the carve commit):**
1. Method-existence preflight in `fs.py` (Step 3) — converts `AttributeError` into an actionable `RuntimeError` naming the broken contract.
2. Upper-bound pin in `pyproject.toml`: `pyiceberg[...]>=0.8,<0.9`.
3. New `tests/core/test_iceberg_monkeypatch_contract.py` asserting each slot resolves to our `_patched_*` function — catches both rename and silent-bypass in CI.
4. `__init__.py` wraps `import fs` in `try/except` that re-raises with monkeypatch-contract-failure vocabulary, so downstream `ImportError`s point at the actual cause rather than masking it.

### Attack 2 — DuckDB view-binding inside pool acquire (SURVIVES)

**Scenario:** A new code path opens a DuckDB connection that depends on view-binding inside `_Pool.acquire`, defeating F3's lock-released `update_iceberg_view`.

**Why it survives:** Phase 4a does not touch `duckdb_pool.py`, `update_iceberg_view`, `clear_source_caches`, `_view_cache`, or `_service_locks`. The F3 separation lives entirely in `duckdb_pool.py:178-213` (the explicit "Outside lock" comment at `:203-209`). Verified by reading `duckdb_pool.py:317-327`: the acquire path's lock-released call site calls `iceberg.update_iceberg_view` by attribute lookup on the package; the package shim re-exports the same function object, so resolution is identical. The attack premise is fabricated for Phase 4a — it would land against `aggressive-5-modules` (which actually moves `update_iceberg_view` to `view.py`) but not here.

### Attack 3 — Half-deployed state (SURVIVES)

**Scenario:** Mid-deploy, an in-flight cron run holds a reference to the old `iceberg.py` module while the new package loads.

**Why it survives:** The project's deploy model (per `gce-deploy-rebuild` memory) is `docker compose --build` — container REPLACEMENT, not in-process hot reload. Old and new containers are separate Python interpreters with separate `sys.modules`, separate `ThreadPoolExecutor` instances, separate monkeypatch state. The premise that a cron holds a reference to the old module only exists inside the dying old container, until `scheduler.shutdown(wait=False)` (`backend/scheduler.py:223-228`) processes SIGTERM and the container is killed. The new container imports the carved package fresh.

The on-disk and on-FOS state that DOES survive a deploy is byte-for-byte identical: per-service DuckDB files, `iceberg_catalog.db`, `backend.db` are untouched; `snapshot_files_cache.json` / `manifest_metadata_cache.json` use the same `_save_persistent_cache` / `_load_persistent_cache` in `_core.py` with the same on-disk format; Iceberg metadata pointers on FOS use the same `_read_metadata_pointer` / `_write_metadata_pointer`; buffer parquet + tombstone markers use the same `commit_buffer` / `tombstone_buffer_files` / `sweep_tombstoned_buffer_files`; orphan `cron_runs` rows reaped on startup via `metadata_db.reap_running_cron_runs(sid)` at `backend/main.py:79-97`. The preemptive import-order hotfix landed in Step 4 (`from backend.core.iceberg import fs as _install_patches` in `backend/main.py` and `backend/scheduler.py`) eliminates the only realistic Phase-4a-specific startup failure mode.

---

## 10. Open Questions for the User

1. **Upper-bound version pin scope.** `pyproject.toml` Step 3 proposes `pyiceberg[sql,s3fs,duckdb]>=0.8,<0.9`. Should sibling pins also land for `s3fs`, `botocore`, `aiobotocore` to eliminate transitive-upgrade risk on the same attack surface? Recommendation: yes, add `s3fs>=X,<Y` matched to whatever version range you've validated. Confirm acceptable.

2. **Contract test scope.** `tests/core/test_iceberg_monkeypatch_contract.py` could assert just function identity (`S3FileSystem._cat_file is iceberg.fs._patched_cat_file`) or also assert behavioral invariants (cache-hit returns synthesized info, max_concurrency forced to 1, etc.). Identity-only is the minimum to catch upstream rename / silent-bypass. Should we expand to behavioral assertions, or rely on existing E2E coverage for behavior?

3. **ADR commitment.** Should `pending-docs/adr/06-iceberg-package-layout.md` land alongside the carve, or are ADRs currently dormant on this branch? (Step 9 lists it as "if ADRs are being kept current.")

4. **MONKEYPATCHES.md update timing.** Step 9 updates the 24 line-number citations. Should this be part of the same squash commit, or split into a docs-only follow-up for cleaner review?

5. **Phase 4b deferral acknowledgement.** This phase delivers zero LOC reduction. The subclass conversion that would actually shrink the patch surface is deferred to a hypothetical Phase 4b, contingent on pyiceberg upstream merging a "supply-your-own-FileSystem" hook (the doc's stated precondition). Confirm we are content with organizational restructure now, structural reduction later — or do you want a sketch of what Phase 4b would look like in this same doc?

6. **Per-service DuckDB snapshot.** Rollback plan recommends a `/mnt/app-data/snapshots/phase-4a-<timestamp>/` tarball even though there are no on-disk format changes. Belt-and-suspenders or skip?
