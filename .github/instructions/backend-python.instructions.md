---
applyTo: "backend/**/*.py"
---

# Backend (Python / FastAPI)

- Package management is **uv** (not pip/pipenv/poetry). Run tests with `uv run pytest`, never bare `pytest`.
- APIs are **FastAPI + Pydantic** models. Type-hint every function signature. After changing a route or model, run `make gen-types` so `frontend/openapi.json` and the generated TS types stay in sync (`make openapi-drift` gates it).
- Tests are **pytest**. Write the failing test first; never write a test that can't fail.
- Analytics data lives in **DuckDB / DuckLake** (parquet); operational metadata and DuckLake catalogs live in **PostgreSQL**. All SQL goes through `backend/repositories/`, never inline in routers. Preview with `SELECT` before any `DELETE`/`UPDATE`/`MERGE`.
- Error responses use `make_error()` / `@query_errors`. Never echo exception text or secrets back to the client.
- **Import-contract (enforced by `make import-contracts`):** `backend/core/*` must NOT import `backend.cron.scheduler` or anything under `backend.routers.*`. If core needs router/scheduler behavior, inject it as a `Callable` from the caller.
- Cron jobs run inside the `cron_run` context manager (`backend/cron/scheduler/lifecycle.py`), which owns the lease, the terminal `cron_runs` row and progress cleanup. A hand-rolled lifecycle that leaks a `status='running'` row freezes ingestion.
- Service config edits go through `mutate_config()` (locked read-modify-write); never `load_config` + `save_config` for a partial edit, and never write the config file directly. The `rum.cid_salt` is write-once outside `rotate_cid_salt`.
