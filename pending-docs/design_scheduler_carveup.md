# Architectural Design Specification: Scheduler Refactoring (`scheduler.py` Carve-up)

## 1. Context & Motivation

The `backend/scheduler.py` file is a 2,843-line monolithic script that serves as both the orchestration engine and execution body for all asynchronous background cron tasks in the system.

### Complications within the Monolith
1. **Coupling of Jobs and Scheduler:** Code that initializes the APScheduler library, handles thread counts, and manages job triggers resides in the same file as the complex procedural code that downloads `.gz` logs, performs bin-packing local compaction, and runs Iceberg snapshot expiration.
2. **Difficult Local Testing:** Testing a single cron job function (such as `local_compact_data`) requires importing the entire scheduler file, which implicitly brings in heavy scheduler global initializers and unrelated job modules.
3. **Complex Telemetry Attribution:** Isolating execution traces (OTel) and tracking memory allocations is highly complex when all routines execute within a single massive file context.

---

## 2. Refactored Package Directory Architecture

We will decompose `scheduler.py` into a highly structured `backend/cron/` package:

```
backend/cron/
├── __init__.py           # Package exports (e.g. get_scheduler, init_scheduler)
├── scheduler.py          # Orchestrates APScheduler instance & config reload
├── decorators.py         # Standardizes @cron_task, telemetry, and logging wrappers
└── jobs/                 # Isolated modules for individual background jobs
    ├── __init__.py
    ├── sync.py           # Sync raw logs from S3 to local Parquet buffer
    ├── commit.py         # S3 Apache Iceberg commits
    ├── compaction.py     # Hot-tier size-capped local compaction (hourly/daily/weekly)
    ├── optimize.py       # Remote Iceberg table daily compaction
    ├── expire.py         # Weekly Iceberg snapshot expiration
    └── metadata.py       # Metadata SQLite sync and S3 backup upload
```

---

## 3. Structural Design of Decoupled Modules

### Standardizing Job Entrypoints (`decorators.py`)

All job entry points will be wrapped with the standardized `@cron_task` decorator, which encapsulates telemetry tracing, error isolation, call tracking, and usage log flushing.

```python
# backend/cron/decorators.py
import functools
import structlog
from opentelemetry import trace
from backend.utils.usage_logger import flush_usage_log
from backend.utils.telemetry import set_process_context, start_call_tracking

logger = structlog.get_logger()
tracer = trace.get_tracer("backend.cron")

def cron_task(job_name: str):
    """
    Standardized decorator for background cron tasks.
    Wires OTel traces, process tagging, structural logs, and cost metrics flushes.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(service_id: str, *args, **kwargs):
            set_process_context(f"cron:{job_name}:{service_id}")
            start_call_tracking()
            
            with tracer.start_as_current_span(f"cron:{job_name}") as span:
                span.set_attribute("app.service_id", service_id)
                logger.info("Starting background job", job=job_name, service_id=service_id)
                try:
                    result = func(service_id, *args, **kwargs)
                    logger.info("Completed background job successfully", job=job_name, service_id=service_id)
                    return result
                except Exception as e:
                    logger.error("Background job failed with exception", job=job_name, service_id=service_id, error=str(e))
                    span.record_exception(e)
                    raise
                finally:
                    flush_usage_log(service_id)
        return wrapper
    return decorator
```

### Decomposing Job Routines (`jobs/`)

Individual files under `jobs/` will export simple, decorated functions. For example:

#### `backend/cron/jobs/compaction.py`
```python
# backend/cron/jobs/compaction.py
import structlog
from backend.cron.decorators import cron_task
from backend.core.local_compaction import run_local_compaction

logger = structlog.get_logger()

@cron_task("local_compaction")
def job_local_compaction(service_id: str):
    """
    Compact small hourly Parquet logs into size-capped (<= 256MB) files.
    """
    logger.info("Executing local hot-tier compaction", service_id=service_id)
    run_local_compaction(service_id)
```

#### `backend/cron/jobs/sync.py`
```python
# backend/cron/jobs/sync.py
from backend.cron.decorators import cron_task
from backend.core.ingest import run_raw_logs_sync

@cron_task("sync")
def job_raw_sync(service_id: str):
    """
    Fetch, download, transform, and buffer raw Fastly logs concurrently.
    """
    run_raw_logs_sync(service_id)
```

---

## 4. The Core Scheduler Engine (`backend/cron/scheduler.py`)

The job scheduler engine imports these decoupled job functions and registers them onto the APScheduler instance. This maintains separation between **how** a job runs, and **when** it runs.

```python
# backend/cron/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from backend.cron.jobs.sync import job_raw_sync
from backend.cron.jobs.commit import job_commit_table
from backend.cron.jobs.compaction import job_local_compaction
from backend.cron.jobs.optimize import job_optimize_table
from backend.cron.jobs.expire import job_expire_snapshots
from backend.cron.jobs.metadata import job_metadata_sync

class AnalyticsScheduler:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self._active_services: set[str] = set()

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()

    def reload_config(self, services_configs: dict):
        """
        Dynamically adjusts APScheduler triggers based on JSON configuration state.
        Diffs active jobs and cleanly reloads them.
        """
        # Job mapping configuration diffing logic
        pass
```

---

## 5. Refactoring `backend/scheduler.py` (The Entrypoint Shim)

The original `backend/scheduler.py` file becomes a lightweight, backward-compatible entrypoint shim that forwards boot and management hooks:

```python
# backend/scheduler.py
"""
Backward-compatible shim delegating to the modularized backend/cron package.
Implementation details are located under backend/cron/.
"""
from backend.cron.scheduler import AnalyticsScheduler

# Singleton instance matching prior architecture expectations
_global_scheduler = AnalyticsScheduler()

def get_scheduler() -> AnalyticsScheduler:
    return _global_scheduler

def init_scheduler(app):
    """Boot-time initialization hook for FastAPI lifecycles."""
    _global_scheduler.start()
    # Configuration register routines
```

---

## 6. Test and Verification Strategy

By isolating each job into its own module, background operations become highly mockable and testable:
- **Unit Job Verification:** Tests in `tests/cron/` can import a single job (e.g., `tests/cron/jobs/test_compaction.py`) and mock standard S3/S3FileSystem environments directly, avoiding the need to boot or teardown an active APScheduler thread loop.
- **Scheduler Reload Verification:** Tests can mock the triggers of individual job files to ensure the scheduler's reload delta engine accurately calculates additions/deletions on reloads without triggering raw sync side-effects.
