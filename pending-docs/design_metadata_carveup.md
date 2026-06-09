# Architectural Design Specification: Metadata DB Refactoring (`metadata_db.py` Carve-up)

## 1. Context & Motivation

The `backend/core/metadata_db.py` file is a 3,168-line monolithic Python module that manages all read/write operations against the SQLite operational database (`metadata.db`).

### Problems with the Monolithic Approach
1. **Single Point of Change/Contention:** Any feature relating to alert conditions, ingestion tracking, pricing calculations, saved dashboards, or geolocation ASN mapping requires editing the same massive file.
2. **Violates Single Responsibility Principle:** The `MetadataDB` class is simultaneously a database initialization script, an audit logger, a cost estimator, a discrepancy investigator, and an state exporter.
3. **Mypy and Test Clutter:** Writing clean unit tests and maintaining strict type checking is extremely difficult when single class instances carry 50+ unrelated methods.

---

## 2. Refactored Package Directory Architecture

We will decompose `metadata_db.py` into a structured, highly cohesive Python package directory: `backend/core/metadata/`.

```
backend/core/metadata/
├── __init__.py           # Package exports
├── base.py               # Db connection pooling, WAL, and initialization
├── alerts.py             # Alerts CRUD and conditions tracking
├── views.py              # Saved dashboard queries and view CRUD
├── ingest_log.py         # Ingestion manifest, in-flight transaction registry
├── cron_log.py           # APScheduler execution logging and progress tracking
├── asn_cache.py          # Geolocation ASN translates cache
├── usage_log.py          # Cost operations logger (Class A/B)
├── reconciliation.py     # Fastly stats vs ingestion reconciliation
└── state.py              # System configuration backups and export/import
```

---

## 3. Mixin Inheritance Strategy (100% Backward Compatibility)

To achieve 100% backward compatibility with all existing calling code, we will implement the specialized sub-managers as **Mixins**. The main `MetadataDB` class in `backend/core/metadata_db.py` (which other routers and services import) will then simply inherit from all of these mixins.

```mermaid
classDiagram
    class BaseMetadataDB {
        +db_path: str
        +get_cursor()
        +execute_query()
    }
    class AlertsMixin {
        +create_alert()
        +get_alert()
        +delete_alert()
    }
    class ViewsMixin {
        +save_view()
        +get_view()
    }
    class IngestLogMixin {
        +record_in_flight()
        +insert_ingested_files()
    }
    class MetadataDB {
        # Combines all Mixins
    }
    BaseMetadataDB <|-- AlertsMixin
    BaseMetadataDB <|-- ViewsMixin
    BaseMetadataDB <|-- IngestLogMixin
    AlertsMixin <|-- MetadataDB
    ViewsMixin <|-- MetadataDB
    IngestLogMixin <|-- MetadataDB
```

### Module Interface Blueprints

#### `backend/core/metadata/base.py`
```python
import sqlite3
import os
from contextlib import contextmanager

class BaseMetadataDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def connection(self):
        con = sqlite3.connect(self.db_path)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        try:
            yield con
        finally:
            con.close()

    def _init_db(self):
        # Database schema bootstrap tables
        pass
```

#### `backend/core/metadata/alerts.py`
```python
from backend.core.metadata.base import BaseMetadataDB
from typing import List, Dict, Any

class AlertsMixin(BaseMetadataDB):
    def create_alert(self, service_id: str, alert_config: dict) -> str:
        with self.connection() as con:
            # Alert insertion SQL
            pass
            
    def list_alerts(self, service_id: str) -> List[Dict[str, Any]]:
        with self.connection() as con:
            # Fetch alerts SQL
            pass
```

#### `backend/core/metadata/ingest_log.py`
```python
from backend.core.metadata.base import BaseMetadataDB
from typing import List, Tuple

class IngestLogMixin(BaseMetadataDB):
    def record_in_flight(self, source_name: str, buf_filename: str, file_rows: int):
        with self.connection() as con:
            # Write in-flight manifest SQL
            pass

    def insert_ingested_files(self, files: List[Tuple[str, int, int]]):
        with self.connection() as con:
            # Commit ingested files SQL
            pass
```

---

## 4. Refactoring `backend/core/metadata_db.py` (The Shim)

The original file `backend/core/metadata_db.py` will be reduced to under 50 lines. It will load and re-export the multi-inherited class so that **zero caller imports or signatures need to be changed** across the entire 150+ files codebase:

```python
# backend/core/metadata_db.py
from backend.core.metadata.base import BaseMetadataDB
from backend.core.metadata.alerts import AlertsMixin
from backend.core.metadata.views import ViewsMixin
from backend.core.metadata.ingest_log import IngestLogMixin
from backend.core.metadata.cron_log import CronLogMixin
from backend.core.metadata.asn_cache import AsnCacheMixin
from backend.core.metadata.usage_log import UsageLogMixin
from backend.core.metadata.reconciliation import ReconciliationMixin
from backend.core.metadata.state import StateMixin

class MetadataDB(
    AlertsMixin,
    ViewsMixin,
    IngestLogMixin,
    CronLogMixin,
    AsnCacheMixin,
    UsageLogMixin,
    ReconciliationMixin,
    StateMixin
):
    """
    Backward-compatible monolithic wrapper representing the combined SQLite metadata layer.
    All implementation code has been carved out to backend/core/metadata/.
    """
    def __init__(self, db_path: str):
        # Multi-inheritance initializer calls are handled by Python's MRO
        super().__init__(db_path)
```

---

## 5. Test and Validation Strategy

Each mixin can be tested in isolation, and the combined class will undergo comprehensive integration testing:
- **Isolated Unit Testing:** Create mock databases and test mixins (e.g., `AlertsMixin`) directly by instantiating a dummy class subclassing only `BaseMetadataDB` and `AlertsMixin`.
- **Backward-Compatibility Verification:** Run the existing complete integration and unit test suite against the composite `MetadataDB` to verify that all existing queries, data migrations, and reconciliation routines continue to pass with 100% exact matching behavior.
