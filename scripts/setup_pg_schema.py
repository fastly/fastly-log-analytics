"""Create the unified Postgres metadata schema (METADATA_DSN).

The DDL itself lives in ``backend.core.metadata.pg_schema`` so this
script and the boot-time ``ensure_pg_schema()`` apply exactly the same
statements. Safe to re-run: every statement is ``IF NOT EXISTS``-style
and nothing is dropped — an existing, populated database is left intact.
Any DDL failure is printed with the offending statement and the script
exits nonzero (errors are never swallowed).

The backend and the Celery workers now ensure this schema themselves at
startup; this script remains the explicit ops entry point for
provisioning a database ahead of a deploy, or for re-checking one by
hand.
"""

import os
import sys

from backend.core.metadata.pg_connection import get_pg_pool
from backend.core.metadata.pg_schema import apply_pg_schema


def setup() -> None:
    if not os.environ.get("METADATA_DSN"):
        print("Error: METADATA_DSN must be set.")
        sys.exit(1)

    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                applied, raced = apply_pg_schema(cur)
    except Exception as e:
        print("Error applying schema statement:")
        print(e)
        sys.exit(1)

    if raced:
        print(f"Schema applied ({applied} statements, {raced} already created concurrently).")
    else:
        print(f"Schema applied ({applied} statements).")


if __name__ == "__main__":
    setup()
