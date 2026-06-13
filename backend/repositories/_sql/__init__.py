"""SQL templates owned by ``backend/repositories/``.

Phase 5a of the v2.0 cleanup. Every inline SQL string in the repository
layer migrates here as a named constant so:

- routers / repositories never carry inline SQL literals;
- SQL changes are reviewable at one location per concern;
- tests can render the templates against fixture inputs without spinning
  up DuckDB.

Each per-file template module documents the templates' window/filter shape
and the expected output columns. Repositories import the module and call
``str.format`` (or ``%s`` parameter binding) on the constant.

Migration order — see ``pending-docs/sql_ownership_audit.md`` §"Per-
repository SQL extraction targets". Modules below land incrementally.
"""

__all__: list[str] = []
