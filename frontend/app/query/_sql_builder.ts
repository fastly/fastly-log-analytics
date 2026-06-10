import type { SortingState } from '@tanstack/react-table'
import type { FiltersPayload } from '@/types/filters'

/** Escape a string literal for safe SQL embedding (single-quote doubling). */
export function sqlEscape(v: string): string {
  return v.replace(/'/g, "''")
}

/** Quote a column identifier for DuckDB (double-quote, escape inner quotes). */
export function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

// DuckDB numeric types whose IN-list literals should NOT be quoted. Keeping
// these as numeric literals produces cleaner displayed SQL and avoids the
// implicit VARCHAR→INT cast (which works today but is fragile if the cast
// rule ever changes).
const NUMERIC_DUCKDB_TYPES = new Set([
  'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
  'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT',
  'FLOAT', 'DOUBLE', 'REAL', 'DECIMAL',
])

function isNumericType(t: string | undefined): boolean {
  if (!t) return false
  // Strip parameterized suffixes like DECIMAL(18,2)
  const base = t.toUpperCase().split('(')[0].trim()
  return NUMERIC_DUCKDB_TYPES.has(base)
}

const NUMERIC_LITERAL_RE = /^-?\d+(\.\d+)?$/

/**
 * Build a WHERE clause fragment from a FiltersPayload + date range.
 * Returns an empty string when nothing is constrained.
 *
 * `fieldTypes` maps column name → DuckDB type. When provided, numeric
 * columns emit unquoted literals (e.g. `IN (50)` instead of `IN ('50')`).
 * Falls back to quoted strings when the map is absent or the value isn't
 * a parseable number — implicit casts handle the latter.
 */
export function buildWhereClause(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  fieldTypes?: Record<string, string>,
): string {
  const parts: string[] = []

  if (startTime) parts.push(`timestamp >= '${sqlEscape(startTime)}'`)
  if (endTime) parts.push(`timestamp <= '${sqlEscape(endTime)}'`)

  for (const [rawCol, spec] of Object.entries(filters)) {
    if (!spec || !Array.isArray(spec.values) || spec.values.length === 0) continue
    // FilterStore appends `_<n>` to dedupe same-column same-mode buckets; the
    // real column name is everything before the trailing `_<digits>`.
    const col = rawCol.replace(/_\d+$/, '')
    const ident = quoteIdent(col)
    const colIsNumeric = isNumericType(fieldTypes?.[col])
    const literals = spec.values
      .map(v => {
        const s = String(v)
        // Only emit unquoted IF the column is numeric AND the value parses
        // as a number. A non-numeric string filter value on a numeric column
        // (rare, but possible if the value picker shows a non-numeric label)
        // stays quoted so DuckDB's cast/coerce rules still produce a row count.
        if (colIsNumeric && NUMERIC_LITERAL_RE.test(s)) return s
        return `'${sqlEscape(s)}'`
      })
      .join(', ')
    const op = spec.mode === 'exclude' ? 'NOT IN' : 'IN'
    parts.push(`${ident} ${op} (${literals})`)
  }

  return parts.length > 0 ? `WHERE ${parts.join(' AND ')}` : ''
}

/**
 * Generate the canonical Structured-Mode SQL. Sort comes from the table's
 * SortingState so column-header clicks round-trip to the server.
 *
 * `fieldTypes` is forwarded to buildWhereClause so numeric IN-lists render
 * unquoted.
 */
export function buildStructuredSql(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  sorting: SortingState,
  maxRows: number,
  fieldTypes?: Record<string, string>,
): string {
  const where = buildWhereClause(filters, startTime, endTime, fieldTypes)
  const sort = sorting[0]
  const orderBy = sort
    ? `ORDER BY ${quoteIdent(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`
    : 'ORDER BY timestamp DESC'
  return [
    'SELECT *',
    'FROM logs',
    where,
    orderBy,
    `LIMIT ${maxRows}`,
  ].filter(Boolean).join('\n')
}

export type QueryMode = 'structured' | 'raw'
