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

/**
 * Build a WHERE clause fragment from a FiltersPayload + date range.
 * Returns an empty string when nothing is constrained.
 */
export function buildWhereClause(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
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
    const literals = spec.values.map(v => `'${sqlEscape(String(v))}'`).join(', ')
    const op = spec.mode === 'exclude' ? 'NOT IN' : 'IN'
    parts.push(`${ident} ${op} (${literals})`)
  }

  return parts.length > 0 ? `WHERE ${parts.join(' AND ')}` : ''
}

/**
 * Generate the canonical Structured-Mode SQL. Sort comes from the table's
 * SortingState so column-header clicks round-trip to the server.
 */
export function buildStructuredSql(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  sorting: SortingState,
  maxRows: number,
): string {
  const where = buildWhereClause(filters, startTime, endTime)
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
