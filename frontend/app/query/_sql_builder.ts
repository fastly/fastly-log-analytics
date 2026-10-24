import { parseAsString, parseAsStringLiteral } from 'nuqs'
import type { SortingState } from '@tanstack/react-table'
import type { FiltersPayload } from '@/types/filters'

/**
 * /query URL contract (shared links and the dashboard "See Raw Logs" CTA):
 *   ?mode=raw            raw-SQL editor (also implied by ?q=)
 *   ?q=<sql>             seeds the raw editor
 *   ?start_time&end_time absolute range, applied once to the filter store
 *   ?filters=<json>      filter payload, applied once to the filter store
 * Unrecognised `mode` values mean structured mode, as before.
 */
export const queryPageUrlParsers = {
  q: parseAsString,
  mode: parseAsStringLiteral(['raw'] as const),
  start_time: parseAsString,
  end_time: parseAsString,
  filters: parseAsString,
}

/** Escape a string literal for safe SQL embedding (single-quote doubling). */
function sqlEscape(v: string): string {
  return v.replace(/'/g, "''")
}

const SIMPLE_IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

/** Quote a column identifier only when required (unquoted simple identifiers stay compatible with High-Scale ClickHouse console validation). */
function quoteIdent(name: string): string {
  if (SIMPLE_IDENT_RE.test(name)) return name
  return `"${name.replace(/"/g, '""')}"`
}

const NUMERIC_DUCKDB_TYPES = new Set([
  'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
  'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT',
  'FLOAT', 'DOUBLE', 'REAL', 'DECIMAL',
])

function isNumericType(t: string | undefined): boolean {
  if (!t) return false
  const base = t.toUpperCase().split('(')[0].trim()
  return NUMERIC_DUCKDB_TYPES.has(base)
}

const NUMERIC_LITERAL_RE = /^-?\d+(\.\d+)?$/

function toCanonicalSqlTs(ts: string): string {
  const d = new Date(ts)
  return isNaN(d.getTime()) ? ts : d.toISOString()
}

function buildWhereClause(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  fieldTypes?: Record<string, string>,
): string {
  const parts: string[] = []

  if (startTime) parts.push(`timestamp >= '${sqlEscape(toCanonicalSqlTs(startTime))}'`)
  if (endTime) parts.push(`timestamp <= '${sqlEscape(toCanonicalSqlTs(endTime))}'`)

  for (const [rawCol, spec] of Object.entries(filters)) {
    if (!spec || !Array.isArray(spec.values) || spec.values.length === 0) continue
    const col = rawCol.replace(/_\d+$/, '')
    const ident = quoteIdent(col)
    const colIsNumeric = isNumericType(fieldTypes?.[col])
    const literals = spec.values
      .map(v => {
        const s = String(v)
        if (colIsNumeric && NUMERIC_LITERAL_RE.test(s)) return s
        return `'${sqlEscape(s)}'`
      })
      .join(', ')
    const op = spec.mode === 'exclude' ? 'NOT IN' : 'IN'
    parts.push(`${ident} ${op} (${literals})`)
  }

  return parts.length > 0 ? `WHERE ${parts.join(' AND ')}` : ''
}

export function buildStructuredSql(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  sorting: SortingState,
  maxRows: number,
  fieldTypes?: Record<string, string>,
  tableName: string = 'logs',
): string {
  const where = buildWhereClause(filters, startTime, endTime, fieldTypes)
  const sort = sorting[0]
  const orderBy = sort
    ? `ORDER BY ${quoteIdent(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`
    : 'ORDER BY timestamp DESC'
  return [
    'SELECT *',
    `FROM ${tableName}`,
    where,
    orderBy,
    `LIMIT ${maxRows}`,
  ].filter(Boolean).join('\n')
}

export type QueryMode = 'structured' | 'raw'
