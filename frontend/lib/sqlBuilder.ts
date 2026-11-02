import type { SortingState } from '@tanstack/react-table'
import type { FiltersPayload } from '@/types/api'

const SIMPLE_IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/
const NUMERIC_LITERAL_RE = /^-?\d+(\.\d+)?$/
const NUMERIC_DUCKDB_TYPES = new Set([
  'TINYINT',
  'SMALLINT',
  'INTEGER',
  'BIGINT',
  'HUGEINT',
  'UTINYINT',
  'USMALLINT',
  'UINTEGER',
  'UBIGINT',
  'UHUGEINT',
  'FLOAT',
  'DOUBLE',
  'REAL',
  'DECIMAL',
])

export function sqlEscape(v: string): string {
  return v.replace(/'/g, "''")
}

export function quoteIdent(name: string): string {
  return SIMPLE_IDENT_RE.test(name) ? name : `"${name.replace(/"/g, '""')}"`
}

export function isNumericType(t: string | undefined): boolean {
  return Boolean(t && NUMERIC_DUCKDB_TYPES.has(t.toUpperCase().split('(')[0].trim()))
}

export function toCanonicalSqlTs(ts: string): string {
  const d = new Date(ts)
  return isNaN(d.getTime()) ? ts : d.toISOString()
}

export function buildWhereClause(
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
    const colIsNumeric = isNumericType(fieldTypes?.[col])
    const literals = spec.values
      .map((v) => {
        const s = String(v)
        return colIsNumeric && NUMERIC_LITERAL_RE.test(s) ? s : `'${sqlEscape(s)}'`
      })
      .join(', ')
    parts.push(`${quoteIdent(col)} ${spec.mode === 'exclude' ? 'NOT IN' : 'IN'} (${literals})`)
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
  const orderBy = sort ? `ORDER BY ${quoteIdent(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}` : 'ORDER BY timestamp DESC'
  return ['SELECT *', `FROM ${tableName}`, where, orderBy, `LIMIT ${maxRows}`].filter(Boolean).join('\n')
}
