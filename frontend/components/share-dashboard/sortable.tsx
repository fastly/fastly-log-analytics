'use client'

import * as React from 'react'
import { ArrowDown, ArrowUp, ArrowUpDown } from 'lucide-react'

import { TableHead } from '@/components/ui/table'
import { cn } from '@/lib/utils'

export type SortDir = 'asc' | 'desc'

/** A cell value we know how to order: numbers, strings, or "missing". */
type SortValue = string | number | null | undefined

/** Map of sortKey → accessor returning the comparable value for a row. */
export type SortAccessors<T> = Record<string, (row: T) => SortValue>

export interface TableSort<T> {
  sorted: T[]
  sortKey: string
  sortDir: SortDir
  /** Toggle a column: same key flips direction, new key resets to `desc`. */
  toggle: (key: string) => void
}

const isMissing = (v: SortValue): boolean => v === null || v === undefined || v === ''

/** Compare two present (non-missing) values. Missing handling is done by the
 *  caller so it can stay direction-independent (nulls always last). */
function compareValues(a: SortValue, b: SortValue): number {
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
}

/**
 * Lightweight client-side sort for the shadcn `Table` primitive — the
 * heavyweight tanstack `DataTable` is overkill for these small admin panels.
 * Stable (index tie-break), nulls-last, and driven by an explicit accessor map
 * so callers keep full control over how each column compares.
 */
export function useTableSort<T>(
  rows: T[],
  accessors: SortAccessors<T>,
  opts: { defaultKey: string; defaultDir?: SortDir },
): TableSort<T> {
  const [sortKey, setSortKey] = React.useState(opts.defaultKey)
  const [sortDir, setSortDir] = React.useState<SortDir>(opts.defaultDir ?? 'desc')

  const toggle = React.useCallback((key: string) => {
    setSortKey((prevKey) => {
      if (prevKey === key) {
        setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
        return prevKey
      }
      // New column: start descending — the useful default for timestamps/counts.
      setSortDir('desc')
      return key
    })
  }, [])

  const sorted = React.useMemo(() => {
    const accessor = accessors[sortKey]
    if (!accessor) return rows
    const factor = sortDir === 'asc' ? 1 : -1
    // decorate-sort-undecorate keeps the sort stable (Array.sort isn't
    // guaranteed stable for the comparator's equal case across every engine,
    // and the index tie-break also keeps missing-value rows in input order).
    return rows
      .map((row, i) => ({ row, i }))
      .sort((x, y) => {
        const av = accessor(x.row)
        const bv = accessor(y.row)
        // Missing values always sink to the bottom regardless of direction, so
        // a never-logged-in invite never masquerades as "most recent" on desc.
        const aMissing = isMissing(av)
        const bMissing = isMissing(bv)
        if (aMissing || bMissing) {
          if (aMissing && bMissing) return x.i - y.i
          return aMissing ? 1 : -1
        }
        const c = compareValues(av, bv)
        return c !== 0 ? c * factor : x.i - y.i
      })
      .map((d) => d.row)
  }, [rows, accessors, sortKey, sortDir])

  return { sorted, sortKey, sortDir, toggle }
}

interface SortableHeadProps extends Omit<React.ComponentProps<'th'>, 'onClick'> {
  label: React.ReactNode
  sortKey: string
  activeKey: string
  dir: SortDir
  onSort: (key: string) => void
}

/**
 * A `TableHead` whose label is a click-to-sort button, mirroring the icon idiom
 * of the tanstack DataTable header (Up / Down / neutral UpDown). `aria-sort`
 * lives on the `<th>` per ARIA rules so screen readers announce the state and
 * the axe/keyboard gates stay green.
 */
export function SortableHead({
  label,
  sortKey,
  activeKey,
  dir,
  onSort,
  className,
  ...thProps
}: SortableHeadProps) {
  const active = activeKey === sortKey
  const ariaSort = active ? (dir === 'asc' ? 'ascending' : 'descending') : 'none'
  return (
    <TableHead aria-sort={ariaSort} className={cn('p-0', className)} {...thProps}>
      <button
        type="button"
        onClick={() => onSort(sortKey)}
        className="group/sort flex w-full items-center gap-1 px-2 py-2 text-left font-medium hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring rounded-sm"
      >
        <span className="truncate">{label}</span>
        <span className="ml-auto flex items-center shrink-0">
          {active ? (
            dir === 'asc' ? (
              <ArrowUp className="h-3.5 w-3.5" />
            ) : (
              <ArrowDown className="h-3.5 w-3.5" />
            )
          ) : (
            <ArrowUpDown className="h-3.5 w-3.5 opacity-0 group-hover/sort:opacity-50 transition-opacity" />
          )}
        </span>
      </button>
    </TableHead>
  )
}
