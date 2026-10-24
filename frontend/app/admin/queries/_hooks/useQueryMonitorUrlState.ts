'use client'

import * as React from 'react'
import { createParser, parseAsString, parseAsStringLiteral, useQueryStates } from 'nuqs'

import type { AttributionKind, DbFilter, ViewMode } from '../_types'

/**
 * URL-backed filter state for the Live Query Monitor, via nuqs.
 *
 * Param names and formats are the shareable ops-link contract, e.g.
 * `…/admin/queries?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1`.
 * Defaults are stripped on write (`clearOnDefault`) so a clean view yields a
 * clean URL; unknown or malformed values fall back to the default, as the
 * previous hand-rolled hydration did.
 */
const KINDS = ['all', 'analyst', 'admin', 'cron', 'system'] as const satisfies readonly (AttributionKind | 'all')[]
const VIEWS = ['all', 'live', 'past'] as const satisfies readonly ViewMode[]
const DBS = ['all', 'DuckDB', 'Postgres', 'SQLite'] as const satisfies readonly DbFilter[]

// parseInt (not Number) on purpose: `?slow=250ms` keeps hydrating to 250.
const parseAsPositiveInt = createParser<number>({
  parse: (v) => {
    const n = parseInt(v, 10)
    return Number.isFinite(n) && n > 0 ? n : null
  },
  serialize: String,
})

// Stored as the negative (`nogroup=1`) so the default URL stays clean.
const parseAsNoGroupFlag = createParser<boolean>({
  parse: (v) => (v === '1' ? true : null),
  serialize: () => '1',
})

const queryMonitorUrlParsers = (defaultSlowMs: number) => ({
  q: parseAsString.withDefault('').withOptions({ clearOnDefault: true }),
  kind: parseAsStringLiteral(KINDS).withDefault('all').withOptions({ clearOnDefault: true }),
  view: parseAsStringLiteral(VIEWS).withDefault('all').withOptions({ clearOnDefault: true }),
  slow: parseAsPositiveInt.withDefault(defaultSlowMs).withOptions({ clearOnDefault: true }),
  db: parseAsStringLiteral(DBS).withDefault('all').withOptions({ clearOnDefault: true }),
  nogroup: parseAsNoGroupFlag.withDefault(false).withOptions({ clearOnDefault: true }),
})

type Updater<T> = T | ((prev: T) => T)

export function useQueryMonitorUrlState(defaultSlowMs: number) {
  const [params, setParams] = useQueryStates(queryMonitorUrlParsers(defaultSlowMs), {
    history: 'replace',
    shallow: true,
  })

  const setSearch = React.useCallback((v: string) => void setParams({ q: v }), [setParams])
  const setKindFilter = React.useCallback(
    (v: AttributionKind | 'all') => void setParams({ kind: v }),
    [setParams],
  )
  const setDbFilter = React.useCallback((v: DbFilter) => void setParams({ db: v }), [setParams])
  const setViewMode = React.useCallback((v: ViewMode) => void setParams({ view: v }), [setParams])
  const setSlowThresholdMs = React.useCallback((n: number) => void setParams({ slow: n }), [setParams])
  const setGroupCrons = React.useCallback(
    (v: Updater<boolean>) =>
      void setParams((prev) => ({ nogroup: !(typeof v === 'function' ? v(!prev.nogroup) : v) })),
    [setParams],
  )

  return {
    search: params.q,
    kindFilter: params.kind,
    dbFilter: params.db,
    viewMode: params.view,
    slowThresholdMs: params.slow,
    groupCrons: !params.nogroup,
    setSearch,
    setKindFilter,
    setDbFilter,
    setViewMode,
    setSlowThresholdMs,
    setGroupCrons,
  }
}
