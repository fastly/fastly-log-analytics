'use client'

import React, { useEffect, useRef, useState } from 'react'
import { usePathname } from 'next/navigation'
import { useDebugStore } from '@/stores/debugStore'
import { Button } from '@/components/ui/button'
import { ChevronDown, ChevronUp, Database, HardDrive, Network, Trash2 } from 'lucide-react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Badge } from '@/components/ui/badge'
import { client, extractApiError } from '@/lib/api'
import { DEBUG_RESPONSES_COOKIE } from '@/lib/debug-cookie'
import type { components } from '@/types/api.generated'

type SqliteRecentResponse = components['schemas']['RecentSqliteResponse']
type SqliteEntry = components['schemas']['SqliteProfilerEntry']

export function DebugPanel() {
  const { enabled, apiCallsEnabled } = useDebugStore()
  const [isQueryOpen, setIsQueryOpen] = React.useState(false)
  const [isCallsOpen, setIsCallsOpen] = React.useState(false)
  const [isSqliteOpen, setIsSqliteOpen] = React.useState(false)
  const pathname = usePathname()
  const queryClient = useQueryClient()
  const [queries, setQueries] = useState<any[]>([])
  const [calls, setCalls] = useState<any[]>([])
  // SQLite scope: 'page' shows the statements embedded in this page's API
  // responses (_debug_sqlite, request-scoped on the backend); 'buffer'
  // shows the process-global ring buffer, which includes cron/background
  // statements and is the right view for debugging sync/alerts — but NOT
  // for "what did this page cost".
  const [sqliteScope, setSqliteScope] = useState<'page' | 'buffer'>('page')
  const [sqlitePage, setSqlitePage] = useState<SqliteEntry[]>([])
  // Mirror the latest state inside the subscribe callback so we can bail
  // out when the new extraction is semantically equal. Every cache event
  // (sqliteQuery's 5s poll + every API response) used to re-create the
  // arrays and call setQueries/setCalls with fresh references, which
  // re-rendered, which re-fired the cache subscribers, which looped to
  // "Maximum update depth exceeded" in dev.
  const queriesRef = useRef<any[]>([])
  const callsRef = useRef<any[]>([])
  const sqlitePageRef = useRef<SqliteEntry[]>([])

  // SQLite ring-buffer poll. Only active when SQL debug is on AND the
  // process-wide view is selected AND the browser tab is focused (skip
  // when hidden). Refetched every 5s — was 2s but that's ~30 req/min of
  // backend access-log noise per admin tab, dwarfing every other
  // endpoint. 5s is still real-time enough for the panel and 2.5×
  // quieter. The default 'page' view reads _debug_sqlite off the page's
  // own responses and needs no poll at all.
  const sqliteQuery = useQuery<SqliteRecentResponse>({
    queryKey: ['debug', 'recent-sqlite'],
    queryFn: async () => {
      const { data, error } = await client.GET('/api/debug/recent-sqlite', {
        params: { query: { limit: 500 } },
      })
      if (error) throw new Error(extractApiError(error) || 'recent-sqlite failed')
      return data as SqliteRecentResponse
    },
    enabled: enabled && sqliteScope === 'buffer',
    refetchInterval: 5000,
    refetchIntervalInBackground: false,
    staleTime: 0,
  })
  const sqliteEntries: SqliteEntry[] = sqliteScope === 'page' ? sqlitePage : (sqliteQuery.data?.queries ?? [])
  const totalSqliteTime = sqliteEntries.reduce((acc, q) => acc + q.time_ms, 0)

  const clearSqlite = async () => {
    await client.POST('/api/debug/clear-sqlite', {})
    sqliteQuery.refetch()
  }

  // Migration shim: the fla.debugResponses cookie (read by SSR's own fetch,
  // lib/ssr/_transport.ts) is written when a DiagnosticsPanel toggle FLIPS —
  // a browser whose toggle was already persisted on before the cookie
  // mechanism shipped never wrote it, so every SSR-prefetched page hydrates
  // without the debug envelope and this panel reads 0 queries / 0.00ms
  // forever. Heal here: when a toggle is on but the cookie isn't, write it
  // and invalidate the cache so the CURRENT page populates too. Invalidate
  // (not refetchQueries({type:'active'})): the heavy data queries mount
  // enabled:false until service/filter hydration resolves, so a point-in-time
  // refetch misses them — invalidation also marks them stale, forcing a
  // refetch the moment they switch on.
  useEffect(() => {
    if (!enabled && !apiCallsEnabled) return
    const hasCookie = document.cookie.split('; ').some((c) => c === `${DEBUG_RESPONSES_COOKIE}=1`)
    if (!hasCookie) {
      document.cookie = `${DEBUG_RESPONSES_COOKIE}=1; path=/; max-age=31536000; samesite=lax`
      queryClient.invalidateQueries()
    }
  }, [enabled, apiCallsEnabled, queryClient])

  useEffect(() => {
    if (!enabled && !apiCallsEnabled) {
      if (queriesRef.current.length > 0 || callsRef.current.length > 0 || sqlitePageRef.current.length > 0) {
        queriesRef.current = []
        callsRef.current = []
        sqlitePageRef.current = []
        setQueries([])
        setCalls([])
        setSqlitePage([])
      }
      return
    }

    const sameQueries = (a: any[], b: any[]) => {
      if (a.length !== b.length) return false
      for (let i = 0; i < a.length; i++) {
        if (a[i].sql !== b[i].sql || a[i].time_ms !== b[i].time_ms || a[i].is_cached !== b[i].is_cached) return false
      }
      return true
    }
    const sameCalls = (a: any[], b: any[]) => {
      if (a.length !== b.length) return false
      for (let i = 0; i < a.length; i++) {
        if (a[i].service !== b[i].service || a[i].method !== b[i].method || a[i].path !== b[i].path || a[i].time_ms !== b[i].time_ms) return false
      }
      return true
    }
    // seq is the profiler's process-global statement counter — unique per
    // statement, so it's both the dedup key and a sufficient identity check.
    const sameSqlite = (a: SqliteEntry[], b: SqliteEntry[]) => {
      if (a.length !== b.length) return false
      for (let i = 0; i < a.length; i++) {
        if (a[i].seq !== b[i].seq) return false
      }
      return true
    }

    const updateDebugInfo = () => {
      const extractedQueries: any[] = []
      const extractedCalls: any[] = []
      const extractedSqlite: SqliteEntry[] = []
      const seenSql = new Set<string>()
      const seenCalls = new Set<string>()
      const seenSqliteSeq = new Set<number>()

      const extractSqlite = (data: Record<string, unknown>) => {
        if (!enabled || !Array.isArray(data._debug_sqlite)) return
        for (const sq of data._debug_sqlite as SqliteEntry[]) {
          if (!seenSqliteSeq.has(sq.seq)) {
            extractedSqlite.push(sq)
            seenSqliteSeq.add(sq.seq)
          }
        }
      }

      // 1. Find all data from active queries
      const activeQueries = queryClient.getQueryCache().findAll({ type: 'active' })
      for (const q of activeQueries) {
        const data = q.state.data as any
        if (!data || typeof data !== 'object') continue

        // Extract queries
        if (enabled && '_debug_queries' in data && Array.isArray(data._debug_queries)) {
          const isCached = data._is_cached === true
          for (const dq of data._debug_queries) {
            if (!seenSql.has(dq.sql)) {
              extractedQueries.push({ ...dq, is_cached: isCached })
              seenSql.add(dq.sql)
            }
          }
        }

        // Extract API/FOS calls
        if (apiCallsEnabled && '_debug_calls' in data && Array.isArray(data._debug_calls)) {
          for (const dc of data._debug_calls) {
            const callKey = `${dc.service}-${dc.method}-${dc.path}`
            if (!seenCalls.has(callKey)) {
              extractedCalls.push(dc)
              seenCalls.add(callKey)
            }
          }
        }

        extractSqlite(data)
      }

      // 2. Also check mutations
      const activeMutations = queryClient.getMutationCache().findAll({ status: 'success' })
      for (const m of activeMutations) {
        const data = m.state.data as any
        if (!data || typeof data !== 'object') continue

        if (enabled && '_debug_queries' in data && Array.isArray(data._debug_queries)) {
          for (const dq of data._debug_queries) {
            if (!seenSql.has(dq.sql)) {
              extractedQueries.push({ ...dq, is_cached: false })
              seenSql.add(dq.sql)
            }
          }
        }

        if (apiCallsEnabled && '_debug_calls' in data && Array.isArray(data._debug_calls)) {
          for (const dc of data._debug_calls) {
            const callKey = `${dc.service}-${dc.method}-${dc.path}`
            if (!seenCalls.has(callKey)) {
              extractedCalls.push(dc)
              seenCalls.add(callKey)
            }
          }
        }

        extractSqlite(data)
      }

      // Chronological — seq is assigned at execution time on the backend.
      extractedSqlite.sort((a, b) => a.seq - b.seq)

      const queriesChanged = !sameQueries(extractedQueries, queriesRef.current)
      const callsChanged = !sameCalls(extractedCalls, callsRef.current)
      const sqliteChanged = !sameSqlite(extractedSqlite, sqlitePageRef.current)
      if (!queriesChanged && !callsChanged && !sqliteChanged) return
      queriesRef.current = extractedQueries
      callsRef.current = extractedCalls
      sqlitePageRef.current = extractedSqlite
      setTimeout(() => {
        if (queriesChanged) setQueries(extractedQueries)
        if (callsChanged) setCalls(extractedCalls)
        if (sqliteChanged) setSqlitePage(extractedSqlite)
      }, 0)
    }

    updateDebugInfo()

    const unsubQueries = queryClient.getQueryCache().subscribe(() => {
      updateDebugInfo()
    })
    const unsubMutations = queryClient.getMutationCache().subscribe(() => {
      updateDebugInfo()
    })

    return () => {
      unsubQueries()
      unsubMutations()
    }
  }, [enabled, apiCallsEnabled, queryClient, pathname])

  if (!enabled && !apiCallsEnabled) return null

  const totalQueryTime = queries.reduce((acc, q) => acc + q.time_ms, 0)
  const totalCallTime = calls.reduce((acc, c) => acc + c.time_ms, 0)
  const isCached = queries.some(q => q.is_cached)

  return (
    <div className="mt-12 border-t pt-8 space-y-8 pb-12">
      {enabled && (
        <section>
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="bg-primary/10 p-1.5 rounded-md">
                <Database className="h-4 w-4 text-primary" />
              </div>
              <h3 className="text-sm font-bold text-muted-foreground uppercase tracking-tight">DuckDB Queries</h3>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-[10px] px-2"
                onClick={() => setIsQueryOpen(!isQueryOpen)}
              >
                {isQueryOpen ? <ChevronUp className="h-3 w-3 mr-1" /> : <ChevronDown className="h-3 w-3 mr-1" />}
                {isQueryOpen ? 'Hide' : 'Show'} {queries.length} queries
              </Button>
            </div>
            <div className="flex items-center gap-3">
              <div className="text-xs font-mono bg-muted/50 border px-2.5 py-1.5 rounded-md text-muted-foreground flex items-center">
                {isCached && <span className="text-blue-500 font-bold mr-2 uppercase tracking-wider text-[10px] bg-blue-500/10 px-1.5 py-0.5 rounded">Cached Result</span>}
                <span>Total query time: <span className="font-bold text-foreground">{totalQueryTime.toFixed(2)}ms</span></span>
              </div>
            </div>
          </div>

          {isQueryOpen && (
            <div className="grid gap-4 max-h-[500px] overflow-auto pr-2 custom-scrollbar">
              {queries.map((q, i) => (
                <div key={`query-${i}-${q.sql.length}-${q.time_ms}`} className="bg-muted/30 p-4 rounded-md border font-mono text-[11px] relative group">
                  <div className="flex justify-between items-center mb-2 pb-2 border-b border-muted">
                    <span className="text-muted-foreground font-semibold">
                      QUERY #{i + 1} {q.is_cached && <span className="text-blue-500 ml-2">(CACHED)</span>}
                    </span>
                    <Badge variant={q.time_ms > 1000 ? "destructive" : q.time_ms > 200 ? "secondary" : "outline"} className="font-mono">
                      {q.time_ms}ms
                    </Badge>
                  </div>
                  <pre className="whitespace-pre-wrap overflow-x-auto text-muted-foreground leading-relaxed">{q.sql}</pre>
                </div>
              ))}
            </div>
          )}
        </section>
      )}

      {enabled && (
        <section>
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="bg-emerald-500/10 p-1.5 rounded-md">
                <HardDrive className="h-4 w-4 text-emerald-500" />
              </div>
              <h3 className="text-sm font-bold text-muted-foreground uppercase tracking-tight">SQLite Queries</h3>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-[10px] px-2"
                onClick={() => setIsSqliteOpen(!isSqliteOpen)}
              >
                {isSqliteOpen ? <ChevronUp className="h-3 w-3 mr-1" /> : <ChevronDown className="h-3 w-3 mr-1" />}
                {isSqliteOpen ? 'Hide' : 'Show'} {sqliteEntries.length} statements
              </Button>
              <div className="flex items-center rounded-md border overflow-hidden" role="group" aria-label="SQLite statement scope">
                <button
                  type="button"
                  className={`h-6 text-[10px] px-2 ${sqliteScope === 'page' ? 'bg-muted font-semibold text-foreground' : 'text-muted-foreground'}`}
                  onClick={() => setSqliteScope('page')}
                  title="Statements executed by this page's own API requests"
                >
                  This page
                </button>
                <button
                  type="button"
                  className={`h-6 text-[10px] px-2 border-l ${sqliteScope === 'buffer' ? 'bg-muted font-semibold text-foreground' : 'text-muted-foreground'}`}
                  onClick={() => setSqliteScope('buffer')}
                  title="Everything the backend process executed recently, including cron jobs"
                >
                  Process-wide
                </button>
              </div>
              {sqliteScope === 'buffer' && (
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 text-[10px] px-2 text-muted-foreground"
                  onClick={clearSqlite}
                  title="Clear the SQLite capture buffer"
                >
                  <Trash2 className="h-3 w-3 mr-1" />
                  Clear
                </Button>
              )}
            </div>
            <div className="flex items-center gap-3">
              <div className="text-xs font-mono bg-muted/50 border px-2.5 py-1.5 rounded-md text-muted-foreground flex items-center gap-2">
                {sqliteScope === 'buffer' && sqliteQuery.data && sqliteQuery.data.dropped > 0 && (
                  <span className="text-yellow-600 text-[10px] uppercase tracking-wider">
                    {sqliteQuery.data.dropped} dropped
                  </span>
                )}
                {sqliteScope === 'buffer' && (
                  <span>
                    Buffer: <span className="font-bold text-foreground">
                      {sqliteQuery.data?.buffer_size ?? 0}/{sqliteQuery.data?.buffer_cap ?? 0}
                    </span>
                  </span>
                )}
                <span>
                  Total time: <span className="font-bold text-foreground">{totalSqliteTime.toFixed(2)}ms</span>
                </span>
              </div>
            </div>
          </div>

          {isSqliteOpen && (
            <div className="grid gap-2 max-h-[500px] overflow-auto pr-2 custom-scrollbar">
              {sqliteEntries.length === 0 ? (
                <div className="text-muted-foreground text-xs italic p-4 text-center border rounded-md bg-muted/20">
                  {sqliteScope === 'page'
                    ? 'No SQLite statements were executed by this page’s API requests. Switch to Process-wide to see cron/background activity.'
                    : 'No SQLite statements captured yet. Statements appear here as cron jobs and metadata reads execute.'}
                </div>
              ) : (
                [...sqliteEntries].reverse().map((q) => (
                  <div key={q.seq} className="bg-muted/30 p-3 rounded-md border font-mono text-[11px]">
                    <div className="flex justify-between items-center mb-1.5 pb-1.5 border-b border-muted gap-2">
                      <div className="flex items-center gap-2 flex-wrap">
                        <Badge variant="outline" className="text-[9px] uppercase">
                          #{q.seq}
                        </Badge>
                        <Badge variant="secondary" className="text-[9px] uppercase opacity-80">
                          {q.op}
                        </Badge>
                        <span className="text-[9px] text-muted-foreground opacity-70">
                          params: {q.params_kind}
                        </span>
                        {q.rows >= 0 && (
                          <span className="text-[9px] text-muted-foreground opacity-70">
                            rowcount: {q.rows}
                          </span>
                        )}
                        <span className="text-[9px] text-muted-foreground opacity-50">
                          {q.ts.split('T')[1]?.slice(0, 12)}
                        </span>
                      </div>
                      <Badge
                        variant={q.time_ms > 100 ? 'destructive' : q.time_ms > 10 ? 'secondary' : 'outline'}
                        className="font-mono shrink-0"
                      >
                        {q.time_ms.toFixed(2)}ms
                      </Badge>
                    </div>
                    <pre className="whitespace-pre-wrap overflow-x-auto text-muted-foreground leading-relaxed text-[10px]">
                      {q.sql}
                    </pre>
                  </div>
                ))
              )}
            </div>
          )}
        </section>
      )}

      {apiCallsEnabled && (
        <section>
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="bg-orange-500/10 p-1.5 rounded-md">
                <Network className="h-4 w-4 text-orange-600" />
              </div>
              <h3 className="text-sm font-bold text-muted-foreground uppercase tracking-tight">Fastly API & FOS Calls</h3>
              <Button
                variant="ghost"
                size="sm"
                className="h-6 text-[10px] px-2"
                onClick={() => setIsCallsOpen(!isCallsOpen)}
              >
                {isCallsOpen ? <ChevronUp className="h-3 w-3 mr-1" /> : <ChevronDown className="h-3 w-3 mr-1" />}
                {isCallsOpen ? 'Hide' : 'Show'} {calls.length} calls
              </Button>
            </div>
            <div className="flex items-center gap-3">
              <div className="text-xs font-mono bg-muted/50 border px-2.5 py-1.5 rounded-md text-muted-foreground flex items-center">
                <span>Total call time: <span className="font-bold text-foreground">{totalCallTime.toFixed(2)}ms</span></span>
              </div>
            </div>
          </div>

          {isCallsOpen && (
            <div className="grid gap-3 max-h-[500px] overflow-auto pr-2 custom-scrollbar">
              {calls.length === 0 ? (
                <div className="text-muted-foreground text-xs italic p-4 text-center border rounded-md bg-muted/20">
                  No Fastly API or FOS calls were made during this request.
                </div>
              ) : (
                calls.map((c, i) => (
                  <div key={`${c.service}-${c.method}-${c.path}-${i}`} className="bg-muted/30 p-3 rounded-md border font-mono text-[11px] flex flex-col gap-2">
                    <div className="flex justify-between items-start">
                      <div className="flex items-center gap-2 flex-wrap">
                        <Badge variant="outline" className={`text-[10px] ${c.service === 'FOS' ? 'bg-blue-500/10 text-blue-500 border-blue-500/20' : 'bg-orange-500/10 text-orange-500 border-orange-500/20'}`}>
                          {c.service}
                        </Badge>
                        {c.caller && (
                          <Badge variant="secondary" className="text-[10px] font-mono opacity-80">
                            {c.caller}()
                          </Badge>
                        )}
                        <span className="font-bold text-foreground">{c.method}</span>
                        <span className="text-muted-foreground break-all">{c.path}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <Badge variant={c.status === 'Error' ? 'destructive' : 'outline'} className="text-[10px]">
                          {c.status}
                        </Badge>
                        <span className={`font-bold ${c.time_ms > 1000 ? 'text-red-500' : c.time_ms > 200 ? 'text-yellow-600' : 'text-green-500'}`}>
                          {c.time_ms.toFixed(1)}ms
                        </span>
                      </div>
                    </div>
                    {c.details && (
                      <div className="text-[10px] text-muted-foreground bg-muted/50 p-2 rounded border border-muted/50">
                        {c.details}
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>
          )}
        </section>
      )}
    </div>
  )
}
