'use client'

/**
 * Live Query Monitor — real-time view of every executing SQLite/DuckDB
 * statement, with attribution, caller frame, pool slot, and a kind-aware
 * kill button. Admin-only (the route lives under /api/admin/* so
 * RemoteAccessMiddleware structurally blocks analyst sessions).
 *
 * This file is the orchestrator: state machinery + data wiring. Layout
 * details live in `_sections/` and shared types/helpers in `_types.ts` /
 * `_helpers.ts`. Phase 9b split kept this file under 500 lines per
 * cleanup_plan §9b.
 */

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, ArrowLeft, Keyboard, Layers, Search, Volume2, VolumeX } from 'lucide-react'
import Link from 'next/link'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button, buttonVariants } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Input } from '@/components/ui/input'
import { PageHeader } from '@/components/ui/page-header'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { extractApiError } from '@/lib/api'

import { useDocumentVisible } from './_helpers'
import { useKeyboardShortcuts, type ShortcutBinding } from './_hooks/useKeyboardShortcuts'
import { ActiveTable } from './_sections/ActiveTable'
import { CompletedTable } from './_sections/CompletedTable'
import { FilterChips } from './_sections/FilterChips'
import { PollingIndicator } from './_sections/PollingIndicator'
import { ShortcutsHelp } from './_sections/ShortcutsHelp'
import { SummaryStrip } from './_sections/SummaryStrip'
import type {
  ActiveOrPromotedRow,
  ActiveRow,
  AttributionKind,
  CancelResponse,
  CompletedRow,
  MonitorConfig,
  SnapshotResponse,
  ViewMode,
} from './_types'

const SOUND_STORAGE_KEY = 'qm:sound-enabled'
const DEFAULT_SLOW_THRESHOLD_MS = 500

export default function QueryMonitorPage() {
  const queryClient = useQueryClient()
  const visible = useDocumentVisible()
  const [expandedQid, setExpandedQid] = React.useState<number | null>(null)
  const [search, setSearch] = React.useState('')
  const [kindFilter, setKindFilter] = React.useState<AttributionKind | 'all'>('all')
  const [confirmKill, setConfirmKill] = React.useState<ActiveRow | null>(null)
  const [actionError, setActionError] = React.useState<string>('')
  const [viewMode, setViewMode] = React.useState<ViewMode>('all')
  const [slowThresholdMs, setSlowThresholdMs] = React.useState(DEFAULT_SLOW_THRESHOLD_MS)
  const [groupByRun, setGroupByRun] = React.useState(false)
  const [focusedQid, setFocusedQid] = React.useState<number | null>(null)
  const [shortcutsOpen, setShortcutsOpen] = React.useState(false)
  const [soundEnabled, setSoundEnabled] = React.useState(false)
  const searchInputRef = React.useRef<HTMLInputElement>(null)

  // Hydrate filter state from URL on mount. Single-shot; subsequent
  // changes flow URL ← state via the write effect below. Pattern mirrors
  // `useFilterUrlSync` — replaceState (not router.replace) so Next doesn't
  // refresh the page on every filter tweak.
  const hydratedRef = React.useRef(false)
  React.useEffect(() => {
    if (hydratedRef.current) return
    if (typeof window === 'undefined') return
    const p = new URLSearchParams(window.location.search)
    const q = p.get('q')
    const kind = p.get('kind')
    const view = p.get('view')
    const slow = p.get('slow')
    const group = p.get('group')
    if (q !== null) setSearch(q)
    if (kind === 'analyst' || kind === 'admin' || kind === 'cron' || kind === 'system') {
      setKindFilter(kind)
    }
    if (view === 'live' || view === 'past' || view === 'all') setViewMode(view as ViewMode)
    if (slow !== null) {
      const n = parseInt(slow, 10)
      if (Number.isFinite(n) && n > 0) setSlowThresholdMs(n)
    }
    if (group === 'run') setGroupByRun(true)
    // Restore the sound preference (localStorage so it persists across
    // sessions for this browser without leaking into the URL).
    try {
      const stored = window.localStorage.getItem(SOUND_STORAGE_KEY)
      if (stored === '1') setSoundEnabled(true)
    } catch {
      // localStorage blocked (Safari private mode etc) — silently ignore.
    }
    hydratedRef.current = true
  }, [])

  // Write filter/view state to URL on change. Stripped to only the
  // non-default values so the URL stays clean for default views.
  React.useEffect(() => {
    if (!hydratedRef.current) return
    if (typeof window === 'undefined') return
    const url = new URL(window.location.href)
    if (search) url.searchParams.set('q', search)
    else url.searchParams.delete('q')
    if (kindFilter !== 'all') url.searchParams.set('kind', kindFilter)
    else url.searchParams.delete('kind')
    if (viewMode !== 'all') url.searchParams.set('view', viewMode)
    else url.searchParams.delete('view')
    if (slowThresholdMs !== DEFAULT_SLOW_THRESHOLD_MS) url.searchParams.set('slow', String(slowThresholdMs))
    else url.searchParams.delete('slow')
    if (groupByRun) url.searchParams.set('group', 'run')
    else url.searchParams.delete('group')
    window.history.replaceState({}, '', url.toString())
  }, [search, kindFilter, viewMode, slowThresholdMs, groupByRun])

  // Persist the sound toggle separately — localStorage, not URL, since
  // it's a user preference rather than a shareable view.
  React.useEffect(() => {
    if (!hydratedRef.current) return
    if (typeof window === 'undefined') return
    try {
      window.localStorage.setItem(SOUND_STORAGE_KEY, soundEnabled ? '1' : '0')
    } catch {
      // ignore
    }
  }, [soundEnabled])

  // Feature-flag check; if disabled, render a clear empty state.
  const { data: cfg } = useQuery<MonitorConfig>({
    queryKey: ['admin', 'query-monitor', 'config'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/app-config/query-monitor', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    staleTime: 60_000,
  })

  const enabled = cfg?.enabled !== false

  // 300ms while the page is open. The snapshot endpoint returns in <1ms
  // server-side; real analyst/cron queries finish in 0.2-30ms (verified on
  // prod 2026-06-11), so anything slower than 300ms polling means the
  // Active list reads empty even when the system is busy. The cost is one
  // tiny GET every 300ms per admin tab — order of nothing.
  const snapshotQuery = useQuery<SnapshotResponse>({
    queryKey: ['admin', 'query-monitor', 'snapshot'],
    queryFn: async ({ signal }) => {
      const r = await fetch('/api/admin/queries?include_completed=true', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: visible && enabled,
    refetchInterval: 300,
    refetchIntervalInBackground: false,
  })

  const cancelMutation = useMutation({
    mutationFn: async (qid: number): Promise<CancelResponse> => {
      const r = await fetch(`/api/admin/queries/${qid}/cancel`, { method: 'POST' })
      if (!r.ok) {
        const body = await r.text().catch(() => '')
        throw new Error(body || `status ${r.status}`)
      }
      return r.json()
    },
    onSuccess: (res) => {
      setActionError('')
      // Force an immediate refetch so the cancellation appears in the UI
      // without waiting for the next polling tick.
      queryClient.invalidateQueries({ queryKey: ['admin', 'query-monitor', 'snapshot'] })
      if (res.state !== 'cancelled' && res.state !== 'already_finished') {
        setActionError(`Cancel returned: ${res.state}`)
      }
    },
    onError: (err: Error) => setActionError(extractApiError(err) || err.message),
  })

  // "Just finished" — anything that completed in the last 10 seconds.
  // Promoted into the Active section as a faded row with the outcome pill
  // so the user gets visual feedback even when real queries are sub-300ms.
  // Without this the Active list reads empty on typical traffic (verified
  // on prod 2026-06-11: p50 query duration 0.2ms, max 29ms — far below
  // any poll cadence).
  const JUST_FINISHED_WINDOW_S = 10
  const justFinished = React.useMemo(() => {
    const completed = snapshotQuery.data?.completed ?? []
    const cutoff = Date.now() / 1000 - JUST_FINISHED_WINDOW_S
    return completed.filter((c) => c.ended_at_utc >= cutoff)
  }, [snapshotQuery.data])

  // Notable slow queries — anything that took longer than the threshold,
  // regardless of how long ago it finished. The most-investigated case in
  // ops: "I saw the dashboard get slow a minute ago, what was running?".
  // The history ring buffer caps at 200 rows (server-side), so the lookback
  // window in practice is "as far back as the buffer goes".
  const slowQueries = React.useMemo(() => {
    const completed = snapshotQuery.data?.completed ?? []
    return [...completed]
      .filter((c) => c.duration_ms >= slowThresholdMs)
      .sort((a, b) => b.duration_ms - a.duration_ms)
      .slice(0, 30)
  }, [snapshotQuery.data, slowThresholdMs])

  // Filter / search the active list (active rows + just-finished promotions).
  const filteredActive = React.useMemo(() => {
    const active: ActiveOrPromotedRow[] = (snapshotQuery.data?.active ?? []).map((r) => ({ ...r }))
    const justRows: ActiveOrPromotedRow[] = justFinished.map((c: CompletedRow) => ({
      query_id: c.query_id,
      db_type: c.db_type,
      sql_preview: c.sql_preview,
      sql: c.sql,
      sql_len: c.sql_len,
      attribution: c.attribution,
      service_id: c.service_id,
      started_at_utc: c.started_at_utc,
      duration_ms: c.duration_ms,
      cancellable: false,
      cancelled_at: null,
      _completed: c,
    }))
    // Newest first, no dupes (a row could theoretically appear in both).
    const seen = new Set<number>()
    const combined: ActiveOrPromotedRow[] = []
    for (const r of [...active, ...justRows]) {
      if (seen.has(r.query_id)) continue
      seen.add(r.query_id)
      combined.push(r)
    }
    const q = search.trim().toLowerCase()
    return combined.filter((r) => {
      if (kindFilter !== 'all' && r.attribution.kind !== kindFilter) return false
      if (!q) return true
      return (
        r.sql_preview.toLowerCase().includes(q) ||
        r.attribution.caller_qualname.toLowerCase().includes(q) ||
        r.attribution.caller_file.toLowerCase().includes(q) ||
        r.attribution.label.toLowerCase().includes(q)
      )
    })
  }, [snapshotQuery.data, justFinished, search, kindFilter])

  const completed = snapshotQuery.data?.completed ?? []

  // Keep focusedQid valid: drop it if the focused row is no longer in
  // the visible list. The keyboard nav clamps to first/last via
  // `navigateFocus`, but a row that disappeared between renders needs
  // to be cleared so `x` doesn't try to cancel a stale id.
  React.useEffect(() => {
    if (focusedQid === null) return
    if (!filteredActive.some((r) => r.query_id === focusedQid)) {
      setFocusedQid(null)
    }
  }, [filteredActive, focusedQid])

  // Sound notification on NEW errors. Tracks which error query_ids we've
  // already announced so a row sticking around in the completed window
  // doesn't beep on every poll. Reset when the user disables sound.
  const announcedErrorIdsRef = React.useRef<Set<number>>(new Set())
  React.useEffect(() => {
    if (!soundEnabled) {
      announcedErrorIdsRef.current.clear()
      return
    }
    const errorRows = (snapshotQuery.data?.completed ?? []).filter((c) => c.outcome === 'error')
    const newOnes = errorRows.filter((c) => !announcedErrorIdsRef.current.has(c.query_id))
    if (newOnes.length === 0) return
    for (const c of newOnes) announcedErrorIdsRef.current.add(c.query_id)
    // First poll while sound is on: don't beep retroactively for errors
    // that completed before the user enabled sound. Only beep when we
    // already had a baseline.
    if (announcedErrorIdsRef.current.size === newOnes.length) return
    playErrorTone()
  }, [snapshotQuery.data, soundEnabled])

  const requestKill = React.useCallback(
    (row: ActiveRow) => {
      setActionError('')
      if (row.attribution.kind === 'cron' || row.attribution.kind === 'system') {
        setConfirmKill(row)
      } else {
        cancelMutation.mutate(row.query_id)
      }
    },
    [cancelMutation],
  )

  const navigateFocus = React.useCallback(
    (delta: number) => {
      if (filteredActive.length === 0) return
      const ids = filteredActive.map((r) => r.query_id)
      if (focusedQid === null) {
        setFocusedQid(delta > 0 ? ids[0] : ids[ids.length - 1])
        return
      }
      const i = ids.indexOf(focusedQid)
      if (i === -1) {
        setFocusedQid(ids[0])
        return
      }
      const next = Math.max(0, Math.min(ids.length - 1, i + delta))
      setFocusedQid(ids[next])
    },
    [filteredActive, focusedQid],
  )

  const shortcuts = React.useMemo<ShortcutBinding[]>(
    () => [
      {
        key: '/',
        description: 'Focus the search field',
        handler: (e) => {
          e.preventDefault()
          searchInputRef.current?.focus()
          searchInputRef.current?.select()
        },
      },
      {
        key: 'j',
        description: 'Focus next row',
        handler: (e) => {
          e.preventDefault()
          navigateFocus(1)
        },
      },
      {
        key: 'k',
        description: 'Focus previous row',
        handler: (e) => {
          e.preventDefault()
          navigateFocus(-1)
        },
      },
      {
        key: 'Enter',
        description: 'Toggle expand on focused row',
        handler: (e) => {
          if (focusedQid === null) return
          e.preventDefault()
          setExpandedQid((prev) => (prev === focusedQid ? null : focusedQid))
        },
      },
      {
        key: 'x',
        description: 'Cancel focused query',
        handler: (e) => {
          if (focusedQid === null) return
          const row = filteredActive.find((r) => r.query_id === focusedQid && !r._completed) as
            | ActiveRow
            | undefined
          if (!row || !row.cancellable || row.cancelled_at !== null) return
          e.preventDefault()
          requestKill(row)
        },
      },
      {
        key: '?',
        description: 'Show keyboard shortcuts',
        handler: (e) => {
          e.preventDefault()
          setShortcutsOpen(true)
        },
      },
      {
        key: 'Escape',
        description: 'Close drawer / dialog / overlay',
        allowInForms: true,
        handler: () => {
          if (shortcutsOpen) {
            setShortcutsOpen(false)
            return
          }
          if (confirmKill) {
            setConfirmKill(null)
            return
          }
          if (expandedQid !== null) {
            setExpandedQid(null)
            return
          }
          // Last resort: blur the search input so the user can immediately
          // start using row-level shortcuts.
          if (document.activeElement === searchInputRef.current) {
            searchInputRef.current?.blur()
          }
        },
      },
    ],
    [navigateFocus, focusedQid, filteredActive, requestKill, shortcutsOpen, confirmKill, expandedQid],
  )

  useKeyboardShortcuts(shortcuts, enabled)

  return (
    <div className="space-y-6">
      <PageHeader
        title="Live Query Monitor"
        description="Real-time view of every executing DuckDB and SQLite query. Click a row to expand."
      >
        <Link href="/admin" className={buttonVariants({ variant: 'secondary', size: 'sm' })}>
          <ArrowLeft className="h-4 w-4 mr-1" /> Back to Admin
        </Link>
      </PageHeader>

      {!enabled && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Live Query Monitor is disabled</AlertTitle>
          <AlertDescription>
            Set <code>QUERY_MONITOR_ENABLED=1</code> in the backend environment to enable.
          </AlertDescription>
        </Alert>
      )}

      {enabled && (
        <>
          <div className="flex items-center justify-between gap-3">
            <SummaryStrip />
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="sm"
                className="h-8 px-2"
                onClick={() => setSoundEnabled((v) => !v)}
                title={soundEnabled ? 'Disable sound on errors' : 'Enable sound on errors'}
                aria-pressed={soundEnabled}
              >
                {soundEnabled ? (
                  <Volume2 className="h-4 w-4 text-primary" />
                ) : (
                  <VolumeX className="h-4 w-4 text-muted-foreground" />
                )}
                <span className="sr-only">
                  {soundEnabled ? 'Disable sound notifications' : 'Enable sound notifications'}
                </span>
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="h-8 px-2"
                onClick={() => setShortcutsOpen(true)}
                title="Keyboard shortcuts (?)"
              >
                <Keyboard className="h-4 w-4 text-muted-foreground" />
                <span className="sr-only">Show keyboard shortcuts</span>
              </Button>
            </div>
          </div>

          {actionError && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{actionError}</AlertDescription>
            </Alert>
          )}

          <Tabs value={viewMode} onValueChange={(v) => setViewMode(v as ViewMode)}>
            <TabsList>
              <TabsTrigger value="all">All</TabsTrigger>
              <TabsTrigger value="live">Live only</TabsTrigger>
              <TabsTrigger value="past">Past only</TabsTrigger>
            </TabsList>
          </Tabs>

          {viewMode !== 'past' && (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  Active &amp; Just-Finished
                  <Badge variant="secondary">
                    {snapshotQuery.data?.active?.length ?? 0} active
                    {justFinished.length > 0 && ` + ${justFinished.length} just-finished`}
                  </Badge>
                  <PollingIndicator
                    visible={visible}
                    isFetching={snapshotQuery.isFetching}
                    isError={snapshotQuery.isError}
                  />
                </CardTitle>
                <div className="flex items-center gap-2">
                  <Button
                    variant={groupByRun ? 'default' : 'outline'}
                    size="sm"
                    className="h-8 px-2 text-xs gap-1.5"
                    onClick={() => setGroupByRun((v) => !v)}
                    title="Group cron rows by run id"
                    aria-pressed={groupByRun}
                  >
                    <Layers className="h-3.5 w-3.5" /> Group runs
                  </Button>
                  <FilterChips value={kindFilter} onChange={setKindFilter} />
                  <div className="relative">
                    <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                    <Input
                      ref={searchInputRef}
                      placeholder="Filter by SQL or caller…"
                      value={search}
                      onChange={(e) => setSearch(e.target.value)}
                      className="h-8 w-64 pl-7 text-sm"
                    />
                  </div>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <ActiveTable
                  rows={filteredActive}
                  expandedQid={expandedQid}
                  onToggleRow={(qid) => setExpandedQid(expandedQid === qid ? null : qid)}
                  onKill={requestKill}
                  cancellingQid={cancelMutation.variables ?? null}
                  focusedQid={focusedQid}
                  groupByRun={groupByRun}
                />
              </CardContent>
            </Card>
          )}

          {viewMode !== 'live' && (
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  Notable Slow Queries
                  <Badge variant="outline">{slowQueries.length}</Badge>
                  <span className="text-xs text-muted-foreground font-normal">
                    ≥ {slowThresholdMs < 1000 ? `${slowThresholdMs} ms` : `${slowThresholdMs / 1000}s`},
                    sorted slowest first
                  </span>
                </CardTitle>
                <div className="flex items-center gap-1">
                  {[100, 500, 1000, 2000, 5000].map((ms) => (
                    <Button
                      key={ms}
                      variant={slowThresholdMs === ms ? 'default' : 'outline'}
                      size="sm"
                      className="h-7 px-2 text-xs"
                      onClick={() => setSlowThresholdMs(ms)}
                    >
                      {ms < 1000 ? `${ms}ms` : `${ms / 1000}s`}
                    </Button>
                  ))}
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <CompletedTable
                  rows={slowQueries}
                  preserveOrder
                  emptyMessage={`No queries ≥ ${slowThresholdMs < 1000 ? slowThresholdMs + ' ms' : slowThresholdMs / 1000 + ' s'} in recent history.`}
                />
              </CardContent>
            </Card>
          )}

          {viewMode !== 'live' && (
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center gap-2">
                  Recently Completed
                  <Badge variant="outline">{completed.length}</Badge>
                </CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <CompletedTable rows={completed} />
              </CardContent>
            </Card>
          )}
        </>
      )}

      {confirmKill && (
        <ConfirmDialog
          open={!!confirmKill}
          onOpenChange={(open) => !open && setConfirmKill(null)}
          title={`Cancel ${confirmKill.attribution.kind} query?`}
          description={
            confirmKill.attribution.kind === 'cron'
              ? `This is a background ${confirmKill.attribution.cron_job || 'cron'} job. Cancelling may leave its work partial; the next tick will reconcile.`
              : `This is a system query (${confirmKill.attribution.caller_qualname}). Cancelling is rarely the right action.`
          }
          confirmLabel="Cancel query"
          isDangerous
          onConfirm={() => {
            cancelMutation.mutate(confirmKill.query_id)
            setConfirmKill(null)
          }}
        />
      )}

      <ShortcutsHelp open={shortcutsOpen} onOpenChange={setShortcutsOpen} />
    </div>
  )
}

/** Short, attention-getting blip via Web Audio. ~200ms total. No asset
 *  to ship and no permission prompt — just two oscillator pings. Wrapped
 *  in try/catch because AudioContext can throw if the user hasn't yet
 *  interacted with the page (browsers gate autoplay-style audio). The
 *  toggle is opt-in and the user clicks it, which counts as interaction
 *  for the AudioContext gesture requirement. */
function playErrorTone() {
  try {
    const AC = window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext
    if (!AC) return
    const ctx = new AC()
    const now = ctx.currentTime
    for (const [freq, delay] of [
      [880, 0],
      [660, 0.12],
    ] as const) {
      const osc = ctx.createOscillator()
      const gain = ctx.createGain()
      osc.type = 'sine'
      osc.frequency.value = freq
      // Quick attack + decay so it sounds like a UI ping, not a bell.
      gain.gain.setValueAtTime(0.0001, now + delay)
      gain.gain.exponentialRampToValueAtTime(0.12, now + delay + 0.01)
      gain.gain.exponentialRampToValueAtTime(0.0001, now + delay + 0.10)
      osc.connect(gain).connect(ctx.destination)
      osc.start(now + delay)
      osc.stop(now + delay + 0.12)
    }
    // Close the context after the tones finish so we don't leak audio
    // graph state in the page.
    setTimeout(() => ctx.close().catch(() => {}), 400)
  } catch {
    // Autoplay blocked, no Web Audio, etc. The toggle being on is best
    // effort — silent failure beats a crash on a notification.
  }
}
