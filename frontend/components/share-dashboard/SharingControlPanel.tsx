'use client'

import * as React from 'react'
import { Activity, ArrowDown, ArrowUp, ArrowUpDown, ExternalLink, Loader2, Lock, Wifi, WifiOff, X } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { useMounted } from '@/hooks/useBootstrap'
import { client, extractApiError, unwrapClient } from "@/lib/api";
import { cn, formatDeterministicUTC, formatDuration } from "@/lib/utils";
import { useSearchParams } from 'next/navigation'
import type { Simplify, WithRequired, WithRequiredNullable } from '@/types/api'
import type { components } from '@/types/api.generated'

export function useShareMutation(
  onError: (msg: string) => void,
  onRefresh?: () => Promise<void> | void,
) {
  const [busy, setBusy] = React.useState(false)
  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true)
    try {
      await fn()
      await onRefresh?.()
    } catch (e) {
      onError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }
  return { busy, run }
}

type InviteWire = components['schemas']['InviteRecord']
type ShareServiceWire = components['schemas']['ShareServiceEntry']
type ShareStatusWire = components['schemas']['ShareStatusResponse']

export type Invite = WithRequired<InviteWire, 'id' | 'name' | 'email'>

export type ShareSession = components['schemas']['ShareSessionRecord']
export type AuditLog = components['schemas']['ShareAuditLogRow']

export type ShareStatus = Simplify<
  Omit<
    WithRequiredNullable<ShareStatusWire, 'public_endpoint' | 'public_url' | 'forward_port' | 'started_at'>,
    'sharing_active' | 'max_concurrent_sessions' | 'active_session_count' | 'services' | 'invites' | 'sessions' | 'audit_logs'
  > &
    Pick<
      WithRequired<ShareStatusWire, 'sharing_active' | 'max_concurrent_sessions' | 'active_session_count'>,
      'sharing_active' | 'max_concurrent_sessions' | 'active_session_count'
    > & {
      services: WithRequired<ShareServiceWire, 'service_id' | 'name'>[]
      invites: Invite[]
      sessions: ShareSession[]
      audit_logs: AuditLog[]
    }
>

type SortDir = 'asc' | 'desc'

type SortValue = string | number | null | undefined

export type SortAccessors<T> = Record<string, (row: T) => SortValue>

interface TableSort<T> {
  sorted: T[]
  sortKey: string
  sortDir: SortDir
  toggle: (key: string) => void
}

const isMissing = (v: SortValue): boolean => v === null || v === undefined || v === ''

function compareValues(a: SortValue, b: SortValue): number {
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
}

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
      setSortDir('desc')
      return key
    })
  }, [])

  const sorted = React.useMemo(() => {
    const accessor = accessors[sortKey]
    if (!accessor) return rows
    const factor = sortDir === 'asc' ? 1 : -1
    return rows
      .map((row, i) => ({ row, i }))
      .sort((x, y) => {
        const av = accessor(x.row)
        const bv = accessor(y.row)
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

interface SharingControlPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
}

type SharingMode = 'hostname' | 'ip'

const IPV4_RE = /^(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?$/

function buildEndpoint(raw: string): string {
  const trimmed = raw.trim()
  if (!trimmed) return ''
  // If the user already typed a scheme, trust it; otherwise prepend https://.
  return /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`
}

export function SharingControlPanel({ status, onRefresh, onError }: SharingControlPanelProps) {
  const { busy, run } = useShareMutation(onError, onRefresh)
  // /admin/share's share_status is dehydrated into the SSR HTML, so any value
  // that ticks down by the second drifts between the server render and the
  // (slightly later) first client render → React #418. This gates the two
  // live counters below — telemetry uptime and the rate-limit lockout
  // countdown — so server HTML and first client render agree; the live values
  // fill in right after mount. Sibling to formatDeterministicUTC's fix in lib/date.ts.
  const mounted = useMounted()
  const [mode, setMode] = React.useState<SharingMode>('hostname')
  const [hostnameDraft, setHostnameValue] = React.useState('')
  const [ipValue, setIpValue] = React.useState('')

  const searchParams = useSearchParams()
  const activeServiceId = searchParams?.get('service')
  const activeService = React.useMemo(() => {
    return status?.services?.find((s) => s.service_id === activeServiceId)
  }, [status?.services, activeServiceId])

  // An empty field shows the service's saved sharing domain (derived, not copied into state).
  const hostnameValue = hostnameDraft || activeService?.sharing_domain || ''
  // Editable copy of the global concurrent-analyst cap. `capDraft` is null
  // while the input mirrors the server value; once the admin types it holds
  // their edit. Deriving the displayed value during render (rather than syncing
  // via an effect) means a refresh that brings a new server value is reflected
  // automatically whenever the field isn't being edited.
  const [capDraft, setCapDraft] = React.useState<string | null>(null)
  const [capBusy, setCapBusy] = React.useState(false)
  const serverCap = status?.max_concurrent_sessions
  const capValue = capDraft ?? (serverCap != null ? String(serverCap) : '')
  const capDirty = capDraft != null && capDraft !== String(serverCap ?? '')

  const sharingActive = !!status?.sharing_active

  const handleStart = () => {
    onError('')
    const raw = mode === 'hostname' ? hostnameValue : ipValue
    const publicEndpoint = buildEndpoint(raw)
    if (!publicEndpoint) {
      onError(
        mode === 'hostname'
          ? 'Enter a hostname (e.g. logs.example.com).'
          : 'Enter an IP address (e.g. 203.0.113.42:8443).',
      )
      return
    }
    if (mode === 'ip' && !IPV4_RE.test(raw.trim())) {
      onError('Expected an IPv4 address, optionally with a port (e.g. 203.0.113.42:8443).')
      return
    }
    return run(() =>
      client.POST('/api/admin/share/start', {
        body: {
          public_endpoint: publicEndpoint,
          forward_port: 3000,
        },
      }),
    )
  }

  const handleStop = () => run(() => client.POST('/api/admin/share/stop', {}))

  const handlePanic = () => {
    if (!confirm('Sever ALL remote access immediately? This boots every analyst and stops sharing.')) return
    run(() => client.POST('/api/admin/share/panic', {}))
  }

  const handlePreviewInNewTab = () => {
    if (!status?.public_url) return
    window.open(`${status.public_url}/share-login`, '_blank', 'noopener,noreferrer')
  }

  const handleSaveCap = async () => {
    onError('')
    const n = Number(capValue)
    if (!Number.isInteger(n) || n < 1) {
      onError('Max concurrent analysts must be a whole number of 1 or more.')
      return
    }
    setCapBusy(true)
    try {
      await unwrapClient(
        client.PATCH('/api/admin/share/settings', {
          body: { max_concurrent_analyst_sessions: n },
        }),
      )
      await onRefresh()
      // Drop back to mirroring the (now-updated) server value.
      setCapDraft(null)
    } catch (e) {
      onError(extractApiError(e))
    } finally {
      setCapBusy(false)
    }
  }

  return (
    <section
      className="rounded-lg border bg-card p-4 space-y-3"
      data-testid="admin-share-control-panel"
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          {sharingActive ? (
            <Wifi className="h-4 w-4 text-emerald-500" />
          ) : (
            <WifiOff className="h-4 w-4 text-muted-foreground" />
          )}
          <h3 className="font-semibold text-sm">Sharing</h3>
          <Badge
            variant={sharingActive ? 'default' : 'secondary'}
            data-testid="admin-share-status-badge"
          >
            {sharingActive ? 'ACTIVE' : 'OFF'}
          </Badge>
        </div>
        <div className="flex items-center gap-2">
          {sharingActive ? (
            <>
              <Button
                size="sm"
                variant="outline"
                onClick={handleStop}
                disabled={busy}
                data-testid="admin-share-stop-btn"
              >
                {busy ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : null}
                {busy ? 'Stopping…' : 'Stop'}
              </Button>
              <Button
                size="sm"
                variant="destructive"
                onClick={handlePanic}
                disabled={busy}
                data-testid="admin-share-panic-btn"
              >
                {busy ? (
                  <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                ) : (
                  <X className="h-4 w-4 mr-1" />
                )}
                {busy ? 'Severing…' : 'Sever All Access'}
              </Button>
            </>
          ) : (
            <Button
              size="sm"
              onClick={handleStart}
              disabled={busy}
              data-testid="admin-share-start-btn"
            >
              {busy && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
              {busy ? 'Starting…' : 'Start'}
            </Button>
          )}
        </div>
      </div>

      {sharingActive && status?.public_url && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>Public URL:</span>
          <span className="font-mono truncate">{status.public_url}</span>
          <Button
            type="button"
            size="sm"
            variant="ghost"
            onClick={handlePreviewInNewTab}
            title="Open share login in a new incognito-style tab"
            className="h-6 px-2 gap-1"
          >
            <ExternalLink className="h-3 w-3" />
            Preview
          </Button>
        </div>
      )}

      {activeService?.remote_frontend_deployed && (
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>Remote Frontend Domain:</span>
          <a
            href={`https://${activeService.sharing_domain}`}
            target="_blank"
            rel="noopener noreferrer"
            className="font-mono font-semibold text-primary hover:underline flex items-center gap-1"
          >
            {activeService.sharing_domain}
            <ExternalLink className="h-3 w-3" />
          </a>
        </div>
      )}

      {sharingActive && status?.telemetry && (
        <div className="flex items-center gap-3 text-[11px] text-muted-foreground border-t pt-2 mt-2">
          <span className="flex items-center gap-1">
            <Activity className="h-3 w-3" />
            {/* Live seconds counter — placeholder until mounted (see #418 note above). */}
            Uptime:{' '}
            <span className="font-mono">
              {mounted ? formatDuration(status.telemetry.current_uptime_s) : '—'}
            </span>
          </span>
          <span>
            Heartbeat 401s since boot:{' '}
            <span className="font-mono">{status.telemetry.heartbeat_unauth_count}</span>
          </span>
          {(status.telemetry.tunnel_uptime_history?.length ?? 0) > 0 && (
            <span>
              Prior sessions:{' '}
              <span className="font-mono">{status.telemetry.tunnel_uptime_history?.length}</span>
            </span>
          )}
        </div>
      )}

      {!!(status?.rate_limits?.lockouts?.length || status?.rate_limits?.failures?.length) && (
        <div className="text-[11px] text-amber-700 dark:text-amber-400 border-t pt-2 mt-2 space-y-0.5">
          <div className="flex items-center gap-1 font-semibold">
            <Lock className="h-3 w-3" />
            Failed login activity
          </div>
          {/* remaining_s ticks down 1/sec; gate it so SSR HTML == first client
              render (see #418 note above), then show the live value post-mount. */}
          {status?.rate_limits?.lockouts?.map((l) => (
            <div key={`lo-${l.ip}`} className="font-mono">
              {l.ip} — locked out for {mounted ? l.remaining_s : '—'}s
            </div>
          ))}
          {status?.rate_limits?.failures?.map((f) => (
            <div key={`fa-${f.ip}`} className="font-mono">
              {f.ip} — {f.count} failure{f.count === 1 ? '' : 's'} in last {f.window_s}s
            </div>
          ))}
        </div>
      )}

      {!sharingActive && (
        <div className="space-y-3 border-t pt-3 mt-2">
          <fieldset className="space-y-2">
            <legend className="text-sm font-medium">Sharing mode</legend>

            <label
              htmlFor="mode-hostname"
              className="flex items-start gap-2 cursor-pointer rounded-md border p-2 hover:bg-muted/50"
            >
              <input
                type="radio"
                id="mode-hostname"
                name="sharing-mode"
                className="mt-1"
                checked={mode === 'hostname'}
                onChange={() => setMode('hostname')}
              />
              <div className="flex-1 space-y-1">
                <div className="text-sm font-medium">Your own hostname</div>
                <p className="text-xs text-muted-foreground">
                  No third-party relay. Requires a publicly resolvable hostname pointed at this
                  machine, a TLS certificate (Caddy, Cloudflare, Let&apos;s Encrypt), and the
                  forward port reachable from the internet.
                </p>
                {mode === 'hostname' && (
                  <Input
                    id="hostname-value"
                    placeholder="logs.example.com"
                    value={hostnameValue}
                    onChange={(e) => setHostnameValue(e.target.value)}
                    className="mt-1"
                  />
                )}
              </div>
            </label>

            <label
              htmlFor="mode-ip"
              className="flex items-start gap-2 cursor-pointer rounded-md border p-2 hover:bg-muted/50"
            >
              <input
                type="radio"
                id="mode-ip"
                name="sharing-mode"
                className="mt-1"
                checked={mode === 'ip'}
                onChange={() => setMode('ip')}
              />
              <div className="flex-1 space-y-1">
                <div className="text-sm font-medium">Your public IP address</div>
                <p className="text-xs text-muted-foreground">
                  No third-party relay and no DNS required. Still needs HTTPS — analyst cookies
                  require <code className="font-mono">secure=true</code>. Public CAs won&apos;t
                  issue certs for IPs, so you&apos;ll need a self-signed cert (browser warnings)
                  or a reverse proxy.
                </p>
                {mode === 'ip' && (
                  <Input
                    id="ip-value"
                    placeholder="203.0.113.42:8443"
                    value={ipValue}
                    onChange={(e) => setIpValue(e.target.value)}
                    className="mt-1"
                  />
                )}
              </div>
            </label>
          </fieldset>
        </div>
      )}

      <div className="flex items-center justify-between gap-3 border-t pt-3 mt-2">
        <div className="space-y-0.5">
          <label htmlFor="max-concurrent-analysts" className="text-sm font-medium">
            Max concurrent analysts
          </label>
          <p className="text-[11px] text-muted-foreground">
            Cap on simultaneous analyst sessions; logins past the cap are refused.
            {status?.active_session_count != null && (
              <> Currently <span className="font-mono">{status.active_session_count}</span> active.</>
            )}
          </p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Input
            id="max-concurrent-analysts"
            type="number"
            min={1}
            value={capValue}
            onChange={(e) => setCapDraft(e.target.value)}
            className="w-20"
          />
          <Button size="sm" variant="outline" onClick={handleSaveCap} disabled={capBusy || !capDirty}>
            {capBusy ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : null}
            {capBusy ? 'Saving…' : 'Save'}
          </Button>
        </div>
      </div>
    </section>
  )
}

interface SessionsPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
}

export function SessionsPanel({ status, onRefresh, onError }: SessionsPanelProps) {
  const { busy, run } = useShareMutation(onError, onRefresh)

  const lockoutsByIp = React.useMemo(
    () => new Map((status?.rate_limits?.lockouts || []).map((l) => [l.ip, l])),
    [status?.rate_limits?.lockouts],
  )

  const failuresByIp = React.useMemo(
    () => new Map((status?.rate_limits?.failures || []).map((f) => [f.ip, f])),
    [status?.rate_limits?.failures],
  )

  const handleBootSession = (sid: string) =>
    run(() =>
      client.POST('/api/admin/share/sessions/{session_id}/boot', {
        params: { path: { session_id: sid } },
      }),
    )

  const sessions = status?.sessions || []

  const accessors = React.useMemo<SortAccessors<ShareSession>>(
    () => ({
      analyst: (s) => (s.name || s.email || '').toLowerCase(),
      signin: (s) => s.auth_method || 'passcode',
      ip: (s) => s.ip_address || '',
      last_active_time: (s) => s.last_active_time ?? null,
    }),
    [],
  )
  const { sorted, sortKey, sortDir, toggle } = useTableSort(sessions, accessors, {
    defaultKey: 'last_active_time',
    defaultDir: 'desc',
  })

  return (
    <section
      className="rounded-lg border bg-card p-4 space-y-3"
      data-testid="admin-share-sessions-panel"
    >
      <h4 className="text-sm font-semibold">Active sessions</h4>
      <Table>
        <TableHeader>
          <TableRow>
            <SortableHead label="Analyst" sortKey="analyst" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Sign-in" sortKey="signin" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="IP" sortKey="ip" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Last activity" sortKey="last_active_time" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <TableHead className="text-right">Action</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {sorted.map((s: ShareSession) => (
            <TableRow key={s.session_id}>
              <TableCell className="text-xs">
                {s.name || s.email}
                <div className="text-[10px] text-muted-foreground">{s.email}</div>
              </TableCell>
              <TableCell className="text-xs">
                {s.auth_method === 'oauth' ? (
                  <Badge variant="outline" className="text-[10px] font-normal">
                    SSO{s.oauth_provider ? ` · ${s.oauth_provider}` : ''}
                  </Badge>
                ) : (
                  <span className="text-[10px] text-muted-foreground">Passcode</span>
                )}
              </TableCell>
              <TableCell className="text-xs font-mono">
                <div className="flex items-center gap-1 flex-wrap">
                  <span>{s.ip_address}</span>
                  {lockoutsByIp.has(s.ip_address) && (
                    <Badge variant="destructive" className="text-[10px]">
                      <Lock className="h-2 w-2 mr-0.5" />
                      locked {lockoutsByIp.get(s.ip_address)?.remaining_s}s
                    </Badge>
                  )}
                  {!lockoutsByIp.has(s.ip_address) && failuresByIp.has(s.ip_address) && (
                    <Badge
                      variant="outline"
                      className="text-[10px] text-amber-700 dark:text-amber-400"
                    >
                      {failuresByIp.get(s.ip_address)?.count} fail
                    </Badge>
                  )}
                </div>
              </TableCell>
              <TableCell className="text-xs">{formatDeterministicUTC(s.last_active_time)}</TableCell>
              <TableCell className="text-right">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => handleBootSession(s.session_id)}
                  disabled={busy}
                >
                  Boot
                </Button>
              </TableCell>
            </TableRow>
          ))}
          {!sessions.length && (
            <TableRow>
              <TableCell colSpan={5} className="text-center text-xs text-muted-foreground">
                No active sessions.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </section>
  )
}
