"use client"

import * as React from "react"
import { useState } from "react"
import { useForm } from "react-hook-form"
import { zodResolver } from "@hookform/resolvers/zod"
import { z } from "zod"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table"
import { QRCodeCanvas } from "qrcode.react"
import { useSearchParams } from "next/navigation"
import { parseAsStringLiteral, useQueryState } from "nuqs"
import {
  Activity,
  AlertTriangle,
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  Check,
  Copy,
  ExternalLink,
  Eye,
  EyeOff,
  FileJson,
  History,
  KeyRound,
  Loader2,
  Lock,
  Pencil,
  Plus,
  QrCode,
  RefreshCw,
  ShieldAlert,
  Sparkles,
  Trash2,
  UserPlus,
  Wifi,
  WifiOff,
  X,
} from "lucide-react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { BackToAdminLink } from "@/components/ReportLayout"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { CopyButton, PageHeader } from "@/components/ui/confirm-dialog"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { DeployRemoteFrontendDialog, TeardownRemoteFrontendDialog } from "@/components/DeployRemoteFrontendDialog/DeployRemoteFrontendDialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useMounted } from "@/hooks/useBootstrap"
import { useAutoFlash, useCopyToClipboard, useNowMs, useOpenGeneration } from "@/hooks/useSSE"
import { client, extractApiError, unwrapClient, queryKeys } from "@/lib/api"
import {
  cn,
  formatDeterministicUTC,
  formatDuration,
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderSolid,
} from "@/lib/utils"
import type { ServiceConfig, Simplify, WithRequired, WithRequiredNullable } from "@/types/api"
import type { components } from "@/types/api.generated"
import Loading from "./loading"

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

  const columns = React.useMemo<ColumnDef<ShareSession>[]>(
    () => [
      {
        id: 'analyst',
        cell: ({ row }) => {
          const s = row.original
          return (
            <>
              {s.name || s.email}
              <div className="text-[10px] text-muted-foreground">{s.email}</div>
            </>
          )
        },
      },
      {
        id: 'signin',
        cell: ({ row }) => {
          const s = row.original
          return s.auth_method === 'oauth' ? (
            <Badge variant="outline" className="text-[10px] font-normal">
              SSO{s.oauth_provider ? ` · ${s.oauth_provider}` : ''}
            </Badge>
          ) : (
            <span className="text-[10px] text-muted-foreground">Passcode</span>
          )
        },
      },
      {
        id: 'ip',
        cell: ({ row }) => {
          const s = row.original
          return (
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
          )
        },
      },
      {
        id: 'last_active_time',
        cell: ({ row }) => formatDeterministicUTC(row.original.last_active_time),
      },
      {
        id: 'action',
        cell: ({ row }) => (
          <Button
            size="sm"
            variant="ghost"
            onClick={() => handleBootSession(row.original.session_id)}
            disabled={busy}
          >
            Boot
          </Button>
        ),
      },
    ],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [lockoutsByIp, failuresByIp, busy],
  )

  const table = useReactTable({
    data: sorted,
    columns,
    getCoreRowModel: getCoreRowModel(),
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
          {table.getRowModel().rows.map((row) => (
            <TableRow key={row.original.session_id}>
              {row.getVisibleCells().map((cell) => (
                <TableCell
                  key={cell.id}
                  className={
                    cell.column.id === 'ip'
                      ? 'text-xs font-mono'
                      : cell.column.id === 'action'
                        ? 'text-right'
                        : 'text-xs'
                  }
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </TableCell>
              ))}
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

const DURATION_OPTIONS = [
  { label: '1 hour', value: 1 },
  { label: '1 day', value: 24 },
  { label: '7 days', value: 168 },
  { label: 'Unlimited', value: 0 },
]

const QUERY_WINDOW_OPTIONS = [
  { label: 'Unlimited', value: 0 },
  { label: 'Last 2 hours', value: 2 },
  { label: 'Last 24 hours', value: 24 },
  { label: 'Last 7 days', value: 168 },
]

interface CreateInviteDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  services: ShareStatus['services']
  onCreated: () => Promise<void> | void
}

export function CreateInviteDialog(props: CreateInviteDialogProps) {
  // Parent gates mounting on `open` so each open spins up a fresh form;
  // returning null when closed avoids setState-in-effect resets.
  if (!props.open) return null
  return <CreateInviteDialogInner {...props} />
}

const createInviteSchema = z.object({
  name: z.string().min(1),
  email: z.string().email(),
  passcode: z.string(),
  duration: z.number(),
  ipWhitelist: z.string(),
  maskIps: z.boolean(),
  allowConcurrent: z.boolean(),
  queryWindow: z.number(),
  serviceIds: z.array(z.string()).min(1),
  authMode: z.enum(['passcode', 'oauth']),
  oauthProvider: z.string(),
})

function CreateInviteDialogInner({
  open,
  onOpenChange,
  services,
  onCreated,
}: CreateInviteDialogProps) {
  const form = useForm<z.infer<typeof createInviteSchema>>({
    resolver: zodResolver(createInviteSchema),
    defaultValues: {
      name: '',
      email: '',
      passcode: '',
      duration: 24,
      ipWhitelist: '',
      maskIps: false,
      allowConcurrent: false,
      queryWindow: 0,
      serviceIds: [],
      authMode: 'passcode',
      oauthProvider: '',
    },
  })
  const [name, setNameState] = React.useState('')
  const [email, setEmailState] = React.useState('')
  const [passcode, setPasscodeState] = React.useState('')
  const [revealPasscode, setRevealPasscode] = React.useState(false)
  const [duration, setDurationState] = React.useState(24)
  const [ipWhitelist, setIpWhitelistState] = React.useState('')
  const [maskIps, setMaskIpsState] = React.useState(false)
  const [allowConcurrent, setAllowConcurrentState] = React.useState(false)
  const [queryWindow, setQueryWindowState] = React.useState(0)
  const [serviceIds, setServiceIdsState] = React.useState<string[]>([])
  const [creating, setCreating] = React.useState(false)
  const [error, setError] = React.useState('')
  const [authMode, setAuthModeState] = React.useState<'passcode' | 'oauth'>('passcode')
  const [oauthProvider, setOauthProviderState] = React.useState('')
  const setName = (v: string) => { setNameState(v); form.setValue('name', v) }
  const setEmail = (v: string) => { setEmailState(v); form.setValue('email', v) }
  const setPasscode = (v: string) => { setPasscodeState(v); form.setValue('passcode', v) }
  const setDuration = (v: number) => { setDurationState(v); form.setValue('duration', v) }
  const setIpWhitelist = (v: string) => { setIpWhitelistState(v); form.setValue('ipWhitelist', v) }
  const setMaskIps = (v: boolean) => { setMaskIpsState(v); form.setValue('maskIps', v) }
  const setAllowConcurrent = (v: boolean) => { setAllowConcurrentState(v); form.setValue('allowConcurrent', v) }
  const setQueryWindow = (v: number) => { setQueryWindowState(v); form.setValue('queryWindow', v) }
  const setServiceIds: React.Dispatch<React.SetStateAction<string[]>> = (updater) => {
    setServiceIdsState((prev) => {
      const next = typeof updater === 'function' ? updater(prev) : updater
      form.setValue('serviceIds', next)
      return next
    })
  }
  const setAuthMode = (v: 'passcode' | 'oauth') => { setAuthModeState(v); form.setValue('authMode', v) }
  const setOauthProvider = (v: string) => { setOauthProviderState(v); form.setValue('oauthProvider', v) }
  const [providers, setProviders] = React.useState<
    Array<{ id: string; display_name: string; enabled: boolean }>
  >([])

  // Configured OIDC providers for the SSO auth-mode dropdown (admin surface —
  // may include disabled providers so an admin can pre-create an invite). Empty
  // unless the feature is configured; the OAuth toggle then stays unsubmittable.
  React.useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const data = await unwrapClient(client.GET('/api/admin/share/oauth-providers', {}))
        if (!cancelled) setProviders(data?.providers ?? [])
      } catch {
        /* leave empty — OAuth mode just stays disabled */
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  const handleWordphrase = async () => {
    try {
      const data = await unwrapClient(client.GET('/api/admin/share/wordphrase', {}))
      setPasscode(data?.passcode ?? '')
    } catch (e) {
      setError(extractApiError(e))
    }
  }

  const handleSubmit = async () => {
    setError('')
    setCreating(true)
    try {
      await unwrapClient(
        client.POST('/api/admin/share/invites', {
          body: {
            name,
            email,
            // OAuth invites carry no passcode — the backend synthesizes an
            // unguessable argon2id placeholder for the NOT NULL column.
            ...(authMode === 'passcode' ? { passcode } : {}),
            auth_method: authMode,
            ...(authMode === 'oauth' ? { oauth_provider: oauthProvider } : {}),
            duration_hours: duration || null,
            ip_whitelist: ipWhitelist || null,
            service_ids: serviceIds,
            pii_policy: { mask_ips: maskIps },
            query_window_hours: queryWindow || null,
            allow_concurrent_sessions: allowConcurrent,
          },
        }),
      )
      await onCreated()
      onOpenChange(false)
    } catch (e) {
      setError(extractApiError(e))
    } finally {
      setCreating(false)
    }
  }

  // Require at least one authorized service: an invite with no services
  // strands the analyst on "No service found" (bootstrap returns zero
  // services), so block creation until one is picked. In OAuth mode the
  // passcode requirement is dropped and a configured provider is required
  // instead (design §5.1).
  const canSubmit =
    !creating &&
    !!name &&
    !!email &&
    serviceIds.length > 0 &&
    (authMode === 'passcode' ? !!passcode : !!oauthProvider)

  const providerLabel = providers.find((p) => p.id === oauthProvider)?.display_name ?? ''

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn('sm:max-w-2xl', panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <DialogTitle className="flex items-center gap-2 text-xl font-bold">
            <KeyRound className="h-5 w-5" />
            New invitation
          </DialogTitle>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0 px-6 py-4 space-y-3">
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="space-y-1">
              <Label htmlFor="invite-name" className="text-xs">Name</Label>
              <Input
                id="invite-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Jane Doe"
              />
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-email" className="text-xs">Email</Label>
              <Input
                id="invite-email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="jane@example.com"
              />
              {authMode === 'oauth' && (
                <p className="text-[11px] text-muted-foreground">
                  The analyst must sign in with the {providerLabel || 'provider'} account for
                  this exact address.
                </p>
              )}
            </div>

            {/* Auth-mode selector — a11y: fieldset+legend + role=radiogroup with
                native radios for arrow-key traversal (design §5.4). */}
            <fieldset className="md:col-span-2 space-y-1">
              <legend className="text-xs font-medium">Sign-in method</legend>
              <div className="flex gap-6 pt-1" role="radiogroup" aria-label="Sign-in method">
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    name="invite-auth-mode"
                    value="passcode"
                    checked={authMode === 'passcode'}
                    onChange={() => setAuthMode('passcode')}
                  />
                  Passcode (local)
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input
                    type="radio"
                    name="invite-auth-mode"
                    value="oauth"
                    checked={authMode === 'oauth'}
                    onChange={() => setAuthMode('oauth')}
                  />
                  SSO / OIDC
                </label>
              </div>
            </fieldset>

            {authMode === 'passcode' && (
            <div className="space-y-1 md:col-span-2">
              <Label htmlFor="invite-passcode" className="text-xs">Passcode</Label>
              <div className="flex items-center gap-2">
                <Input
                  id="invite-passcode"
                  type={revealPasscode ? 'text' : 'password'}
                  value={passcode}
                  onChange={(e) => setPasscode(e.target.value)}
                  placeholder="ocean-breeze-cabin-42"
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  aria-label={revealPasscode ? 'Hide passcode' : 'Reveal passcode'}
                  onClick={() => setRevealPasscode((r) => !r)}
                  title={revealPasscode ? 'Hide passcode' : 'Reveal passcode'}
                >
                  {revealPasscode ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={handleWordphrase}
                  className="gap-1"
                >
                  <Sparkles className="h-3 w-3" />
                  Wordphrase
                </Button>
              </div>
            </div>
            )}

            {authMode === 'oauth' && (
            <div className="space-y-1 md:col-span-2">
              <Label htmlFor="invite-oauth-provider" className="text-xs">Identity provider</Label>
              {providers.length > 0 ? (
                <Select value={oauthProvider} onValueChange={(v) => setOauthProvider(v ?? '')}>
                  <SelectTrigger id="invite-oauth-provider">
                    <SelectValue placeholder="Select a provider" />
                  </SelectTrigger>
                  <SelectContent>
                    {providers.map((p) => (
                      <SelectItem key={p.id} value={p.id}>
                        {p.display_name}
                        {!p.enabled ? ' (disabled)' : ''}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <>
                  <Select disabled>
                    <SelectTrigger id="invite-oauth-provider">
                      <SelectValue placeholder="No identity providers configured" />
                    </SelectTrigger>
                    <SelectContent />
                  </Select>
                  <p className="text-xs text-muted-foreground">
                    No identity providers configured — add one to the registry first.
                  </p>
                </>
              )}
            </div>
            )}
            <div className="space-y-1">
              <Label htmlFor="invite-expiration" className="text-xs">Expiration</Label>
              <Select
                value={String(duration)}
                onValueChange={(v) => setDuration(Number(v))}
              >
                <SelectTrigger id="invite-expiration">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DURATION_OPTIONS.map((opt) => (
                    <SelectItem key={opt.value} value={String(opt.value)}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <Label htmlFor="invite-query-window" className="text-xs">Query window</Label>
              <Select
                value={String(queryWindow)}
                onValueChange={(v) => setQueryWindow(Number(v))}
              >
                <SelectTrigger id="invite-query-window">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {QUERY_WINDOW_OPTIONS.map((opt) => (
                    <SelectItem key={opt.value} value={String(opt.value)}>
                      {opt.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1 md:col-span-2">
              <Label htmlFor="invite-ipwl" className="text-xs">IP whitelist (optional)</Label>
              <Input
                id="invite-ipwl"
                value={ipWhitelist}
                onChange={(e) => setIpWhitelist(e.target.value)}
                placeholder="192.168.1.50, 10.0.0.0/24"
              />
            </div>
            <div className="md:col-span-2 space-y-2">
              <Label className="text-xs">Authorized services <span className="text-destructive">*</span></Label>
              <div className="grid grid-cols-2 gap-2">
                {services.map((svc) => {
                  const checked = serviceIds.includes(svc.service_id)
                  return (
                    <label
                      key={svc.service_id}
                      className="flex items-center gap-2 text-sm cursor-pointer"
                    >
                      <Checkbox
                        checked={checked}
                        onCheckedChange={(v) => {
                          setServiceIds((prev) =>
                            v
                              ? [...prev, svc.service_id]
                              : prev.filter((id) => id !== svc.service_id),
                          )
                        }}
                      />
                      <span className="truncate">{svc.name}</span>
                    </label>
                  )
                })}
                {!services.length && (
                  <p className="text-xs text-muted-foreground col-span-2">
                    No services configured. Provision a service first.
                  </p>
                )}
              </div>
              {!!services.length && serviceIds.length === 0 && (
                <p className="text-xs text-muted-foreground">
                  Select at least one service this analyst can view.
                </p>
              )}
            </div>
            <div className="md:col-span-2 flex items-center gap-2">
              <Checkbox
                id="invite-mask-ips"
                checked={maskIps}
                onCheckedChange={(v) => setMaskIps(!!v)}
              />
              <Label htmlFor="invite-mask-ips" className="text-xs cursor-pointer">
                Anonymize client IPs (mask PII)
              </Label>
            </div>
            <div className="md:col-span-2 space-y-1">
              <div className="flex items-center gap-2">
                <Checkbox
                  id="invite-allow-concurrent"
                  checked={allowConcurrent}
                  onCheckedChange={(v) => setAllowConcurrent(!!v)}
                />
                <Label htmlFor="invite-allow-concurrent" className="text-xs cursor-pointer">
                  Allow multiple people to use this link at the same time
                </Label>
              </div>
              <p className="text-xs text-muted-foreground">
                Off (default): each new login signs out the previous one. On: several
                people can share this link — audit events won&apos;t distinguish between them.
              </p>
            </div>
            <div className="md:col-span-2 rounded-md bg-muted/40 p-2 text-xs text-muted-foreground">
              Note: Analysts are required to review and agree to the Terms of Access upon their first login before dashboard access is granted.
            </div>
          </div>
        </div>

        <DialogFooter className="px-6 py-3 border-t bg-muted/10 shrink-0">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={creating}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={!canSubmit}>
            {creating && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            Create invite
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ── InviteAnalystDialog (Path A standalone viewer key generator) ──────────

type AnalystInvite = components['schemas']['AnalystInvite']

export interface InviteAnalystDialogProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

type InviteStep = 'confirm' | 'creating' | 'result'

const REVEAL_TTL_MS = 5 * 60 * 1000
export const recentInvites = new Map<string, { invite: AnalystInvite; issuedAt: number }>()

export function getRecentInvite(serviceId: string): AnalystInvite | null {
  const entry = recentInvites.get(serviceId)
  if (!entry) return null
  if (Date.now() - entry.issuedAt > REVEAL_TTL_MS) {
    recentInvites.delete(serviceId)
    return null
  }
  return entry.invite
}

export function CopyField({ label, value, secret, multiline = false }: { label: string; value: string; secret?: boolean; multiline?: boolean }) {
  const [revealed, setRevealed] = useState(false)
  const display = secret && !revealed ? '•'.repeat(Math.min(value.length, 24)) : value

  return (
    <div className="space-y-1.5">
      <Label className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">{label}</Label>
      <div className="flex items-start gap-2">
        <div className={`flex-1 font-mono text-sm bg-muted/50 border rounded-md px-3 py-2 min-w-0 ${multiline ? 'break-all overflow-hidden max-h-32 overflow-y-auto' : 'flex items-center gap-2 truncate'}`}>
          <span className="flex-1">{display}</span>
        </div>
        <div className="flex items-center gap-1">
          {secret && (
            <Button
              variant="ghost"
              size="icon"
              aria-label={revealed ? 'Hide value' : 'Reveal value'}
              className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
              onClick={() => setRevealed((r) => !r)}
              title={revealed ? 'Hide' : 'Reveal'}
            >
              {revealed ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
            </Button>
          )}
          <CopyButton
            text={value}
            ariaLabel="Copy to clipboard"
            title="Copy"
            className="h-9 w-9 shrink-0 text-muted-foreground hover:text-foreground"
            iconClassName="h-4 w-4"
          />
        </div>
      </div>
    </div>
  )
}

export function CachedInviteBanner({ serviceId, onReveal }: { serviceId: string; onReveal: () => void }) {
  const nowMs = useNowMs()
  const entry = recentInvites.get(serviceId)
  if (!entry || !getRecentInvite(serviceId)) return null
  const ageMs = nowMs ? nowMs - entry.issuedAt : 0
  const remainingMin = Math.max(1, Math.ceil(Math.max(0, REVEAL_TTL_MS - ageMs) / 60000))
  return (
    <Alert className="bg-primary/5 border-primary/20">
      <History className="h-4 w-4" />
      <AlertDescription className="text-sm ml-1 flex items-center justify-between gap-3">
        <span>A key was issued for this service in the last {remainingMin} min — still available in this tab.</span>
        <Button variant="outline" size="sm" onClick={onReveal} className="h-8 shrink-0">
          <Eye className="h-3.5 w-3.5 mr-1.5" />
          Reveal
        </Button>
      </AlertDescription>
    </Alert>
  )
}

export function InviteAnalystDialog(props: InviteAnalystDialogProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <InviteAnalystDialogBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function InviteAnalystDialogBody({ service, open, onOpenChange }: InviteAnalystDialogProps) {
  const [result, setResult] = useState<AnalystInvite | null>(null)
  const { copied: jsonCopied, copy: copyJson } = useCopyToClipboard(2000)

  const createInviteMutation = useMutation({
    mutationFn: async (serviceId: string) => {
      const data = await unwrapClient(client.POST('/api/services/{service_id}/generate-viewer-key', {
        params: { path: { service_id: serviceId } },
      }))
      return data as AnalystInvite
    },
    onSuccess: (invite, serviceId) => {
      recentInvites.set(serviceId, { invite, issuedAt: Date.now() })
      setResult(invite)
    },
  })

  if (!service) return null

  const step: InviteStep = result ? 'result' : createInviteMutation.isPending ? 'creating' : 'confirm'
  const error = createInviteMutation.error
    ? (createInviteMutation.error instanceof Error && createInviteMutation.error.message
        ? createInviteMutation.error.message
        : 'Failed to create analyst key')
    : ''

  const handleCopyJson = () => {
    if (!result) return
    const config: Record<string, string> = {
      name: result.name,
      service_id: result.service_id,
      fos_bucket: result.fos_bucket,
      fos_region: result.fos_region,
      fos_endpoint: result.fos_endpoint,
      fos_prefix: result.fos_prefix,
      access_key_id: result.access_key_id,
      secret_key: result.secret_key,
    }
    if (result.cdn_url) config.cdn_url = result.cdn_url
    if (result.cdn_service_id) config.cdn_service_id = result.cdn_service_id
    if (result.cdn_secret) config.cdn_secret = result.cdn_secret
    if (result.iceberg_metadata_location) config.iceberg_metadata_location = result.iceberg_metadata_location
    void copyJson(JSON.stringify(config, null, 2))
  }

  return (
    <Dialog open={open} onOpenChange={(isOpen) => { if (step !== 'creating') onOpenChange(isOpen) }}>
      <DialogContent className={cn('sm:max-w-xl', panelDialogContent)} showCloseButton={step !== 'creating'}>
        <DialogHeader className={panelDialogHeaderSolid}>
          <div className="flex items-center justify-between mb-1">
            <DialogTitle className="flex items-center gap-2 text-xl font-bold">
              <UserPlus className="h-5 w-5" />
              Invite Analyst
            </DialogTitle>
            <div className="flex items-center gap-1.5 mr-6">
              <div className={`h-1.5 w-6 rounded-full transition-colors ${step === 'confirm' ? 'bg-primary' : 'bg-muted'}`} />
              <div className={`h-1.5 w-6 rounded-full transition-colors ${step === 'result' ? 'bg-primary' : 'bg-muted'}`} />
            </div>
          </div>
          <Badge variant="secondary" className="w-fit font-mono text-[10px] font-normal tracking-tight uppercase">
            {service.name}
          </Badge>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {step === 'confirm' && (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-right-4 duration-300">
              <CachedInviteBanner serviceId={service.service_id} onReveal={() => {
                const cached = getRecentInvite(service.service_id)
                if (cached) setResult(cached)
              }} />
              <div className="rounded-lg border bg-muted/30 p-4 space-y-3">
                <div className="flex items-center gap-2 text-sm font-semibold">
                  <KeyRound className="h-4 w-4 text-primary" />
                  What will happen
                </div>
                <ul className="text-sm text-muted-foreground space-y-1.5 ml-6 list-disc">
                  <li>A new <strong className="text-foreground">read-only</strong> Fastly Object Storage access key will be created, scoped to this service&apos;s bucket.</li>
                  <li>Your stored Fastly API token will be used — no re-entry needed.</li>
                  <li>You&apos;ll receive a JSON config to send to the analyst.</li>
                </ul>
              </div>
              {error && (
                <Alert variant="destructive" className="bg-destructive/5 border-destructive/20">
                  <AlertTriangle className="h-4 w-4" />
                  <AlertDescription className="text-sm ml-1">{error}</AlertDescription>
                </Alert>
              )}
            </div>
          )}

          {step === 'creating' && (
            <div className="p-8 flex flex-col items-center justify-center gap-4 text-center min-h-[300px]">
              <Loader2 className="h-10 w-10 animate-spin text-primary/50" />
              <div className="space-y-1">
                <p className="font-bold text-lg tracking-tight">Generating Analyst Access…</p>
                <p className="text-sm text-muted-foreground">Provisioning read-only keys via Fastly API.</p>
              </div>
            </div>
          )}

          {step === 'result' && result && (
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
              <Alert className="bg-amber-500/10 border-amber-500/30 text-amber-700 dark:text-amber-400">
                <AlertTriangle className="h-4 w-4" />
                <AlertDescription className="text-sm ml-1 font-medium">
                  Save the secret key now — available in this browser tab for {REVEAL_TTL_MS / 60000} min, then unrecoverable.
                </AlertDescription>
              </Alert>
              <div className="space-y-4">
                <CopyField label="Display Name" value={result.name} />
                <CopyField label="Fastly Service ID" value={result.service_id} />
                <CopyField label="FOS Bucket" value={result.fos_bucket} />
                <CopyField label="FOS Region" value={result.fos_region} />
                <CopyField label="Access Key ID" value={result.access_key_id} />
                <CopyField label="Secret Key" value={result.secret_key} secret />
                {result.cdn_url && <CopyField label="CDN URL" value={result.cdn_url} />}
                {result.cdn_secret && <CopyField label="CDN Secret" value={result.cdn_secret} secret />}
              </div>
            </div>
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {step === 'confirm' && (
            <Button onClick={() => createInviteMutation.mutate(service.service_id)} className="h-10 px-8 font-bold">
              <UserPlus className="h-4 w-4 mr-2" />
              Generate Invite
            </Button>
          )}
          {step === 'result' && (
            <>
              <Button variant="outline" onClick={handleCopyJson} className="h-10 px-6 gap-2">
                {jsonCopied ? <Check className="h-4 w-4 text-emerald-500" /> : <FileJson className="h-4 w-4" />}
                {jsonCopied ? 'Copied!' : 'Copy JSON'}
              </Button>
              <Button onClick={() => onOpenChange(false)} className="h-10 px-8">Done</Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ── InvitationsPanel (Path B live share invitations) ──────────────────────

interface InvitationsPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
  onViewAuditLogs?: (email: string) => void
}

const PasscodeDraftSchema = z.string().min(1)

function buildShareCard(
  invite: Invite,
  publicUrl: string | null,
  allServices: Array<{ service_id: string; name?: string }> = []
): string {
  const url = publicUrl || '(sharing not active)'
  const nameById = new Map(allServices.map((s) => [s.service_id, s.name || '']))
  const formatted = (invite.service_ids || []).map((id: string) => {
    const name = nameById.get(id)
    return name ? `${name} (${id})` : id
  })
  const services = formatted.length ? formatted.join(', ') : 'all assigned services'
  const credLines =
    invite.auth_method === 'oauth'
      ? `Sign-in:      ${invite.oauth_provider ? `${invite.oauth_provider} (SSO)` : 'SSO'}\n              Sign in at the link with the account for ${invite.email}.`
      : `Passcode:     (delivered separately — never paste here)`
  return `==================================================
FASTLY LOG ANALYSIS - SHARE DASHBOARD INVITATION
==================================================
You have been invited to view the analyst dashboard.

Access Link:  ${url}/share-login
Login Email:  ${invite.email}
${credLines}
Authorized Services: ${services}
Valid Until:  ${invite.expires_at ? new Date(invite.expires_at).toUTCString() : 'Unlimited'}

Note: You will be required to review and agree to the Terms of Access upon signing in before accessing the dashboard.
==================================================`
}

export function InvitationsPanel({ status, onRefresh, onError, onViewAuditLogs }: InvitationsPanelProps) {
  const [createOpen, setCreateOpen] = React.useState(false)
  const { busy, run } = useShareMutation(onError, onRefresh)
  const [lastCopiedInviteId, setLastCopiedInviteId] = React.useState<string | null>(null)
  const { copied, copy } = useCopyToClipboard(1500)
  const [qrInviteId, setQrInviteId] = React.useState<string | null>(null)
  const [editingServicesFor, setEditingServicesFor] = React.useState<string | null>(null)
  const [editingServicesDraft, setEditingServicesDraft] = React.useState<string[]>([])
  const [editingMaskIps, setEditingMaskIps] = React.useState(false)
  const [editingAllowConcurrent, setEditingAllowConcurrent] = React.useState(false)
  const [savingServices, setSavingServices] = React.useState(false)
  const [editingPasscodeFor, setEditingPasscodeFor] = React.useState<string | null>(null)
  const [passcodeDraft, setPasscodeDraft] = React.useState('')
  const [passcodeVisible, setPasscodeVisible] = React.useState(false)
  const [savingPasscode, setSavingPasscode] = React.useState(false)
  const [passcodeError, setPasscodeError] = React.useState<string | null>(null)
  const { value: passcodeSavedFor, flash: flashPasscodeSaved } = useAutoFlash<string>(2000)

  const services = status?.services || []
  const invites = status?.invites || []

  const onlineInviteIds = React.useMemo(
    () => new Set((status?.sessions || []).map((s) => s.invite_id)),
    [status?.sessions],
  )
  const svcNameById = React.useMemo(
    () => new Map(services.map((s) => [s.service_id, s.name])),
    [services],
  )

  const accessors = React.useMemo<SortAccessors<Invite>>(
    () => ({
      name: (i) => (i.name || '').toLowerCase(),
      email: (i) => (i.email || '').toLowerCase(),
      last_login_at: (i) => i.last_login_at ?? null,
      expires_at: (i) => i.expires_at ?? null,
    }),
    [],
  )
  const { sorted, sortKey, sortDir, toggle } = useTableSort(invites, accessors, {
    defaultKey: 'last_login_at',
    defaultDir: 'desc',
  })

  const handleRevokeInvite = (id: string) => {
    if (!confirm('Delete this invite and boot any sessions linked to it? This cannot be undone.')) return
    run(() => client.DELETE('/api/admin/share/invites/{invite_id}', { params: { path: { invite_id: id } } }))
  }

  const openEditServices = (invite: Invite) => {
    setEditingServicesFor(invite.id)
    setEditingServicesDraft(invite.service_ids || [])
    setEditingMaskIps(!!invite.pii_policy?.mask_ips)
    setEditingAllowConcurrent(!!invite.allow_concurrent_sessions)
  }

  const handleSaveServices = async (inviteId: string) => {
    setSavingServices(true)
    onError('')
    try {
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/services', {
        params: { path: { invite_id: inviteId } },
        body: { service_ids: editingServicesDraft },
      }))
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/pii', {
        params: { path: { invite_id: inviteId } },
        body: { mask_ips: editingMaskIps },
      }))
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/sharing', {
        params: { path: { invite_id: inviteId } },
        body: { allow_concurrent_sessions: editingAllowConcurrent },
      }))
      setEditingServicesFor(null)
      await onRefresh()
    } catch (e) {
      onError(extractApiError(e))
    } finally {
      setSavingServices(false)
    }
  }

  const openEditPasscode = (invite: Invite) => {
    setEditingPasscodeFor(invite.id)
    setPasscodeDraft('')
    setPasscodeVisible(false)
    setPasscodeError(null)
  }

  const handleSavePasscode = async (inviteId: string) => {
    const parsed = PasscodeDraftSchema.safeParse(passcodeDraft)
    if (!parsed.success) return
    setSavingPasscode(true)
    setPasscodeError(null)
    try {
      await unwrapClient(client.PATCH('/api/admin/share/invites/{invite_id}/passcode', {
        params: { path: { invite_id: inviteId } },
        body: { passcode: parsed.data },
      }))
      setEditingPasscodeFor(null)
      setPasscodeDraft('')
      flashPasscodeSaved(inviteId)
      await onRefresh()
    } catch (e) {
      setPasscodeError(extractApiError(e))
    } finally {
      setSavingPasscode(false)
    }
  }

  const handleCopyShareCard = (invite: Invite) => {
    setLastCopiedInviteId(invite.id)
    void copy(buildShareCard(invite, status?.public_url ?? null, services))
  }

  const inviteColumns = React.useMemo<ColumnDef<Invite>[]>(
    () => [
      { id: 'name', cell: ({ row }) => row.original.name },
      { id: 'email', cell: ({ row }) => row.original.email },
      { id: 'last_login_at', cell: ({ row }) => row.original.last_login_at },
      { id: 'services', cell: ({ row }) => row.original.service_ids },
      { id: 'expires_at', cell: ({ row }) => row.original.expires_at },
      { id: 'actions', cell: ({ row }) => row.original.id },
    ],
    [],
  )
  const inviteTable = useReactTable({
    data: sorted,
    columns: inviteColumns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <section className="rounded-lg border bg-card p-4 space-y-3" data-testid="admin-share-invites-panel">
      <div className="flex items-center justify-between gap-2">
        <h4 className="text-sm font-semibold">Active invitations</h4>
        <Button size="sm" onClick={() => setCreateOpen(true)} data-testid="admin-share-new-invite-btn">
          <Plus className="h-4 w-4 mr-1" />
          New invitation
        </Button>
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <SortableHead label="Name" sortKey="name" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Email" sortKey="email" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <SortableHead label="Last login" sortKey="last_login_at" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <TableHead>Services</TableHead>
            <SortableHead label="Expires" sortKey="expires_at" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            <TableHead className="text-right">Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {inviteTable.getRowModel().rows.map((tableRow) => {
            const invite = tableRow.original
            const online = onlineInviteIds.has(invite.id)
            return (
              <TableRow key={invite.id}>
                <TableCell className="font-medium">
                  <div className="flex items-center gap-1.5">
                    {online && (
                      <span
                        className="h-2 w-2 rounded-full bg-emerald-500 shrink-0 animate-pulse"
                        role="img"
                        title="Online now (has an active session)"
                        aria-label="Online now"
                      />
                    )}
                    <span>{invite.name}</span>
                    <Badge
                      variant="outline"
                      className="text-[10px] font-normal"
                      title={invite.auth_method === 'oauth' ? 'Signs in via SSO / OIDC' : 'Signs in with a passcode'}
                    >
                      {invite.auth_method === 'oauth'
                        ? `SSO${invite.oauth_provider ? ` · ${invite.oauth_provider}` : ''}`
                        : 'Passcode'}
                    </Badge>
                    {invite.allow_concurrent_sessions && (
                      <Badge variant="outline" className="text-[10px] font-normal" title="Multiple people can use this link at the same time">
                        Shared
                      </Badge>
                    )}
                  </div>
                  {onViewAuditLogs && (
                    <button
                      type="button"
                      onClick={() => onViewAuditLogs(invite.email)}
                      aria-label={`View audit logs for ${invite.email}`}
                      className="text-[10px] text-primary hover:underline block mt-0.5 text-left font-normal"
                    >
                      view audit logs
                    </button>
                  )}
                </TableCell>
                <TableCell className="text-xs">{invite.email}</TableCell>
                <TableCell className="text-xs whitespace-nowrap">
                  {invite.last_login_at ? formatDeterministicUTC(invite.last_login_at) : <span className="text-muted-foreground">Never</span>}
                </TableCell>
                <TableCell className="text-xs">
                  <div className="flex flex-col gap-1 items-start">
                    {(invite.service_ids || []).map((s: string) => {
                      const nm = svcNameById.get(s)
                      return (
                        <Badge key={s} variant="secondary" className="text-[10px]" title={nm ? `${nm} (${s})` : s}>
                          {nm ? <>{nm} <span className="opacity-60">({s})</span></> : s}
                        </Badge>
                      )
                    })}
                  </div>
                </TableCell>
                <TableCell className="text-xs">{formatDeterministicUTC(invite.expires_at)}</TableCell>
                <TableCell className="text-right space-x-1">
                  <Button size="sm" variant="ghost" onClick={() => handleCopyShareCard(invite)} title="Copy share card">
                    {copied && lastCopiedInviteId === invite.id ? <Check className="h-3 w-3 text-emerald-500" /> : <Copy className="h-3 w-3" />}
                  </Button>
                  <Popover open={qrInviteId === invite.id} onOpenChange={(o) => setQrInviteId(o ? invite.id : null)}>
                    <PopoverTrigger
                      render={(props: React.ComponentPropsWithRef<'button'>) => (
                        <Button {...props} size="sm" variant="ghost" title="QR code" disabled={!status?.public_url}>
                          <QrCode className="h-3 w-3" />
                        </Button>
                      )}
                    />
                    <PopoverContent className="w-56 items-center text-center">
                      <div className="text-xs font-semibold">{invite.name || invite.email}</div>
                      {status?.public_url ? (
                        <>
                          <QRCodeCanvas value={`${status.public_url}/share-login`} size={180} className="mx-auto" aria-label={`QR code for ${invite.email}`} />
                          <div className="text-[10px] text-muted-foreground break-all font-mono">{status.public_url}/share-login</div>
                        </>
                      ) : (
                        <div className="text-xs text-muted-foreground">Start sharing to generate a QR.</div>
                      )}
                    </PopoverContent>
                  </Popover>
                  <Popover open={editingServicesFor === invite.id} onOpenChange={(o) => (o ? openEditServices(invite) : setEditingServicesFor(null))}>
                    <PopoverTrigger
                      render={(props: React.ComponentPropsWithRef<'button'>) => (
                        <Button {...props} size="sm" variant="ghost" title="Edit invite (services, privacy, shared logins)">
                          <Pencil className="h-3 w-3" />
                        </Button>
                      )}
                    />
                    <PopoverContent className="w-72">
                      <div className="text-xs font-semibold mb-1">Authorized services</div>
                      <div className="space-y-1 max-h-48 overflow-y-auto">
                        {services.map((svc) => {
                          const checked = editingServicesDraft.includes(svc.service_id)
                          return (
                            <label key={svc.service_id} className="flex items-center gap-2 text-sm cursor-pointer">
                              <Checkbox
                                checked={checked}
                                onCheckedChange={(v) =>
                                  setEditingServicesDraft((prev) =>
                                    v ? [...prev, svc.service_id] : prev.filter((id) => id !== svc.service_id),
                                  )
                                }
                              />
                              <span className="truncate">{svc.name}</span>
                            </label>
                          )
                        })}
                        {!services.length && <p className="text-xs text-muted-foreground">No services available.</p>}
                      </div>
                      <label className="flex items-center gap-2 text-sm cursor-pointer pt-2 mt-2 border-t">
                        <Checkbox checked={editingMaskIps} onCheckedChange={(v) => setEditingMaskIps(!!v)} />
                        <span>Anonymize client IPs (mask PII)</span>
                      </label>
                      <label className="flex items-center gap-2 text-sm cursor-pointer pt-2 mt-2 border-t">
                        <Checkbox checked={editingAllowConcurrent} onCheckedChange={(v) => setEditingAllowConcurrent(!!v)} />
                        <span>Allow shared logins (multiple people at once)</span>
                      </label>
                      <div className="flex justify-end gap-1 pt-2 border-t">
                        <Button size="sm" variant="ghost" onClick={() => setEditingServicesFor(null)} disabled={savingServices}>
                          Cancel
                        </Button>
                        <Button size="sm" onClick={() => handleSaveServices(invite.id)} disabled={savingServices}>
                          {savingServices && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                          Save
                        </Button>
                      </div>
                    </PopoverContent>
                  </Popover>
                  {invite.auth_method !== 'oauth' && (
                    <Popover open={editingPasscodeFor === invite.id} onOpenChange={(o) => (o ? openEditPasscode(invite) : setEditingPasscodeFor(null))}>
                      <PopoverTrigger
                        render={(props: React.ComponentPropsWithRef<'button'>) => (
                          <Button {...props} size="sm" variant="ghost" title="Update passcode (re-send share card afterwards)">
                            {passcodeSavedFor === invite.id ? <Check className="h-3 w-3 text-emerald-500" /> : <KeyRound className="h-3 w-3" />}
                          </Button>
                        )}
                      />
                      <PopoverContent className="w-72">
                        <div className="text-xs font-semibold mb-1">New passcode</div>
                        <div className="text-[10px] text-muted-foreground mb-2">
                          Sessions using the old passcode keep working until they expire — but new logins will need the new one.
                        </div>
                        <div className="flex items-center gap-1">
                          <input
                            type={passcodeVisible ? 'text' : 'password'}
                            className="flex h-8 w-full rounded border border-input bg-background px-2 py-1 text-xs"
                            value={passcodeDraft}
                            onChange={(e) => {
                              setPasscodeDraft(e.target.value)
                              setPasscodeError(null)
                            }}
                            placeholder="e.g. ocean-breeze-cabin-42"
                            autoFocus
                          />
                          <Button size="sm" variant="ghost" type="button" onClick={() => setPasscodeVisible((v) => !v)} title={passcodeVisible ? 'Hide' : 'Show'}>
                            {passcodeVisible ? <EyeOff className="h-3 w-3" /> : <Eye className="h-3 w-3" />}
                          </Button>
                        </div>
                        {passcodeError && <div className="text-[11px] text-destructive pt-1">{passcodeError}</div>}
                        <div className="flex justify-end gap-1 pt-2 border-t mt-2">
                          <Button size="sm" variant="ghost" onClick={() => setEditingPasscodeFor(null)} disabled={savingPasscode}>
                            Cancel
                          </Button>
                          <Button size="sm" onClick={() => handleSavePasscode(invite.id)} disabled={savingPasscode || !passcodeDraft}>
                            {savingPasscode && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
                            Save
                          </Button>
                        </div>
                      </PopoverContent>
                    </Popover>
                  )}
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => handleRevokeInvite(invite.id)}
                    disabled={busy}
                    title="Delete invite (boots any active sessions)"
                  >
                    <Trash2 className="h-3 w-3 text-destructive" />
                  </Button>
                </TableCell>
              </TableRow>
            )
          })}
          {!invites.length && (
            <TableRow>
              <TableCell colSpan={6} className="text-center text-xs text-muted-foreground">
                No invitations yet.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <CreateInviteDialog open={createOpen} onOpenChange={setCreateOpen} services={services} onCreated={onRefresh} />
    </section>
  )
}

const SHARE_TABS = ['invites', 'sessions', 'audit'] as const
type ShareTab = (typeof SHARE_TABS)[number]
const VALID_SHARE_TABS: ReadonlySet<string> = new Set(SHARE_TABS)

const shareTabParser = parseAsStringLiteral(SHARE_TABS)
  .withDefault('invites')
  .withOptions({ clearOnDefault: true, history: 'replace', shallow: true })


const EVENT_TYPE_FILTER_OPTIONS = [
  'ALL',
  'LOGIN_SUCCESS',
  'LOGIN_FAIL',
  'LOCKOUT',
  'INVITE_CREATE',
  'INVITE_REVOKE',
  'INVITE_DELETE',
  'INVITE_CLAIMED',
  'SESSION_BOOT',
  'SESSION_TIMEOUT',
  'TOS_ACCEPTED',
  'TUNNEL_START',
  'TUNNEL_RESUMED',
  'SHARE_START',
  'SHARE_STOP',
  'PANIC_TRIGGERED',
  'BACKUP_EXPORTED',
  'BACKUP_IMPORTED',
  'CLAIM_FAIL',
  'OAUTH_AUTH_INIT',
  'OAUTH_CALLBACK_FAIL',
  'OAUTH_VERIFY_FAIL',
  'OAUTH_INVITE_NOT_FOUND',
  'LOGIN_SUCCESS_OAUTH',
]

type AuditFilters = {
  event_type: string
  email: string
  since: string
  until: string
}

interface AuditLogPanelProps {
  status: ShareStatus | null
  onError: (msg: string) => void
  initialEmailFilter?: string
  onClearInitialFilter?: () => void
}

export function AuditLogPanel({ status, onError, initialEmailFilter, onClearInitialFilter }: AuditLogPanelProps) {
  const [filters, setFilters] = React.useState<AuditFilters>({
    event_type: 'ALL',
    email: initialEmailFilter || '',
    since: '',
    until: '',
  })
  const [appliedFilters, setAppliedFilters] = React.useState<AuditFilters | null>(() =>
    initialEmailFilter
      ? { event_type: 'ALL', email: initialEmailFilter, since: '', until: '' }
      : null,
  )

  const [seenInitialEmail, setSeenInitialEmail] = React.useState(initialEmailFilter)
  if (initialEmailFilter !== seenInitialEmail) {
    setSeenInitialEmail(initialEmailFilter)
    setFilters((f) => ({ ...f, email: initialEmailFilter || '' }))
    setAppliedFilters(
      initialEmailFilter
        ? { event_type: 'ALL', email: initialEmailFilter, since: '', until: '' }
        : null,
    )
  }

  const auditQuery = useQuery({
    queryKey: ['share-audit-logs', appliedFilters],
    enabled: appliedFilters !== null,
    retry: false,
    queryFn: async () => {
      onError('')
      try {
        const params: Record<string, string | number> = { limit: 200 }
        if (appliedFilters?.event_type && appliedFilters.event_type !== 'ALL') {
          params.event_type = appliedFilters.event_type
        }
        if (appliedFilters?.email) params.email = appliedFilters.email
        if (appliedFilters?.since) params.since = appliedFilters.since
        if (appliedFilters?.until) params.until = appliedFilters.until
        const data = await unwrapClient(
          client.GET('/api/admin/share/audit-logs', {
            params: { query: params },
          }),
        )
        return data?.audit_logs || []
      } catch (e) {
        onError(extractApiError(e))
        throw e
      }
    },
  })

  const applyFilters = () => {
    setAppliedFilters({ ...filters })
    if (
      appliedFilters &&
      appliedFilters.event_type === filters.event_type &&
      appliedFilters.email === filters.email &&
      appliedFilters.since === filters.since &&
      appliedFilters.until === filters.until
    ) {
      auditQuery.refetch()
    }
  }

  const handleClear = () => {
    setFilters({ event_type: 'ALL', email: '', since: '', until: '' })
    setAppliedFilters(null)
    if (onClearInitialFilter) onClearInitialFilter()
  }

  const filteredLogs = appliedFilters !== null ? (auditQuery.data ?? null) : null
  const loading = auditQuery.isFetching
  const rows = filteredLogs ?? status?.audit_logs ?? []

  const auditAccessors = React.useMemo<SortAccessors<AuditLog>>(
    () => ({
      timestamp: (r) => r.timestamp ?? null,
      event_type: (r) => r.event_type ?? '',
      email: (r) => (r.email ?? '').toLowerCase(),
      ip_address: (r) => r.ip_address ?? '',
      details: (r) => r.details ?? '',
    }),
    [],
  )
  const { sorted, sortKey, sortDir, toggle } = useTableSort(rows, auditAccessors, {
    defaultKey: 'timestamp',
    defaultDir: 'desc',
  })

  const auditColumns = React.useMemo<ColumnDef<AuditLog>[]>(
    () => [
      {
        id: 'timestamp',
        cell: ({ row }) => formatDeterministicUTC(row.original.timestamp),
      },
      {
        id: 'event_type',
        cell: ({ row }) => (
          <Badge variant="outline" className="text-[10px]">
            {row.original.event_type}
          </Badge>
        ),
      },
      {
        id: 'email',
        cell: ({ row }) => row.original.email || '—',
      },
      {
        id: 'ip_address',
        cell: ({ row }) => row.original.ip_address || '—',
      },
      {
        id: 'details',
        cell: ({ row }) => row.original.details,
      },
    ],
    [],
  )
  const auditTable = useReactTable({
    data: sorted,
    columns: auditColumns,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <div className="space-y-3" data-testid="admin-share-audit-panel">
      <section className="rounded-lg border bg-card p-3 space-y-2">
        <h4 className="text-sm font-semibold">Filter audit log</h4>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-2">
          <div className="space-y-1">
            <Label htmlFor="audit-event-type" className="text-[10px]">Event type</Label>
            <Select
              value={filters.event_type}
              onValueChange={(v) => setFilters((f) => ({ ...f, event_type: v ?? 'ALL' }))}
            >
              <SelectTrigger id="audit-event-type">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {EVENT_TYPE_FILTER_OPTIONS.map((opt) => (
                  <SelectItem key={opt} value={opt}>
                    {opt}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-email" className="text-[10px]">Email contains</Label>
            <Input
              id="audit-email"
              value={filters.email}
              onChange={(e) => setFilters((f) => ({ ...f, email: e.target.value }))}
              placeholder="alice@…"
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-since" className="text-[10px]">Since (ISO-Z)</Label>
            <Input
              id="audit-since"
              value={filters.since}
              onChange={(e) => setFilters((f) => ({ ...f, since: e.target.value }))}
              placeholder="2026-05-25T00:00:00Z"
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="audit-until" className="text-[10px]">Until (ISO-Z)</Label>
            <Input
              id="audit-until"
              value={filters.until}
              onChange={(e) => setFilters((f) => ({ ...f, until: e.target.value }))}
              placeholder="2026-05-27T23:59:59Z"
            />
          </div>
        </div>
        <div className="flex justify-end gap-2">
          <Button
            size="sm"
            variant="ghost"
            onClick={handleClear}
            data-testid="admin-share-audit-clear-btn"
          >
            Clear
          </Button>
          <Button
            size="sm"
            onClick={applyFilters}
            disabled={loading}
            data-testid="admin-share-audit-apply-btn"
          >
            {loading && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
            Apply
          </Button>
        </div>
      </section>
      <section className="rounded-lg border bg-card p-4 max-h-[480px] overflow-y-auto">
        <h4 className="text-sm font-semibold mb-2">
          {filteredLogs ? 'Filtered audit events' : 'Recent audit events'}
        </h4>
        <Table className="font-mono text-[11px]">
          <TableHeader>
            <TableRow>
              <SortableHead label="Time" sortKey="timestamp" activeKey={sortKey} dir={sortDir} onSort={toggle} className="w-[180px]" />
              <SortableHead label="Event" sortKey="event_type" activeKey={sortKey} dir={sortDir} onSort={toggle} />
              <SortableHead label="Email" sortKey="email" activeKey={sortKey} dir={sortDir} onSort={toggle} />
              <SortableHead label="IP" sortKey="ip_address" activeKey={sortKey} dir={sortDir} onSort={toggle} />
              <SortableHead label="Details" sortKey="details" activeKey={sortKey} dir={sortDir} onSort={toggle} />
            </TableRow>
          </TableHeader>
          <TableBody>
            {auditTable.getRowModel().rows.map((row, i: number) => (
              <TableRow key={`${row.original.id || i}`}>
                {row.getVisibleCells().map((cell) => (
                  <TableCell
                    key={cell.id}
                    className={
                      cell.column.id === 'timestamp'
                        ? 'text-muted-foreground align-top whitespace-nowrap'
                        : cell.column.id === 'ip_address'
                          ? 'align-top whitespace-nowrap'
                          : cell.column.id === 'details'
                            ? 'text-muted-foreground align-top whitespace-normal break-words max-w-[420px]'
                            : 'align-top'
                    }
                  >
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))}
            {!rows.length && (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-xs text-muted-foreground">
                  No audit events yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </section>
    </div>
  )
}

// Shared query key so the hover-prefetch on the Admin → Share Dashboard
// link (in /admin/page.tsx) populates the same React Query cache entry
// the page reads on mount. Resulting UX: by the time the operator
// clicks Share Dashboard, the status payload is already in cache —
// page paints real content immediately instead of skeleton-then-swap.
const SHARE_STATUS_QUERY_KEY = queryKeys.admin.shareStatus()
// Companion key for the lean /live poll — keeps its own cache slot so
// the mount-time /status query isn't refetched on the 10-s tick.
const SHARE_LIVE_QUERY_KEY = queryKeys.admin.shareLive()

type ShareLiveFields = Partial<
  Pick<
    ShareStatus,
    'sharing_active' | 'public_url' | 'active_session_count' | 'rate_limits' | 'telemetry'
  >
>

function normalizeShareTab(raw: string | null | undefined): ShareTab {
  return raw && VALID_SHARE_TABS.has(raw) ? (raw as ShareTab) : 'invites'
}

function ShareDashboardPageContent() {
  const queryClient = useQueryClient()
  const searchParams = useSearchParams()
  const activeServiceId = searchParams?.get('service')

  const [actionError, setActionError] = React.useState('')
  const [activeTab, setActiveTab] = useQueryState('tab', shareTabParser)
  const [auditEmailFilter, setAuditEmailFilter] = React.useState('')

  const handleTabChange = React.useCallback(
    (nextTab: string) => {
      void setActiveTab(normalizeShareTab(nextTab))
    },
    [setActiveTab],
  )

  // /status carries the full mount-time payload — services, invites,
  // sessions, audit logs (~11 KB). Mutations refetch this. No
  // refetchInterval; the 10-s live tick rides on /live below.
  const {
    data: status,
    error: statusError,
    isLoading: statusLoading,
    isFetching: statusFetching,
    refetch,
  } = useQuery({
    queryKey: SHARE_STATUS_QUERY_KEY,
    queryFn: async ({ signal }) => {
      return (await unwrapClient(client.GET('/api/admin/share/status', { signal }))) as ShareStatus
    },
    // 30s staleTime so the hover-prefetch from the /admin PageHeader chip
    // is reused on click even when the user lingers on hover. Mutations
    // call refresh() to bust this when invites/sessions change.
    staleTime: 30_000,
  })

  // /live carries only the fields that actually change in real time
  // (sharing_active, public_url, active_session_count, rate_limits,
  // telemetry). ~1.5 KB instead of ~11 KB. Freshness via the admin event
  // stream's `share` channel (see note below); this query is a one-shot
  // snapshot on mount plus a 5-min safety net for silently-dropped streams.
  const { data: live } = useQuery({
    queryKey: SHARE_LIVE_QUERY_KEY,
    queryFn: async ({ signal }) => {
      return (await unwrapClient(client.GET('/api/admin/share/live', { signal }))) as ShareLiveFields
    },
    staleTime: 10_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  // SSE push of /live changes rides the multiplexed admin event stream's
  // `share` channel (SyncStatusBadge adds it on /admin/share), which writes
  // this same ['admin','share','live'] cache key — one shared connection
  // instead of a second dedicated stream over the H1 admin tunnel. The
  // useQuery above keeps the one-shot mount snapshot + 5-min safety net.
  const refresh = React.useCallback(async () => {
    await refetch()
    // Also invalidate /live so the operator sees their just-revoked
    // session disappear without waiting for the 10-s tick.
    queryClient.invalidateQueries({ queryKey: SHARE_LIVE_QUERY_KEY })
  }, [refetch, queryClient])

  const statusErrorMsg = statusError
    ? extractApiError(statusError) || (statusError as Error).message || 'unable to load status'
    : ''

  // Merge /live fields into /status so SharingControlPanel (which reads
  // active_session_count + rate_limits + telemetry) sees fresh data on
  // every 10-s tick without us touching the full /status payload.
  const statusForPanels: ShareStatus | null = status
    ? { ...status, ...(live ?? {}) }
    : null

  const isInitialLoading = statusForPanels === null && !statusErrorMsg && statusLoading
  const isRefreshing = statusFetching && !isInitialLoading

  const activeService = status?.services?.find((s) => s.service_id === activeServiceId)

  return (
    <div className="space-y-6" data-testid="admin-share-root">
      <PageHeader
        title="Remote Dashboard Sharing"
        description="Start the share tunnel, manage analyst invitations, monitor live sessions, and review the audit trail."
        icon={ShieldAlert}
      >
        {activeService && activeService.remote_frontend_deployed ? (
          <TeardownRemoteFrontendDialog
            serviceId={activeService.service_id}
            domainName={activeService.sharing_domain || ''}
            onSuccess={refresh}
          />
        ) : (
          <DeployRemoteFrontendDialog />
        )}
        <Button
          variant="outline"
          size="sm"
          onClick={refresh}
          disabled={isRefreshing}
          data-testid="admin-share-refresh-btn"
        >
          <RefreshCw className={cn('h-4 w-4 mr-1', isRefreshing && 'animate-spin')} />
          Refresh
        </Button>
        <BackToAdminLink />
      </PageHeader>

      <Alert className="border-amber-300 bg-amber-50 text-amber-900 [&>svg]:text-amber-900">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>
          <span className="font-semibold">Secure your server before sharing.</span>{' '}
          Remote sharing exposes this dashboard to invited analysts over the public internet.
          Before enabling the tunnel, confirm the host has an up-to-date OS, firewall rules
          restricting inbound access to the share port, and (recommended) only allows the
          tunnel through a reverse proxy you control. Each invited analyst gets read-only
          access scoped to the services you grant — but the underlying server is yours to
          harden.
        </AlertDescription>
      </Alert>

      {statusErrorMsg && (
        <Alert variant="destructive" data-testid="admin-share-status-error">
          <AlertDescription>{statusErrorMsg}</AlertDescription>
        </Alert>
      )}
      {actionError && (
        <Alert variant="destructive" data-testid="admin-share-action-error">
          <AlertDescription>{actionError}</AlertDescription>
        </Alert>
      )}

      <div
        className={cn(
          'min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto] transition-opacity',
          isRefreshing && 'opacity-40 pointer-events-none',
        )}
        data-testid="admin-share-control-reservation"
      >
        {statusForPanels === null && !statusErrorMsg ? (
          <div className="space-y-3" aria-busy="true">
            <Skeleton className="h-[220px] w-full rounded-xl" />
          </div>
        ) : (
          <SharingControlPanel
            status={statusForPanels}
            onRefresh={refresh}
            onError={setActionError}
          />
        )}
      </div>

      <Tabs
        value={activeTab}
        onValueChange={handleTabChange}
        className="w-full"
        data-testid="admin-share-tabs"
      >
        <TabsList>
          <TabsTrigger value="invites" data-testid="admin-share-tab-invites">
            Invitations
          </TabsTrigger>
          <TabsTrigger value="sessions" data-testid="admin-share-tab-sessions">
            Sessions
          </TabsTrigger>
          <TabsTrigger value="audit" data-testid="admin-share-tab-audit">
            Audit
          </TabsTrigger>
        </TabsList>

        <div
          className={cn(
            'min-h-[360px] [contain-intrinsic-size:auto_360px] [content-visibility:auto] transition-opacity',
            isRefreshing && 'opacity-40 pointer-events-none',
          )}
          data-testid="admin-share-panel-reservation"
        >
          {statusForPanels === null && !statusErrorMsg ? (
            <div className="pt-4 space-y-3" aria-busy="true">
              <Skeleton className="h-[360px] w-full rounded-xl" />
            </div>
          ) : (
            <>
              <TabsContent value="invites" className="pt-4">
                {activeTab === 'invites' && (
                  <InvitationsPanel
                    status={statusForPanels}
                    onRefresh={refresh}
                    onError={setActionError}
                    onViewAuditLogs={(email) => {
                      setAuditEmailFilter(email)
                      handleTabChange('audit')
                    }}
                  />
                )}
              </TabsContent>

              <TabsContent value="sessions" className="pt-4">
                {activeTab === 'sessions' && (
                  <SessionsPanel
                    status={statusForPanels}
                    onRefresh={refresh}
                    onError={setActionError}
                  />
                )}
              </TabsContent>

              <TabsContent value="audit" className="pt-4">
                {activeTab === 'audit' && (
                  <AuditLogPanel
                    status={statusForPanels}
                    onError={setActionError}
                    initialEmailFilter={auditEmailFilter}
                    onClearInitialFilter={() => setAuditEmailFilter('')}
                  />
                )}
              </TabsContent>
            </>
          )}
        </div>
      </Tabs>
    </div>
  )
}

export default function ShareDashboardPage() {
  return (
    <React.Suspense fallback={<Loading />}>
      <ShareDashboardPageContent />
    </React.Suspense>
  )
}
