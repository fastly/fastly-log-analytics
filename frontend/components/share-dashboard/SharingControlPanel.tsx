'use client'

import * as React from 'react'
import { Activity, ExternalLink, Loader2, Lock, Wifi, WifiOff, X } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { client, extractApiError } from '@/lib/api'

import { formatUptime, type ShareStatus } from './utils'

interface SharingControlPanelProps {
  status: ShareStatus | null
  onRefresh: () => Promise<void> | void
  onError: (msg: string) => void
}

type SharingMode = 'tunnel' | 'hostname' | 'ip'

const IPV4_RE = /^(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?$/

function buildEndpoint(mode: SharingMode, raw: string): string {
  const trimmed = raw.trim()
  if (!trimmed) return ''
  // If the user already typed a scheme, trust it; otherwise prepend https://.
  return /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`
}

export function SharingControlPanel({ status, onRefresh, onError }: SharingControlPanelProps) {
  const [busy, setBusy] = React.useState(false)
  const [mode, setMode] = React.useState<SharingMode>('tunnel')
  const [hostnameValue, setHostnameValue] = React.useState('')
  const [ipValue, setIpValue] = React.useState('')

  const sharingActive = !!status?.sharing_active

  const handleStart = async () => {
    onError('')
    const raw = mode === 'hostname' ? hostnameValue : mode === 'ip' ? ipValue : ''
    const publicEndpoint = buildEndpoint(mode, raw)
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
    setBusy(true)
    try {
      await client.POST('/api/admin/share/start' as any, {
        body: {
          public_endpoint: publicEndpoint,
          forward_port: 3000,
        },
      } as any)
      await onRefresh()
    } catch (e: any) {
      onError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  const handleStop = async () => {
    setBusy(true)
    try {
      await client.POST('/api/admin/share/stop' as any, {} as any)
      await onRefresh()
    } catch (e: any) {
      onError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  const handlePanic = async () => {
    if (!confirm('Sever ALL remote access immediately? This boots every analyst and stops the tunnel.')) return
    setBusy(true)
    try {
      await client.POST('/api/admin/share/panic' as any, {} as any)
      await onRefresh()
    } catch (e: any) {
      onError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  const handlePreviewInNewTab = () => {
    if (!status?.public_url) return
    window.open(`${status.public_url}/share-login`, '_blank', 'noopener,noreferrer')
  }

  return (
    <section className="rounded-lg border bg-card p-4 space-y-3">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          {sharingActive ? (
            <Wifi className="h-4 w-4 text-emerald-500" />
          ) : (
            <WifiOff className="h-4 w-4 text-muted-foreground" />
          )}
          <h3 className="font-semibold text-sm">Sharing</h3>
          <Badge variant={sharingActive ? 'default' : 'secondary'}>
            {sharingActive ? 'ACTIVE' : 'OFF'}
          </Badge>
        </div>
        <div className="flex items-center gap-2">
          {sharingActive ? (
            <>
              <Button size="sm" variant="outline" onClick={handleStop} disabled={busy}>
                {busy ? <Loader2 className="h-3 w-3 mr-1 animate-spin" /> : null}
                {busy ? 'Stopping…' : 'Stop'}
              </Button>
              <Button size="sm" variant="destructive" onClick={handlePanic} disabled={busy}>
                {busy ? (
                  <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                ) : (
                  <X className="h-4 w-4 mr-1" />
                )}
                {busy ? 'Severing…' : 'Sever All Access'}
              </Button>
            </>
          ) : (
            <Button size="sm" onClick={handleStart} disabled={busy}>
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

      {sharingActive && status?.telemetry && (
        <div className="flex items-center gap-3 text-[11px] text-muted-foreground border-t pt-2 mt-2">
          <span className="flex items-center gap-1">
            <Activity className="h-3 w-3" />
            Uptime: <span className="font-mono">{formatUptime(status.telemetry.current_uptime_s)}</span>
          </span>
          <span>
            Heartbeat 401s since boot:{' '}
            <span className="font-mono">{status.telemetry.heartbeat_unauth_count}</span>
          </span>
          {status.telemetry.tunnel_uptime_history.length > 0 && (
            <span>
              Prior sessions:{' '}
              <span className="font-mono">{status.telemetry.tunnel_uptime_history.length}</span>
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
          {status?.rate_limits?.lockouts?.map((l) => (
            <div key={`lo-${l.ip}`} className="font-mono">
              {l.ip} — locked out for {l.remaining_s}s
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
              htmlFor="mode-tunnel"
              className="flex items-start gap-2 cursor-pointer rounded-md border p-2 hover:bg-muted/50"
            >
              <input
                type="radio"
                id="mode-tunnel"
                name="sharing-mode"
                className="mt-1"
                checked={mode === 'tunnel'}
                onChange={() => setMode('tunnel')}
              />
              <div className="flex-1 space-y-0.5">
                <div className="text-sm font-medium">SSH reverse tunnel (localhost.run)</div>
                <p className="text-xs text-muted-foreground">
                  Easiest option. Analyst traffic transits a third-party relay
                  (<a
                    href="https://localhost.run"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline"
                  >
                    localhost.run
                  </a>
                  ). Review with your legal team if you have GDPR or HIPAA exposure.
                </p>
              </div>
            </label>

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
    </section>
  )
}
