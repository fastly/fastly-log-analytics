'use client'

import * as React from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Eye, EyeOff, Loader2, AlertTriangle } from 'lucide-react'
import type { components } from '@/types/api.generated'
import { analystFetch } from '@/lib/analystFetch'

type LoginResponse = components['schemas']['ShareLoginResponse']

// Cap the post-login redirect to in-app paths only. Without this,
// ?return=//evil.example/ would relative-resolve to evil.example via
// window.location.assign, turning the login screen into an open
// redirector. Any value not starting with a single forward slash falls
// back to the server-suggested redirect.
export function safeReturnTarget(raw: string | null | undefined): string | null {
  if (!raw) return null
  // Strip whitespace before the prefix check. The browser removes embedded
  // tabs/newlines/CRs when resolving a URL, so `/\t/evil.com` would slip past
  // an exact-prefix blocklist and then resolve to the protocol-relative
  // `//evil.com`. Normalizing first collapses those smuggled forms back into
  // the `//` / `/\` shapes the blocklist already rejects.
  const normalized = raw.replace(/\s/g, '')
  if (!normalized.startsWith('/')) return null
  if (normalized.startsWith('//') || normalized.startsWith('/\\')) return null
  return raw
}

export function ShareLoginForm() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const returnTarget = safeReturnTarget(searchParams.get('return'))
  const [email, setEmail] = React.useState('')
  const [passcode, setPasscode] = React.useState('')
  const [reveal, setReveal] = React.useState(false)
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const [retryAfter, setRetryAfter] = React.useState<number | null>(null)
  const emailInputRef = React.useRef<HTMLInputElement>(null)
  const errorAlertRef = React.useRef<HTMLDivElement>(null)

  // Countdown for rate-limit lockouts.
  React.useEffect(() => {
    if (retryAfter == null || retryAfter <= 0) return
    const t = setTimeout(() => setRetryAfter((s) => (s == null ? null : s - 1)), 1000)
    return () => clearTimeout(t)
  }, [retryAfter])

  // Auto-focus the email field on mount so keyboard users land on the
  // first form input instead of the skip-link.
  React.useEffect(() => {
    emailInputRef.current?.focus()
  }, [])

  // Move focus to the error alert when one appears so SR users hear it
  // and keyboard users immediately know where they are.
  React.useEffect(() => {
    if (error) errorAlertRef.current?.focus()
  }, [error])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (busy) return
    setError(null)
    setBusy(true)
    try {
      // Raw fetch (not typed `client`): the share-login UX needs per-status
      // branching (429 rate-limit countdown, 403 IP-whitelist, 401 invalid,
      // 503 capacity) and a relative URL so the request flows through the
      // Next.js proxy in remote-analyst mode. The typed client's middleware
      // throws on any non-OK response, collapsing those distinctions.
      const res = await analystFetch('/api/share/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, passcode }),
      })
      const body = await res.json().catch(() => null)
      if (res.status === 429) {
        const retry =
          body?.detail?.retry_after_s ?? Number(res.headers.get('Retry-After') || 60)
        setRetryAfter(retry)
        setError('Too many failed attempts — temporarily locked out.')
        return
      }
      if (res.status === 403) {
        setError(
          body?.detail?.error === 'ip_not_whitelisted'
            ? 'Your IP address is not on the approved list for this invitation.'
            : 'Access is currently blocked.',
        )
        return
      }
      if (res.status === 401) {
        setError('Invalid email or passcode.')
        return
      }
      if (res.status === 503) {
        setError('The dashboard is at capacity. Try again shortly.')
        return
      }
      if (!res.ok) {
        setError(body?.detail?.error || `Login failed (HTTP ${res.status}).`)
        return
      }
      const data = body as LoginResponse
      if (data.tos_pending) {
        // /share-login/acknowledge still uses bootstrap, but its own
        // useEffect fetches heartbeat first which is unauth — no stale
        // cache concern. Client-side push is fine here. Preserve the
        // return param so the TOS acknowledge can bounce them back.
        const ack = returnTarget
          ? `/share-login/acknowledge?return=${encodeURIComponent(returnTarget)}`
          : '/share-login/acknowledge'
        router.push(ack)
      } else {
        // E-3: honor the ?return= param set by the 401-redirect fetch
        // middleware in lib/api.ts so the analyst lands back on the
        // page they were viewing before their session died. Falls back
        // to the server-suggested redirect, then /dashboard.
        // Hard reload to bypass the React Query bootstrap cache that was
        // populated BEFORE the session cookie was set (otherwise AppLayout
        // sees stale needs_login=true and bounces back here).
        window.location.assign(returnTarget ?? data.redirect ?? '/dashboard')
      }
    } catch (err: any) {
      setError(err?.message || 'Network error reaching the server.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      <div className="space-y-1">
        <Label htmlFor="email" className="text-xs">Email</Label>
        <Input
          id="email"
          ref={emailInputRef}
          type="email"
          autoComplete="email"
          required
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          disabled={busy}
        />
      </div>
      <div className="space-y-1">
        <Label htmlFor="passcode" className="text-xs">Passcode</Label>
        <div className="flex items-center gap-2">
          <Input
            id="passcode"
            type={reveal ? 'text' : 'password'}
            autoComplete="current-password"
            required
            value={passcode}
            onChange={(e) => setPasscode(e.target.value)}
            disabled={busy}
          />
          <Button
            type="button"
            variant="ghost"
            size="icon"
            onClick={() => setReveal((r) => !r)}
            aria-label={reveal ? 'Hide passcode' : 'Reveal passcode'}
          >
            {reveal ? <EyeOff className="h-4 w-4" aria-hidden="true" /> : <Eye className="h-4 w-4" aria-hidden="true" />}
          </Button>
        </div>
      </div>

      {error && (
        <Alert variant="destructive" ref={errorAlertRef} tabIndex={-1}>
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            {error}
            {retryAfter && retryAfter > 0 ? ` Retry in ${retryAfter}s.` : ''}
          </AlertDescription>
        </Alert>
      )}

      <Button
        type="submit"
        className="w-full"
        disabled={busy || (retryAfter != null && retryAfter > 0)}
      >
        {busy && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
        Sign in
      </Button>
    </form>
  )
}
