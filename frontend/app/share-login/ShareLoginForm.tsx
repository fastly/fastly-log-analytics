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
type AuthConfig = components['schemas']['AuthConfigResponse']

// Fixed code→copy allowlist for ?oauth_error=<code>. The raw param is NEVER
// rendered (reflected-content hygiene on an unauth page) — an unknown code
// falls back to the generic message. Kept deliberately non-enumerating: the
// "not_invited" copy is identical for uninvited AND sub/provider mismatch
// (design §2.9 / §5.2).
const OAUTH_ERROR_COPY: Record<string, string> = {
  unverified_email: "Your account's email isn't verified. Contact your admin.",
  not_invited:
    "This account isn't invited to a dashboard. Ask the dashboard owner for an invitation.",
  wrong_domain: 'Please use your organization account.',
  idp_unavailable: 'Sign-in is temporarily unavailable. Try again, or sign in with a passcode.',
}
const OAUTH_ERROR_DEFAULT = 'Sign-in failed. Please try again.'

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
  const oauthErrorCode = searchParams.get('oauth_error')
  const [email, setEmail] = React.useState('')
  const [passcode, setPasscode] = React.useState('')
  const [reveal, setReveal] = React.useState(false)
  const [busy, setBusy] = React.useState(false)
  // Seed the banner from ?oauth_error via the fixed allowlist (never the raw
  // param). A passcode-login error later overwrites this.
  const [error, setError] = React.useState<string | null>(() =>
    oauthErrorCode ? (OAUTH_ERROR_COPY[oauthErrorCode] ?? OAUTH_ERROR_DEFAULT) : null,
  )
  const [retryAfter, setRetryAfter] = React.useState<number | null>(null)
  const [redirecting, setRedirecting] = React.useState<string | null>(null)
  // Auth-config drives graceful degradation. 'loading' → render nothing that
  // pops (avoid CLS); 'failed' → fail OPEN to the passcode form.
  const [config, setConfig] = React.useState<AuthConfig | null>(null)
  const [configState, setConfigState] = React.useState<'loading' | 'loaded' | 'failed'>('loading')
  const emailInputRef = React.useRef<HTMLInputElement>(null)
  const errorAlertRef = React.useRef<HTMLDivElement>(null)

  // Fetch the unauthenticated auth-config on mount to learn which modes to show.
  React.useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const res = await analystFetch('/api/share/auth-config')
        if (!res.ok) throw new Error(`status ${res.status}`)
        const data = (await res.json()) as AuthConfig
        if (!cancelled) {
          setConfig(data)
          setConfigState('loaded')
        }
      } catch {
        if (!cancelled) setConfigState('failed') // fail-open to passcode
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  const providers = config?.providers ?? []
  // Fail-open: on a config fetch failure, offer the passcode form.
  const passcodeEnabled = configState === 'failed' ? true : (config?.passcode_enabled ?? true)
  const showProviders = configState === 'loaded' && providers.length > 0
  const showPasscode = configState === 'failed' || (configState === 'loaded' && passcodeEnabled)
  const showLockout = configState === 'loaded' && !passcodeEnabled && providers.length === 0
  const showDivider = showProviders && showPasscode

  // Countdown for rate-limit lockouts.
  React.useEffect(() => {
    if (retryAfter == null || retryAfter <= 0) return
    const t = setTimeout(() => setRetryAfter((s) => (s == null ? null : s - 1)), 1000)
    return () => clearTimeout(t)
  }, [retryAfter])

  // Auto-focus the email field once the passcode form is actually shown so
  // keyboard users land on the first input.
  React.useEffect(() => {
    if (showPasscode) emailInputRef.current?.focus()
  }, [showPasscode])

  // Move focus to the error alert when one appears so SR users hear it
  // and keyboard users immediately know where they are.
  React.useEffect(() => {
    if (error) errorAlertRef.current?.focus()
  }, [error])

  const startSso = (providerId: string) => {
    setRedirecting(providerId)
    const q = new URLSearchParams({ provider: providerId })
    if (returnTarget) q.set('return', returnTarget)
    window.location.assign(`/api/share/oauth/authorize?${q.toString()}`)
  }

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
    <div className="space-y-4">
      {error && (
        <Alert variant="destructive" ref={errorAlertRef} tabIndex={-1}>
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            {error}
            {retryAfter && retryAfter > 0 ? ` Retry in ${retryAfter}s.` : ''}
          </AlertDescription>
        </Alert>
      )}

      {/* Escape hatch for the wrong-account case — the SSO buttons already force
          the account chooser (prompt=select_account), so point a stuck analyst
          at them (design §5.2). */}
      {showProviders &&
        (oauthErrorCode === 'not_invited' || oauthErrorCode === 'wrong_domain') && (
          <p className="text-xs text-muted-foreground text-center">
            Signed in with the wrong account? Use a sign-in button below — you can pick a
            different account.
          </p>
        )}

      {showProviders && (
        <div className="space-y-2">
          {providers.map((p) => (
            <Button
              key={p.id}
              type="button"
              variant="outline"
              className="w-full"
              disabled={redirecting != null}
              onClick={() => startSso(p.id)}
            >
              {redirecting === p.id && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
              Sign in with {p.display_name}
            </Button>
          ))}
        </div>
      )}

      {showDivider && (
        <div className="relative flex items-center py-1" aria-hidden="true">
          <div className="flex-grow border-t" />
          <span className="mx-3 text-xs text-muted-foreground">OR</span>
          <div className="flex-grow border-t" />
        </div>
      )}

      {showLockout && (
        <p className="text-sm text-muted-foreground text-center">
          Sign-in is not configured for this dashboard. Contact the dashboard owner.
        </p>
      )}

      {showPasscode && (
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

          <Button
            type="submit"
            className="w-full"
            disabled={busy || (retryAfter != null && retryAfter > 0)}
          >
            {busy && <Loader2 className="h-4 w-4 mr-2 animate-spin" />}
            Sign in
          </Button>
        </form>
      )}
    </div>
  )
}
