'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Eye, EyeOff, KeyRound, Loader2, AlertTriangle } from 'lucide-react'
import type { components } from '@/types/api.generated'

type LoginResponse = components['schemas']['ShareLoginResponse']

export default function ShareLoginPage() {
  const router = useRouter()
  const [email, setEmail] = React.useState('')
  const [passcode, setPasscode] = React.useState('')
  const [reveal, setReveal] = React.useState(false)
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const [retryAfter, setRetryAfter] = React.useState<number | null>(null)

  // Countdown for rate-limit lockouts.
  React.useEffect(() => {
    if (retryAfter == null || retryAfter <= 0) return
    const t = setTimeout(() => setRetryAfter((s) => (s == null ? null : s - 1)), 1000)
    return () => clearTimeout(t)
  }, [retryAfter])

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
      const res = await fetch('/api/share/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Remote-Analyst': '1',
        },
        body: JSON.stringify({ email, passcode }),
        credentials: 'include',
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
        // cache concern. Client-side push is fine here.
        router.push('/share-login/acknowledge')
      } else {
        // Hard reload to bypass the React Query bootstrap cache that was
        // populated BEFORE the session cookie was set (otherwise AppLayout
        // sees stale needs_login=true and bounces back here).
        window.location.assign(data.redirect ?? '/dashboard')
      }
    } catch (err: any) {
      setError(err?.message || 'Network error reaching the server.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="min-h-screen flex justify-center bg-muted/40 p-6 pt-20">
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-2">
          <CardTitle className="flex items-center gap-2 text-xl">
            <KeyRound className="h-5 w-5" />
            Analyst sign-in
          </CardTitle>
          <p className="text-sm text-muted-foreground">
            Access to this dashboard is invite-only. Enter the email address the
            invite was sent to, and the passcode from the invitation message. If
            you don&apos;t have an invite, ask the dashboard owner to send you one.
          </p>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-1">
              <Label htmlFor="email" className="text-xs">Email</Label>
              <Input
                id="email"
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
                  {reveal ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </Button>
              </div>
            </div>

            {error && (
              <Alert variant="destructive">
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
        </CardContent>
      </Card>
    </div>
  )
}
