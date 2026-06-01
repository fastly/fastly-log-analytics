'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { AlertTriangle, Check, Loader2 } from 'lucide-react'

type TosPayload = { version: string; text: string }

export default function AcknowledgePage() {
  const router = useRouter()
  const [tos, setTos] = React.useState<TosPayload | null>(null)
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  React.useEffect(() => {
    let cancelled = false
    // Raw fetch: the share-* routes use a relative path so the request flows
    // through the Next.js proxy in remote-analyst mode rather than the typed
    // client's `getApiBase()` which routes direct to 127.0.0.1:8000.
    fetch('/api/share/heartbeat', {
      credentials: 'include',
      headers: { 'X-Remote-Analyst': '1' },
    })
      .then(async (res) => {
        if (cancelled) return
        if (res.status === 401) {
          router.replace('/share-login')
          return
        }
        // The /heartbeat response doesn't include TOS text — pull it from the
        // login response that just preceded this navigation. Fall back to a
        // generic acknowledgment text if we landed here cold (refresh).
        setTos({
          version: '__current__',
          text:
            'I acknowledge that I am viewing third-party operational log data, ' +
            'that my access is logged, and that I will not retain, redistribute, ' +
            'or use this data outside the scope of my engagement.',
        })
      })
      .catch(() => {
        if (!cancelled) setError('Could not reach the server.')
      })
    return () => {
      cancelled = true
    }
  }, [router])

  const accept = async () => {
    if (!tos) return
    setBusy(true)
    setError(null)
    try {
      // Raw fetch: share-* routes — see comment above on heartbeat call.
      const res = await fetch('/api/share/acknowledge', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Remote-Analyst': '1',
        },
        body: JSON.stringify({ version: tos.version }),
        credentials: 'include',
      })
      if (!res.ok) {
        const body = await res.json().catch(() => null)
        setError(body?.detail?.error || `Acknowledgment failed (${res.status}).`)
        return
      }
      // Hard reload, not client-side router.replace: AppLayout's bootstrap
      // query was cached BEFORE the session cookie was set, so it still
      // says is_remote_analyst=true,needs_login=true. A SPA navigation
      // re-renders /dashboard with that stale cache → AppLayout bounces
      // back to /share-login. .assign() forces a fresh document load that
      // re-fetches bootstrap with the new cookie.
      window.location.assign('/dashboard')
    } catch (err: any) {
      setError(err?.message || 'Network error.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-muted/40 p-6">
      <Card className="w-full max-w-xl">
        <CardHeader>
          <CardTitle>Terms of access</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {error && (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          {tos ? (
            <p className="text-sm leading-relaxed text-muted-foreground">{tos.text}</p>
          ) : (
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
          )}
          <div className="flex justify-end">
            <Button onClick={accept} disabled={busy || !tos}>
              {busy ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Check className="h-4 w-4 mr-2" />
              )}
              I acknowledge
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
