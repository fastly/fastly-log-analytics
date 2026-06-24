'use client'

import * as React from 'react'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { AlertTriangle, Check, Loader2 } from 'lucide-react'
import { analystFetch } from '@/lib/analystFetch'

interface AcknowledgeButtonProps {
  version: string
}

export function AcknowledgeButton({ version }: AcknowledgeButtonProps) {
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const errorAlertRef = React.useRef<HTMLDivElement>(null)

  // Move focus to the error alert when one appears so SR users hear it
  // and keyboard users immediately know where they are.
  React.useEffect(() => {
    if (error) errorAlertRef.current?.focus()
  }, [error])

  const accept = async () => {
    setBusy(true)
    setError(null)
    try {
      // Raw fetch: share-* routes use a relative path so the request flows
      // through the Next.js proxy in remote-analyst mode rather than the
      // typed client's direct-to-loopback path.
      const res = await analystFetch('/api/share/acknowledge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version }),
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
    <>
      {error && (
        <Alert variant="destructive" ref={errorAlertRef} tabIndex={-1}>
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      <div className="flex justify-end">
        <Button onClick={accept} disabled={busy}>
          {busy ? (
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <Check className="h-4 w-4 mr-2" />
          )}
          I acknowledge
        </Button>
      </div>
    </>
  )
}
