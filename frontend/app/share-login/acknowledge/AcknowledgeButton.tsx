'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { AlertTriangle, Check, Loader2 } from 'lucide-react'
import { analystFetch } from '@/lib/analystFetch'
import type { components } from '@/types/api.generated'
import { safeReturnTarget } from '../ShareLoginForm'

type TosPayload = components['schemas']['TosDocument']

interface AcknowledgeButtonProps {
  version: string
  returnTo?: string
}

export function AcknowledgeButton({ version, returnTo }: AcknowledgeButtonProps) {
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const errorAlertRef = React.useRef<HTMLDivElement>(null)

  React.useEffect(() => {
    if (error) errorAlertRef.current?.focus()
  }, [error])

  const accept = async () => {
    setBusy(true)
    setError(null)
    try {
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
      const search = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : null
      const safeTarget = safeReturnTarget(
        returnTo ?? search?.get('return') ?? search?.get('next'),
      )
      window.location.assign(safeTarget ?? '/dashboard')
    } catch (err) {
      setError(err instanceof Error && err.message ? err.message : 'Network error.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div
      data-testid="share-acknowledge-actions"
      className={busy ? 'space-y-4 opacity-40 pointer-events-none transition-opacity' : 'space-y-4 transition-opacity'}
    >
      {error && (
        <Alert variant="destructive" ref={errorAlertRef} tabIndex={-1} data-testid="share-acknowledge-error">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      <div className="flex justify-end">
        <Button onClick={accept} disabled={busy} data-testid="share-acknowledge-submit">
          {busy ? (
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <Check className="h-4 w-4 mr-2" />
          )}
          I acknowledge
        </Button>
      </div>
    </div>
  )
}

export function AcknowledgeFallback() {
  const router = useRouter()
  const [tos, setTos] = React.useState<TosPayload | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const errorAlertRef = React.useRef<HTMLDivElement>(null)

  React.useEffect(() => {
    if (error) errorAlertRef.current?.focus()
  }, [error])

  React.useEffect(() => {
    let cancelled = false
    analystFetch('/api/share/tos')
      .then(async (res) => {
        if (cancelled) return
        if (res.status === 401) {
          router.replace('/share-login')
          return
        }
        if (!res.ok) {
          setError(`Could not load the terms (HTTP ${res.status}).`)
          return
        }
        const body = (await res.json()) as TosPayload
        setTos({ version: body.version, text: body.text })
      })
      .catch(() => {
        if (!cancelled) setError('Could not reach the server.')
      })
    return () => {
      cancelled = true
    }
  }, [router])

  return (
    <div
      data-testid="share-acknowledge-fallback"
      className="space-y-4 min-h-[140px] [contain-intrinsic-size:auto_140px] [content-visibility:auto]"
    >
      {error && (
        <Alert variant="destructive" ref={errorAlertRef} tabIndex={-1}>
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {tos ? (
        <>
          <p className="text-sm leading-relaxed text-muted-foreground">{tos.text}</p>
          <AcknowledgeButton version={tos.version} />
        </>
      ) : (
        !error && (
          <div
            role="status"
            data-testid="share-acknowledge-loading"
            className="min-h-[120px] [contain-intrinsic-size:auto_120px] [content-visibility:auto] flex flex-col items-center justify-center gap-2 text-xs text-muted-foreground"
          >
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
            <span>Verifying share session &amp; security policies...</span>
            <span className="sr-only">Loading the terms…</span>
          </div>
        )
      )}
    </div>
  )
}
