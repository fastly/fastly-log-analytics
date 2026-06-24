'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { AlertTriangle, Loader2 } from 'lucide-react'
import { analystFetch } from '@/lib/analystFetch'
import { AcknowledgeButton } from './AcknowledgeButton'

type TosPayload = { version: string; text: string }

// Fallback for when the SSR TOS fetch failed (backend hiccup, missing
// API_PROXY_URL). Reproduces the original useEffect-based behavior so the
// user still gets a working page when the server-side path is broken.
export function AcknowledgeFallback() {
  const router = useRouter()
  const [tos, setTos] = React.useState<TosPayload | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const errorAlertRef = React.useRef<HTMLDivElement>(null)

  // Move focus to the error alert when one appears so SR users hear it and
  // keyboard users immediately know where they are (mirrors AcknowledgeButton).
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
    <>
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
          <div role="status">
            <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
            <span className="sr-only">Loading the terms…</span>
          </div>
        )
      )}
    </>
  )
}
