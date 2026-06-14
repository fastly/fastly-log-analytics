'use client'

import * as React from 'react'
import { useRouter } from 'next/navigation'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { AlertTriangle, Loader2 } from 'lucide-react'
import { fetchWithTimeout } from '@/lib/fetchWithTimeout'
import { AcknowledgeButton } from './AcknowledgeButton'

type TosPayload = { version: string; text: string }

// Fallback for when the SSR TOS fetch failed (backend hiccup, missing
// API_PROXY_URL). Reproduces the original useEffect-based behavior so the
// user still gets a working page when the server-side path is broken.
export function AcknowledgeFallback() {
  const router = useRouter()
  const [tos, setTos] = React.useState<TosPayload | null>(null)
  const [error, setError] = React.useState<string | null>(null)

  React.useEffect(() => {
    let cancelled = false
    fetchWithTimeout('/api/share/tos', {
      credentials: 'include',
      headers: { 'X-Remote-Analyst': '1' },
    })
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
        <Alert variant="destructive">
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
        !error && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
      )}
    </>
  )
}
