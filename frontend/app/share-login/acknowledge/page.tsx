import * as React from 'react'
import { redirect } from 'next/navigation'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { fetchTosServerSide } from '@/lib/ssr/upstream'
import { AcknowledgeButton, AcknowledgeFallback } from '../page'

// Per-request SSR — the TOS payload + auth gate land in initial HTML.
export const dynamic = 'force-dynamic'

export default async function AcknowledgePage() {
  const tos = await fetchTosServerSide()
  if (tos === 'unauthenticated') {
    redirect('/share-login')
  }

  return (
    <div
      data-testid="share-acknowledge-page"
      className="min-h-screen flex items-center justify-center bg-muted/40 p-6"
    >
      <Card
        data-testid="share-acknowledge-card"
        className="w-full max-w-xl min-h-[260px] [contain-intrinsic-size:auto_260px] [content-visibility:auto]"
      >
        <CardHeader>
          <CardTitle>Terms of access</CardTitle>
        </CardHeader>
        <CardContent
          data-testid="share-acknowledge-card-content"
          className="space-y-4 min-h-[160px] [contain-intrinsic-size:auto_160px] [content-visibility:auto]"
        >
          {tos ? (
            <>
              <p className="text-sm leading-relaxed text-muted-foreground">{tos.text}</p>
              <AcknowledgeButton version={tos.version} />
            </>
          ) : (
            <AcknowledgeFallback />
          )}
        </CardContent>
      </Card>
    </div>
  )
}
