import * as React from 'react'
import { redirect } from 'next/navigation'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { fetchTosServerSide } from '@/lib/ssr/tos'
import { AcknowledgeButton } from './AcknowledgeButton'
import { AcknowledgeFallback } from './AcknowledgeFallback'

// Per-request SSR — the TOS payload + auth gate land in initial HTML.
export const dynamic = 'force-dynamic'

export default async function AcknowledgePage() {
  const tos = await fetchTosServerSide()
  if (tos === 'unauthenticated') {
    redirect('/share-login')
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-muted/40 p-6">
      <Card className="w-full max-w-xl">
        <CardHeader>
          <CardTitle>Terms of access</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
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
