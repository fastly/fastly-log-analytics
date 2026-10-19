import * as React from 'react'
import { Suspense } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { KeyRound, Loader2 } from 'lucide-react'
import { ShareLoginForm } from './ShareLoginForm'

export default function ShareLoginPage() {
  return (
    <div
      data-testid="share-login-page"
      className="min-h-screen flex justify-center bg-muted/40 p-6 pt-20"
    >
      {/* A-15 (WCAG 1.3.1 / 2.4.6): CardTitle below is a styled <div>, so
          this page would otherwise have zero headings. Sr-only <h1> gives
          screen-reader heading nav a landing spot without affecting the
          visual design. */}
      <h1 className="sr-only">Analyst sign-in</h1>
      <Card
        data-testid="share-login-card"
        className="w-full max-w-md self-start min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]"
      >
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
        <CardContent
          data-testid="share-login-card-content"
          className="min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]"
        >
          {/* ShareLoginForm reads ?oauth_error / ?return / ?next / ?token via
              useSearchParams, which forces a client bailout — boundary it so
              prefetch/loading.tsx behavior is preserved (design §5.2).
              Matching min-h-[240px] fallback avoids CLS while search params resolve. */}
          <Suspense
            fallback={
              <div
                data-testid="share-login-suspense-fallback"
                role="status"
                aria-live="polite"
                className="min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto] flex flex-col justify-center items-center gap-2 text-xs text-muted-foreground"
              >
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
                <span>Verifying share session &amp; security policies...</span>
              </div>
            }
          >
            <ShareLoginForm />
          </Suspense>
        </CardContent>
      </Card>
    </div>
  )
}
