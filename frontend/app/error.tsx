'use client'

// Next.js App Router segment-level error boundary. Catches uncaught
// render errors anywhere in the route tree below the root layout —
// the layout's <AppLayout> shell (sidebar + header + filter bar) keeps
// rendering, only the page body is replaced with the fallback.
//
// Pair with global-error.tsx (which replaces the entire <html> if the
// root layout itself throws). Without these two files, every render
// error bubbles to the single class-based ErrorBoundary wired around
// {children} in layout.tsx — that catches but loses route-segment
// isolation (the boundary stays tripped across navigations until the
// app reloads). This file resets automatically on navigation per the
// Next.js convention.

import { useEffect } from 'react'
import { AlertCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'

export default function ErrorPage({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  useEffect(() => {
    // Surface the error so the existing console + Sentry/PostHog
    // listeners (if/when wired) pick it up. Digest is the build-time
    // hash Next.js assigns each unique error site — useful for de-dup
    // in any aggregator.
    if (typeof console !== 'undefined') {
      console.error('[app/error] uncaught render error:', error, error.digest)
    }
  }, [error])

  return (
    <div className="flex flex-1 items-center justify-center p-6">
      <div
        role="alert"
        aria-live="assertive"
        className="flex w-full max-w-md flex-col items-start gap-3 rounded-lg border border-destructive/30 bg-destructive/5 p-6"
      >
        <div className="flex items-center gap-2 text-destructive">
          <AlertCircle className="h-5 w-5" aria-hidden="true" />
          {/* text-foreground (not the inherited destructive red): red text on
              the destructive/5 tint is sub-AA contrast. Keep the icon red. */}
          <h2 className="text-base font-semibold text-foreground">Something went wrong on this page.</h2>
        </div>
        <p className="text-sm text-muted-foreground">
          The view failed to render. The rest of the app is still working —
          you can retry, navigate to another page, or reload.
        </p>
        {error?.message ? (
          // tabIndex={0} so keyboard users can scroll the overflow-x region
          // (axe: scrollable-region-focusable — a scrollable area must be
          // reachable by keyboard).
          <pre
            tabIndex={0}
            aria-label="Error details"
            className="w-full overflow-x-auto rounded bg-muted/50 p-2 font-mono text-xs text-muted-foreground"
          >
            {error.message}
          </pre>
        ) : null}
        <div className="flex gap-2">
          <Button type="button" variant="default" size="sm" onClick={() => reset()}>
            Try again
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => {
              if (typeof window !== 'undefined') window.location.reload()
            }}
          >
            Reload page
          </Button>
        </div>
        {error?.digest ? (
          <div className="text-[10px] text-muted-foreground">
            Reference: <span className="font-mono">{error.digest}</span>
          </div>
        ) : null}
      </div>
    </div>
  )
}
