'use client'

import { useEffect } from 'react'
import { AlertCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'

interface RouteErrorBoundaryProps {
  error: Error & { digest?: string }
  reset: () => void
  pageName?: string
  logPrefix?: string
}

function RouteErrorBoundary({
  error,
  reset,
  pageName,
  logPrefix = '[app/error]',
}: RouteErrorBoundaryProps) {
  useEffect(() => {
    // Surface the error so the existing console + Sentry/PostHog
    // listeners (if/when wired) pick it up. Digest is the build-time
    // hash Next.js assigns each unique error site — useful for de-dup
    // in any aggregator.
    if (typeof console !== 'undefined') {
      console.error(`${logPrefix} uncaught render error:`, error, error.digest)
    }
  }, [error, logPrefix])

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
          <h2 className="text-base font-semibold text-foreground">
            {pageName
              ? `Something went wrong on the ${pageName} page.`
              : 'Something went wrong on this page.'}
          </h2>
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

export function createRouteErrorBoundary(pageName?: string, logPrefix?: string) {
  function RouteErrorPage({
    error,
    reset,
  }: {
    error: Error & { digest?: string }
    reset: () => void
  }) {
    return (
      <RouteErrorBoundary
        error={error}
        reset={reset}
        pageName={pageName}
        logPrefix={logPrefix}
      />
    )
  }
  RouteErrorPage.displayName = pageName
    ? `${pageName.replace(/\s+/g, '')}ErrorPage`
    : 'ErrorPage'
  return RouteErrorPage
}
