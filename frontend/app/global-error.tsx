'use client'

// Next.js App Router root-error boundary. Replaces the entire <html>
// when the root layout itself throws (which the segment-level
// app/error.tsx can't catch, because it's mounted inside the
// layout). Must therefore include its own <html> + <body> tags.
//
// In practice this fires when the SSR'd /api/bootstrap or the
// QueryProvider hydration throws — the whole shell is unmountable
// at that point so we paint a minimal page with retry. Keep deps
// tiny (no shadcn, no themed Card) — the failure mode might itself
// be a missing module, so leaning on the rest of the app's UI
// kit would risk a cascading boundary.

import { useEffect } from 'react'

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  useEffect(() => {
    if (typeof console !== 'undefined') {
      console.error('[app/global-error] root layout failed:', error, error.digest)
    }
  }, [error])

  return (
    <html lang="en">
      <body
        style={{
          fontFamily:
            'ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
          margin: 0,
          minHeight: '100vh',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          background: '#fafafa',
          color: '#171717',
        }}
      >
        <div
          role="alert"
          aria-live="assertive"
          style={{
            maxWidth: 480,
            padding: 24,
            border: '1px solid #fecaca',
            borderRadius: 8,
            background: '#fef2f2',
          }}
        >
          <h1 style={{ margin: '0 0 8px', fontSize: 18, fontWeight: 600, color: '#991b1b' }}>
            Couldn&apos;t load the dashboard.
          </h1>
          <p style={{ margin: '0 0 12px', fontSize: 14, color: '#525252' }}>
            The application shell failed to start. This is usually transient — please retry.
          </p>
          {error?.message ? (
            // tabIndex={0} so keyboard users can scroll the overflow-x region
            // (axe: scrollable-region-focusable).
            <pre
              tabIndex={0}
              aria-label="Error details"
              style={{
                margin: '0 0 12px',
                padding: 8,
                background: '#f5f5f5',
                fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
                fontSize: 12,
                color: '#525252',
                overflowX: 'auto',
                borderRadius: 4,
              }}
            >
              {error.message}
            </pre>
          ) : null}
          <button
            type="button"
            onClick={() => reset()}
            style={{
              padding: '6px 12px',
              fontSize: 14,
              borderRadius: 4,
              border: '1px solid #dc2626',
              background: '#dc2626',
              color: 'white',
              cursor: 'pointer',
            }}
          >
            Try again
          </button>
          {error?.digest ? (
            <div style={{ marginTop: 12, fontSize: 10, color: '#737373' }}>
              Reference:{' '}
              <span style={{ fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' }}>
                {error.digest}
              </span>
            </div>
          ) : null}
        </div>
      </body>
    </html>
  )
}
