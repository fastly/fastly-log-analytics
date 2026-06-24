'use client'

import React from 'react'
import { usePathname } from 'next/navigation'

interface ErrorBoundaryProps {
  children: React.ReactNode
  fallback?: (error: Error, reset: () => void) => React.ReactNode
}

interface ErrorBoundaryState {
  error: Error | null
}

export class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error }
  }

  componentDidCatch(error: Error, info: React.ErrorInfo) {
    if (typeof console !== 'undefined') {
      console.error('[ErrorBoundary] uncaught render error:', error, info?.componentStack)
    }
  }

  reset = () => this.setState({ error: null })

  render() {
    if (this.state.error) {
      if (this.props.fallback) {
        return this.props.fallback(this.state.error, this.reset)
      }
      return (
        <div
          role="alert"
          className="flex flex-col items-start gap-2 rounded border border-red-300 bg-red-50 p-4 text-sm text-red-900 dark:border-red-700 dark:bg-red-950 dark:text-red-100"
        >
          <div className="font-semibold">Something went wrong rendering this view.</div>
          <div className="font-mono text-xs opacity-80">{this.state.error.message}</div>
          <button
            type="button"
            onClick={this.reset}
            className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900"
          >
            Try again
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

/**
 * E-4 (audit): the class-based ErrorBoundary above tracks `error` in
 * component state, which means once an error is captured the boundary
 * stays tripped across client-side navigations — Next.js keeps the
 * layout (and therefore the boundary instance) mounted while only
 * swapping the page segment, so React's reconciler reuses the same
 * boundary instance and its error state persists. Without this
 * wrapper, a user who hits a render error on one page would see the
 * "Something went wrong" fallback continue to render after navigating
 * to a completely unrelated route, with no recovery short of a full
 * page reload.
 *
 * Keying the boundary on `pathname` forces React to unmount and
 * remount it on every route change, which clears the error state for
 * free without any imperative reset wiring.
 */
export function ErrorBoundaryWithRouteReset({
  children,
  fallback,
}: ErrorBoundaryProps) {
  const pathname = usePathname()
  return (
    <ErrorBoundary key={pathname ?? '/'} fallback={fallback}>
      {children}
    </ErrorBoundary>
  )
}
