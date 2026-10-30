'use client'

import React, { useEffect, useMemo, useRef, useState, useSyncExternalStore, type RefObject } from 'react'
import { usePathname } from 'next/navigation'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { PlotlyChart } from '@/components/PlotlyChart'
import { makeTimeXAxis, TIME_HOVER_LAYOUT, cn } from "@/lib/utils";
import { Loader2, HelpCircle, Inbox, Lock } from 'lucide-react'
import { HelpDialog } from '@/components/ui/confirm-dialog'
import { Button } from '@/components/ui/button'
import type { PlotlyLayout, PlotlyRelayoutEvent, PlotlyTrace } from '@/types/api'

// ─── ErrorBoundary ──────────────────────────────────────────────────────────

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

// ─── TimeSeriesChart ────────────────────────────────────────────────────────

interface TimeSeriesChartProps {
  data: PlotlyTrace[]
  startTime?: string | null
  endTime?: string | null
  timezone: string
  layout?: PlotlyLayout
  className?: string
  height?: number | string
  onRelayout?: (event: PlotlyRelayoutEvent) => void
}

function TimeSeriesChartImpl({
  data,
  startTime,
  endTime,
  timezone,
  layout,
  className,
  height,
  onRelayout,
}: TimeSeriesChartProps) {
  const mergedLayout = useMemo(
    (): PlotlyLayout => ({
      ...TIME_HOVER_LAYOUT,
      xaxis: makeTimeXAxis(startTime, endTime, timezone),
      ...layout,
    }),
    [startTime, endTime, timezone, layout],
  )
  return (
    <ErrorBoundary>
      <PlotlyChart
        data={data}
        layout={mergedLayout}
        className={className}
        height={height}
        onRelayout={onRelayout}
      />
    </ErrorBoundary>
  )
}

export const TimeSeriesChart = React.memo(TimeSeriesChartImpl)

// ─── CardErrorState / ChartEmptyState ───────────────────────────────────────

export function CardErrorState({
  icon,
  title,
  message,
  onRetry,
  variant = 'default',
}: {
  icon?: React.ReactNode
  title: string
  message: string
  onRetry?: () => void
  variant?: 'default' | 'stacked'
}) {
  const isStacked = variant === 'stacked'
  return (
    <div
      className={
        isStacked
          ? 'flex flex-col items-start gap-3 p-4 border border-destructive/20 bg-destructive/5 rounded-md'
          : 'border border-destructive/20 bg-destructive/5 rounded-md p-4'
      }
    >
      <div className="flex items-center gap-2 text-destructive">
        {icon}
        <span className="text-sm font-medium">{title}</span>
      </div>
      <p className={isStacked ? 'text-xs text-muted-foreground' : 'text-xs text-muted-foreground mt-1'}>{message}</p>
      {onRetry && (
        <Button size="sm" variant="outline" className={isStacked ? undefined : 'mt-3'} onClick={onRetry}>
          Retry
        </Button>
      )}
    </div>
  )
}

export function ChartEmptyState({ requires }: { requires?: string }) {
  return (
    <div className="flex flex-col items-center justify-center h-full text-muted-foreground text-center px-4">
      <span className="text-sm font-medium mb-1">
        {requires ? 'No data available' : 'No data in this time range yet'}
      </span>
      {requires && <span className="text-[10px] opacity-70">Requires {requires}</span>}
    </div>
  )
}

// ─── LazyMount / useInViewOnce ──────────────────────────────────────────────

const subscribeNever = () => () => {}

interface UseInViewOnceOptions<T extends Element> {
  rootMargin?: string
  findRoot?: (node: T) => Element | null
}

export function useInViewOnce<T extends Element>(
  ref: RefObject<T | null>,
  { rootMargin = '0px', findRoot }: UseInViewOnceOptions<T> = {},
): boolean {
  const [seen, setSeen] = useState(false)
  const supported = useSyncExternalStore(
    subscribeNever,
    () => typeof IntersectionObserver !== 'undefined',
    () => true,
  )

  useEffect(() => {
    const node = ref.current
    if (seen || !supported || !node) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setSeen(true)
          observer.disconnect()
        }
      },
      { root: findRoot ? findRoot(node) : null, rootMargin },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [ref, seen, supported, rootMargin, findRoot])

  return seen || !supported
}

interface LazyMountProps {
  children: React.ReactNode
  minHeight?: number | string
  rootMargin?: string
  className?: string
}

export function LazyMount({
  children,
  minHeight = 300,
  rootMargin = '600px',
  className,
}: LazyMountProps) {
  const ref = useRef<HTMLDivElement>(null)
  const visible = useInViewOnce(ref, { rootMargin })

  return (
    <div ref={ref} className={className} style={visible ? undefined : { minHeight }}>
      {visible ? children : null}
    </div>
  )
}

export function UpdatingBadge() {
  // text-foreground (not text-primary): primary-coloured text on the
  // primary/10 tint is sub-AA contrast (same hue). Keep the primary accent
  // via the pulsing dot + tint; the label reads in foreground so it meets
  // WCAG 2.1 AA in both themes.
  //
  // animate-pulse lives on the DOT, not the pill: pulsing the whole element
  // drops the text to ~0.5 opacity mid-animation, which blends the
  // foreground into the tint (~3.4:1, sub-AA) and trips axe color-contrast.
  // Confining the pulse to the (decorative, non-text) dot keeps the live
  // affordance while the label stays at full opacity / AA contrast.
  return (
    <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-primary/10 text-foreground text-[10px] font-bold uppercase tracking-wider">
      <span className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse" />
      Updating
    </div>
  )
}

// U-9: surface distinct UX for loading vs empty vs 403. Callers were collapsing
// all three into a single "No data available" string, so a slow connection,
// a filter with zero rows, and a real permission denial all rendered the same.
// These props are additive — existing call sites continue to work and only
// see the new states when they explicitly opt in.
export type AnalyticsCardError = Error & { status?: number; code?: string }

interface AnalyticsCardProps {
  title: React.ReactNode
  icon?: React.ReactNode
  isLoading?: boolean
  isFetching?: boolean
  /** Query succeeded but returned zero rows. Renders the empty-state message
   *  instead of children. Ignored when isLoading or error is present. */
  isEmpty?: boolean
  /** Query failed. `status === 403` renders the no-access message; any other
   *  status falls back to a generic error message. */
  error?: AnalyticsCardError | null
  /** Optional callback for the "Clear filter" link in the empty state. When
   *  omitted, the link is hidden (caller has no active filter to clear). */
  onClearFilter?: () => void
  children: React.ReactNode
  className?: string
  contentClassName?: string
  headerAction?: React.ReactNode
  description?: string
  footer?: React.ReactNode
  helpContent?: React.ReactNode
  helpTitle?: string
  /** Contextual copy shown during initial loading (defaults to "Loading data..."). */
  loadingMessage?: string
}

export function AnalyticsCard({
  title,
  icon,
  isLoading,
  isFetching,
  loadingMessage,
  isEmpty,
  error,
  onClearFilter,
  children,
  className,
  contentClassName,
  headerAction,
  description,
  footer,
  helpContent,
  helpTitle,
}: AnalyticsCardProps) {
  const [isHelpOpen, setIsHelpOpen] = React.useState(false)

  // Precedence: loading > error > empty > children. Loading wins so that a
  // refetch which transiently sets isEmpty=true (stale empty state) doesn't
  // flash the empty message before the spinner takes over. Error wins over
  // empty so a 403 isn't misread as "just no rows."
  //
  // The NO_SERVICE sentinel (lib/api.ts:124) gets folded into the loading
  // state: it's a race between query mount and the activeService store
  // transitioning to null, and the next render with a non-null id refires
  // the query naturally. Surfacing it as a red error confused devs (and
  // briefly flashed on every service switch) when it's really just "data
  // not ready yet."
  const isNoServiceSentinel = error?.code === 'NO_SERVICE'
  const showLoading = isLoading || isNoServiceSentinel
  const showError = !showLoading && error != null
  const showEmpty = !showLoading && !showError && isEmpty === true
  const is403 = showError && error?.status === 403

  return (
    <>
    <Card className={cn("flex flex-col gap-0 py-0 overflow-hidden", className)}>
      <CardHeader className="px-4 pt-4 pb-3 border-b space-y-0">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-2 min-w-0">
            {icon && <div className="text-primary">{icon}</div>}
            <div className="min-w-0">
              <CardTitle className="text-sm font-medium">{title}</CardTitle>
              {description && <p className="text-xs text-muted-foreground mt-0.5 line-clamp-1">{description}</p>}
            </div>
          </div>
          <div className="flex items-center gap-3 shrink-0">
            {isFetching && !showLoading && <UpdatingBadge />}
            <div className="flex items-center gap-3 shrink-0">
              {headerAction}
              {helpContent && (
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label="About this chart"
                  className="h-6 w-6 text-muted-foreground hover:text-foreground"
                  onClick={() => setIsHelpOpen(true)}
                  title="About this chart"
                >
                  <HelpCircle className="h-4 w-4" />
                </Button>
              )}
            </div>
          </div>
        </div>
      </CardHeader>
      <CardContent className={cn("px-4 pt-4 pb-4 flex-1 relative min-h-0", contentClassName)}>
        {showLoading ? (
          // bg-background (opaque) rather than bg-background/50: when data is
          // undefined the children render their unit suffixes anyway (e.g.
          // `summary.data?.ottfb_p50_ms?.toFixed(1)}ms` becomes literal "ms"),
          // which bled through the half-transparent overlay during cold load
          // and made the card look half-broken. Opaque overlay hides them.
          // The refetch-with-old-data UX is preserved by the separate
          // `isFetching && !isLoading` opacity-40 branch on the children below.
          // A-10 (WCAG 4.1.3 Status Messages): role=status + aria-live=polite so
          // screen readers announce "Loading data..." when the overlay appears
          // and an implicit "data loaded" when it's removed. The spinner stays
          // aria-hidden — the text node carries the announcement.
          <div
            role="status"
            aria-live="polite"
            className="absolute inset-0 flex items-center justify-center bg-background z-10"
          >
            <div className="flex flex-col items-center gap-2">
              <Loader2 className="h-6 w-6 animate-spin text-primary" aria-hidden="true" />
              <span className="text-xs text-foreground animate-pulse font-medium">{loadingMessage ?? "Loading data..."}</span>
            </div>
          </div>
        ) : null}
        {showError ? (
          // role=status keeps these announcements consistent with the loading
          // overlay (A-10). 403 gets a distinct icon + copy so it doesn't read
          // as a transient failure or empty filter.
          <div
            role="status"
            aria-live="polite"
            className="absolute inset-0 flex items-center justify-center bg-background z-10 p-4"
          >
            <div className="flex flex-col items-center gap-2 text-center max-w-xs">
              {is403 ? (
                <>
                  <Lock className="h-6 w-6 text-muted-foreground" aria-hidden="true" />
                  <span className="text-sm font-medium">You don&apos;t have access to this data</span>
                  <p className="text-xs text-muted-foreground">
                    This card requires permissions your account doesn&apos;t have.
                  </p>
                </>
              ) : (
                <>
                  <span className="text-sm font-medium">Something went wrong</span>
                  <p className="text-xs text-muted-foreground">
                    {error?.message || 'Unable to load this data. Try again in a moment.'}
                  </p>
                </>
              )}
            </div>
          </div>
        ) : null}
        {showEmpty ? (
          <div
            role="status"
            aria-live="polite"
            className="absolute inset-0 flex items-center justify-center bg-background z-10 p-4"
          >
            <div className="flex flex-col items-center gap-2 text-center max-w-xs">
              <Inbox className="h-6 w-6 text-muted-foreground" aria-hidden="true" />
              <span className="text-sm font-medium">No data for this filter</span>
              <p className="text-xs text-muted-foreground">
                Try expanding the time range or removing filters.
              </p>
              {onClearFilter ? (
                <button
                  type="button"
                  onClick={onClearFilter}
                  className="text-xs text-primary hover:underline mt-1"
                >
                  Clear filter
                </button>
              ) : null}
            </div>
          </div>
        ) : null}
        <div className={cn("h-full", isFetching && !showLoading && "opacity-40 pointer-events-none transition-opacity")}>
          {children}
        </div>
      </CardContent>
      {footer && (
        <div className="p-2 border-t bg-muted/30">
          {footer}
        </div>
      )}
    </Card>

    {helpContent && (
      <HelpDialog
        open={isHelpOpen}
        onOpenChange={setIsHelpOpen}
        title={helpTitle ?? title}
        icon={icon}
        size="xl"
      >
        {helpContent}
      </HelpDialog>
    )}
    </>
  )
}
