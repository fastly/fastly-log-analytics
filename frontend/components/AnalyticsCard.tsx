'use client'

import React from 'react'
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { cn } from '@/lib/utils'
import { Loader2, HelpCircle, Inbox, Lock } from 'lucide-react'
import { HelpDialog } from '@/components/ui/help-dialog'
import { Button } from '@/components/ui/button'
import { UpdatingBadge } from '@/components/UpdatingBadge'

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
  /** Optional href shown in the 403 state (e.g. mailto:admin or a docs link). */
  noAccessHref?: string
  children: React.ReactNode
  className?: string
  contentClassName?: string
  headerAction?: React.ReactNode
  description?: string
  footer?: React.ReactNode
  helpContent?: React.ReactNode
  helpTitle?: string
}

export function AnalyticsCard({
  title,
  icon,
  isLoading,
  isFetching,
  isEmpty,
  error,
  onClearFilter,
  noAccessHref,
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
          <div className="flex items-center gap-2">
            {icon && <div className="text-primary">{icon}</div>}
            <div>
              <CardTitle className="text-sm font-medium">{title}</CardTitle>
              {description && <p className="text-xs text-muted-foreground mt-0.5">{description}</p>}
            </div>
          </div>
          <div className="flex items-center gap-3">
            {isFetching && !showLoading && <UpdatingBadge />}
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
              <span className="text-xs text-muted-foreground animate-pulse font-medium">Loading data...</span>
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
                  {noAccessHref ? (
                    <a
                      href={noAccessHref}
                      className="text-xs text-primary hover:underline mt-1"
                    >
                      Contact an admin
                    </a>
                  ) : null}
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
        size="lg"
      >
        {helpContent}
      </HelpDialog>
    )}
    </>
  )
}
