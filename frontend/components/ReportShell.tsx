'use client'

import React from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useEffectiveServiceId, useIsDataReady, useBootstrapResolved } from '@/hooks/useIsDataReady'
import { useShallow } from 'zustand/react/shallow'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { PageHeader } from '@/components/ui/page-header'
import { Loader2, LucideIcon } from 'lucide-react'
import { cn } from '@/lib/utils'
import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

interface ReportShellProps {
  title: string
  description?: string | React.ReactNode
  icon?: LucideIcon
  headerActions?: React.ReactNode
  children: React.ReactNode
  isReadyOverride?: boolean
  requireService?: boolean
  className?: string
  /**
   * E-6 (audit): when the report's primary query failed, render an
   * inline alert above the content with a Retry callback. Without this
   * slot the children render with `data === undefined` and look like an
   * empty-but-loading state, hiding the real failure from the user.
   */
  queryError?: { message: string; onRetry?: () => void } | null
}

export function ReportShell({
  title,
  description,
  icon: Icon,
  headerActions,
  children,
  isReadyOverride,
  requireService = true,
  className,
  queryError
}: ReportShellProps) {
  // useEffectiveServiceId falls back to bootstrap.active_service_id
  // from the SSR-hydrated cache so the page doesn't flash "No service
  // selected" before useBootstrap's post-mount effect populates the
  // persisted Zustand store.
  const activeServiceId = useEffectiveServiceId()
  const { isAutoRange, hasSyncedExtents } = useFilterStore(
    useShallow(s => ({ isAutoRange: s.isAutoRange, hasSyncedExtents: s.hasSyncedExtents }))
  )
  const isDataReady = useIsDataReady()
  const rangeReady = !isAutoRange || hasSyncedExtents

  const isReady = isReadyOverride ?? (requireService ? isDataReady : rangeReady)

  // Gate the "No service selected" fallback on the bootstrap query
  // having actually resolved — without it, a cold load with empty
  // localStorage flashes the fallback for the render tick before
  // HydrationBoundary commits (or the client-side fetch returns).
  // See useBootstrapResolved for the full rationale.
  const bootstrapResolved = useBootstrapResolved()

  if (requireService && !activeServiceId && bootstrapResolved) {
    const FallbackIcon = Icon || Loader2
    return (
      <NoServiceSelected
        icon={FallbackIcon}
        message={`Please select a service from the header to view ${title.toLowerCase()}.`}
      />
    )
  }

  return (
    <div className={cn("space-y-6", className)}>
      <PageHeader title={title} description={description}>
        {headerActions}
      </PageHeader>

      {queryError ? (
        <div
          role="alert"
          className="flex flex-col items-start gap-2 rounded-md border border-red-300 bg-red-50 p-4 text-sm text-red-900 dark:border-red-700 dark:bg-red-950 dark:text-red-100"
        >
          <div className="font-semibold">Failed to load {title.toLowerCase()}.</div>
          <div className="font-mono text-xs opacity-80 break-all">{queryError.message}</div>
          {queryError.onRetry ? (
            <button
              type="button"
              onClick={queryError.onRetry}
              className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900"
            >
              Retry
            </button>
          ) : null}
        </div>
      ) : null}

      {!isReady ? (
        // Content-shaped skeleton instead of a centered spinner. The
        // prior "Initializing analysis…" loader was small and centered
        // in 400px of empty space — users perceived it as "the page
        // isn't loading" because it didn't look like real content
        // taking shape. The skeleton mirrors the dashboard layout so
        // the swap to real content doesn't reflow the page.
        <DashboardSkeleton />
      ) : (
        children
      )}
    </div>
  )
}
