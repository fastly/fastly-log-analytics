'use client'

import React from 'react'
import { useFilterStore } from '@/stores/filterStore'
import { useEffectiveServiceId, useIsDataReady } from '@/hooks/useIsDataReady'
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
}

export function ReportShell({
  title,
  description,
  icon: Icon,
  headerActions,
  children,
  isReadyOverride,
  requireService = true,
  className
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

  if (requireService && !activeServiceId) {
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
