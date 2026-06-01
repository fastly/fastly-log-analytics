'use client'

import React from 'react'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { useShallow } from 'zustand/react/shallow'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { PageHeader } from '@/components/ui/page-header'
import { Loader2, LucideIcon } from 'lucide-react'
import { cn } from '@/lib/utils'

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
  const activeServiceId = useServiceStore(s => s.activeServiceId)
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
        <div className="flex flex-col items-center justify-center min-h-[400px] bg-muted/10 border border-dashed rounded-lg">
          <div className="flex flex-col items-center gap-3">
            <Loader2 className="h-8 w-8 animate-spin text-primary/60" />
            <p className="text-sm font-medium text-muted-foreground animate-pulse">
              Initializing analysis...
            </p>
          </div>
        </div>
      ) : (
        children
      )}
    </div>
  )
}

