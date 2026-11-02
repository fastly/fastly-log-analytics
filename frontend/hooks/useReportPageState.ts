'use client'

import { useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  useAnalyticsReportQuery,
  usePlotlyRangeZoom,
  useReportRangeAnchor,
  useServiceQuery,
  type ReportConfiguration,
} from '@/hooks/useDashboardCards'
import { useFieldLabel } from '@/hooks/useBootstrap'
import { useColumnVisibility } from '@/hooks/useSSE'

export interface ReportPageState {
  relativeRange: string | null
  isAutoRange: boolean
  anchor: string
  getFieldLabel: (field: string) => string
}

/**
 * Shared client-report state hook that bundles range anchor resolution and
 * field label lookup across report client pages (Performance, Origin,
 * Streaming/CMCD, Sessions).
 */
export function useReportPageState(nowServerStr?: string): ReportPageState {
  const { relativeRange, isAutoRange, anchor } = useReportRangeAnchor(nowServerStr)
  const getFieldLabel = useFieldLabel()
  return { relativeRange, isAutoRange, anchor, getFieldLabel }
}

/**
 * Cancels the given query keys when a report client page unmounts.
 */
export function useReportCancelQueries(queryKeysToCancel: ReadonlyArray<readonly unknown[]>) {
  const queryClient = useQueryClient()
  useEffect(() => {
    return () => {
      for (const queryKey of queryKeysToCancel) {
        void queryClient.cancelQueries({ queryKey })
      }
    }
  }, [queryClient, queryKeysToCancel])
}

export {
  useAnalyticsReportQuery,
  useColumnVisibility,
  useFieldLabel,
  usePlotlyRangeZoom,
  useReportRangeAnchor,
  useServiceQuery,
  type ReportConfiguration,
}
