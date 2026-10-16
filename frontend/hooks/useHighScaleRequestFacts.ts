'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { client, extractApiError } from '@/lib/api'
import type { components } from '@/types/api.generated'

const REQUEST_FACTS_PATH = '/api/high-scale/services/{service_id}/request-facts' as const
const DEFAULT_PAGE_SIZE = 100

type RequestFactsResponse = components['schemas']['HighScaleRequestFactResponse']
type RequestFactsRequest = components['schemas']['HighScaleRequestFactRequest']

export interface HighScaleRequestFactsOptions {
  serviceId: string | null | undefined
  startTime: string | null | undefined
  endTime: string | null | undefined
  pageSize?: number
  status?: string | number | null
  domain?: string | null
  pop?: string | null
  country?: string | null
  batchId?: string | null
  emulateStandard?: boolean
}

export function useHighScaleRequestFacts({
  serviceId,
  startTime,
  endTime,
  pageSize = DEFAULT_PAGE_SIZE,
  status,
  domain,
  pop,
  country,
  batchId,
  emulateStandard,
}: HighScaleRequestFactsOptions) {
  const filterKey = `${status ?? ''}|${domain ?? ''}|${pop ?? ''}|${country ?? ''}|${batchId ?? ''}|${emulateStandard ?? ''}`
  const rangeKey = `${serviceId ?? ''}|${startTime ?? ''}|${endTime ?? ''}|${pageSize}|${filterKey}`
  const [pagination, setPagination] = useState<{
    key: string
    pageIndex: number
    cursors: Array<string | null>
  }>({ key: rangeKey, pageIndex: 0, cursors: [null] })
  const activePagination = pagination.key === rangeKey
    ? pagination
    : { key: rangeKey, pageIndex: 0, cursors: [null] }
  const { pageIndex, cursors } = activePagination
  const cursor = cursors[pageIndex] ?? null

  const queryKey = filterKey === '|||||'
    ? ['high-scale', 'request-facts', serviceId, startTime, endTime, pageSize, cursor]
    : ['high-scale', 'request-facts', serviceId, startTime, endTime, pageSize, cursor, filterKey]

  const query = useQuery<RequestFactsResponse, Error>({
    queryKey,
    enabled: Boolean(serviceId && startTime && endTime),
    queryFn: async ({ signal }) => {
      if (!serviceId || !startTime || !endTime) {
        throw new Error('A service and time range are required')
      }

      const body: RequestFactsRequest = {
        start_time: startTime,
        end_time: endTime,
        limit: pageSize,
        cursor,
        emulate_standard: Boolean(emulateStandard),
      }
      if (status !== undefined && status !== null && String(status).trim() !== '') {
        body.status = status
      }
      if (domain !== undefined && domain !== null && domain.trim() !== '') {
        body.domain = domain.trim()
      }
      if (pop !== undefined && pop !== null && pop.trim() !== '') {
        body.pop = pop.trim()
      }
      if (country !== undefined && country !== null && country.trim() !== '') {
        body.country = country.trim()
      }
      if (batchId !== undefined && batchId !== null && batchId.trim() !== '') {
        body.batch_id = batchId.trim()
      }
      const { data, error } = await client.POST(REQUEST_FACTS_PATH, {
        params: {
          path: { service_id: serviceId },
        },
        body,
        signal,
      })
      if (error || !data) {
        throw new Error(extractApiError(error))
      }
      return data
    },
    retry: false,
  })

  const nextPage = () => {
    const nextCursor = query.data?.next_cursor
    if (!nextCursor) return
    setPagination((current) => {
      const currentPage = current.key === rangeKey ? current.pageIndex : 0
      const currentCursors = current.key === rangeKey ? current.cursors : [null]
      if (currentCursors[currentPage + 1] === nextCursor) return current
      const nextCursors = currentCursors.slice(0, currentPage + 1)
      nextCursors.push(nextCursor)
      return { key: rangeKey, pageIndex: currentPage + 1, cursors: nextCursors }
    })
  }

  const previousPage = () => {
    setPagination((current) => ({
      key: rangeKey,
      pageIndex: Math.max(0, current.key === rangeKey ? current.pageIndex - 1 : 0),
      cursors: current.key === rangeKey ? current.cursors : [null],
    }))
  }

  return {
    ...query,
    pageIndex,
    nextPage,
    previousPage,
    canNextPage: Boolean(query.data?.next_cursor),
    canPreviousPage: pageIndex > 0,
  }
}
