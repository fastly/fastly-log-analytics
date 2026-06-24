'use client'

import { useQuery } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import { useShallow } from 'zustand/react/shallow'

interface UseFieldValuesOptions {
  field: string
  search: string
  limit?: number
  enabled?: boolean
}

export function useFieldValues({ field, search, limit = 50, enabled = true }: UseFieldValuesOptions) {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const { startTime, endTime } = useFilterStore(
    useShallow(s => ({ startTime: s.startTime, endTime: s.endTime }))
  )
  const filterPayload = useDebouncedFilterPayload()

  return useQuery({
    queryKey: ['dashboard', 'field_values', activeServiceId, startTime, endTime, filterPayload, field, search],
    queryFn: async () => {
      const { data } = await client.POST('/api/dashboard/field-values', {
        body: {
          start_time: startTime,
          end_time: endTime,
          filters: filterPayload,
          field,
          search,
          limit,
        },
      })
      return data
    },
    enabled: !!activeServiceId && enabled,
  })
}
