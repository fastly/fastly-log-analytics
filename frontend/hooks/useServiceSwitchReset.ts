'use client'

import { useEffect, useEffectEvent, useRef } from 'react'
import { useServiceStore } from '@/stores/serviceStore'

/**
 * Invokes `callback(prevServiceId, nextServiceId)` whenever the active service
 * changes away from a previously non-null service ID (ignoring initial mount /
 * hydration from null -> initial service).
 */
export function useServiceSwitchReset(
  activeServiceId: string | null | undefined,
  callback: (prevServiceId: string, nextServiceId: string | null) => void,
) {
  const storeActiveServiceId = useServiceStore((s) => s.activeServiceId)
  const resolvedServiceId =
    activeServiceId === undefined ? storeActiveServiceId : activeServiceId
  const prevServiceRef = useRef<string | null>(resolvedServiceId)
  const onReset = useEffectEvent(callback)

  useEffect(() => {
    const prev = prevServiceRef.current
    if (prev && prev !== resolvedServiceId) {
      onReset(prev, resolvedServiceId)
    }
    prevServiceRef.current = resolvedServiceId
  }, [resolvedServiceId])
}
