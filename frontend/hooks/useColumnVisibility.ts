'use client'

import { useCallback, useState } from 'react'
import type { VisibilityState } from '@tanstack/react-table'

export function useColumnVisibility(initial?: VisibilityState) {
  const [visibility, setVisibility] = useState<VisibilityState>(initial ?? {})
  const handleChange = useCallback(
    (id: string, vis: boolean) => setVisibility(prev => ({ ...prev, [id]: vis })),
    []
  )
  return [visibility, setVisibility, handleChange] as const
}
