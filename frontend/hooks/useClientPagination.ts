'use client'

import { useMemo, useState } from 'react'

export interface ClientPaginationResult<T> {
  page: number
  pageSize: number
  totalPages: number
  totalItems: number
  pageItems: T[]
  setPage: (page: number) => void
  setPageSize: (size: number) => void
}

export function useClientPagination<T>(
  items: readonly T[],
  initialPageSize = 25,
): ClientPaginationResult<T> {
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(initialPageSize)

  const totalItems = items.length
  const totalPages = Math.max(1, Math.ceil(totalItems / Math.max(1, pageSize)))
  const safePage = Math.min(Math.max(1, page), totalPages)

  const pageItems = useMemo(() => {
    const start = (safePage - 1) * pageSize
    return items.slice(start, start + pageSize)
  }, [items, safePage, pageSize])

  return {
    page: safePage,
    pageSize,
    totalPages,
    totalItems,
    pageItems,
    setPage,
    setPageSize,
  }
}
