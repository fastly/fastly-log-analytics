'use client'

import * as React from 'react'
import type { SortingState, VisibilityState } from '@tanstack/react-table'

interface UseDataTableStateArgs<TData = unknown> {
  data?: TData[]
  controlledSorting?: SortingState
  onSortingChange?: (sorting: SortingState) => void
  initialSorting?: SortingState
  controlledVisibility?: VisibilityState
  onColumnVisibilityChange?: (visibility: VisibilityState) => void
  initialVisibility?: VisibilityState
}

/**
 * Controlled-or-internal sorting + column-visibility state + stabilized
 * tableData shared verbatim by DataTable and DataTableReadonly. Pulls no
 * @dnd-kit, so importing it into the readonly variant keeps that bundle lean.
 */
export function useDataTableState<TData = unknown>({
  data = [],
  controlledSorting,
  onSortingChange,
  initialSorting = [],
  controlledVisibility,
  onColumnVisibilityChange,
  initialVisibility = {},
}: UseDataTableStateArgs<TData>) {
  const isControlled = controlledVisibility !== undefined
  const isSortingControlled = controlledSorting !== undefined
  const [internalSorting, setInternalSorting] = React.useState<SortingState>(initialSorting)
  const sorting = isSortingControlled ? controlledSorting : internalSorting

  const [internalVisibility, setInternalVisibility] = React.useState<VisibilityState>(initialVisibility)
  const columnVisibility = isControlled ? controlledVisibility! : internalVisibility
  const setColumnVisibility = React.useCallback(
    (updater: VisibilityState | ((prev: VisibilityState) => VisibilityState)) => {
      if (isControlled) {
        const next = typeof updater === 'function' ? updater(controlledVisibility!) : updater
        onColumnVisibilityChange?.(next)
      } else {
        setInternalVisibility(updater)
      }
    },
    [isControlled, controlledVisibility, onColumnVisibilityChange],
  )

  const [stableData, setStableData] = React.useState<TData[]>(data)
  const isSameData =
    stableData === data ||
    (Array.isArray(stableData) &&
      Array.isArray(data) &&
      stableData.length === data.length &&
      stableData.every((row, i) => row === data[i]))
  if (!isSameData) {
    setStableData(data)
  }
  const tableData = isSameData ? stableData : data

  const nextVisibilityJson = JSON.stringify(initialVisibility)
  const [prevInitialVisibilityJson, setPrevInitialVisibilityJson] = React.useState(nextVisibilityJson)
  if (nextVisibilityJson !== prevInitialVisibilityJson) {
    setPrevInitialVisibilityJson(nextVisibilityJson)
    if (!isControlled && Object.keys(initialVisibility).length > 0) {
      setInternalVisibility(initialVisibility)
    }
  }

  const handleSortingChange = (updater: SortingState | ((prev: SortingState) => SortingState)) => {
    const next = typeof updater === 'function' ? updater(sorting) : updater
    if (isSortingControlled) {
      onSortingChange?.(next)
    } else {
      setInternalSorting(next)
    }
  }

  return { sorting, isSortingControlled, columnVisibility, setColumnVisibility, handleSortingChange, tableData }
}
