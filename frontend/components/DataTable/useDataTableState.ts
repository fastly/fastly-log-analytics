'use client'

import * as React from 'react'
import type { SortingState, VisibilityState } from '@tanstack/react-table'

interface UseDataTableStateArgs {
  controlledSorting?: SortingState
  onSortingChange?: (sorting: SortingState) => void
  initialSorting?: SortingState
  controlledVisibility?: VisibilityState
  onColumnVisibilityChange?: (visibility: VisibilityState) => void
  initialVisibility?: VisibilityState
}

/**
 * Controlled-or-internal sorting + column-visibility state shared verbatim by
 * DataTable and DataTableReadonly. Pulls no @dnd-kit, so importing it into the
 * readonly variant keeps that bundle lean. Behaviour is identical to the two
 * former inline copies: when a controlled value is provided the setter forwards
 * to the on*Change callback, otherwise it drives the internal useState.
 */
export function useDataTableState({
  controlledSorting,
  onSortingChange,
  initialSorting = [],
  controlledVisibility,
  onColumnVisibilityChange,
  initialVisibility = {},
}: UseDataTableStateArgs) {
  const isControlled = controlledVisibility !== undefined
  const isSortingControlled = controlledSorting !== undefined
  const [internalSorting, setInternalSorting] = React.useState<SortingState>(initialSorting)
  const sorting = isSortingControlled ? controlledSorting : internalSorting

  const [internalVisibility, setInternalVisibility] = React.useState<VisibilityState>(initialVisibility)
  const columnVisibility = isControlled ? controlledVisibility! : internalVisibility
  const setColumnVisibility = (updater: VisibilityState | ((prev: VisibilityState) => VisibilityState)) => {
    const next = typeof updater === 'function' ? updater(columnVisibility) : updater
    if (isControlled) {
      onColumnVisibilityChange?.(next)
    } else {
      setInternalVisibility(next)
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

  return { sorting, isSortingControlled, columnVisibility, setColumnVisibility, handleSortingChange }
}
