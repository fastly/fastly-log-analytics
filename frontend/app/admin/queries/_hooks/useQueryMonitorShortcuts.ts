'use client'

import * as React from 'react'

import { useKeyboardShortcuts, type ShortcutBinding } from './useKeyboardShortcuts'
import type { ActiveOrPromotedRow, ActiveRow } from '../_types'

/**
 * Owns all keyboard-driven behaviour for the Live Query Monitor page:
 * - The shortcut bindings (`/`, `j`/`k`, `Enter`, `x`, `?`, `Esc`).
 * - The focus-validity effect that clears `focusedQid` when the focused
 *   row falls out of the visible list (otherwise `x` could try to cancel
 *   a query that's already gone).
 *
 * Returns nothing — wires side effects via the passed setters.
 */
export function useQueryMonitorShortcuts({
  enabled,
  filteredActive,
  focusedQid,
  expandedQid,
  shortcutsOpen,
  confirmKillOpen,
  searchInputRef,
  setFocusedQid,
  setExpandedQid,
  setShortcutsOpen,
  closeConfirmKill,
  requestKill,
}: {
  enabled: boolean
  filteredActive: ActiveOrPromotedRow[]
  focusedQid: number | null
  expandedQid: number | null
  shortcutsOpen: boolean
  confirmKillOpen: boolean
  searchInputRef: React.RefObject<HTMLInputElement | null>
  setFocusedQid: (qid: number | null) => void
  setExpandedQid: React.Dispatch<React.SetStateAction<number | null>>
  setShortcutsOpen: (open: boolean) => void
  closeConfirmKill: () => void
  requestKill: (row: ActiveRow) => void
}): void {
  // Drop stale focusedQid when its row leaves the visible list. Cheaper
  // than searching the array on every shortcut fire.
  React.useEffect(() => {
    if (focusedQid === null) return
    if (!filteredActive.some((r) => r.query_id === focusedQid)) {
      setFocusedQid(null)
    }
  }, [filteredActive, focusedQid, setFocusedQid])

  const navigateFocus = React.useCallback(
    (delta: number) => {
      if (filteredActive.length === 0) return
      const ids = filteredActive.map((r) => r.query_id)
      if (focusedQid === null) {
        setFocusedQid(delta > 0 ? ids[0] : ids[ids.length - 1])
        return
      }
      const i = ids.indexOf(focusedQid)
      if (i === -1) {
        setFocusedQid(ids[0])
        return
      }
      const next = Math.max(0, Math.min(ids.length - 1, i + delta))
      setFocusedQid(ids[next])
    },
    [filteredActive, focusedQid, setFocusedQid],
  )

  const shortcuts = React.useMemo<ShortcutBinding[]>(
    () => [
      {
        key: '/',
        description: 'Focus the search field',
        handler: (e) => {
          e.preventDefault()
          searchInputRef.current?.focus()
          searchInputRef.current?.select()
        },
      },
      {
        key: 'j',
        description: 'Focus next row',
        handler: (e) => {
          e.preventDefault()
          navigateFocus(1)
        },
      },
      {
        key: 'k',
        description: 'Focus previous row',
        handler: (e) => {
          e.preventDefault()
          navigateFocus(-1)
        },
      },
      {
        key: 'Enter',
        description: 'Toggle expand on focused row',
        handler: (e) => {
          if (focusedQid === null) return
          e.preventDefault()
          setExpandedQid((prev) => (prev === focusedQid ? null : focusedQid))
        },
      },
      {
        key: 'x',
        description: 'Cancel focused query',
        handler: (e) => {
          if (focusedQid === null) return
          const row = filteredActive.find((r) => r.query_id === focusedQid && !r._completed) as
            | ActiveRow
            | undefined
          if (!row || !row.cancellable || row.cancelled_at !== null) return
          e.preventDefault()
          requestKill(row)
        },
      },
      {
        key: '?',
        description: 'Show keyboard shortcuts',
        handler: (e) => {
          e.preventDefault()
          setShortcutsOpen(true)
        },
      },
      {
        key: 'Escape',
        description: 'Close drawer / dialog / overlay',
        allowInForms: true,
        handler: () => {
          if (shortcutsOpen) {
            setShortcutsOpen(false)
            return
          }
          if (confirmKillOpen) {
            closeConfirmKill()
            return
          }
          if (expandedQid !== null) {
            setExpandedQid(null)
            return
          }
          // Last resort: blur the search input so the user can immediately
          // start using row-level shortcuts.
          if (document.activeElement === searchInputRef.current) {
            searchInputRef.current?.blur()
          }
        },
      },
    ],
    [
      navigateFocus,
      focusedQid,
      filteredActive,
      requestKill,
      shortcutsOpen,
      confirmKillOpen,
      expandedQid,
      searchInputRef,
      setExpandedQid,
      setShortcutsOpen,
      closeConfirmKill,
    ],
  )

  useKeyboardShortcuts(shortcuts, enabled)
}
