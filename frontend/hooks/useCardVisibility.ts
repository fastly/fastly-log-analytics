import { useState, useEffect, useCallback, useMemo } from 'react'

export interface CardVisibilityMigration {
  /** Bump when the set of IDs to retire or surface changes. Stored under `<storageKey>_mv`. */
  version: number
  /** IDs to strip from the saved set on first load after a version bump. */
  removeIds?: string[]
  /** IDs to insert into the saved set on first load after a version bump. */
  addIds?: string[]
}

/**
 * Persists a Set of visible card IDs to localStorage and keeps it in sync
 * across re-renders.
 *
 * @param storageKey   - The localStorage key to persist under.
 * @param allIds       - Every possible card ID (used to build the "show all" set).
 * @param defaultIds   - Optional subset of `allIds` to use as the initial visible set
 *                       (e.g., fields currently in the active log format). If omitted,
 *                       defaults to `allIds`.
 * @param migration    - Optional one-time prune. When `version` exceeds the stored
 *                       `<storageKey>_mv` value, `removeIds` are stripped from the
 *                       saved set and the new version is recorded. Lets us retire
 *                       cards from existing browsers without nuking other choices.
 */
export function useCardVisibility(
  storageKey: string,
  allIds: string[],
  defaultIds?: string[],
  migration?: CardVisibilityMigration,
) {
  const allIdsStr = JSON.stringify(allIds)
  const defaultIdsStr = JSON.stringify(defaultIds ?? allIds)
  const allVisible = useMemo(() => new Set<string>(JSON.parse(allIdsStr)), [allIdsStr])
  const defaultVisible = useMemo(() => new Set<string>(JSON.parse(defaultIdsStr)), [defaultIdsStr])

  const migrationKey = `${storageKey}_mv`
  const migrationVersion = migration?.version
  const migrationRemoveStr = JSON.stringify(migration?.removeIds ?? [])
  const migrationAddStr = JSON.stringify(migration?.addIds ?? [])

  const load = useCallback((): Set<string> => {
    if (typeof window === 'undefined') return defaultVisible
    try {
      const stored = localStorage.getItem(storageKey)
      if (!stored) return defaultVisible

      const set = new Set<string>(JSON.parse(stored))

      if (migrationVersion !== undefined) {
        const storedVersion = Number(localStorage.getItem(migrationKey) ?? 0)
        if (storedVersion < migrationVersion) {
          const removeIds: string[] = JSON.parse(migrationRemoveStr)
          const addIds: string[] = JSON.parse(migrationAddStr)
          for (const id of removeIds) set.delete(id)
          for (const id of addIds) set.add(id)
          try {
            localStorage.setItem(storageKey, JSON.stringify([...set]))
            localStorage.setItem(migrationKey, String(migrationVersion))
          } catch {}
        }
      }

      return set
    } catch {}
    return defaultVisible
  }, [storageKey, defaultVisible, migrationKey, migrationVersion, migrationRemoveStr, migrationAddStr])

  // SSR-safe initial state: ALWAYS start from the deterministic default set.
  // Reading localStorage in this initializer was correct for first-paint on a
  // pure CSR app, but under the force-dynamic SSR layout it made the first
  // client render diverge from the server's default-set render — the
  // visibility-count badge and the rendered card set differed → React #418 on
  // /charts and the dashboard for any user who had toggled card visibility.
  // The useEffect below promotes to the persisted value (and applies the
  // migration) right after mount, so the saved choice still lands — just one
  // tick later, off the hydration path.
  const [visibleCards, setVisibleCards] = useState<Set<string>>(defaultVisible)

  // Keep the load() reactive so callers whose ``allIds`` / ``defaultIds``
  // change after mount (e.g. dashboard's allCards arriving from the
  // catalog query) still get migrations applied and defaults refreshed.
  // The first render's initializer above handles the cold-mount case
  // synchronously; this useEffect handles subsequent changes.
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setVisibleCards(load())
  }, [load])

  const toggleCard = useCallback((id: string) => {
    setVisibleCards(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      try { localStorage.setItem(storageKey, JSON.stringify([...next])) } catch {}
      return next
    })
  }, [storageKey])

  const showAll = useCallback(() => {
    setVisibleCards(allVisible)
    try { localStorage.setItem(storageKey, JSON.stringify([...allVisible])) } catch {}
  }, [storageKey, allVisible])

  const reset = useCallback(() => {
    setVisibleCards(defaultVisible)
    try { localStorage.setItem(storageKey, JSON.stringify([...defaultVisible])) } catch {}
  }, [storageKey, defaultVisible])

  return { visibleCards, toggleCard, showAll, reset, defaultVisible }
}
