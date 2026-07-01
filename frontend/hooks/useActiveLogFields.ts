'use client'

import { useMemo } from 'react'
import { useBootstrap } from './useBootstrap'
import { useLogFieldsCatalog } from './useLogFieldsCatalog'

/**
 * Single source of truth for "is this log field / field-group actually enabled
 * for the active service?" — derived from `bootstrap.active_log_field_ids`
 * (the flat set of field-ids present in the active log format + enabled custom
 * fields). This is the same signal `useDashboardCards` uses for the dashboard
 * Top-10 cards' `inActiveFormat`, generalised so every page can distinguish
 * "field group not enabled" from "enabled but no data in this window yet".
 *
 * Before this hook, most pages inferred "not enabled" from "the query returned
 * 0 rows", so a freshly-provisioned (or low-traffic) service wrongly showed
 * "Requires Group X to be enabled" on panels whose group WAS enabled — there
 * was just no matching data yet.
 *
 * `ready` is false until bootstrap lands; while not ready we report everything
 * as active so callers never flash a "requires X" message during load (mirrors
 * `useDashboardCards`' `activeIds.size === 0 ? true` guard). Prefer
 * `isFieldActive(<the specific field a panel needs>)` for precision; use
 * `isGroupActive(<groupId>)` when a panel depends on a whole group.
 */
export function useActiveLogFields() {
  const { data: bootstrap } = useBootstrap()
  const { data: catalog } = useLogFieldsCatalog()

  return useMemo(() => {
    const ids =
      (bootstrap as { active_log_field_ids?: string[] } | undefined)?.active_log_field_ids ?? []
    const activeIds = new Set<string>(ids)
    // activeIds empty == bootstrap not ready yet → treat everything as active.
    const ready = activeIds.size > 0

    const isFieldActive = (id: string): boolean => !ready || activeIds.has(id)

    const groups = (catalog?.groups ?? []) as Array<{ id: string; fields: string[] }>
    const isGroupActive = (groupId: string): boolean => {
      if (!ready) return true
      const grp = groups.find((g) => g?.id === groupId)
      // A group counts as enabled iff any of its fields is in the active format
      // — fields within a group are individually toggleable, so a partially
      // enabled group is still "on".
      return grp ? grp.fields.some((f) => activeIds.has(f)) : false
    }

    return { ready, isFieldActive, isGroupActive }
  }, [bootstrap, catalog])
}
