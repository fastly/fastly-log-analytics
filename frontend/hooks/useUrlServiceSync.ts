'use client'

import { startTransition, useEffect } from 'react'
import { useQueryState } from 'nuqs'

import { useServiceStore } from '@/stores/serviceStore'

/**
 * Bidirectional sync between the active service ID and the URL's
 * `?service=` query param.
 *
 * This hook is the FIRST nuqs adoption in the codebase (Phase 9a
 * proof-of-concept). The pattern
 * here replaces the previous useSearchParams + router.replace
 * dance with a single useQueryState call that handles URL ↔ React
 * state in one binding. The Zustand store stays the source of
 * truth for the 34 components that read `activeServiceId`; this
 * hook just keeps the URL slot in lockstep.
 *
 * Why keep Zustand alongside nuqs (instead of dropping the store
 * and reading the URL directly everywhere): the broader nuqs
 * migration (Phase 9a.6) would touch 62 store consumers across
 * 4 stores. Doing it incrementally — one store at a time, starting
 * with the smallest — keeps each step reviewable. Once filterStore
 * (16 consumers) lands the same way, dropping the Zustand layer
 * becomes a separate, scoped change.
 *
 * Sync semantics:
 *   - On mount: if the URL has ?service=X and store is empty or
 *     different, store wins after a single tick (see useBootstrap
 *     which also writes activeServiceId from the SSR'd response).
 *     We DON'T overwrite the store here unconditionally — that
 *     races with the bootstrap-derived initialization in unhelpful
 *     ways.
 *   - When the store's activeServiceId changes after init: write
 *     to the URL via setUrlService, wrapped in startTransition so
 *     loading.tsx skeleton paints first.
 *   - When the URL changes (back/forward nav, paste-and-go): the
 *     useEffect on `urlService` writes back into the store.
 */
export function useUrlServiceSync() {
  const activeServiceId = useServiceStore(state => state.activeServiceId)
  const services = useServiceStore(state => state.services)
  const isInitialized = useServiceStore(state => state.isInitialized)
  const setActiveServiceId = useServiceStore(state => state.setActiveServiceId)

  // `shallow: true` keeps the URL update from triggering a full
  // server-component re-render — we only want the query-string to
  // change. `history: 'replace'` matches the previous router.replace
  // semantics so the back button still works the way users expect.
  const [urlService, setUrlService] = useQueryState('service', {
    history: 'replace',
    shallow: true,
  })

  // URL → store: catch user-initiated nav (back/forward, paste-and-go,
  // shared link). Only fires when the URL value differs from the
  // store; bootstrap's own initialization writes the store
  // unconditionally.
  useEffect(() => {
    if (!urlService) return
    if (urlService === activeServiceId) return
    // Once bootstrap has loaded the real list, don't adopt a `?service=`
    // that isn't a known service — e.g. a value left in localStorage by a
    // previous install, or a dead shared link. Adopting it here would fight
    // useBootstrap's reconcile-to-default/null and strand a fresh clone on
    // ?service=<dead-id> instead of landing on the provisioning wizard.
    // Before init we can't validate yet, so keep adopting (deep-link fast path).
    if (isInitialized && !services.some(s => s.id === urlService)) return
    setActiveServiceId(urlService)
  }, [urlService, activeServiceId, services, isInitialized, setActiveServiceId])

  // Store → URL: keep ?service= in lockstep with activeServiceId
  // once the store is initialized. If there are no services at all,
  // strip the query param entirely (matches prior behaviour).
  useEffect(() => {
    if (!isInitialized) return
    const target = services.length > 0 ? activeServiceId : null
    if (target === urlService) return
    startTransition(() => {
      setUrlService(target)
    })
  }, [activeServiceId, services, isInitialized, urlService, setUrlService])
}
