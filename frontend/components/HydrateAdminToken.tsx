'use client'

import { useAdminTokenStore } from '@/stores/adminTokenStore'

// One-shot hydrator for the admin-token Zustand store. Receives the
// token the SSR layout pulled out of the server-side /api/bootstrap
// fetch (settings.admin_token; null for analyst bootstraps and for
// admin bootstraps when ADMIN_SHARED_SECRET isn't configured).
//
// Why this exists: with the SSR bootstrap hydration in app/layout.tsx,
// useBootstrap's queryFn is short-circuited by the
// HydrationBoundary-seeded cache on first mount. The setToken(...)
// call inside queryFn therefore NEVER runs on the SSR path, leaving
// the Zustand store empty when ServicesTable's useQuery fires on the
// same render → the admin gate sees a request with no X-Admin-Token →
// 401 "admin_token_required" → "Couldn't load services" banner.
//
// Calling Zustand setState() inline during render is normally a
// smell, but here it's a one-shot idempotent write that has to land
// before any sibling renders — useEffect would commit too late
// (children's useQuery callbacks have already registered by then).
// The module-level `hydrated` flag guarantees we only write once per
// page load, so repeat renders of this component (route nav within
// the same SPA session) don't clobber a later store update.

let hydrated = false

export function HydrateAdminToken({ token }: { token: string | null }) {
  if (!hydrated && typeof token === 'string' && token) {
    useAdminTokenStore.setState({ token })
    hydrated = true
  }
  return null
}
