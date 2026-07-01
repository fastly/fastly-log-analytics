import { create } from 'zustand'

// Mirrors the caller's role as the backend reports it in /api/bootstrap's
// settings.is_remote_analyst. Set by useBootstrap on every bootstrap resolve
// (queryFn for the pure-CSR fast path AND the useEffect for the SSR-hydrated
// path, exactly like adminTokenStore).
//
// Why this exists: the typed-client 401 handler in lib/api.ts treats a dead
// session by bouncing the user to the analyst /share-login flow. That flow is
// ONLY meaningful to a remote analyst — an admin (loopback / SSH tunnel) has
// no analyst credentials and can't use it. A 401 on an admin-only endpoint
// (e.g. a provision call that's missing its Fastly token) must surface as a
// normal error, NOT eject the operator to a sign-in screen. The redirect
// reads this flag and no-ops for admins.
//
// is_remote_analyst describes the transport branch (Fastly-fronted vs
// loopback), not session validity, so it stays true for an analyst's whole
// session — a genuine mid-session cookie expiry still redirects correctly.
//
// Not persisted: it re-arrives on every bootstrap fetch (mirrors the
// adminTokenStore rationale — a stale persisted value would survive past a
// role/topology change until the next bootstrap miss).

interface SessionRoleState {
  isRemoteAnalyst: boolean
  setIsRemoteAnalyst: (value: boolean) => void
}

export const useSessionRoleStore = create<SessionRoleState>((set) => ({
  isRemoteAnalyst: false,
  setIsRemoteAnalyst: (isRemoteAnalyst) => set({ isRemoteAnalyst }),
}))
