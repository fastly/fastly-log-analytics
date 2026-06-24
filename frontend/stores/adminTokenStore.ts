import { create } from 'zustand'

// Holds the shared-secret admin token the backend exposes via
// /api/bootstrap when ADMIN_SHARED_SECRET is configured on the server.
// The fetch client middleware reads this in onRequest and injects
// X-Admin-Token on every outbound admin call. Analyst (Fastly-fronted)
// sessions never get a token — settings.admin_token comes back null —
// so the interceptor no-ops, leaving the analyst auth path untouched.
//
// Not persisted: the value re-arrives on every bootstrap fetch, and
// stashing it in localStorage would survive past env-var rotations
// (operator rotates secret → old persisted token keeps getting injected
// until next page reload + bootstrap miss). Session-scoped only.

interface AdminTokenState {
  token: string | null
  setToken: (token: string | null) => void
}

export const useAdminTokenStore = create<AdminTokenState>((set) => ({
  token: null,
  setToken: (token) => set({ token }),
}))
