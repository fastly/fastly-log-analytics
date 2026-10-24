// Cookie names and helpers shared between SSR Server Components and client stores.
// No 'use client' directive — imported by both Server Components (app/layout.tsx)
// and Client Components (AppLayout.tsx, StoreHydrator.tsx, DebugPanel.tsx).
// Adding 'use client' here would wrap string constants in Next's client-reference
// proxy so they evaluate to `undefined` inside Server Components.

export const ACTIVE_SERVICE_COOKIE = 'fla.activeServiceId'

export function setActiveServiceCookie(serviceId: string | null) {
  if (typeof document === 'undefined') return
  if (serviceId && /^[A-Za-z0-9]{10,30}$/.test(serviceId)) {
    document.cookie = `${ACTIVE_SERVICE_COOKIE}=${serviceId}; path=/; max-age=31536000; samesite=lax`
  } else {
    document.cookie = `${ACTIVE_SERVICE_COOKIE}=; path=/; max-age=0; samesite=lax`
  }
}

export const DEBUG_RESPONSES_COOKIE = 'fla.debugResponses'

export const SIDEBAR_COLLAPSED_COOKIE = 'fla.sidebarCollapsed'
