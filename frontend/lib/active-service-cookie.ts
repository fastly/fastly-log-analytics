// Cookie name shared between the server layout (reads it via next/headers
// for SSR initial state) and the client store (writes it on change).
export const ACTIVE_SERVICE_COOKIE = 'fla.activeServiceId'

export function setActiveServiceCookie(serviceId: string | null) {
  if (typeof document === 'undefined') return
  if (serviceId) {
    document.cookie = `${ACTIVE_SERVICE_COOKIE}=${serviceId}; path=/; max-age=31536000; samesite=lax`
  } else {
    document.cookie = `${ACTIVE_SERVICE_COOKIE}=; path=/; max-age=0; samesite=lax`
  }
}
