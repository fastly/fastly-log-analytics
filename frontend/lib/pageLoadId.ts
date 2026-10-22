let pageLoadId: string | null = null

export function getPageLoadId(): string | null {
  if (typeof window === "undefined") return null
  if (!pageLoadId) {
    pageLoadId = typeof crypto?.randomUUID === "function"
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(36).slice(2)}`
  }
  return pageLoadId
}

export function rotatePageLoadId(): string | null {
  if (typeof window === "undefined") return null
  pageLoadId = typeof crypto?.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2)}`
  return pageLoadId
}
