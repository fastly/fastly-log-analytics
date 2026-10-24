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

export function extractApiError(error: unknown): string {
  if (!error) return 'Unknown error'
  if (typeof error === 'string') return error
  if (error instanceof Error && error.message) return error.message
  const e = error as {
    detail?: unknown;
    message?: unknown;
    error?: unknown;
    errors?: unknown;
  }
  if (e.detail) {
    if (typeof e.detail === 'string') return e.detail
    if (Array.isArray(e.detail)) {
      return (e.detail as Array<{ loc?: unknown[]; msg?: unknown }>).map((err) => {
        const field = Array.isArray(err.loc) && err.loc.length > 0 ? String(err.loc[err.loc.length - 1]) : 'Field'
        const msg = typeof err.msg === 'string' ? err.msg : 'Invalid value'
        return `${field}: ${msg}`
      }).join(', ')
    }
    if (typeof e.detail === 'object' && e.detail !== null) {
      const detailObj = e.detail as {
        errors?: unknown;
        messages?: unknown;
        message?: unknown;
        error?: unknown;
      }
      if (Array.isArray(detailObj.errors)) {
        return detailObj.errors.map(String).join(', ')
      }
      if (Array.isArray(detailObj.messages) && detailObj.messages.length > 0) {
        return detailObj.messages.map(String).join(', ')
      }
      if (typeof detailObj.message === 'string' && detailObj.message) {
        return detailObj.message
      }
      if (detailObj.error) {
        return String(detailObj.error)
      }
    }
    return JSON.stringify(e.detail)
  }
  if (typeof e.message === 'string' && e.message) return e.message
  if (e.error) return String(e.error)
  if (Array.isArray(e.errors)) return e.errors.map(String).join(', ')
  try { return JSON.stringify(error) } catch { return 'Unknown error' }
}

/**
 * Unwraps an `openapi-fetch` response promise (`{ data, error, response }`),
 * throwing a structured Error on HTTP 4xx/5xx or when `error` is populated.
 */
export async function unwrapClient<T>(
  promise: Promise<{ data?: T; error?: unknown; response?: Response }>,
  fallbackMessage?: string,
): Promise<T> {
  const { data, error, response } = await promise;
  if (error || (response && !response.ok)) {
    const status = response?.status ?? 500;
    const statusText = fallbackMessage || response?.statusText || `HTTP ${status}`;
    const msg = error ? extractApiError(error) : statusText;
    const err = new Error(msg && msg !== 'Unknown error' ? msg : statusText) as Error & {
      status?: number;
      response?: Response;
      detail?: unknown;
    };
    err.status = status;
    if (response) err.response = response;
    err.detail = error;
    throw err;
  }
  return data as T;
}
