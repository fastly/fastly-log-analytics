const STORAGE_KEY = 'fla.cloud-session-references'
const LIMIT = 16

function references(): Record<string, unknown> {
  const saved = sessionStorage.getItem(STORAGE_KEY)
  const parsed: unknown = saved ? JSON.parse(saved) : null
  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) return { ...parsed }
  return {}
}

export function saveCloudSessionReference(reference: string, token: string): void {
  const entries = Object.entries(references()).filter(([id]) => id !== reference)
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(Object.fromEntries([...entries.slice(1 - LIMIT), [reference, token]])))
}

export function readCloudSessionReference(reference: string): string | null {
  if (!/^[0-9a-f-]{36}$/i.test(reference)) return null
  const saved = references()[reference]
  return typeof saved === 'string' ? saved : null
}
