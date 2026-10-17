// Runtime guards for admin payloads whose generated schema is a free dict.
// Callers narrow to a local shape only after the top-level container checks
// out, so a malformed body degrades to the caller's fallback instead of
// throwing deep inside render.

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

export function recordOrNull<T extends object>(v: T | null | undefined): T | null {
  return isRecord(v) ? (v as T) : null
}

export function arrayOrEmpty<T>(v: readonly T[] | null | undefined): T[] {
  return Array.isArray(v) ? [...v] : []
}
