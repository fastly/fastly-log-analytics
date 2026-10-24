// Runtime guards for admin payloads whose generated schema is a free dict.
// Callers narrow to a local shape only after the top-level container checks
// out, so a malformed body degrades to the caller's fallback instead of
// throwing deep inside render.

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

export function recordOrNull<T extends object>(v: T | null | undefined): T | null {
  return isRecord(v) ? (v as T) : null
}

export function arrayOrEmpty<T>(v: readonly T[] | null | undefined): T[] {
  return Array.isArray(v) ? [...v] : []
}

/** Narrow a plain string (e.g. a Select's value) to one member of a generated string-enum union. */
export function oneOf<T extends string>(allowed: readonly T[], value: string | null | undefined): T | undefined {
  return allowed.find((a) => a === value)
}
