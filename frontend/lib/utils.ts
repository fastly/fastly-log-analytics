import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { formatDate } from '@/lib/date'


export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDateTime(isoString: string | null | undefined, timeZone?: string): string {
  if (!isoString) return 'Unknown'
  return formatDate(isoString, timeZone || 'UTC', 'MMM d, yyyy h:mm:ss a')
}

import { downloadBlob, downloadAsCsv } from '@/lib/download'

export { downloadBlob, downloadAsCsv }

/**
 * Pair each item with a React key built from its own label plus the number of earlier items
 * that share it. For positional data with no natural id (table cells, repeated labels) this
 * keeps keys stable and unique without keying on the array index.
 */
export function keyByOccurrence<T>(items: readonly T[], label: (item: T) => string): { key: string; item: T }[] {
  const seen = new Map<string, number>()
  return items.map((item) => {
    const base = label(item)
    const n = seen.get(base) ?? 0
    seen.set(base, n + 1)
    return { key: n === 0 ? base : `${base}#${n}`, item }
  })
}
