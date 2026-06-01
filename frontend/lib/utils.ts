import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { formatForInput, parseFromInput, formatDate } from '@/lib/date'

export { formatBytes } from '@/lib/format'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDateTime(isoString: string | null | undefined, timeZone?: string): string {
  if (!isoString) return 'Unknown'
  return formatDate(isoString, timeZone || 'UTC', 'MMM d, yyyy h:mm:ss a')
}

/** Formats a date for <input type="datetime-local"> (YYYY-MM-DDTHH:mm:ss) in a specific timezone */
export function toLocalISO(isoString: string | null | undefined, timeZone?: string): string {
  if (!isoString) return ''
  return formatForInput(isoString, timeZone || 'UTC')
}

/** Parses a date from <input type="datetime-local"> in a specific timezone back to UTC ISO */
export function fromLocalISO(localString: string, timeZone?: string): string {
  if (!localString) return ''
  return parseFromInput(localString, timeZone || 'UTC') ?? ''
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = window.URL.createObjectURL(blob)
  const a = Object.assign(document.createElement('a'), {
    href: url,
    download: filename,
  })
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}

export function downloadAsCsv(rows: Record<string, any>[], columns: string[], filename: string) {
  const escape = (v: any): string => {
    if (v == null) return ''
    const s = String(v)
    return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
  }
  const csv = [
    columns.map(escape).join(','),
    ...rows.map(r => columns.map(c => escape(r[c])).join(',')),
  ].join('\n')
  downloadBlob(new Blob([csv], { type: 'text/csv;charset=utf-8;' }), filename)
}
