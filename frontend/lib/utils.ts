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
