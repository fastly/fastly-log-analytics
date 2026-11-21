export function downloadBlob(blob: Blob, filename: string) {
  const url = window.URL.createObjectURL(blob)
  const a = Object.assign(document.createElement('a'), { href: url, download: filename })
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  window.URL.revokeObjectURL(url)
}

const PLAIN_NUMERIC_RE = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/
const LEADING_CTRL_OR_SPACE_RE = /^[\s\x00-\x1f\x7f\u200b-\u200f\u2028-\u202f\ufeff]*/u
const FORMULA_TRIGGER_RE = /^[=+\-@|%]/

export function sanitizeCsvCell(v: unknown, alwaysQuote = false): string {
  if (v == null) return alwaysQuote ? '""' : ''
  if (typeof v === 'number') {
    const numStr = Number.isFinite(v) ? String(v) : ''
    return alwaysQuote ? `"${numStr}"` : numStr
  }
  if (typeof v === 'boolean') {
    const boolStr = String(v)
    return alwaysQuote ? `"${boolStr}"` : boolStr
  }
  const s = String(v)
  let safe = s
  if (!PLAIN_NUMERIC_RE.test(s)) {
    if (/^[\t\r\n]/.test(s) || FORMULA_TRIGGER_RE.test(s.replace(LEADING_CTRL_OR_SPACE_RE, ''))) {
      safe = `'${s}`
    }
  }
  if (alwaysQuote || /[",\n\r]/.test(safe)) {
    return `"${safe.replace(/"/g, '""')}"`
  }
  return safe
}

export function downloadAsCsv(rows: Record<string, unknown>[], columns: string[], filename: string) {
  const csv = [
    columns.map((c) => sanitizeCsvCell(c)).join(','),
    ...rows.map((r) => columns.map((c) => sanitizeCsvCell(r[c])).join(',')),
  ].join('\n')
  downloadBlob(new Blob([csv], { type: 'text/csv;charset=utf-8;' }), filename)
}

export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    return false
  }
}
