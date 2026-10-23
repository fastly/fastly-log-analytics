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

export function downloadAsCsv(rows: Record<string, unknown>[], columns: string[], filename: string) {
  const escape = (v: unknown): string => {
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

export function downloadText(content: string, filename: string, mimeType = 'text/plain;charset=utf-8;') {
  downloadBlob(new Blob([content], { type: mimeType }), filename)
}

export function downloadJson(data: unknown, filename: string) {
  downloadText(JSON.stringify(data, null, 2), filename, 'application/json;charset=utf-8;')
}
