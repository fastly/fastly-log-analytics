import { downloadBlob, downloadAsCsv } from '@/lib/utils'

export { downloadBlob, downloadAsCsv }

export function downloadText(content: string, filename: string, mimeType = 'text/plain;charset=utf-8;') {
  downloadBlob(new Blob([content], { type: mimeType }), filename)
}

export function downloadJson(data: unknown, filename: string) {
  downloadText(JSON.stringify(data, null, 2), filename, 'application/json;charset=utf-8;')
}
