/**
 * Centralized formatting utilities for the Fastly Log Analysis frontend.
 */

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

/**
 * Formats a raw value (like bytes or country codes) based on its field name.
 */
export function formatValue(field: string | undefined, value: string | number | null | undefined): string {
  if (value === null || value === undefined) return 'null'
  
  if (typeof value === 'number') {
    if (field?.includes('bytes')) return formatBytes(value)
    return value.toLocaleString()
  }
  
  const str = String(value)
  
  // Country Code resolution
  if (field === 'country' && str.length === 2 && typeof Intl !== 'undefined') {
    try {
      const regionNames = new Intl.DisplayNames(['en'], { type: 'region' })
      return regionNames.of(str.toUpperCase()) || str
    } catch {
      return str
    }
  }

  if (field === 'pop' || field === 'region') {
    return str.toUpperCase()
  }

  if (field === 'city') {
    return str.toLowerCase().replace(/(^|[^\w])(\w)/g, m => m.toUpperCase())
  }

  if (field === 'proto' || field === 'tls') {
    const num = Number(value)
    if (!isNaN(num)) return parseFloat(num.toFixed(1)).toString()
  }

  return str
}

/**
 * Calculates the percentage delta between two numeric values.
 */
export function calculateDelta(current: number, previous: number | undefined): number | null {
  if (previous === undefined || previous === 0) return null;
  return ((current - previous) / previous) * 100;
}
