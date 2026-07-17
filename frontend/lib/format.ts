/**
 * Centralized formatting utilities for the Fastly Log Analysis frontend.
 */

// Binary units (1 KB = 1024 B); matches GCP/AWS/Fastly console conventions.
// Bumps to the next unit once the value would show 4+ digits (e.g. 1023.24 MB
// reads as "1 GB" instead), rather than only at the strict 1024 boundary.
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  let value = bytes
  let i = 0
  while (value >= 1000 && i < sizes.length - 1) {
    value /= k
    i++
  }
  return parseFloat(value.toFixed(2)) + ' ' + sizes[i]
}

/**
 * Resolve an ISO-3166 alpha-2 country code to its English display name.
 *
 * Intl.DisplayNames is constructed PER CALL (not memoised at module scope)
 * on purpose: format.test.ts swaps the constructor with a throwing stub at
 * test time and expects the catch-fallback to fire — a module-scope instance
 * would be built before the swap and defeat that test. Non-2-char codes,
 * missing Intl, and ICU failures all fall through to the raw code unchanged.
 */
export function resolveCountryName(code: string): string {
  if (!code || code.length !== 2 || typeof Intl === 'undefined') return code
  try {
    const regionNames = new Intl.DisplayNames(['en'], { type: 'region' })
    return regionNames.of(code.toUpperCase()) || code
  } catch {
    return code
  }
}

/**
 * Compact count with a k/M suffix, trailing ".0" stripped (e.g. 1_000_000 →
 * "1M", 1_500 → "1.5k", 999 → "999").
 */
export function formatCompactCount(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1).replace(/\.0$/, '') + 'k'
  return String(n)
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
  if (field === 'country' && str.length === 2) {
    return resolveCountryName(str)
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

export function formatCurrency(amount: number): string {
  if (amount >= 1000) return '$' + (amount / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
  if (amount >= 1) return '$' + amount.toFixed(2)
  return '$' + amount.toFixed(4)
}

/**
 * Calculates the percentage delta between two numeric values.
 */
export function calculateDelta(current: number, previous: number | undefined): number | null {
  if (previous === undefined || previous === 0) return null;
  return ((current - previous) / previous) * 100;
}
