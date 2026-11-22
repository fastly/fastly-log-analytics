export function parseFilterToken(
  text: string,
): { mode: 'include' | 'exclude'; field: string; value: string } | null {
  const sign = text[0]
  if (sign !== '+' && sign !== '-') return null
  const colon = text.indexOf(':')
  if (colon < 2) return null
  const field = text.slice(1, colon)
  const value = text.slice(colon + 1)
  if (!field || value === '') return null
  return { mode: sign === '+' ? 'include' : 'exclude', field, value }
}
