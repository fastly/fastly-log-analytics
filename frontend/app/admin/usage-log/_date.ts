export function toQueryDate(d: Date): string {
  return d.toISOString().slice(0, 19) + 'Z'
}
