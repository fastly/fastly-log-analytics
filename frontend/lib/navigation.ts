export function buildServiceHref(path: string, serviceId: string | null | undefined): string {
  if (!serviceId) return path
  const sep = path.includes('?') ? '&' : '?'
  return `${path}${sep}service=${serviceId}`
}
