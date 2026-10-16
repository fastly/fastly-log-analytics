/** Tailwind badge classes keyed by HTTP status class (2xx emerald, 3xx sky, 4xx amber, 5xx destructive). */
export function statusBadgeClass(status: number | string | null | undefined): string {
  const code = typeof status === 'number' ? status : parseInt(String(status), 10)
  if (isNaN(code)) return 'bg-muted text-muted-foreground'
  if (code >= 200 && code < 300) return 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'
  if (code >= 300 && code < 400) return 'bg-sky-500/15 text-sky-700 dark:text-sky-400 border-sky-500/20'
  if (code >= 400 && code < 500) return 'bg-amber-500/15 text-amber-700 dark:text-amber-400 border-amber-500/20'
  if (code >= 500) return 'bg-destructive/15 text-destructive border-destructive/20 font-bold'
  return 'bg-muted text-muted-foreground'
}
