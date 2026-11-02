export function statusBadgeClass(
  status: number | string | null | undefined,
  variant: 'class' | 'traffic-light' = 'class',
): string {
  if (variant === 'traffic-light') {
    const n = typeof status === 'number' ? status : status == null ? 0 : Number(status)
    if (!n) return 'bg-muted text-muted-foreground'
    if (n >= 200 && n < 300) return 'bg-green-500/10 text-green-500 border-green-500/20'
    return n >= 300 && n < 400
      ? 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'
      : 'bg-red-500/10 text-red-500 border-red-500/20'
  }
  const code = typeof status === 'number' ? status : parseInt(String(status), 10)
  if (isNaN(code)) return 'bg-muted text-muted-foreground'
  if (code >= 200 && code < 300) return 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/20'
  if (code >= 300 && code < 400) return 'bg-sky-500/15 text-sky-700 dark:text-sky-400 border-sky-500/20'
  if (code >= 400 && code < 500) return 'bg-amber-500/15 text-amber-800 dark:text-amber-400 border-amber-500/20'
  return code >= 500
    ? 'bg-destructive/15 text-destructive border-destructive/20 font-bold'
    : 'bg-muted text-muted-foreground'
}

export const CHART_COLORS = {
  success: '#10b981',
  danger: '#ef4444',
  warning: '#f59e0b',
  info: '#3b82f6',
  neutral: '#94a3b8',
} as const

export function statusHex(code: number): string {
  return code >= 500
    ? CHART_COLORS.danger
    : code >= 400
      ? CHART_COLORS.warning
      : code >= 300
        ? CHART_COLORS.info
        : CHART_COLORS.success
}

export const panelDialogContent = 'max-h-[90vh] flex flex-col p-0 overflow-hidden'
export const panelDialogHeaderMuted = 'px-6 pt-6 pb-4 border-b bg-muted/10 shrink-0'
export const panelDialogHeaderSolid = 'px-6 pt-6 pb-4 border-b bg-background shrink-0'
export const panelDialogFooter = 'px-6 py-4 border-t bg-muted/10 shrink-0'
