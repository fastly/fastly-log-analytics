'use client'

import { useElapsedTime } from '@/hooks/useElapsedTime'

/** Ticking "Ns" / "Nm Ns" elapsed label for a running job. Callers supply the
 *  typography via ``className`` (the tick itself is the shared useNowMs ticker). */
export function LiveElapsed({ startedAt, className }: { startedAt: string; className?: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className={className}>{fmt}</span>
}
