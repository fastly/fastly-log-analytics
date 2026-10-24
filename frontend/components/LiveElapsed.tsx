'use client'

import { useElapsedTime, useNowMs } from '@/hooks/useNowSeconds'
import { formatCompactDuration, toUTCDate } from '@/lib/date'

/** Ticking "Ns" / "Nm Ns" elapsed label for a running job. Callers supply the
 *  typography via ``className`` (the tick itself is the shared useNowMs ticker). */
export function LiveElapsed({ startedAt, className }: { startedAt: string; className?: string }) {
  const elapsed = useElapsedTime(startedAt)
  const fmt = elapsed < 60 ? `${elapsed.toFixed(0)}s` : `${Math.floor(elapsed / 60)}m ${Math.floor(elapsed % 60)}s`
  return <span className={className}>{fmt}</span>
}

/** Ticking countdown until ``when`` (or "Disabled" when nullish). */
export function NextRunCountdown({ when }: { when: string | null | undefined }) {
  const nowMs = useNowMs()
  if (!when) return <>Disabled</>
  return <>{formatCompactDuration(Math.floor((toUTCDate(when).getTime() - nowMs) / 1000))}</>
}
