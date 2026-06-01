import { useEffect, useState } from 'react'

function computeElapsed(startedAt: string | null | undefined): number {
  if (!startedAt) return 0
  return Math.max(0, (Date.now() - new Date(startedAt).getTime()) / 1000)
}

/**
 * Returns seconds elapsed since `startedAt`, ticking every `intervalMs`.
 * Pass null/undefined to pause.
 */
export function useElapsedTime(startedAt: string | null | undefined, intervalMs = 1000): number {
  const [elapsed, setElapsed] = useState(() => computeElapsed(startedAt))

  useEffect(() => {
    if (!startedAt) return
    const id = setInterval(() => setElapsed(computeElapsed(startedAt)), intervalMs)
    return () => clearInterval(id)
  }, [startedAt, intervalMs])

  return elapsed
}
