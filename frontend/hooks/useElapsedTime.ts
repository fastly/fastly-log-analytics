import { useNowMs } from '@/hooks/useNowSeconds'

/**
 * Returns seconds elapsed since `startedAt`.
 *
 * Subscribes to the SHARED global 1-second tick (useNowMs) instead of
 * registering its own setInterval. Pre-fix each consumer spawned a
 * private 1Hz setState; N consumers on a page meant N independent
 * timers and N re-renders per second. Now the whole tree shares one
 * timer (lazily registered on first subscribe, torn down on last
 * unsubscribe).
 *
 * Pass null/undefined to pause (returns 0). Note: still subscribes to the
 * shared useNowMs tick — rules-of-hooks require the unconditional call — so
 * a paused timer still re-renders at 1 Hz, but on the single shared interval.
 */
export function useElapsedTime(startedAt: string | null | undefined): number {
  // Always call the hook (React rules of hooks) — but ignore the value
  // when there's no startedAt so we don't pin a subscription for a
  // paused timer.
  const nowMs = useNowMs()
  if (!startedAt) return 0
  return Math.max(0, (nowMs - new Date(startedAt).getTime()) / 1000)
}
