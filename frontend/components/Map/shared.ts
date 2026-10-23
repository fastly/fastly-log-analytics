export const NORMALIZE_COUNTRY: Record<string, string> = {
  'United States': 'United States of America',
  'Russia': 'Russia',
  'South Korea': 'South Korea',
  'Vietnam': 'Vietnam',
  'Taiwan': 'Taiwan',
}

/**
 * rAF-throttle a function so it fires at most once per animation frame.
 * Wrapping MapLibre `mousemove` handlers caps the per-frame re-render
 * cost to display refresh rate while preserving the latest position.
 */
export function rafThrottle<TArgs extends unknown[]>(fn: (...args: TArgs) => void) {
  let queued = false
  let lastArgs: TArgs | null = null
  return (...args: TArgs) => {
    lastArgs = args
    if (queued) return
    queued = true
    requestAnimationFrame(() => {
      queued = false
      if (lastArgs) fn(...lastArgs)
      lastArgs = null
    })
  }
}
