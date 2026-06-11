// Web worker that hosts buildTrafficData() off the main thread.
//
// buildTrafficData is O(n) → O(n²) in trend-window mode and the
// dashboard's 7d/30d windows can push it past 10k time-series rows.
// Running it inline in useMemo blocks React's render loop and shows
// up as high TBT (Total Blocking Time) in Lighthouse / DevTools.
// This worker accepts the same input shape, runs the transform, and
// posts the trace array back. The caller (buildTrafficData.ts)
// terminates the worker after each call.
//
// hiddenCategories is a `Set<string>` in the caller; structured-clone
// preserves Sets across postMessage so we re-use the same shape.

import { buildTrafficData, type BuildTrafficDataParams } from '@/app/dashboard/_sections/chartHelpers'

self.addEventListener('message', (event: MessageEvent<BuildTrafficDataParams>) => {
  try {
    const traces = buildTrafficData(event.data)
    self.postMessage({ success: true, data: traces })
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    self.postMessage({ success: false, error: msg })
  }
})
