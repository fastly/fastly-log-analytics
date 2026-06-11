import { buildTrafficData, type BuildTrafficDataParams } from '@/app/dashboard/_sections/chartHelpers'

/**
 * Async wrapper around buildTrafficData that runs on a Web Worker
 * when the dataset is large enough to benefit, otherwise calls the
 * sync version inline.
 *
 * Why threshold-gated:
 *   - Worker startup + structured-clone overhead is ~1-5 ms on a
 *     modern laptop. For tiny datasets (e.g., 24h @ 1-hour interval
 *     = 24 rows) the sync path is faster + simpler.
 *   - The cost-benefit crossover is around ~2k rows. Below that, the
 *     transform is sub-millisecond inline and any worker savings are
 *     drowned by the clone overhead.
 *
 * SSR-safe: returns the sync result when `window` is undefined
 * (next dev SSR, vitest). Same exit on Node test envs.
 */
const WORKER_THRESHOLD = 2000

export function buildTrafficDataAsync(params: BuildTrafficDataParams): Promise<any[]> {
  const n = params.aggregates?.time_series?.length ?? 0

  // SSR / test path: synchronous. Skip worker.
  if (typeof window === 'undefined' || process.env.NODE_ENV === 'test') {
    return Promise.resolve(buildTrafficData(params))
  }

  // Small-dataset path: synchronous. Worker overhead > savings.
  if (n < WORKER_THRESHOLD) {
    return Promise.resolve(buildTrafficData(params))
  }

  return new Promise((resolve, reject) => {
    let worker: Worker
    try {
      worker = new Worker(new URL('./chartDataWorker.ts', import.meta.url))
    } catch (e) {
      // Worker construction itself failed (rare — e.g., a sandboxed
      // context that doesn't allow Worker). Fall back to sync.
      resolve(buildTrafficData(params))
      return
    }
    worker.onmessage = (event) => {
      worker.terminate()
      if (event.data?.success) {
        resolve(event.data.data)
      } else {
        // Worker reported a transform error — propagate so the caller
        // can show its existing error UI rather than silently empty.
        reject(new Error(event.data?.error ?? 'chartDataWorker failed'))
      }
    }
    worker.onerror = (e) => {
      worker.terminate()
      // Worker runtime error (script load failed, etc.) — fall back to
      // sync rather than blank the chart for the user.
      try {
        resolve(buildTrafficData(params))
      } catch (sync) {
        reject(sync)
      }
    }
    worker.postMessage(params)
  })
}
