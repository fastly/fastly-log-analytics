const WORKER_THRESHOLD = 100_000

/**
 * Parse a JSON string, offloading to a Web Worker when the payload is large
 * enough (>= 100 KB) that synchronous `JSON.parse` would noticeably block the
 * main thread. Falls back to synchronous `JSON.parse` when `Worker` is
 * unavailable or fails to spawn.
 */
export function parseJsonAsync<T = unknown>(jsonString: string): Promise<T> {
  if (
    typeof window === 'undefined' ||
    typeof Worker === 'undefined' ||
    jsonString.length < WORKER_THRESHOLD
  ) {
    try {
      return Promise.resolve(JSON.parse(jsonString) as T)
    } catch (err) {
      return Promise.reject(err)
    }
  }

  return new Promise<T>((resolve, reject) => {
    let worker: Worker
    try {
      worker = new Worker(new URL('./json-worker.ts', import.meta.url))
    } catch {
      try {
        resolve(JSON.parse(jsonString) as T)
      } catch (err) {
        reject(err)
      }
      return
    }

    worker.onmessage = (event) => {
      worker.terminate()
      if (event.data?.success) {
        resolve(event.data.data as T)
      } else {
        reject(new Error(event.data?.error ?? 'json-worker failed'))
      }
    }

    worker.onerror = () => {
      worker.terminate()
      try {
        resolve(JSON.parse(jsonString) as T)
      } catch (syncErr) {
        reject(syncErr)
      }
    }

    worker.postMessage(jsonString)
  })
}
