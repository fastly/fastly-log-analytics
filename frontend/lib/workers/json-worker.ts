// Web worker that runs JSON.parse off the main thread for large payloads
// (e.g. /api/query responses with 50k+ rows × many columns).

self.addEventListener('message', (event: MessageEvent<string>) => {
  try {
    const parsed = JSON.parse(event.data)
    self.postMessage({ success: true, data: parsed })
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    self.postMessage({ success: false, error: msg })
  }
})
