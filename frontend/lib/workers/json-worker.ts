self.addEventListener('message', (event) => {
  try {
    const parsed = JSON.parse(event.data);
    self.postMessage({ success: true, data: parsed });
  } catch (err) {
    self.postMessage({ success: false, error: err instanceof Error ? err.message : String(err) });
  }
});
