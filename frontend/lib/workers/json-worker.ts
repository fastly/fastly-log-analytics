self.addEventListener('message', (event) => {
  try {
    const parsed = JSON.parse(event.data);
    self.postMessage({ success: true, data: parsed });
  } catch (err: any) {
    self.postMessage({ success: false, error: err.message });
  }
});
