const WORKER_THRESHOLD = 100_000

export function parseJsonAsync<T>(jsonString: string): Promise<T> {
  return new Promise((resolve, reject) => {
    if (typeof window === 'undefined' || process.env.NODE_ENV === 'test' || jsonString.length < WORKER_THRESHOLD) {
      try {
        resolve(JSON.parse(jsonString));
      } catch (e) {
        reject(e);
      }
      return;
    }
    const worker = new Worker(new URL('./json-worker.ts', import.meta.url));
    worker.onmessage = (event) => {
      worker.terminate();
      if (event.data.success) {
        resolve(event.data.data);
      } else {
        reject(new Error(event.data.error));
      }
    };
    worker.onerror = (error) => {
      worker.terminate();
      reject(error);
    };
    worker.postMessage(jsonString);
  });
}
