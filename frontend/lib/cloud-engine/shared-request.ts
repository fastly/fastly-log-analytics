type Entry<T, P> = {
  promise: Promise<T>;
  controller: AbortController;
  listeners: Set<(progress: P) => void>;
  users: number;
};

export type SharedRequestOptions<P> = {
  signal?: AbortSignal;
  onProgress?: (progress: P) => void;
};

const abortError = () => new DOMException("Aborted", "AbortError");

/**
 * Concurrent callers with the same key share one in-flight request. Each caller
 * can abort on its own; the request itself is aborted only when every caller has
 * gone. Nothing is kept once the request settles, so failures are not cached.
 */
export function createSharedRequests<T, P = never>() {
  const inflight = new Map<string, Entry<T, P>>();
  return (
    key: string,
    start: (signal: AbortSignal, notify: (progress: P) => void) => Promise<T>,
    { signal, onProgress }: SharedRequestOptions<P> = {},
  ): Promise<T> => {
    if (signal?.aborted) return Promise.reject(abortError());
    const listener = onProgress && ((progress: P) => onProgress(progress));
    let entry = inflight.get(key);
    if (entry) {
      entry.users += 1;
      if (listener) entry.listeners.add(listener);
    } else {
      const controller = new AbortController();
      const listeners = new Set(listener ? [listener] : []);
      let promise: Promise<T>;
      try {
        promise = start(controller.signal, (progress) => {
          for (const notify of listeners) notify(progress);
        });
      } catch (error) {
        promise = Promise.reject(error);
      }
      const created: Entry<T, P> = { promise, controller, listeners, users: 1 };
      inflight.set(key, created);
      const settled = () => {
        if (inflight.get(key) === created) inflight.delete(key);
      };
      promise.then(settled, settled);
      entry = created;
    }
    const shared = entry;
    return new Promise<T>((resolve, reject) => {
      let done = false;
      const leave = () => {
        done = true;
        shared.users -= 1;
        if (listener) shared.listeners.delete(listener);
        signal?.removeEventListener("abort", onAbort);
      };
      const onAbort = () => {
        if (done) return;
        leave();
        if (shared.users === 0) {
          if (inflight.get(key) === shared) inflight.delete(key);
          shared.controller.abort();
        }
        reject(abortError());
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      shared.promise.then(
        (value) => {
          if (done) return;
          leave();
          resolve(value);
        },
        (error: unknown) => {
          if (done) return;
          leave();
          reject(error);
        },
      );
    });
  };
}
