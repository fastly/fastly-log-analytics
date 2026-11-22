/** Backoff before each retry of a fetch that failed before any response arrived. */
export const FETCH_RETRY_DELAYS_MS: readonly number[] = [250, 1000];

/**
 * Fetch acquisition and response body reads reject with TypeError for
 * transient transport failures (DNS, reset connection, dropped socket).
 * Aborts and HTTP statuses are not retried.
 */
export function isNetworkFailure(error: unknown): error is TypeError {
  return error instanceof TypeError;
}

export async function retryNetwork<T>(
  operation: () => Promise<T>,
  signal?: AbortSignal | null,
  delays: readonly number[] = FETCH_RETRY_DELAYS_MS,
): Promise<T> {
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      if (!isNetworkFailure(error) || signal?.aborted || attempt >= delays.length) throw error;
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => { signal?.removeEventListener("abort", onAbort); resolve(); }, delays[attempt]);
        const onAbort = () => { clearTimeout(timer); reject(signal?.reason); };
        signal?.addEventListener("abort", onAbort, { once: true });
      });
    }
  }
}

export async function fetchRetrying(
  input: string | URL | Request,
  init?: RequestInit,
  delays: readonly number[] = FETCH_RETRY_DELAYS_MS,
): Promise<Response> {
  const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
  return retryNetwork(
    () => fetch(input instanceof Request ? input.clone() : input, init),
    signal,
    delays,
  );
}
