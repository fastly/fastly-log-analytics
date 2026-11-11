/** Backoff before each retry of a fetch that failed before any response arrived. */
export const FETCH_RETRY_DELAYS_MS: readonly number[] = [250, 1000];

/**
 * `fetch` rejects with a TypeError only when no response arrived at all (DNS,
 * reset connection, dropped socket). Those are the transient failures worth
 * retrying; aborts and HTTP statuses are not.
 */
export function isNetworkFailure(error: unknown): error is TypeError {
  return error instanceof TypeError;
}

export async function fetchRetrying(
  input: string | URL | Request,
  init?: RequestInit,
  delays: readonly number[] = FETCH_RETRY_DELAYS_MS,
): Promise<Response> {
  const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);
  for (let attempt = 0; ; attempt += 1) {
    try {
      return await fetch(input instanceof Request ? input.clone() : input, init);
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
