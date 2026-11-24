import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchRetrying } from "./fetch-retry";

const networkFailure = () => new TypeError("Failed to fetch");

describe("fetchRetrying", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("retries a network failure and returns the eventual response", async () => {
    const fetchMock = vi.fn()
      .mockRejectedValueOnce(networkFailure())
      .mockRejectedValueOnce(networkFailure())
      .mockResolvedValueOnce(new Response("ok"));
    vi.stubGlobal("fetch", fetchMock);
    const response = await fetchRetrying("/api/file/x", undefined, [0, 0]);
    expect(await response.text()).toBe("ok");
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("gives up after the configured retries", async () => {
    const fetchMock = vi.fn().mockRejectedValue(networkFailure());
    vi.stubGlobal("fetch", fetchMock);
    await expect(fetchRetrying("/api/file/x", undefined, [0])).rejects.toBeInstanceOf(TypeError);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not retry aborts or non-network errors", async () => {
    const abort = new DOMException("Aborted", "AbortError");
    const fetchMock = vi.fn().mockRejectedValue(abort);
    vi.stubGlobal("fetch", fetchMock);
    await expect(fetchRetrying("/api/file/x", undefined, [0, 0])).rejects.toBe(abort);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("stops waiting when the signal aborts during the backoff", async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const fetchMock = vi.fn().mockRejectedValue(networkFailure());
    vi.stubGlobal("fetch", fetchMock);
    const pending = fetchRetrying("/api/file/x", { signal: controller.signal }, [60_000]);
    const settled = pending.catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(10);
    controller.abort(new DOMException("Aborted", "AbortError"));
    const error = await settled;
    expect(error).toBeInstanceOf(DOMException);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("re-reads the request body on each attempt", async () => {
    const seen: string[] = [];
    const fetchMock = vi.fn(async (request: Request) => {
      seen.push(await request.text());
      if (seen.length === 1) throw networkFailure();
      return new Response("ok");
    });
    vi.stubGlobal("fetch", fetchMock);
    await fetchRetrying(new Request("http://x/api/files", { method: "POST", body: "payload" }), undefined, [0]);
    expect(seen).toEqual(["payload", "payload"]);
  });
});
