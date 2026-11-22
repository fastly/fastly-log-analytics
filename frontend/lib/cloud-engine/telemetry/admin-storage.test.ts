import { afterEach, expect, it, vi } from "vitest";
import { adminRequest } from "./admin";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

it("rejects a partial JSON read instead of returning a successful empty object", async () => {
  const fetchMock = vi.fn().mockResolvedValue(
    new Response("{\"nodes\":[", { status: 200, headers: { "content-type": "application/json" } }),
  );
  vi.stubGlobal("fetch", fetchMock);

  await expect(adminRequest("/api/admin/storage-tree")).rejects.toBeInstanceOf(SyntaxError);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it.each([
  ["HEAD", 200],
  ["GET", 204],
] as const)("returns an empty object for a bodyless %s success", async (method, status) => {
  const response = new Response(null, { status });
  const jsonMock = vi.spyOn(response, "json");
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response));

  await expect(adminRequest<Record<string, never>>(
    "/api/admin/storage-tree",
    { method },
  )).resolves.toEqual({});
  expect(jsonMock).not.toHaveBeenCalled();
});

it("retries a transient failure while reading a GET response body", async () => {
  vi.useFakeTimers();
  const first = Response.json({ nodes: [] });
  vi.spyOn(first, "json").mockRejectedValueOnce(new TypeError("terminated"));
  const second = Response.json({
    nodes: [{ key: "processed/request/one.parquet" }],
    complete: true,
  });
  const fetchMock = vi.fn()
    .mockResolvedValueOnce(first)
    .mockResolvedValueOnce(second);
  vi.stubGlobal("fetch", fetchMock);
  const init = { method: "GET", headers: { "x-request": "same" } };

  const pending = adminRequest("/api/admin/storage-tree?cursor=next", init);
  const expectation = expect(pending).resolves.toEqual({
    nodes: [{ key: "processed/request/one.parquet" }],
    complete: true,
  });
  await vi.runAllTimersAsync();

  await expectation;
  expect(fetchMock).toHaveBeenCalledTimes(2);
  expect(String(fetchMock.mock.calls[0][0])).toMatch(
    /\/api\/admin\/storage-tree\?cursor=next$/,
  );
  expect(fetchMock.mock.calls[1][0]).toBe(fetchMock.mock.calls[0][0]);
  expect(fetchMock.mock.calls[1][1]).toBe(fetchMock.mock.calls[0][1]);
  expect(fetchMock.mock.calls[0][1]).toEqual({
    credentials: "same-origin",
    ...init,
  });
});

it("does not retry response body failures for non-idempotent requests", async () => {
  const response = Response.json({ started: true });
  vi.spyOn(response, "json").mockRejectedValueOnce(new TypeError("terminated"));
  const fetchMock = vi.fn().mockResolvedValue(response);
  vi.stubGlobal("fetch", fetchMock);

  await expect(adminRequest("/api/admin/usage/reconcile-storage", {
    method: "POST",
  })).rejects.toBeInstanceOf(TypeError);
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("recovers a dropped storage cursor read without changing the request", async () => {
  vi.useFakeTimers();
  const fetchMock = vi.fn()
    .mockRejectedValueOnce(new TypeError("Failed to fetch"))
    .mockResolvedValueOnce(Response.json({
      nodes: [{ key: "processed/request/one.parquet" }],
      complete: true,
    }));
  vi.stubGlobal("fetch", fetchMock);

  const pending = adminRequest("/api/admin/storage-tree?cursor=next");
  const expectation = expect(pending).resolves.toEqual({
    nodes: [{ key: "processed/request/one.parquet" }],
    complete: true,
  });
  await vi.runAllTimersAsync();

  await expectation;
  expect(fetchMock).toHaveBeenCalledTimes(2);
  const urls = fetchMock.mock.calls.map(([url]) => String(url));
  expect(urls).toHaveLength(2);
  expect(urls[0].endsWith("/api/admin/storage-tree?cursor=next")).toBe(true);
  expect(urls[1]).toBe(urls[0]);
});
