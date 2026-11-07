import { afterEach, describe, expect, it, vi } from "vitest";
import { act, renderHook, waitFor } from "@testing-library/react";
import { createTestQueryClient, makeQueryWrapper } from "../helpers/page-smoke";
import { client } from "@/lib/api";
import { useFieldValues } from "@/hooks/useBootstrap";
import { useFilterStore, useServiceStore } from "@/stores/filterStore";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("field search requests", () => {
  it("cancels the request when its dialog closes and starts fresh on reopening", async () => {
    useServiceStore.setState({ activeServiceId: "search-service" });
    useFilterStore.setState({ filters: [], edgeOnly: false });
    const requests: AbortSignal[] = [];
    vi.spyOn(client, "POST").mockImplementation((_path, options) => {
      const signal = (options as { signal?: AbortSignal } | undefined)?.signal;
      if (signal) requests.push(signal);
      return new Promise<never>(() => {});
    });
    const queryClient = createTestQueryClient({ queries: { gcTime: 0 } });
    const { rerender, unmount } = renderHook(
      ({ enabled }) => useFieldValues({ field: "host", search: "", enabled }),
      { initialProps: { enabled: true }, wrapper: makeQueryWrapper(queryClient) },
    );
    await waitFor(() => expect(client.POST).toHaveBeenCalledTimes(1));
    expect(requests).toHaveLength(1);
    act(() => rerender({ enabled: false }));
    await waitFor(() => expect(requests[0].aborted).toBe(true));
    act(() => rerender({ enabled: true }));
    await waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[1].aborted).toBe(false);
    unmount();
    expect(requests[1].aborted).toBe(true);
    queryClient.clear();
  });
});
