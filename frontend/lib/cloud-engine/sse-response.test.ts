import { describe, expect, it } from "vitest";
import { sseFrame, sseResponse } from "./sse-response";

describe("Cloud SSE response helpers", () => {
  it("serializes one complete SSE data frame", () => {
    expect(sseFrame({ type: "done", data: { count: 2 } })).toBe(
      'data: {"type":"done","data":{"count":2}}\n\n',
    );
  });

  it("closes the response after the producer finishes", async () => {
    const response = sseResponse(async (emit) => {
      emit({ type: "status", message: "working" });
      emit({ type: "done" });
    });

    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(await response.text()).toBe(
      'data: {"type":"status","message":"working"}\n\ndata: {"type":"done"}\n\n',
    );
  });
});
