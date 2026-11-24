export type SsePayload = Record<string, unknown>;
export type SseEmit = (payload: SsePayload) => void;

const encoder = new TextEncoder();

export function sseFrame(payload: SsePayload): string {
  return `data: ${JSON.stringify(payload)}\n\n`;
}

export function createSseEmitter(
  controller: ReadableStreamDefaultController<Uint8Array>,
): SseEmit {
  return (payload) => controller.enqueue(encoder.encode(sseFrame(payload)));
}

export function sseResponse(
  produce: (emit: SseEmit) => Promise<void> | void,
  signal?: AbortSignal,
): Response {
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const emit = createSseEmitter(controller);
      try {
        await produce(emit);
      } catch (error) {
        if (!signal?.aborted) controller.error(error);
      } finally {
        try {
          controller.close();
        } catch {
          /* already closed */
        }
      }
    },
  });
  return new Response(stream, { headers: { "content-type": "text/event-stream" } });
}
