import { afterEach, expect, it, vi } from "vitest";
import { registerPasskey, signInWithPasskey } from "./passkeys";

afterEach(() => vi.unstubAllGlobals());

it("serializes the real browser credential and preserves challenge ID", async () => {
  const calls: { url: string; body: unknown }[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (request: Request) => {
      const body = await request.json();
      calls.push({ url: new URL(request.url).pathname, body });
      if (request.url.endsWith("/options")) {
        return Response.json({
          challenge_id: "one-use",
          public_key: {
            challenge: "AQID",
            rp: { id: "localhost", name: "FLP" },
            user: { id: "BAUG", name: "Admin", displayName: "Admin" },
            pubKeyCredParams: [{ type: "public-key", alg: -7 }],
          },
        });
      }
      return Response.json({
        user: { id: "admin", display_name: "Admin", role: "admin" },
      });
    }),
  );
  const create = vi.fn().mockResolvedValue({
    toJSON: () => ({
      id: "credential",
      response: { attestationObject: "signed" },
    }),
  });
  Object.defineProperty(navigator, "credentials", {
    configurable: true,
    value: { create },
  });
  await registerPasskey("Admin");
  expect(create.mock.calls[0][0].publicKey.challenge).toEqual(
    new Uint8Array([1, 2, 3]),
  );
  expect(calls.at(-1)?.body).toEqual({
    challenge_id: "one-use",
    credential: { id: "credential", response: { attestationObject: "signed" } },
  });
});

it("surfaces user cancellation instead of verifying an empty assertion", async () => {
  const fetcher = vi.fn(async () =>
    Response.json({
      challenge_id: "cancelled",
      public_key: { challenge: "AQID" },
    }),
  );
  vi.stubGlobal("fetch", fetcher);
  Object.defineProperty(navigator, "credentials", {
    configurable: true,
    value: { get: vi.fn().mockResolvedValue(null) },
  });
  await expect(signInWithPasskey()).rejects.toThrow("No passkey was selected");
  expect(fetcher).toHaveBeenCalledTimes(1);
});
