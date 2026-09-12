import { describe, expect, it } from "vitest";
import { shouldLoadRumScript } from "@/lib/rum-script";

describe("shouldLoadRumScript", () => {
  it("loads RUM for an analyst request through the real proxy domain", () => {
    expect(
      shouldLoadRumScript({
        host: "fastly-log-analytics.example.com",
        proxiedByCaddy: true,
        rumEnabled: true,
      }),
    ).toBe(true);
  });

  it.each(["localhost:13002", "127.0.0.1:13002", "[::1]:13002"])(
    "does not load RUM on local host %s",
    (host) => {
      expect(
        shouldLoadRumScript({ host, proxiedByCaddy: true, rumEnabled: true }),
      ).toBe(false);
    },
  );

  it("does not load RUM when the request is not from the analyst proxy", () => {
    expect(
      shouldLoadRumScript({
        host: "fastly-log-analytics.example.com",
        proxiedByCaddy: false,
        rumEnabled: true,
      }),
    ).toBe(false);
  });

  it("does not load RUM when the active service has it disabled", () => {
    expect(
      shouldLoadRumScript({
        host: "fastly-log-analytics.example.com",
        proxiedByCaddy: true,
        rumEnabled: false,
      }),
    ).toBe(false);
  });
});
