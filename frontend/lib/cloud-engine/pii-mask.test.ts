import { describe, expect, it } from "vitest";
import { maskIp } from "./pii-mask";

describe("maskIp (parity with backend/core/share_db/__init__.py:mask_ip)", () => {
  it("masks IPv4 addresses to a.b.c.xxx and preserves already-masked IPv4", () => {
    expect(maskIp("198.51.100.42")).toBe("198.51.100.xxx");
    expect(maskIp("192.0.2.1")).toBe("192.0.2.xxx");
    expect(maskIp("10.0.0.0")).toBe("10.0.0.xxx");
    expect(maskIp("198.51.100.xxx")).toBe("198.51.100.xxx");
  });

  it("masks IPv6 addresses to first 3 hextets :: in canonical RFC 5952 form", () => {
    expect(maskIp("2001:db8:85a3::8a2e:370:7334")).toBe("2001:db8:85a3::");
    expect(maskIp("2001:0db8:0000:0000:0000:0000:0000:0001")).toBe("2001:db8::");
    expect(maskIp("2001:0:85a3::1")).toBe("2001:0:85a3::");
    expect(maskIp("fe80::1")).toBe("fe80::");
    expect(maskIp("::1")).toBe("::");
    expect(maskIp("::ffff:192.0.2.1")).toBe("::");
    expect(maskIp("2001:db8:85a3::")).toBe("2001:db8:85a3::");
  });

  it("returns [redacted] for empty, nullish, or invalid addresses", () => {
    expect(maskIp("")).toBe("[redacted]");
    expect(maskIp("   ")).toBe("[redacted]");
    expect(maskIp(null)).toBe("[redacted]");
    expect(maskIp(undefined)).toBe("[redacted]");
    expect(maskIp("not-an-ip")).toBe("[redacted]");
    expect(maskIp("999.1.1.1")).toBe("[redacted]");
    expect(maskIp("192.0.2.xxx.1")).toBe("[redacted]");
    expect(maskIp("gggg::1")).toBe("[redacted]");
    expect(maskIp("1:2:3:4:5:6:7:8:9")).toBe("[redacted]");
  });
});
