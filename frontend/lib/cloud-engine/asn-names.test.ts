import { afterEach, describe, expect, it, vi } from "vitest";
import {
  formatAsnDisplayLabel,
  formatCityCountryLabel,
  getCachedAsnName,
  loadAsnNames,
  resetAsnNamesForTest,
  setAsnNamesForTest,
} from "./asn-names";

describe("cloud-engine asn-names and country label parity", () => {
  afterEach(() => {
    resetAsnNamesForTest();
    vi.unstubAllGlobals();
  });

  it("parses /geo/asn-names.tsv once and formats labels to match High-Scale Name (ASN)", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: async () =>
        "7922\tComcast Cable Communications, LLC, US\n14061\tDigitalOcean, LLC, US\n16628\tDedFiberCo, US\n",
    });
    vi.stubGlobal("window", {});
    vi.stubGlobal("fetch", fetchMock);

    const first = await loadAsnNames();
    const second = await loadAsnNames();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith("/geo/asn-names.tsv", expect.any(Object));
    expect(first).toBe(second);
    expect(getCachedAsnName(7922)).toBe("Comcast Cable Communications, LLC, US");
    expect(formatAsnDisplayLabel(7922)).toBe("Comcast Cable Communications, LLC, US (7922)");
    expect(formatAsnDisplayLabel("AS16628")).toBe("DedFiberCo, US (16628)");
    expect(formatAsnDisplayLabel(64500)).toBe("AS64500");
  });

  it("prefers explicit non-AS logged as_name over bundled lookup and avoids double-appending (ASN)", () => {
    setAsnNamesForTest([[15169, "Google LLC, US"]]);
    expect(formatAsnDisplayLabel(15169, "Custom Org")).toBe("Custom Org (15169)");
    expect(formatAsnDisplayLabel(15169, "AS15169")).toBe("Google LLC, US (15169)");
    expect(formatAsnDisplayLabel(15169, "Google LLC, US (15169)")).toBe("Google LLC, US (15169)");
    expect(formatAsnDisplayLabel(15169, "")).toBe("Google LLC, US (15169)");
    expect(formatAsnDisplayLabel(null)).toBe("Unknown ASN");
  });

  it("fails open when /geo/asn-names.tsv fetch fails and allows a later retry", async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce({ ok: true, text: async () => "54113\tFastly, Inc., US\n" });
    vi.stubGlobal("window", {});
    vi.stubGlobal("fetch", fetchMock);

    const empty = await loadAsnNames();
    expect(empty.size).toBe(0);
    expect(formatAsnDisplayLabel(54113)).toBe("AS54113");

    const loaded = await loadAsnNames();
    expect(loaded.get(54113)).toBe("Fastly, Inc., US");
    expect(formatAsnDisplayLabel(54113)).toBe("Fastly, Inc., US (54113)");
  });

  it("formats city, region, and full country names matching High-Scale format_city_label", () => {
    expect(formatCityCountryLabel("council bluffs", "US", "ia")).toBe(
      "Council Bluffs, IA, United States",
    );
    expect(formatCityCountryLabel("Denver", "US")).toBe("Denver, United States");
    expect(formatCityCountryLabel("", "BR")).toBe("Brazil");
    expect(formatCityCountryLabel("", "")).toBe("Unknown");
  });
});
