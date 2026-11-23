import { afterEach, expect, it, vi } from "vitest";
import { measureCloudQuery } from "./readiness";
import { setCloudModeRuntime } from "./mode";

afterEach(() => setCloudModeRuntime(false));

it("captures the originating service through a late query completion without requiring window", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  const scope = window;
  scope.addEventListener("fla:cloud-query", listener);
  try {
    vi.stubGlobal("window", undefined);
    let finish!: (data: { total_rows: number }) => void;
    const result = measureCloudQuery("dashboard.bundle",
      () => new Promise(resolve => { finish = resolve; }), undefined, "service-a");
    expect(events).toMatchObject([{ serviceId: "service-a", state: "pending" }]);
    finish({ total_rows: 0 });
    await result;
    expect(events).toMatchObject([
      { serviceId: "service-a", state: "pending" },
      { serviceId: "service-a", state: "success", totalRows: 0 },
    ]);
  } finally {
    vi.unstubAllGlobals();
    scope.removeEventListener("fla:cloud-query", listener);
  }
});

it("reports real completion only after a pending query resolves, preserving zero rows", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    let finish!: (data: { aggregates: { total_rows: number } }) => void;
    const result = measureCloudQuery("dashboard.bundle", () => new Promise(resolve => { finish = resolve; }));
    expect(events).toMatchObject([{ state: "pending", section: "dashboard.bundle" }]);
    finish({ aggregates: { total_rows: 0 } });
    expect(await result).toEqual({ aggregates: { total_rows: 0 } });
    expect(events).toMatchObject([
      { state: "pending" }, { state: "success", totalRows: 0 },
    ]);
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("does not turn stale-view exceptions into successful readiness", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const error = new Error("sentinel-private-query-token");
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw error; })).rejects.toBe(error);
    expect(events).toMatchObject([{ state: "pending" }, { state: "error", totalRows: null, error: "Error: [redacted]" }]);
    expect(JSON.stringify(events)).not.toContain(error.message);
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("reports a bounded, redacted error message so a failed query can be diagnosed", async () => {
  setCloudModeRuntime(true);
  const events: { error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const oom = new Error("Out of Memory Error: could not allocate block of size 256.0 KiB (243.8 MiB/244.1 MiB used)");
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw oom; })).rejects.toBe(oom);
    expect(events[1].error).toBe(`Error: ${oom.message}`);

    const leaky = new Error([
      "Binder Error: no column 'sentinel-literal' at https://user:sentinel-pass@sentinel-host.example/files?sig=sentinel-sig",
      "token=sentinelshort Authorization: Bearer sentinelbearer key sentinelkey api_key=sentinelapikey",
      "ops@sentinel-mail.example AKIAQWERTYSENTINEL0123456789",
      "padding ".repeat(60),
    ].join(" ") + "\nLINE 1: SELECT * FROM base WHERE ip = '10.0.0.1' sentinel-sql");
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw leaky; })).rejects.toBe(leaky);
    const reported = events[3].error!;
    expect(reported.startsWith("Error: Binder Error: no column [redacted] at [url]")).toBe(true);
    expect(reported.length).toBeLessThanOrEqual(240);
    expect(reported).not.toMatch(/sentinel|10\.0\.0\.1|LINE 1/);

    const quoteShift = new Error('Failed fetching "https://fos.example.com/a.parquet" for user "MyShortPass123" in query');
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw quoteShift; })).rejects.toBe(quoteShift);
    expect(events[5].error).not.toContain("MyShortPass123");
    expect(events[5].error).not.toContain("fos.example.com");

    const basicAuth = new Error("Header Authorization: Basic YWRtaW46c2VjcmV0 rejected");
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw basicAuth; })).rejects.toBe(basicAuth);
    expect(events[7].error).not.toContain("YWRtaW46c2VjcmV0");

    // BUG-T5-ITER25-03: Multiline quoted strings and unterminated trailing quotes must be redacted,
    // while contractions like can't / doesn't remain intact:
    for (const secretErr of [
      new Error('Failed fetching for user "MyShortPass123\ninvalid"'),
      new Error("Binder Error: syntax error near 'SecretTok99!\nSELECT 1'"),
      new Error("Template literal error `BacktickSec88\nline2`"),
      new Error("Parser Error: unterminated quoted string at or near 'UnclosedSec77"),
      new Error("Parser Error: unterminated escaped string near E'EscapedSec66"),
    ]) {
      const beforeLen = events.length;
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw secretErr; })).rejects.toBe(secretErr);
      const outErr = events[beforeLen + 1]?.error ?? "";
      for (const secretFrag of ["MyShortPass123", "SecretTok99", "BacktickSec88", "UnclosedSec77", "EscapedSec66"]) {
        expect(outErr).not.toContain(secretFrag);
      }
      expect(outErr).toContain("[redacted]");
    }

    const contractionErr = new Error("Backend can't reach cache metadata");
    const cIdx = events.length;
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw contractionErr; })).rejects.toBe(contractionErr);
    expect(events[cIdx + 1]?.error).toBe("Error: Backend can't reach cache metadata");

    // BUG-T5-ITER26-02 / 01: Contractions/possessives before single-quoted secrets, E'...' backslash
    // escapes, JSON \" escapes, and SQL dollar-quoted strings ($$...$$ / $tag$...$tag$):
    const multiContractionErr = new Error("Backend can't reach don't user's metadata");
    const mcIdx = events.length;
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw multiContractionErr; })).rejects.toBe(multiContractionErr);
    expect(events[mcIdx + 1]?.error).toBe("Error: Backend can't reach don't user's metadata");

    for (const [advMsg, forbiddenFrags, requiredSubstr] of [
      ["Binder Error: can't parse value '198.51.100.42' in filter", ["198.51.100.42"], "Error: Binder Error: can't parse value [redacted] in filter"],
      [`Binder Error: user's column "SecCol99" has value 'SecVal88'`, ["SecCol99", "SecVal88"], "Error: Binder Error: user's column [redacted] has value [redacted]"],
      ["Parser Error: invalid literal E'prefix\\'SecEsc77'", ["SecEsc77", "prefix"], "Error: Parser Error: invalid literal [redacted]"],
      ['JSON Error: bad value "foo\\"AliceRowValue99"', ["AliceRowValue99", "foo"], "Error: JSON Error: bad value [redacted]"],
      ["Binder Error: invalid literal $$SecDollar66$$ in query", ["SecDollar66"], "Error: Binder Error: invalid literal [redacted] in query"],
      ["Binder Error: invalid literal $tag$SecTag55\nSELECT 1$tag$ in query", ["SecTag55", "SELECT 1"], "Error: Binder Error: invalid literal [redacted] in query"],
      // BUG-T5-ITER27-01: Unicode possessives, plural possessives, trailing SQL backslashes, N'/R' prefixes, unspaced keyword quotes, and raw IPv4:
      ["Binder Error: café's owner '198.51.100.42' in filter", ["198.51.100.42"], "Error: Binder Error: café's owner [redacted] in filter"],
      ["Binder Error: users' 'SecAfterPlural' in filter", ["SecAfterPlural"], "Error: Binder Error: users' [redacted] in filter"],
      ["Binder Error: invalid pattern LIKE '%\\' for client '198.51.100.42'", ["198.51.100.42", "%\\"], "Error: Binder Error: invalid pattern LIKE [redacted] for client [redacted]"],
      ["Binder Error: invalid pattern LIKE '%\\\\' for client '198.51.100.42'", ["198.51.100.42", "%\\\\"], "Error: Binder Error: invalid pattern LIKE [redacted] for client [redacted]"],
      ["Parser Error: invalid literal N'198.51.100.42' or n'SecNat88' or R'SecRaw77'", ["198.51.100.42", "SecNat88", "SecRaw77"], "Error: Parser Error: invalid literal [redacted] or [redacted] or [redacted]"],
      ["Parser Error: syntax error near SELECT'198.51.100.42' or LIKE'SecNoSpace' or WHERE'SecPhrase With Spaces'", ["198.51.100.42", "SecNoSpace", "SecPhrase"], "Error: Parser Error: syntax error near SELECT[redacted] or LIKE[redacted] or WHERE[redacted]"],
      ["Runtime Error: upstream client 198.51.100.42 timed out", ["198.51.100.42"], "Error: Runtime Error: upstream client [redacted] timed out"],
    ] as const) {
      const advErr = new Error(advMsg);
      const aIdx = events.length;
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw advErr; })).rejects.toBe(advErr);
      const got = events[aIdx + 1]?.error ?? "";
      for (const frag of forbiddenFrags) {
        expect(got).not.toContain(frag);
      }
      expect(got).toBe(requiredSubstr);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("names a dropped connection a network error without exposing the failing URL", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const error = new TypeError("Failed to fetch https://sentinel-private-host/api/file");
    await expect(measureCloudQuery("api./api/insight-availability", async () => { throw error; })).rejects.toBe(error);
    expect(events).toMatchObject([{ state: "pending" }, { state: "error", error: "NetworkError" }]);
    expect(JSON.stringify(events)).not.toContain("sentinel");
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("emits cancelled instead of error on AbortError without poisoning readiness", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const error = new DOMException("Aborted", "AbortError");
    await expect(measureCloudQuery("api./api/services/svc/scoring/labels", async () => { throw error; })).rejects.toBe(error);
    expect(events).toMatchObject([{ state: "pending" }, { state: "cancelled", totalRows: null }]);
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("reports failed local API responses as errors without exposing response bodies", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const response = Response.json({ secret: "sentinel-private-response" }, { status: 503 });
    expect(await measureCloudQuery("api./api/origin/aggregates", async () => response)).toBe(response);
    expect(events).toMatchObject([{ state: "pending" }, { state: "error", status: 503, error: "HTTP 503" }]);
    expect(JSON.stringify(events)).not.toContain("sentinel");
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("names an over-budget scan without exposing its response body", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const response = Response.json({ detail: "sentinel-private-detail" }, { status: 413 });
    await measureCloudQuery("api./api/origin/aggregates", async () => response);
    expect(events).toMatchObject([{ state: "pending" },
      { state: "error", status: 413, error: "HTTP 413: query exceeds the browser scan budget" }]);
    expect(JSON.stringify(events)).not.toContain("sentinel");
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("keeps High-Scale silent and allowlists worker statistics without SQL or tokens", async () => {
  for (const name of ["NEXT_PUBLIC_DEPLOYMENT_MODE", "NEXT_PUBLIC_STATIC_EXPORT", "NEXT_PUBLIC_RUNTIME_MODE"]) {
    vi.stubEnv(name, "");
  }
  // IS_CLOUD_MODE is fixed at import, and the Cloud CI job exports the Cloud flags.
  vi.resetModules();
  const mode = await import("./mode");
  const readiness = await import("./readiness");
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-worker", listener);
  try {
    expect(mode.IS_CLOUD_MODE).toBe(false);
    await readiness.measureCloudQuery("dashboard.bundle", async () => ({ aggregates: { total_rows: 42 } }));
    readiness.publishCloudWorkerTelemetry({ files_fetched: 3, sql: "sentinel-private-sql" });
    expect(events).toEqual([]);
    mode.setCloudModeRuntime(true);
    readiness.publishCloudWorkerTelemetry({ files_fetched: 3, files_cached: 0, sql: "sentinel-private-sql" });
    expect(events).toMatchObject([{ filesFetched: 3, filesCached: 0 }]);
    expect(JSON.stringify(events)).not.toContain("sentinel");
  } finally {
    mode.setCloudModeRuntime(false);
    vi.unstubAllEnvs();
    window.removeEventListener("fla:cloud-worker", listener);
  }
});

it("BUG-T5-ITER27-01: redacts real @duckdb/duckdb-wasm runtime conversion errors over apostrophe-bearing rows", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query("CREATE OR REPLACE TABLE t_apostrophe AS SELECT '/cb?u=O''Brien&tok=SecRowTok99&ip=198.51.100.42' AS url");
    await expect(
      measureCloudQuery("query.console", () => backend.query("SELECT CAST(url AS INT) FROM t_apostrophe")),
    ).rejects.toThrow();
    const rowErr = events.at(-1)?.error ?? "";
    expect(rowErr).toContain("Could not convert string [redacted]");
    for (const leaked of ["Brien", "SecRowTok99", "198.51.100.42"]) {
      expect(rowErr).not.toContain(leaked);
    }

    await expect(
      measureCloudQuery("query.console", () => backend.query("SELECT CAST(E'prefix\\'198.51.100.42' AS INT)")),
    ).rejects.toThrow();
    const escErr = events.at(-1)?.error ?? "";
    expect(escErr).toContain("Could not convert string [redacted]");
    for (const leaked of ["prefix", "198.51.100.42"]) {
      expect(escErr).not.toContain(leaked);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
});
