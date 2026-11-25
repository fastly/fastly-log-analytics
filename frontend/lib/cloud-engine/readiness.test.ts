import { afterEach, expect, it, vi } from "vitest";
import { measureCloudQuery } from "./readiness";
import { setCloudModeRuntime } from "./mode";

const V8_COV_SCALE = (globalThis as { __vitest_worker__?: { config?: { coverage?: { enabled?: boolean } } } }).__vitest_worker__?.config?.coverage?.enabled ? 4 : 1;

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
    expect(events[mcIdx + 1]?.error).toBe("Error: Backend can't reach don't [redacted]");

    for (const [advMsg, forbiddenFrags, requiredSubstr] of [
      ["Binder Error: can't parse value '198.51.100.42' in filter", ["198.51.100.42"], "Error: Binder Error: can't parse value [redacted] in filter"],
      [`Binder Error: user's column "SecCol99" has value 'SecVal88'`, ["SecCol99", "SecVal88", "user's"], "Error: Binder Error: [redacted]"],
      ["Parser Error: invalid literal E'prefix\\'SecEsc77'", ["SecEsc77", "prefix"], "Error: Parser Error: invalid literal [redacted]"],
      ['JSON Error: bad value "foo\\"AliceRowValue99"', ["AliceRowValue99", "foo"], "Error: JSON Error: bad value [redacted]"],
      ["Binder Error: invalid literal $$SecDollar66$$ in query", ["SecDollar66"], "Error: Binder Error: invalid literal [redacted] in query"],
      ["Binder Error: invalid literal $tag$SecTag55\nSELECT 1$tag$ in query", ["SecTag55", "SELECT 1"], "Error: Binder Error: invalid literal [redacted] in query"],
      // BUG-T5-ITER27-01 (tightened in Iter32 closed-vocab): Unicode possessives, plural possessives, trailing SQL backslashes, N'/R' prefixes, unspaced keyword quotes, and raw IPv4:
      ["Binder Error: café's owner '198.51.100.42' in filter", ["198.51.100.42", "café"], "Error: Binder Error: [redacted]"],
      ["Binder Error: users' 'SecAfterPlural' in filter", ["SecAfterPlural", "users"], "Error: Binder Error: [redacted]"],
      ["Binder Error: invalid pattern LIKE '%\\' for client '198.51.100.42'", ["198.51.100.42", "%\\"], "Error: Binder Error: invalid pattern LIKE [redacted]"],
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

it("BUG-T5-ITER28-FE-01 & BUG-T5-ITER28-02: redacts internal apostrophe+delimiter URLs, unspaced SQL keyword quotes, name apostrophes, adjacent quotes, and IPv6 literals", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    for (const [tbl, rawUrl, forbidden] of [
      [
        "t_iter28_plural_url",
        "/search?q=users'' profile&cid=SecCid99&ip6=2001:db8::42",
        ["SecCid99", "2001:db8::42", "profile", "users"],
      ],
      [
        "t_iter28_sqli_url",
        "/login?u=admin'';&sid=SecSid88&ip=198.51.100.42",
        ["SecSid88", "198.51.100.42", "admin"],
      ],
    ] as const) {
      await backend.query(`CREATE OR REPLACE TABLE ${tbl} AS SELECT '${rawUrl}' AS url`);
      await expect(
        measureCloudQuery("query.console", () => backend.query(`SELECT CAST(url AS INT) FROM ${tbl}`)),
      ).rejects.toThrow();
      const gotReal = events.at(-1)?.error ?? "";
      expect(gotReal).toContain("Could not convert string [redacted]");
      for (const frag of forbidden) {
        expect(gotReal).not.toContain(frag);
      }
    }

    for (const [msg, forbidden, expectedExact] of [
      [
        "Conversion Error: Could not convert string 'Mozilla/5.0 (users', AliceLaptop)' to INT32",
        ["AliceLaptop", "Mozilla"],
        "Error: Conversion Error: Could not convert string [redacted] to INT32",
      ],
      [
        "Conversion Error: invalid string '/docs/users' SecRowTok99' in column",
        ["SecRowTok99", "users"],
        "Error: Conversion Error: invalid string [redacted] in column",
      ],
      [
        "Parser Error: syntax error near VALUES' SecVal99'",
        ["SecVal99"],
        "Error: Parser Error: syntax error near VALUES[redacted]",
      ],
      [
        "Parser Error: syntax error near SELECT 1 AS' SecretRow99'",
        ["SecretRow99"],
        "Error: Parser Error: syntax error near SELECT 1 AS[redacted]",
      ],
      [
        "Parser Error: syntax error near SELECT's SecretRow99'",
        ["SecretRow99"],
        "Error: Parser Error: syntax error near SELECT[redacted]",
      ],
      [
        "Parser Error: syntax error near WHERE't SecretRow99'",
        ["SecretRow99"],
        "Error: Parser Error: syntax error near WHERE[redacted]",
      ],
      [
        "Binder Error: d'Artagnan filter '.SecDotTok99' and ' SecSpaceTok88'",
        ["SecDotTok99", "SecSpaceTok88", "Artagnan"],
        "Error: Binder Error: [redacted]",
      ],
      [
        "Binder Error: O'Brien filter '?cid=SecQmark77' failed",
        ["SecQmark77", "Brien"],
        "Error: Binder Error: [redacted]?[redacted]",
      ],
      [
        "Parser Error: syntax error in 'foo'='SecAdjUnclosed33",
        ["SecAdjUnclosed33", "foo"],
        "Error: Parser Error: syntax error in [redacted]=[redacted]",
      ],
      [
        "Runtime Error: DB::Exception at 12:34:56 for client 2001:db8:85a3::8a2e:370:7334 or 2001:db8::42 or ::ffff:198.51.100.42",
        ["2001:db8", "8a2e", "198.51.100.42", "12:34:56"],
        "Error: Runtime Error: DB::Exception at [redacted] for client [redacted] or [redacted] or [redacted]",
      ],
    ] as const) {
      const err = new Error(msg);
      const before = events.length;
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events[before + 1]?.error ?? "";
      for (const frag of forbidden) {
        expect(got).not.toContain(frag);
      }
      expect(got).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
});

it("SEC-T5-ITER29-FE-01 & BUG-T5-ITER29-FE-01: redacts multiline DuckDB casts, double-quoted parse errors, multi-quote strings, IPv6 zone/port, and 2200-trial fuzzer", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query(
      "CREATE OR REPLACE TABLE t_iter30_adv AS SELECT " +
        "'/cb?q=users'' SecMultiLine99' || chr(10) || 'line2' AS u_nl, " +
        "'/up?m=go'' to VIP&u=SecToLeak88' || chr(10) || 'tail' AS u_to, " +
        "'/q=admin''; SecMid88''; Tail99' AS s_semi, " +
        "'/search?q=a\" SecDqNl99' || chr(10) || 'line2' AS s_dq_nl",
    );

    const realQueries: readonly [string, readonly string[]][] = [
      ["SELECT CAST(u_nl AS INT) FROM t_iter30_adv", ["SecMultiLine99", "users", "line2"]],
      ["SELECT CAST(u_to AS INT) FROM t_iter30_adv", ["SecToLeak88", "VIP", "tail"]],
      ["SELECT CAST(s_semi AS STRUCT(a INT)) FROM t_iter30_adv", ["SecMid88", "Tail99", "admin"]],
      ["SELECT CAST(s_semi AS INT[]) FROM t_iter30_adv", ["SecMid88", "Tail99", "admin"]],
      ["SELECT CAST(s_dq_nl AS TIMESTAMP) FROM t_iter30_adv", ["SecDqNl99", "search", "line2"]],
      [
        "SELECT regexp_matches('x', '[unclosed_SecRegex66_198.51.100.42_2001:db8::42')",
        ["SecRegex66", "198.51.100.42", "2001:db8", "db8::42"],
      ],
    ];
    for (const [sql, forbidden] of realQueries) {
      await expect(
        measureCloudQuery("query.console", () => backend.query(sql)),
      ).rejects.toThrow();
      const gotReal = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(gotReal, `sql=${sql} got=${gotReal}`).not.toContain(frag);
      }
    }

    const syntheticCases: readonly [string, readonly string[], string][] = [
      [
        "Conversion Error: invalid timestamp field format: \"/search?q=a\" SecDqNl99\nline2\", expected format is (YYYY-MM-DD)",
        ["SecDqNl99", "search", "line2"],
        "Error: Conversion Error: invalid timestamp field format: [redacted], expected format is (YYYY-MM-DD)",
      ],
      [
        "Invalid Input Error: Could not parse string \"/u=a\" SecStrp88\ntail\" according to format specifier \"%Y-%m-%d\"",
        ["SecStrp88", "/u=a"],
        "Error: Invalid Input Error: Could not parse string [redacted] according to format specifier [redacted]",
      ],
      [
        "Invalid Input Error: Malformed JSON at byte 4 of input: unexpected character. Input: \"{\\\"k\\\":\\\"SecJson99\nline2\\\"}\"",
        ["SecJson99", "line2"],
        "Error: Invalid Input Error: Malformed JSON at byte 4 of input: unexpected character. Input: [redacted]",
      ],
      [
        "Conversion Error: invalid string 'O'Brien's users' customers' SecProf99' in column",
        ["SecProf99", "customers", "users", "Brien"],
        "Error: Conversion Error: invalid string [redacted] in column",
      ],
      [
        "Conversion Error: invalid string 'Mozilla/5.0 (a', SecUA88') end' in column",
        ["SecUA88", "Mozilla"],
        "Error: Conversion Error: invalid string [redacted] in column",
      ],
      [
        "Conversion Error: invalid string '/u=a';SecTwoQuotes88'&z=1' in column",
        ["SecTwoQuotes88", "/u=a"],
        "Error: Conversion Error: invalid string [redacted] in column",
      ],
      [
        "Parser Error: syntax error in ('SecIn11','SecIn22','SecUnclosed33",
        ["SecIn11", "SecIn22", "SecUnclosed33"],
        "Error: Parser Error: syntax error in ([redacted],[redacted],[redacted]",
      ],
      [
        "Parser Error: syntax error in 'a'+'SecPlusUnclosed22",
        ["SecPlusUnclosed22"],
        "Error: Parser Error: syntax error in [redacted]+[redacted]",
      ],
      [
        "Binder Error: N'Djamena '.users' failed",
        ["users", "Djamena"],
        "Error: Binder Error: [redacted]",
      ],
      [
        "Binder Error: ce n'est '.users' ici",
        ["users", "ce", "ici"],
        "Error: Binder Error: [redacted]",
      ],
      [
        "Binder Error: B'Elanna and R'lyeh saw 'SecLore77' vs N'SecNat66'",
        ["SecLore77", "SecNat66", "Elanna", "lyeh"],
        "Error: Binder Error: [redacted]",
      ],
      [
        "Runtime Error: DB::Exception at 12:34:56 for fe80::1%eth0 or [fe80::2%en0]:443 or 2001:db8:85a3:0:0:8a2e:370:7334:443 at 09:15:00",
        ["fe80", "eth0", "en0", "2001:db8", "8a2e", "7334", "12:34:56", "09:15:00"],
        "Error: Runtime Error: DB::Exception at [redacted] for [redacted] or [[redacted]]:443 or [redacted] at [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of syntheticCases) {
      const err = new Error(msg);
      const before = events.length;
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events[before + 1]?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg}`).not.toContain(frag);
      }
      expect(got).toBe(expectedExact);
    }

    // Deterministic 2,200-trial Mulberry32 property fuzzer across single/double-quoted & multiline templates:
    let state = 20261123 >>> 0;
    const rand = () => {
      state = (state + 0x6d2b79f5) >>> 0;
      let t = Math.imul(state ^ (state >>> 15), 1 | state);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
    const pick = <T,>(arr: readonly T[]): T => arr[Math.floor(rand() * arr.length)]!;
    const frags = [
      "users' ",
      "O'Brien's ",
      "'; ",
      "', ",
      "') ",
      "' to VIP&",
      "\" ",
      "\";",
      "\nline2_",
      "/cb?x=1&",
      "198.51.100.42",
      "fe80::1%eth0",
      "2001:db8:85a3:0:0:8a2e:370:7334:443",
    ];
    const prosePrefixes = [
      "Binder Error: N'Djamena ",
      "Binder Error: ce n'est ",
      "Binder Error: B'Elanna ",
      "Conversion Error: ",
    ];
    for (let trial = 0; trial < 2200; trial += 1) {
      const sentinel = `SecFz${trial}Z`;
      const parts = [pick(frags), sentinel, pick(frags), pick(frags)];
      const payload = parts.join("");
      const kind = trial % 6;
      const prefix = pick(prosePrefixes);
      let rawMsg: string;
      if (kind === 0) {
        rawMsg = `${prefix}Could not convert string '${payload}' to INT32 when casting from source column u`;
      } else if (kind === 1) {
        rawMsg = `${prefix}Type VARCHAR with value '${payload}' can't be cast to the destination type INTEGER[]`;
      } else if (kind === 2) {
        rawMsg = `${prefix}invalid timestamp field format: "${payload}", expected format is (YYYY-MM-DD)`;
      } else if (kind === 3) {
        rawMsg = `${prefix}Could not parse string "${payload}" according to format specifier "%Y"`;
      } else if (kind === 4) {
        rawMsg = `${prefix}invalid string '${payload}' in column url`;
      } else {
        const unclosedPayload = `u=${payload.replace(/[\n)]/g, "_")}`;
        rawMsg =
          trial % 12 === 5
            ? `Parser Error: syntax error in ('a','${unclosedPayload}`
            : `Parser Error: syntax error in 'a'+'${sentinel}`;
      }
      const err = new Error(rawMsg);
      const before = events.length;
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events[before + 1]?.error ?? "";
      expect(got, `trial=${trial} kind=${kind} raw=${JSON.stringify(rawMsg)} got=${got}`).not.toContain(sentinel);
      expect(got).not.toContain("198.51.100.42");
      expect(got).not.toContain("fe80::1");
      expect(got).not.toContain("eth0");
      expect(got).not.toContain("2001:db8");
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
}, 30000);

it("SEC-T5-ITER30-FE-01: closed-vocabulary O(N) sanitizer blocks DECIMAL/TIME/base64 casts, RE2 unquoted echoes, multi-span quote phase inversion, IPv6 colon/zone/port, 5000-trial fuzzer, and runs <50ms on 8KB", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query(
      "CREATE OR REPLACE TABLE t_iter31_sec AS SELECT "
        + "'/p?\" SecDecEven1 \" SecDecEven2' AS c_dq_even, "
        + "' sec val ' AS secret_col",
    );
    const realDuckRepros: readonly [string, readonly string[]][] = [
      ["SELECT CAST(c_dq_even AS DECIMAL(3,1)) FROM t_iter31_sec", ["SecDecEven1", "SecDecEven2", "c_dq_even"]],
      ["SELECT CAST(c_dq_even AS TIME) FROM t_iter31_sec", ["SecDecEven1", "SecDecEven2", "c_dq_even"]],
      ["SELECT from_base64(c_dq_even) FROM t_iter31_sec", ["SecDecEven1", "SecDecEven2"]],
      ["SELECT regexp_matches('x', '[u=Alice SecBrackSpace99')", ["Alice", "SecBrackSpace99"]],
      ["SELECT regexp_matches('x', '(unclosed_SecParen99')", ["SecParen99", "unclosed"]],
      ["SELECT regexp_matches('x', '(u=Alice&s=Sec99')", ["Alice", "Sec99"]],
      ["SELECT regexp_matches('x', '(2001:db8::42:end')", ["2001:db8", "db8::42"]],
      ["SELECT regexp_matches('x', '(fe80::1%eth0:end')", ["fe80", "eth0"]],
      ["SELECT regexp_matches('x', '(?P<bad' || secret_col) FROM t_iter31_sec", ["sec val", "?P<"]],
      ["SELECT regexp_matches('x', '[z-a]\\p{Alice_Sec77}')", ["Alice", "Sec77", "z-a"]],
      ["SELECT * FROM no_such_tbl_SecTbl", ["no_such_tbl", "SecTbl"]],
      ["SELECT no_such_fn_SecFn(1)", ["no_such_fn", "secfn"]],
      ["SELECT CAST(999999999999999 AS TINYINT)", ["999999999999999"]],
      ["SELECT 9223372036854775807 + 1", ["9223372036854775807"]],
    ];
    for (const [sql, forbidden] of realDuckRepros) {
      await expect(measureCloudQuery("query.console", () => backend.query(sql))).rejects.toThrow();
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `sql=${sql} got=${got}`).not.toContain(frag);
      }
    }

    const syntheticRepros: readonly [string, readonly string[]][] = [
      ["Invalid Input Error: Failed to cast value: '/p?'; SecWordBeta 'tail'", ["SecWordBeta", "/p?", "tail"]],
      ["Error: span 'span1' failed: value ' sec leak ' rejected", ["span1", "sec leak"]],
      ["Error: key 'sec_a' failed: value ' sec leak ' rejected", ["sec_a", "sec leak"]],
      ["Binder Error: filter 'sec_a' could not be matched against value ' sec leak ' in query", ["sec_a", "sec leak"]],
      ["Binder Error: column 'sec_a' not found!\nCandidate bindings: ' sec leak '", ["sec_a", "sec leak", "Candidate"]],
      ["Catalog Error: Scalar Function with name 'f' does not exist!\nDid you mean ' secret fn '?", ["secret fn", "Did you mean"]],
      ["Binder Error: column 'sec_a' not found\nCandidate bindings: 'pub_b", ["sec_a", "pub_b", "Candidate"]],
      ["Invalid Input Error: missing ]: [unclosed SecSpace99", ["SecSpace99", "unclosed"]],
      ["Invalid Input Error: missing ): (unclosed SecParenSpace99", ["SecParenSpace99", "unclosed"]],
      ["Invalid Input Error: invalid named capture group: (?P<SecGroup99", ["SecGroup99", "?P<"]],
      ["Invalid Input Error: invalid character class range: \\p{SecRange99}", ["SecRange99"]],
      ["IO Error: Could not connect to 2001:db8::42: connection refused", ["2001:db8", "db8::42"]],
      ["Runtime Error: failed for fe80::1%eth0: permission denied", ["fe80", "eth0"]],
      ["Runtime Error: failed for 2001:db8::1:443: connection reset", ["2001:db8", "443"]],
      ["Invalid Input Error: failed processing '/q=admin' Select User 'SecProseMid99' end", ["admin", "Select", "User", "SecProseMid99"]],
      ["Invalid Input Error: bad header \"Cookie: a=\"SecDqMid99\"; b=1\" in request", ["Cookie", "SecDqMid99", "b=1"]],
      ["Binder Error: can't parse E'secretword\\'SecEsc77' in filter", ["secretword", "SecEsc77"]],
      ["Parser Error: unterminated string near can't 'secretword", ["secretword"]],
      ["Parser Error: invalid literal n'secret word'", ["secret", "word"]],
      ["Parser Error: unterminated quoted string at or near \"'foo\" secretword \"bar\"", ["secretword", "foo", "bar"]],
      ["IO Error: No files found that match the pattern \"/p/\" secretword \"/a.parquet\"", ["secretword", "parquet"]],
      ["Binder Error: JSON path error near '[secretword' in query", ["secretword"]],
    ];
    for (const [msg, forbidden] of syntheticRepros) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
    }

    // Deterministic 5,000-trial Mulberry32 property fuzzer across DECIMAL/TIME/base64/RE2/multi-span/newline/IPv6/Cookie frames:
    let fzState = 20261131 >>> 0;
    const fzRand = () => {
      fzState = (fzState + 0x6d2b79f5) >>> 0;
      let t = Math.imul(fzState ^ (fzState >>> 15), 1 | fzState);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
    const fzPick = <T,>(arr: readonly T[]): T => arr[Math.floor(fzRand() * arr.length)]!;
    const fzNoise = [
      "\" ",
      "\" \"",
      "'; ",
      "' failed: '",
      "\nCandidate bindings: '",
      " 2001:db8::42: ",
      " fe80::1%eth0: ",
      " 2001:db8::1:443 ",
      " O'Brien ",
      " can't ",
    ];
    for (let trial = 0; trial < 5000; trial += 1) {
      const sentinel = `SecIter31Fz${trial}Q`;
      const body = `${fzPick(fzNoise)}${sentinel}${fzPick(fzNoise)}`;
      const kind = trial % 10;
      let rawMsg: string;
      if (kind === 0) {
        rawMsg = `Conversion Error: Could not convert string "${body}" to DECIMAL(3,1) when casting from source column c_${sentinel}`;
      } else if (kind === 1) {
        rawMsg = `Conversion Error: time field value out of range: "${body}", expected format is ([YYYY-MM-DD ]HH:MM:SS[.MS]) when casting from source column c_${sentinel}`;
      } else if (kind === 2) {
        rawMsg = `Conversion Error: Could not decode string "${body}" as base64: length must be a multiple of 4`;
      } else if (kind === 3) {
        rawMsg = `Invalid Input Error: missing ): (${body}`;
      } else if (kind === 4) {
        rawMsg = `Invalid Input Error: invalid character class range: \\p{${sentinel}} ${body}`;
      } else if (kind === 5) {
        rawMsg = `Binder Error: span 'a' failed: value ' ${sentinel} ' rejected\nCandidate bindings: '${sentinel}_b`;
      } else if (kind === 6) {
        rawMsg = `Invalid Input Error: bad header "Cookie: a=\\"${sentinel}\\"; b=1" in request`;
      } else if (kind === 7) {
        rawMsg = `IO Error: connect to 2001:db8::42: ${sentinel} or fe80::1%eth0: failed`;
      } else if (kind === 8) {
        rawMsg = `Catalog Error: Table with name tbl_${sentinel} does not exist!`;
      } else {
        rawMsg = `Invalid Input Error: Failed to cast value: '/p?'; ${sentinel} 'tail'`;
      }
      const err = new Error(rawMsg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      expect(got, `trial=${trial} kind=${kind} got=${got}`).not.toContain(sentinel);
      expect(got).not.toContain("2001:db8");
      expect(got).not.toContain("fe80::1");
      expect(got).not.toContain("eth0");
    }

    const eightKbErr = new Error("Error: " + "'a' ".repeat(2000));
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw eightKbErr; })).rejects.toBe(eightKbErr);
    const t0 = performance.now();
    for (let k = 0; k < 10; k += 1) {
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw eightKbErr; })).rejects.toBe(eightKbErr);
    }
    expect((performance.now() - t0) / 10).toBeLessThan(50);
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
}, 45000);

it("SEC-T5-ITER31-FE-01: closed DIAGNOSTIC_VOCAB, anchored Stage-1 templates, compound/Bignum/array/ENUM/Binder/Duplicate/strptime frames, no safeLabel/clock leaks, and <20ms 60KB O(N)", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query("CREATE TABLE t_iter32 (id INT PRIMARY KEY, u VARCHAR, u_q VARCHAR, \"bad\"\"secret\" VARCHAR)");
    await backend.query("INSERT INTO t_iter32 VALUES (1, 'alice private email token', 'alice'' to INT32 bob private', 'x')");
    await backend.query("CREATE TABLE t_iter32_pk (k VARCHAR PRIMARY KEY, v VARCHAR)");
    await backend.query("INSERT INTO t_iter32_pk VALUES ('alice violates bob customer pii', 'a')");
    await backend.query("CREATE TYPE iter32_enum AS ENUM ('canaryenum')");

    const realDuckCases: readonly [string, readonly string[], string?][] = [
      ["SELECT CAST(u AS BIGNUM) FROM t_iter32", ["alice", "private", "email", "token"], "Bignum"],
      ["SELECT CAST(u_q AS BIGNUM) FROM t_iter32", ["alice", "bob", "private", "INT32"], "Bignum"],
      ["SELECT CAST(u AS VARINT) FROM t_iter32", ["alice", "private", "email", "token"], "Bignum"],
      ["SELECT CAST(u AS INTEGER[3]) FROM t_iter32", ["alice", "private", "email", "token"], "INTEGER[3]"],
      ["SELECT CAST(u AS STRUCT(secretfield INT)) FROM t_iter32", ["alice", "private", "secretfield"], "STRUCT([redacted])"],
      ['SELECT CAST(u AS STRUCT("a)secretfield" INT)) FROM t_iter32', ["alice", "private", "secretfield"], "STRUCT([redacted])"],
      [
        "SELECT CAST(u AS UNION(secretmember INT, canarymember DOUBLE)) FROM t_iter32",
        ["secretmember", "canarymember", "alice"],
        "UNION([redacted])",
      ],
      ["SELECT CAST(1 AS STRUCT(secretfield INT))", ["secretfield"], "STRUCT([redacted])"],
      ["SELECT CAST(u AS MAP(VARCHAR, INT)) FROM t_iter32", ["alice", "private"], "MAP([redacted])"],
      ["SELECT abs(CAST('canaryenum' AS iter32_enum))", ["canaryenum"]],
      [
        "SELECT regexp_matches('x', '[' || 'alice bob private at or near x')",
        ["alice", "bob", "private", "at or near"],
      ],
      [
        "SELECT regexp_matches('x', '(?P<bad alice bob Could not convert string a to INT(secret word)')",
        ["alice", "bob", "secret", "word", "Could not convert"],
      ],
      ["SELECT strptime('x', 'canaryfmt%')", ["canaryfmt"], "Failed to parse format specifier [redacted]"],
      ["SELECT strftime(DATE '2025-01-01', 'canaryfmt%')", ["canaryfmt"], "Failed to parse format specifier [redacted]"],
      ["SELECT strptime('x', '%1canaryfmt')", ["canaryfmt"], "Failed to parse format specifier [redacted]"],
      ["SELECT error(u) FROM t_iter32", ["alice", "private", "email", "token"]],
      ['SELECT "missing""secret" FROM t_iter32', ["missing", "secret"], "Referenced column [redacted] not found in FROM clause!"],
      ['SELECT t_iter32."missing""secret" FROM t_iter32', ["t_iter32", "missing", "secret"], "Table [redacted] does not have a column named [redacted]"],
      [
        'SELECT "a""secret" FROM (SELECT 1 AS "a""secret") x, (SELECT 2 AS "a""secret") y',
        ["secret"],
        "Ambiguous reference to column name [redacted]",
      ],
      ['SELECT "bad""secret", count(*) FROM t_iter32', ["bad", "secret"], "must appear in the GROUP BY clause"],
      [
        "INSERT INTO t_iter32_pk VALUES ('alice violates bob customer pii', 'b')",
        ["alice", "bob", "customer", "pii"],
        "Duplicate key [redacted] violates primary key constraint.",
      ],
    ];
    for (const [sql, forbidden, requiredSubstr] of realDuckCases) {
      await expect(measureCloudQuery("query.console", () => backend.query(sql))).rejects.toThrow();
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `sql=${sql} got=${got}`).not.toContain(frag);
      }
      if (requiredSubstr) {
        expect(got, `sql=${sql} got=${got}`).toContain(requiredSubstr);
      }
    }

    const syntheticWave31: readonly [string, readonly string[], string?][] = [
      // Closed DIAGNOSTIC_VOCAB blocks arbitrary lowercase row tokens:
      [
        "Invalid Input Error: alice bob private secretfield canaryalpha canaryfmt mysecretword privateemail secretmember canaryenum",
        ["alice", "bob", "private", "secretfield", "canaryalpha", "canaryfmt", "mysecretword", "privateemail", "secretmember", "canaryenum"],
        "Error: Invalid Input Error: [redacted]",
      ],
      // Mixed-case apostrophes and unterminated hex-prefix literals are redacted:
      ["Binder Error: Secret'Token failed", ["Secret", "Token"], "Error: Binder Error: [redacted]"],
      ["Binder Error: O'Connor failed", ["Connor"], "Error: Binder Error: [redacted]"],
      ["Parser Error: X'deadbeef", ["deadbeef", "X'"], "Error: Parser Error: [redacted]"],
      ["Parser Error: x'deadbeef", ["deadbeef", "x'"], "Error: Parser Error: [redacted]"],
      // Explicit closed contractions remain intact:
      [
        "Binder Error: can't doesn't couldn't didn't isn't wasn't won't don't haven't hasn't aren't weren't",
        [],
        "Error: Binder Error: can't doesn't couldn't didn't isn't wasn't won't don't haven't hasn't aren't weren't",
      ],
      // Quote before colon does NOT bypass redactPostColonDiagnosticTail:
      [
        "Invalid Input Error: bad 'x' details: canaryalpha",
        ["canaryalpha"],
        "Error: Invalid Input Error: bad [redacted] details: [redacted]",
      ],
      [
        "Invalid Input Error: bad 'x' details: column value error",
        ["column", "value", "error"],
        "Error: Invalid Input Error: bad [redacted] details: [redacted]",
      ],
      // Could not convert string never backtracks into untrusted row text and handles Varint / ENUM / unknown types:
      [
        "Conversion Error: Could not convert string 'alice to INT32 bob' to CustomSecretType",
        ["alice", "bob", "INT32", "CustomSecretType"],
        "Error: Conversion Error: Could not convert string [redacted] to [redacted]",
      ],
      [
        "Conversion Error: Could not convert string 'alice' to Varint",
        ["alice"],
        "Error: Conversion Error: Could not convert string [redacted] to Varint",
      ],
      [
        "Conversion Error: Could not convert string 'alice' to ENUM('canaryenum')",
        ["alice", "canaryenum"],
        "Error: Conversion Error: Could not convert string [redacted] to ENUM([redacted])",
      ],
      // Unquoted GROUP BY column identifier is redacted:
      [
        "Binder Error: column secretcol must appear in the GROUP BY clause or must be part of an aggregate function.",
        ["secretcol"],
        "Error: Binder Error: column [redacted] must appear in the GROUP BY clause or must be part of an aggregate function.",
      ],
      // Duplicate key redacts custom constraint text / identifiers after violates:
      [
        "Constraint Error: Duplicate key k violates bob customer pii",
        ["bob", "customer", "pii"],
        "Error: Constraint Error: Duplicate key [redacted] violates [redacted]",
      ],
      [
        'Constraint Error: Duplicate key k violates unique constraint "secret_idx"',
        ["secret_idx"],
        "Error: Constraint Error: Duplicate key [redacted] violates [redacted]",
      ],
      // Stage-2B never echoes arbitrary secret key labels (tenant42_api_key / acme99_token):
      [
        "Runtime Error: tenant42_api_key=supersecret acme99_token: mysecretval corp7_secret=abc",
        ["tenant42", "api_key", "supersecret", "acme99", "token", "mysecretval", "corp7", "secret"],
        "Error: Runtime Error: [redacted]",
      ],
      // Raw clock timestamps are redacted (tryScanClockTime removed):
      [
        "Runtime Error: query failed at 14:25:36.123456 and 09:05:01",
        ["14:25:36", "123456", "09:05:01"],
        "Error: Runtime Error: query failed at [redacted] and [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of syntheticWave31) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      if (expectedExact) {
        expect(got, `msg=${msg}`).toBe(expectedExact);
      }
    }

    // 60 KB O(N^2) regression check: must stay strictly O(N) (< 20ms) and redact cleanly:
    const dos60Kb = new Error("Conversion Error: Could not convert string '" + " to INT(".repeat(7500));
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw dos60Kb; })).rejects.toBe(dos60Kb);
    expect(events.at(-1)?.error).toBe("Error: Conversion Error: Could not convert string [redacted] to [redacted]");
    const tDos0 = performance.now();
    for (let k = 0; k < 5; k += 1) {
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw dos60Kb; })).rejects.toBe(dos60Kb);
    }
    expect((performance.now() - tDos0) / 5).toBeLessThan(20);
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
}, 45000);

it("SEC-T5-ITER32-FE-01: redacts real DuckDB numeric cast/overflow/range row values and Stage-2B auth=Scheme credentials", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query("CREATE TABLE t_num (pin INT, neg INT, dec_v DOUBLE, txt VARCHAR)");
    await backend.query("INSERT INTO t_num VALUES (847291, -847291, 847.29, '847291' || chr(10) || 'line2')");

    const realDuckNumericCases: readonly [string, readonly string[], string][] = [
      [
        "SELECT CAST(pin AS DECIMAL(4, 0)) FROM t_num",
        ["847291", "pin"],
        "Error: Conversion Error: Could not cast value [redacted] to DECIMAL(4,0) when casting from source column [redacted]",
      ],
      [
        "SELECT CAST(code AS DECIMAL(4, 0)) FROM (SELECT 847291 AS code)",
        ["847291", "code"],
        "Error: Conversion Error: Could not cast value [redacted] to DECIMAL(4,0) when casting from source column [redacted]",
      ],
      [
        "SELECT CAST(neg AS DECIMAL(4, 0)) FROM t_num",
        ["847291", "neg"],
        "Error: Conversion Error: Could not cast value [redacted] to DECIMAL(4,0) when casting from source column [redacted]",
      ],
      [
        "SELECT CAST(dec_v AS DECIMAL(4, 2)) FROM t_num",
        ["847.29", "847", "dec_v"],
        "Error: Conversion Error: Could not cast value [redacted] to DECIMAL(4,2) when casting from source column [redacted]",
      ],
      [
        "SELECT CAST(txt AS DECIMAL(4, 0)) FROM t_num",
        ["847291", "line2", "txt"],
        "Error: Conversion Error: Could not convert string [redacted] to DECIMAL(4,0) when casting from source column [redacted]",
      ],
      [
        "SELECT CAST(999999 AS DECIMAL(4, 0))",
        ["999999"],
        "Error: Conversion Error: Could not cast value [redacted] to DECIMAL(4,0)",
      ],
      [
        "SELECT CAST(CAST(847291 AS DECIMAL(10, 0)) AS TINYINT)",
        ["847291"],
        "Error: Conversion Error: Failed to cast decimal value [redacted] to type INT8",
      ],
      [
        "SELECT CAST(CAST(847291 AS DECIMAL(10, 0)) AS DECIMAL(4, 0))",
        ["847291"],
        "Error: Conversion Error: Casting value [redacted] to type DECIMAL(4,0) failed: value is out of range!",
      ],
      [
        "SELECT CAST(1 AS INT) << pin FROM t_num",
        ["847291"],
        "Error: Out of Range Error: Left-shift value [redacted] is out of range",
      ],
      [
        "SELECT to_millennia(pin) FROM t_num",
        ["847291"],
        "Error: Out of Range Error: Interval value [redacted] out of range",
      ],
      [
        "SELECT CAST(-32768 AS SMALLINT) // CAST(-1 AS SMALLINT)",
        ["32768"],
        "Error: Out of Range Error: Overflow in division of [redacted]",
      ],
    ];
    for (const [sql, forbidden, expectedExact] of realDuckNumericCases) {
      await expect(measureCloudQuery("query.console", () => backend.query(sql))).rejects.toThrow();
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `sql=${sql} got=${got}`).not.toContain(frag);
      }
      expect(got, `sql=${sql}`).toBe(expectedExact);
    }

    const syntheticWave32: readonly [string, readonly string[], string][] = [
      [
        "Conversion Error: Could not cast value 847291 to DECIMAL(4,0) when casting from source column pin",
        ["847291", "pin"],
        "Error: Conversion Error: Could not cast value [redacted] to DECIMAL(4,0) when casting from source column [redacted]",
      ],
      [
        "Conversion Error: Could not cast value 847291 to CustomSecretType",
        ["847291", "CustomSecretType"],
        "Error: Conversion Error: Could not cast value [redacted] to [redacted]",
      ],
      [
        "Conversion Error: Could not cast value 'bad\n847291' to INT32",
        ["847291", "bad"],
        "Error: Conversion Error: Could not cast value [redacted] to INT32",
      ],
      [
        "Conversion Error: Could not convert value 847291 to DECIMAL(4,0) when casting from source column code",
        ["847291", "code"],
        "Error: Conversion Error: Could not convert value [redacted] to DECIMAL(4,0) when casting from source column [redacted]",
      ],
      [
        "Conversion Error: Could not cast 847291 to DECIMAL(4,0)",
        ["847291"],
        "Error: Conversion Error: Could not cast [redacted] to DECIMAL(4,0)",
      ],
      [
        "Conversion Error: Could not convert bignum '847291' to Double",
        ["847291"],
        "Error: Conversion Error: Could not convert bignum [redacted] to DOUBLE",
      ],
      [
        "Conversion Error: Could not cast string to double: \"847291\"",
        ["847291"],
        "Error: Conversion Error: Could not cast string to DOUBLE: [redacted]",
      ],
      [
        "Conversion Error: Could not convert DATE (847291) to microseconds",
        ["847291"],
        "Error: Conversion Error: Could not convert DATE ([redacted])",
      ],
      [
        "Out of Range Error: Overflow in division of 847291 / 0",
        ["847291"],
        "Error: Out of Range Error: Overflow in division of [redacted]",
      ],
      [
        "Out of Range Error: Overflow in HUGEINT addition: 170141183460469231731687303715884105727 + 847291",
        ["847291", "170141"],
        "Error: Out of Range Error: Overflow in HUGEINT addition: [redacted]",
      ],
      [
        "Out of Range Error: Overflow in addition of INT128 (847291\n+ 1)",
        ["847291"],
        "Error: Out of Range Error: Overflow in addition of INT128 ([redacted])",
      ],
      [
        "Out of Range Error: Timestamp microseconds out of range: 847291",
        ["847291"],
        "Error: Out of Range Error: [redacted] out of range: [redacted]",
      ],
      [
        "Out of Range Error: input value 847291 is out of range for numeric function cotangent",
        ["847291", "cotangent"],
        "Error: Out of Range Error: input value [redacted] is out of range",
      ],
      [
        "Out of Range Error: Failed to cast value: 847291",
        ["847291"],
        "Error: Out of Range Error: Failed to cast value: [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of syntheticWave32) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
}, 45000);

it("SEC-T5-ITER32-FE-02: redacts Stage-2B auth-scheme credentials after = and : separators without swallowing [url]", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const authCases: readonly [string, readonly string[], string][] = [
      [
        "Runtime Error: authorization=Bearer 847291 rejected",
        ["847291", "Bearer"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization=BEARER 847291 failed",
        ["847291", "BEARER"],
        "Error: Runtime Error: [redacted] failed",
      ],
      [
        "Runtime Error: api_auth=Basic 123456 rejected",
        ["123456", "Basic"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: x_token=Digest 654321 rejected",
        ["654321", "Digest"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: auth=Token 987654 rejected",
        ["987654", "Token"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: authorization:Bearer 847291 rejected",
        ["847291", "Bearer"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: authorization= Bearer 847291 rejected",
        ["847291", "Bearer"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: token= 847291 rejected",
        ["847291"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: authorization = Bearer 847291 rejected",
        ["847291", "Bearer"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: authorization=Bearer https://secret.example/path rejected",
        ["secret.example", "Bearer"],
        "Error: Runtime Error: [redacted] [url] rejected",
      ],
      [
        "Runtime Error: authorization=Bearer '847291' rejected",
        ["847291", "Bearer"],
        "Error: Runtime Error: [redacted] rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of authCases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER32-FE-03: caps RFC 3986 URL-scheme lookahead so valid schemes redact to [url] while 64 KiB DoS inputs finish in <5ms", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const dosInputs = [
      "a+".repeat(32768),
      "a+".repeat(164) + "a".repeat(65000),
    ];
    for (const payload of dosInputs) {
      const err = new Error(payload);
      // Warm up once before measuring 5-trial median & max:
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const trials: number[] = [];
      for (let k = 0; k < 5; k += 1) {
        const t0 = performance.now();
        await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
        trials.push(performance.now() - t0);
      }
      const sorted = [...trials].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)]!;
      const max = sorted.at(-1)!;
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(5 * V8_COV_SCALE);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(150);
    }

    const validSchemeCases: readonly [string, readonly string[], string][] = [
      [
        "IO Error: failed fetching https://user:pass@secret.example/a.parquet",
        ["secret.example", "pass"],
        "Error: IO Error: failed fetching [url]",
      ],
      [
        "IO Error: failed reading s3://private-bucket/prefix/file.parquet",
        ["private-bucket", "file.parquet"],
        "Error: IO Error: failed reading [url]",
      ],
      [
        "IO Error: connect postgres+asyncpg://admin:secret@db.internal:5432/prod",
        ["postgres", "db.internal", "secret"],
        "Error: IO Error: connect [url]",
      ],
      [
        "IO Error: open custom-scheme.v1+test://internal.host/resource?tok=847291",
        ["custom-scheme", "internal.host", "847291"],
        "Error: IO Error: open [url]",
      ],
      [
        `IO Error: open ${"a".repeat(32)}://secret.host/path`,
        ["secret.host", "path"],
        "Error: IO Error: open [url]",
      ],
      [
        `IO Error: open ${"a".repeat(33)}://secret.host/path`,
        ["secret.host", "path"],
        "Error: IO Error: open [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of validSchemeCases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);

it("SEC-T5-ITER34-FE-01: removes blanket numeric allowlist to redact real DuckDB numeric row leaks while preserving diagnostic numbers and Left-shift", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query("CREATE TABLE t_p (pin INT, small_pin INT)");
    await backend.query("INSERT INTO t_p VALUES (847291, 42)");

    const realDuckNumericLeaks: readonly [string, readonly string[], string?][] = [
      [
        "SELECT get_bit('0101'::BIT, pin) FROM t_p",
        ["847291"],
        "Error: Out of Range Error: bit index [redacted] out of valid range ([redacted])",
      ],
      [
        "SELECT set_bit('0101'::BIT, pin, 1) FROM t_p",
        ["847291"],
        "Error: Out of Range Error: bit index [redacted] out of valid range ([redacted])",
      ],
      [
        "SELECT get_bit('0101'::BIT, small_pin) FROM t_p",
        ["42"],
        "Error: Out of Range Error: bit index [redacted] out of valid range ([redacted])",
      ],
      [
        "SELECT regexp_extract_all('abc', '(a)', pin) FROM t_p",
        ["847291"],
      ],
      [
        "SELECT error('code ' || pin) FROM t_p",
        ["847291"],
        "Error: Invalid Input Error: code [redacted]",
      ],
      [
        "SELECT error('index ' || small_pin) FROM t_p",
        ["42"],
        "Error: Invalid Input Error: index [redacted]",
      ],
      [
        "SELECT CAST(1 AS INT) << pin FROM t_p",
        ["847291"],
        "Error: Out of Range Error: Left-shift value [redacted] is out of range",
      ],
    ];
    for (const [sql, forbidden, expectedExact] of realDuckNumericLeaks) {
      await expect(measureCloudQuery("query.console", () => backend.query(sql))).rejects.toThrow();
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `sql=${sql} got=${got}`).not.toContain(frag);
      }
      if (expectedExact) {
        expect(got, `sql=${sql}`).toBe(expectedExact);
      }
    }

    const numericCases: readonly [string, readonly string[], string][] = [
      // Arbitrary row-value numbers must be redacted:
      [
        "Out of Range Error: bit index 847291 out of valid range (0..3)",
        ["847291"],
        "Error: Out of Range Error: bit index [redacted] out of valid range ([redacted])",
      ],
      [
        "Invalid Input Error: Pattern has 1 groups. Cannot access group 847291",
        ["847291"],
        "Error: Invalid Input Error: [redacted] has [redacted]. Cannot access group [redacted]",
      ],
      [
        "Runtime Error: failed for code 123456 and index 42",
        ["123456", "42"],
        "Error: Runtime Error: failed for code [redacted] and index [redacted]",
      ],
      // Left-shift clause-start word preserved while redacting value:
      [
        "Out of Range Error: Left-shift value 847291 is out of range",
        ["847291"],
        "Error: Out of Range Error: Left-shift value [redacted] is out of range",
      ],
      // Genuine diagnostic numbers stay preserved:
      [
        "IO Error: upstream HTTP 503 timeout after 30s",
        [],
        "Error: IO Error: upstream HTTP 503 timeout after 30s",
      ],
      [
        "IO Error: HTTP 404 response for request",
        [],
        "Error: IO Error: HTTP 404 response for request",
      ],
      [
        "Runtime Error: buffer exceeded 1024 bytes (16 MB limit)",
        [],
        "Error: Runtime Error: buffer exceeded 1024 bytes (16 MB limit)",
      ],
      [
        "Conversion Error: Could not cast value 847291 to DECIMAL(18,3)",
        ["847291"],
        "Error: Conversion Error: Could not cast value [redacted] to DECIMAL(18,3)",
      ],
      [
        "Parser Error: syntax error at line 1: unexpected input",
        [],
        "Error: Parser Error: syntax error at line 1: unexpected input",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of numericCases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
}, 45000);

it("SEC-T5-ITER34-FE-02: redacts Stage-2A/2B delimited/spaced secrets, spaced SQL key assignments, Basic/Digest/Cookie headers, JSON secret pairs, suffix stems, and stays <25ms on 64 KiB", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const wave34Cases: readonly [string, readonly string[], string][] = [
      // Finding 2: delimiter immediately after = / :
      ["Runtime Error: token=(847291) rejected", ["847291", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token=[847291] rejected", ["847291", "token"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token={847291} rejected", ["847291", "token"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token:(847291) rejected", ["847291", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: password=(847291) rejected", ["847291", "password", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: api_key=(847291) rejected", ["847291", "api_key", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: cookie=(847291) rejected", ["847291", "cookie", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token=(server) rejected", ["server", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token=[cache] rejected", ["cache", "token"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token={server} rejected", ["server", "token"], "Error: Runtime Error: [redacted] rejected"],

      // Finding 3: spaced parenthesized/bracketed/braced secret values
      ["Runtime Error: token: ( 847291 ) rejected", ["847291", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token = ( 847291 ) rejected", ["847291", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token=Bearer ( 847291 ) rejected", ["847291", "Bearer", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token: ( server cache ) rejected", ["server", "cache", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token = [ ( server ) ] rejected", ["server", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token={ server cache } rejected", ["server", "cache", "token"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token=Bearer ( server cache ) rejected", ["server", "cache", "Bearer", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token=Bearer(847291) rejected", ["847291", "Bearer", "token", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token = ( server 847291", ["server", "847291", "token"], "Error: Runtime Error: [redacted]"],

      // Finding 4: SAFE_KEY_PREFIX_WORDS with whitespace before = / :
      ["Runtime Error: primary key = 847291 violated", ["847291", "key", "="], "Error: Runtime Error: primary [redacted]"],
      ["Runtime Error: map key : 847291 failed", ["847291", "key"], "Error: Runtime Error: map [redacted] failed"],
      ["Runtime Error: unique key = 847291 failed", ["847291", "key", "="], "Error: Runtime Error: unique [redacted] failed"],
      ["Runtime Error: foreign key = 847291 failed", ["847291", "key", "="], "Error: Runtime Error: foreign [redacted] failed"],
      ["Runtime Error: partition key : 847291 failed", ["847291", "key"], "Error: Runtime Error: partition [redacted] failed"],
      ["Runtime Error: primary key = cache failed", ["cache", "key", "="], "Error: Runtime Error: primary [redacted] failed"],
      ["Runtime Error: primary key = ( cache ) failed", ["cache", "key", "=", "("], "Error: Runtime Error: primary [redacted] failed"],
      ["Runtime Error: map key : server failed", ["server", "key"], "Error: Runtime Error: map [redacted] failed"],
      ["Constraint Error: primary key constraint violation", [], "Error: Constraint Error: primary key constraint violation"],

      // Finding 5: Bare Basic/Digest auth schemes & multi-parameter Authorization:/Cookie: headers
      ["Runtime Error: Basic 847291 rejected", ["847291", "Basic"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Basic server rejected", ["server", "Basic"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Basic(server) rejected", ["server", "Basic", "("], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Digest 847291 rejected", ["847291", "Digest"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Digest cache rejected", ["cache", "Digest"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Authorization: Basic YWRtaW46c2VjcmV= rejected", ["YWRtaW46c2VjcmV", "Basic"], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Authorization: Digest username=alice, response = 847291", ["alice", "response", "847291", "Digest", "="], "Error: Runtime Error: [redacted]"],
      ["Runtime Error: Authorization: Digest username=client, response = cache rejected", ["client", "response", "cache", "Digest", "="], "Error: Runtime Error: [redacted] rejected"],
      ['Runtime Error: Authorization: Digest username="alice", nonce="999", response = ( 847291 ) rejected', ["alice", "999", "response", "847291", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Cookie: sid=tok_sentinel_999; pin = 847291", ["tok_sentinel_999", "sid", "pin", "847291", "="], "Error: Runtime Error: [redacted]"],
      ["Runtime Error: Cookie: sid=tok_sentinel_999; mode = cache rejected", ["tok_sentinel_999", "mode", "cache", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Cookie:sid = cache; mode = server rejected", ["cache", "server", "mode", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: Set-Cookie: sid=tok_sentinel_999; HttpOnly; Secure; pin = cache rejected", ["tok_sentinel_999", "HttpOnly", "Secure", "pin", "cache", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: basic syntax error", [], "Error: Runtime Error: [redacted] syntax error"],

      // Finding 6: JSON secret keys + values redacted before Stage 2A strips key names
      ['Runtime Error: {"secret": 847291, "token": 123456}', ["secret", "847291", "token", "123456"], "Error: Runtime Error: [redacted], [redacted]"],
      ['Runtime Error: {"a": "", "secret": server}', ["secret", "server"], "Error: Runtime Error: [redacted]: [redacted], [redacted]"],
      ['Runtime Error: {"secret": true, "token": false, "api_key": null}', ["secret", "true", "token", "false", "api_key", "null"], "Error: Runtime Error: [redacted], [redacted], [redacted]"],
      ['Runtime Error: {"session_id": [1, "server", true], "auth_code": {"nested": true, "cache": 847291}} rejected', ["session_id", "server", "true", "auth_code", "nested", "cache", "847291"], "Error: Runtime Error: [redacted], [redacted] rejected"],
      ['Runtime Error: {"password_hash": "val\\"847291", "pin": server} rejected', ["password_hash", "847291", "pin", "server"], "Error: Runtime Error: [redacted]"],
      ["Runtime Error: {'secret': 847291, 'token': cache} rejected", ["secret", "847291", "token", "cache"], "Error: Runtime Error: [redacted], [redacted] rejected"],

      // Finding 7: Suffix-qualified secret stems & session
      ["Runtime Error: session = 847291 expired", ["847291", "session", "="], "Error: Runtime Error: [redacted] expired"],
      ["Runtime Error: session = cache expired", ["cache", "session", "="], "Error: Runtime Error: [redacted] expired"],
      ["Runtime Error: session_id = 847291 expired", ["847291", "session_id", "="], "Error: Runtime Error: [redacted] expired"],
      ["Runtime Error: session_id = server expired", ["server", "session_id", "="], "Error: Runtime Error: [redacted] expired"],
      ["Runtime Error: token_id = 847291 rejected", ["847291", "token_id", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: token_id = valid rejected", ["valid", "token_id", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: password_hash = 847291 rejected", ["847291", "password_hash", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: password_hash = cache rejected", ["cache", "password_hash", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: auth_code = 847291 rejected", ["847291", "auth_code", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: auth_code = client rejected", ["client", "auth_code", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: user_pwd_v2 = cache rejected", ["cache", "user_pwd_v2", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: client_apikey_v1 = server rejected", ["server", "client_apikey_v1", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: jwt_payload = cache rejected", ["cache", "jwt_payload", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: csrf_nonce_id = valid rejected", ["valid", "csrf_nonce_id", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: mfa_otp_code = value rejected", ["value", "mfa_otp_code", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: card_pin_num = cache rejected", ["cache", "card_pin_num", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: req_hmac_val = server rejected", ["server", "req_hmac_val", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: signature_b64 = client rejected", ["client", "signature_b64", "="], "Error: Runtime Error: [redacted] rejected"],
      ["Runtime Error: session expired", [], "Error: Runtime Error: session expired"],
    ];
    for (const [msg, forbidden, expectedExact] of wave34Cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }

    // 64 KiB O(N) performance guards (< 25 ms per call):
    const perfPayloads = [
      "Runtime Error: token=" + "(".repeat(65000),
      "Runtime Error: token = ( " + "a ".repeat(32000),
      'Runtime Error: {"secret": ' + "{".repeat(65000),
      'Runtime Error: ' + '{"secret":1},'.repeat(5000),
      "Runtime Error: Cookie: " + "a=1; ".repeat(13000),
      "Runtime Error: Authorization: Digest " + "a=1, ".repeat(13000),
      "Runtime Error: primary key" + " ".repeat(65000) + "= 847291",
    ];
    for (const payload of perfPayloads) {
      const err = new Error(payload);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const trials: number[] = [];
      for (let k = 0; k < 5; k += 1) {
        const t0 = performance.now();
        await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
        trials.push(performance.now() - t0);
      }
      const sorted = [...trials].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)]!;
      const max = sorted.at(-1)!;
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(25);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(150);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);

it("SEC-T5-ITER35-FE-01: redacts camelCase and dot-separated secret keys in Stage 2A JSON and Stage 2B k=v without over-redacting benign identifiers", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const camelDotSecretCases: readonly [string, readonly string[], string][] = [
      [
        'Runtime Error: {"accessToken": cache, "clientSecret": true, "sessionId": server, "userPassword": valid, "auth.token": cache}',
        ["accessToken", "clientSecret", "sessionId", "userPassword", "auth.token", "cache", "true", "server", "valid"],
        "Error: Runtime Error: [redacted], [redacted], [redacted], [redacted], [redacted]",
      ],
      [
        'Runtime Error: {"accessToken": 500ms, "apiKey": 1024 bytes} rejected',
        ["accessToken", "500ms", "apiKey", "1024", "bytes"],
        "Error: Runtime Error: [redacted], [redacted] rejected",
      ],
      [
        'Runtime Error: {"auth.token": server, "user.secret": cache, "x.api.key": valid} rejected',
        ["auth.token", "user.secret", "x.api.key", "server", "cache", "valid"],
        "Error: Runtime Error: [redacted], [redacted], [redacted] rejected",
      ],
      [
        'Runtime Error: {"csrfToken": cache, "authToken": server, "refreshToken": valid, "bearerToken": true, "privateKey": cache, "secretKey": server, "signingKey": valid, "sessionCookie": cache, "oauthToken": server, "req.cookie": cache, "http.authorization": server} rejected',
        ["csrfToken", "authToken", "refreshToken", "bearerToken", "privateKey", "secretKey", "signingKey", "sessionCookie", "oauthToken", "req.cookie", "http.authorization", "cache", "server", "valid", "true"],
        "Error: Runtime Error: [redacted], [redacted], [redacted], [redacted], [redacted], [redacted], [redacted], [redacted], [redacted], [redacted], [redacted] rejected",
      ],
      [
        'Runtime Error: {"APIKey": cache, "CSRFToken": server, "sessionID": valid, "myJWT": cache} rejected',
        ["APIKey", "CSRFToken", "sessionID", "myJWT", "cache", "server", "valid"],
        "Error: Runtime Error: [redacted], [redacted], [redacted], [redacted] rejected",
      ],
      [
        "Runtime Error: accessToken = cache clientSecret = true sessionId = server auth.token = cache",
        ["accessToken", "clientSecret", "sessionId", "auth.token", "cache", "true", "server", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: accessToken=(server) rejected",
        ["accessToken", "server", "=", "("],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: auth.token = server rejected",
        ["auth.token", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: apiKey = 1024 bytes rejected",
        ["apiKey", "1024", "bytes", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: req.cookie: sid=tok99; mode = server rejected",
        ["req.cookie", "sid", "tok99", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: http.authorization: Digest username=alice, response = cache rejected",
        ["http.authorization", "Digest", "alice", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of camelDotSecretCases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }

    // Benign camelCase / dot-separated non-secret terms and legitimate diagnostics must NOT over-redact:
    const benignCases: readonly [string, string][] = [
      ["Runtime Error: chunkSize = 1024 bytes", "Error: Runtime Error: [redacted] = 1024 bytes"],
      ["Runtime Error: rowCount = 500ms", "Error: Runtime Error: [redacted] = 500ms"],
      ["Runtime Error: cacheHit = true", "Error: Runtime Error: [redacted] = true"],
      ["Runtime Error: serverError = HTTP 503", "Error: Runtime Error: [redacted] = HTTP 503"],
      ["Runtime Error: db.table = cache", "Error: Runtime Error: [redacted] = cache"],
      ["Runtime Error: byteOffset = 1024 bytes fileName = valid tableName = cache columnType = text bufferLength = 16 MB memoryLimit = 256 MiB", "Error: Runtime Error: [redacted] = 1024 bytes [redacted] = valid [redacted] = cache [redacted] = text [redacted] = 16 MB [redacted] = 256 MiB"],
      ['Runtime Error: {"chunkSize": 1024 bytes, "cacheHit": true, "db.table": cache}', "Error: Runtime Error: [redacted]: 1024 bytes, [redacted]: true, [redacted]: cache[redacted]"],
      ["Runtime Error: basic syntax error", "Error: Runtime Error: [redacted] syntax error"],
      ["Runtime Error: digest syntax error", "Error: Runtime Error: [redacted] syntax error"],
      ["Runtime Error: (token=847291) rejected", "Error: Runtime Error: ([redacted]) rejected"],
      ["Out of Memory Error: could not allocate block of size 256.0 KiB (243.8 MiB/244.1 MiB used)", "Error: Out of Memory Error: could not allocate block of size 256.0 KiB (243.8 MiB/244.1 MiB used)"],
      ["IO Error: upstream HTTP 503 timeout after 30s", "Error: IO Error: upstream HTTP 503 timeout after 30s"],
      ["Invalid Input Error: Malformed JSON at byte 4 of input: unexpected character. Input: \"x\"", "Error: Invalid Input Error: Malformed JSON at byte 4 of input: unexpected character. Input: [redacted]"],
      ["Out of Range Error: Left-shift value 847291 is out of range", "Error: Out of Range Error: Left-shift value [redacted] is out of range"],
    ];
    for (const [msg, expectedExact] of benignCases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      expect(events.at(-1)?.error, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER35-FE-02: redacts Stage 2A quoted secret keys with = and backticks, glued post-quote values, and real @duckdb/duckdb-wasm error() probes", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query("CREATE TABLE t_iter35 (pin INT)");
    await backend.query("INSERT INTO t_iter35 VALUES (847291)");

    const realDuckWave34: readonly [string, readonly string[], string][] = [
      [
        `SELECT error('"secret" = server')`,
        ["secret", "server", "="],
        "Error: Invalid Input Error: [redacted]",
      ],
      [
        `SELECT error('{"accessToken": cache, "apiKey": 1024 bytes}')`,
        ["accessToken", "cache", "apiKey", "1024", "bytes"],
        "Error: Invalid Input Error: [redacted], [redacted]",
      ],
      [
        `SELECT error('Authorization: OAuth field = server, response = cache rejected')`,
        ["OAuth", "field", "server", "response", "cache", "="],
        "Error: Invalid Input Error: [redacted]",
      ],
      [
        `SELECT error('Authorization=OAuth field = server, response = cache rejected')`,
        ["OAuth", "field", "server", "response", "cache", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('Set-Cookie: sid=tok99; Expires=Wed, 21 Oct 2026 07:28:00 GMT; mode = server rejected')`,
        ["sid", "tok99", "Expires", "2026", "GMT", "mode", "server", "="],
        "Error: Invalid Input Error: [redacted]",
      ],
      [
        `SELECT error('Set-Cookie=sid=tok99; Expires=Wed, 21 Oct 2026 07:28:00 GMT; mode = server rejected')`,
        ["sid", "tok99", "Expires", "2026", "GMT", "mode", "server", "="],
        "Error: Invalid Input Error: [redacted]: [redacted]",
      ],
      [
        `SELECT error('Cookie=sid=tok99; $Path=/; mode = server rejected')`,
        ["Cookie", "sid", "tok99", "Path", "mode", "server", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('dIgEsT username=client, response = cache rejected')`,
        ["dIgEsT", "username", "client", "response", "cache", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('bAsIc server rejected')`,
        ["bAsIc", "server"],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('accessToken = ' || pin || ' rejected') FROM t_iter35`,
        ["accessToken", "847291", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('"token" = ' || pin || ' rejected') FROM t_iter35`,
        ["token", "847291", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
    ];
    for (const [sql, forbidden, expectedExact] of realDuckWave34) {
      await expect(measureCloudQuery("query.console", () => backend.query(sql))).rejects.toThrow();
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `sql=${sql} got=${got}`).not.toContain(frag);
      }
      expect(got, `sql=${sql}`).toBe(expectedExact);
    }

    const quotedEqCases: readonly [string, readonly string[], string][] = [
      [
        'Runtime Error: "secret" = server and "api_key" = cache rejected',
        ["secret", "server", "api_key", "cache", "="],
        "Error: Runtime Error: [redacted] and [redacted] rejected",
      ],
      [
        'Invalid Input Error: "secret" = server',
        ["secret", "server", "="],
        "Error: Invalid Input Error: [redacted]",
      ],
      [
        "Runtime Error: `secret` = server rejected",
        ["secret", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: 'token' = ( cache ) rejected",
        ["token", "cache", "=", "("],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: token='a'server rejected",
        ["token", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: token = 'a'server rejected",
        ["token", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: token=prefix'a'server rejected",
        ["token", "prefix", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: Bearer 'a'server rejected",
        ["Authorization", "Bearer", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        `Runtime Error: "secret" = 'a'server rejected`,
        ["secret", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "secret" := server and "token" => cache and "apiKey" == valid rejected',
        ["secret", "server", "token", "cache", "apiKey", "valid", "="],
        "Error: Runtime Error: [redacted] and [redacted] and [redacted] rejected",
      ],
      [
        "Runtime Error: token := server and apiKey => cache and secret == valid rejected",
        ["token", "server", "apiKey", "cache", "secret", "valid", "="],
        "Error: Runtime Error: [redacted] and [redacted] and [redacted] rejected",
      ],
      [
        "Runtime Error: token:= server and apiKey=> cache and secret== valid rejected",
        ["token", "server", "apiKey", "cache", "secret", "valid", "="],
        "Error: Runtime Error: [redacted] and [redacted] and [redacted] rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of quotedEqCases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
}, 45000);

it("SEC-T5-ITER35-FE-03: redacts Cookie/Set-Cookie/Authorization headers with no-space colons, quoted keys, expanded/mixed-case schemes, URL params, RFC 6265 Expires, and RFC 2965 $ attrs", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const headerCases: readonly [string, readonly string[], string][] = [
      [
        'Runtime Error: Cookie:sid="tok_999"; mode = cache',
        ["Cookie", "sid", "tok_999", "mode", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Cookie: "sid"="tok_999"; mode = cache',
        ["Cookie", "sid", "tok_999", "mode", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Cookie:"sid"="tok_999"; mode = cache',
        ["Cookie", "sid", "tok_999", "mode", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest "realm"="foo", response = cache rejected',
        ["Authorization", "Digest", "realm", "foo", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: Authorization:username="alice", response = cache rejected',
        ["Authorization", "username", "alice", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: OAuth server rejected",
        ["Authorization", "OAuth", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: OAuth field = server, response = cache rejected",
        ["Authorization", "OAuth", "field", "server", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: ApiKey server rejected",
        ["Authorization", "ApiKey", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: HMAC server rejected",
        ["Authorization", "HMAC", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: JWT server rejected",
        ["Authorization", "JWT", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: Negotiate server rejected",
        ["Authorization", "Negotiate", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: AWS4-HMAC-SHA256 Credential=alice, SignedHeaders=host, Signature = cache rejected",
        ["Authorization", "AWS4", "Credential", "alice", "SignedHeaders", "host", "Signature", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: bAsIc server rejected",
        ["bAsIc", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: dIgEsT username=client, response = cache rejected",
        ["dIgEsT", "username", "client", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: digest username=client, response = cache rejected",
        ["digest", "username", "client", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: Digest username=alice, uri=https://secret.example/x, response = cache rejected",
        ["Authorization", "Digest", "username", "alice", "uri", "secret.example", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Cookie: next=https://example.com/ ; mode = server rejected",
        ["Cookie", "next", "example.com", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Cookie: next=https://example.com/; mode = server rejected",
        ["Cookie", "next", "example.com", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Set-Cookie: sid=tok99; Expires=Wed, 21 Oct 2026 07:28:00 GMT; mode = server rejected",
        ["Set-Cookie", "sid", "tok99", "Expires", "Wed", "Oct", "2026", "GMT", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Set-Cookie: sid=tok99; Expires=Wed, 21 Oct 2026 07:28:00 GMT rejected",
        ["Set-Cookie", "sid", "tok99", "Expires", "Wed", "Oct", "2026", "GMT", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Cookie: $Version=1; sid=tok99; mode = server rejected",
        ["Cookie", "Version", "sid", "tok99", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Cookie: sid=tok99; $Path=/; $Domain=.example.com; mode = server rejected",
        ["Cookie", "sid", "tok99", "Path", "Domain", "example.com", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: Cookie: $Version = 1; sid="tok99"; mode = server rejected',
        ["Cookie", "Version", "sid", "tok99", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Cookie: prefs=a,b,c; mode = server rejected",
        ["Cookie", "prefs", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "IO Error: failed fetching https://secret.example/a,cache;mode=server",
        ["secret.example", "cache", "mode", "server"],
        "Error: IO Error: failed fetching [url]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of headerCases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER35-FE-04: maintains strict O(N) performance (<10ms) across 64 KiB Wave 34/35 adversarial inputs", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const perfPayloads64k = [
      "Runtime Error: " + "aB".repeat(32000) + " = server",
      "Runtime Error: " + "abcDef".repeat(10500) + " = server",
      "Runtime Error: " + '{"accessToken": 500ms},'.repeat(2800),
      "Runtime Error: Set-Cookie: " + "Expires=Wed, 21 Oct 2026 07:28:00 GMT; ".repeat(1600),
      "Runtime Error: Cookie: " + "Expires=x ".repeat(6400),
      "Runtime Error: Authorization: Digest " + "uri=https://s.example/x, ".repeat(2500),
      "Runtime Error: Cookie: " + "$Version=1; ".repeat(5000),
      "Runtime Error: " + "`secret` = server ".repeat(3500),
      "Runtime Error: " + "token='a'server ".repeat(4000),
      "Runtime Error: " + "digest ".repeat(9000),
    ];
    for (const payload of perfPayloads64k) {
      const err = new Error(payload);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const trials: number[] = [];
      for (let k = 0; k < 5; k += 1) {
        const t0 = performance.now();
        await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
        trials.push(performance.now() - t0);
      }
      const sorted = [...trials].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)]!;
      const max = sorted.at(-1)!;
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(10 * V8_COV_SCALE);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(150);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);

it("SEC-T5-ITER36-FE-01: preserves quoted secret key context for auth/cookie values, key= value spacing, glued compound separators, and bare AWS4-HMAC-SHA256", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      // Finding 1: Stage 2A quoted secret key context for multi-token auth/cookie/digest values
      [
        'Runtime Error: "Authorization": Bearer server rejected',
        ["Authorization", "Bearer", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Authorization" = Bearer server rejected',
        ["Authorization", "Bearer", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: `Authorization` = Bearer server rejected",
        ["Authorization", "Bearer", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "token": Bearer server rejected',
        ["token", "Bearer", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Cookie": sid=tok99; mode = server rejected',
        ["Cookie", "sid", "tok99", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Authorization": Digest username=alice, response = cache rejected',
        ["Authorization", "Digest", "username", "alice", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Authorization": Digest "realm"="foo", "response" = cache rejected',
        ["Authorization", "Digest", "realm", "foo", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Cookie": sid="tok 99"; mode = server rejected',
        ["Cookie", "sid", "tok", "99", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Authorization": Bearer https://secret.example/path rejected',
        ["Authorization", "Bearer", "secret.example"],
        "Error: Runtime Error: [redacted] [url] rejected",
      ],
      // Finding 2: Stage 2B key= value (space after =, no space before =) in multi-param auth headers
      [
        "Runtime Error: Authorization: Digest username=alice, response= cache rejected",
        ["Authorization", "Digest", "username", "alice", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: OAuth field= server, response= cache rejected",
        ["Authorization", "OAuth", "field", "server", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      // Finding 3: Stage 2B glued compound separators (=>, :=, ==, ::)
      [
        "Runtime Error: Authorization=>Bearer server rejected",
        ["Authorization", "Bearer", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization:=Bearer server rejected",
        ["Authorization", "Bearer", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization==Bearer server rejected",
        ["Authorization", "Bearer", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization::Bearer server rejected",
        ["Authorization", "Bearer", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Error: api_key:: 100ms",
        ["api_key", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: token:: memory",
        ["token", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: password:: 42 MB",
        ["password", "42", "MB"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Cookie:: sid=1; a = memory",
        ["Cookie", "sid", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Cookie::sid=1; a = memory",
        ["Cookie", "sid", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        "Error: api_key :: 100ms",
        ["api_key", "100ms"],
        "Error: Error: [redacted]",
      ],
      // Finding 4: Stage 2B bare AWS4-HMAC-SHA256 multi-param auth mode
      [
        "Runtime Error: AWS4-HMAC-SHA256 Credential=abc, SignedHeaders = server rejected",
        ["AWS4", "HMAC", "Credential", "abc", "SignedHeaders", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: AWS4-HMAC-SHA256 Credential=abc, response = cache rejected",
        ["AWS4", "HMAC", "Credential", "abc", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: aws4-hmac-sha256 Credential=abc, response= cache rejected",
        ["aws4", "hmac", "Credential", "abc", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER36-FE-02: redacts non-ASCII/vertical inline whitespace secrets and WWW-Authenticate/Proxy-Authenticate/Negotiate headers", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      // Finding 5: Non-ASCII and vertical inline whitespace (\u00a0, \v, \f, \u2000, \u3000)
      [
        "Error: Authorization:\u00a0Bearer\u00a0memory",
        ["Authorization", "Bearer", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Authorization: Bearer\u00a0memory",
        ["Authorization", "Bearer", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: api_key:\u00a0100ms",
        ["api_key", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: password=\u00a042 MB",
        ["password", "42", "MB", "="],
        "Error: Error: [redacted]",
      ],
      [
        'Error: "token":\u00a0memory',
        ["token", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: api_key:\u000bmemory",
        ["api_key", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: token:\u000c100ms",
        ["token", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: secret:\u2000memory",
        ["secret", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: token:\u300042 MB",
        ["token", "42", "MB"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: password=\u00a042\u00a0MB",
        ["password", "42", "MB", "="],
        "Error: Error: [redacted]",
      ],
      [
        'Error: "Authorization":\u00a0Bearer\u3000server rejected',
        ["Authorization", "Bearer", "server"],
        "Error: Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Cookie:\u00a0sid=1;\u00a0mode\u00a0=\u00a0server rejected",
        ["Cookie", "sid", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      // Finding 6: WWW-Authenticate / Proxy-Authenticate + Negotiate
      [
        "Error: WWW-Authenticate: Negotiate memory",
        ["WWW-Authenticate", "Negotiate", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Proxy-Authenticate: Negotiate 100ms",
        ["Proxy-Authenticate", "Negotiate", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Negotiate 100ms",
        ["Negotiate", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: WWW-Authenticate: Bearer realm=a, error = memory",
        ["WWW-Authenticate", "Bearer", "realm", "error", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        "Error: WWW-Authenticate: Basic realm=a, charset = memory",
        ["WWW-Authenticate", "Basic", "realm", "charset", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        'Runtime Error: "WWW-Authenticate": Bearer realm=a, error = memory rejected',
        ["WWW-Authenticate", "Bearer", "realm", "error", "memory", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER36-FE-03: handles empty/URL continuation segments, non-separator key prefixes, and >32-char dollar-quoted tags", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const tag33 = "a".repeat(33);
    const tag128 = "t_" + "z".repeat(126);
    const cases: readonly [string, readonly string[], string][] = [
      // Finding 7: Empty continuation segments and mid-header [url] / [redacted]
      [
        "Error: Cookie: a=1; ; b = memory",
        ["Cookie", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Cookie: a=1; https://x.example/p; b = memory",
        ["Cookie", "x.example", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Authorization: Digest realm=a, , response = memory",
        ["Authorization", "Digest", "realm", "response", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        "Error: Authorization: Digest realm=a, https://x.example/p, response = memory",
        ["Authorization", "Digest", "realm", "x.example", "response", "memory", "="],
        "Error: Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest realm=a, "opaque", response = memory rejected',
        ["Authorization", "Digest", "realm", "opaque", "response", "memory", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Cookie": a=1; https://x.example/p; b = memory rejected',
        ["Cookie", "x.example", "memory", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      // Finding 8: Non-separator prefixes (/, :, @, $) and multi-colon tokens
      [
        "Error: req:Authorization: 100ms",
        ["req", "Authorization", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: /token: memory",
        ["token", "memory"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: @secret: 100ms",
        ["secret", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: $secret: 100ms",
        ["secret", "100ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: headers/cookie: sid=a; max-age = 100ms",
        ["headers", "cookie", "sid", "max-age", "100ms", "="],
        "Error: Error: [redacted]",
      ],
      [
        "Runtime Error: req:Authorization:Bearer server rejected",
        ["req", "Authorization", "Bearer", "server"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: req:Cookie:sid=a; max-age = 100ms rejected",
        ["req", "Cookie", "sid", "max-age", "100ms", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      // Finding 9: Dollar-quoted tags > 32 chars (closed, unclosed, multiline)
      [
        `Runtime Error: $${tag33}$memory 42 MB 100ms$${tag33}$ rejected`,
        [tag33, "memory", "42", "MB", "100ms"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        `Runtime Error: $${tag128}$memory 42 MB\n100ms$${tag128}$ rejected`,
        [tag128, "memory", "42", "MB", "100ms"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        `Runtime Error: $${tag33}$memory 42 MB 100ms`,
        [tag33, "memory", "42", "MB", "100ms"],
        "Error: Runtime Error: [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER36-FE-04: redacts Wave 35 bypass classes end-to-end through real @duckdb/duckdb-wasm SELECT error() probes", async () => {
  const { createNodeBackend } = await import("./dashboard/duckdb");
  const backend = await createNodeBackend();
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    await backend.query("CREATE TABLE t_iter36 (secret_val VARCHAR)");
    await backend.query("INSERT INTO t_iter36 VALUES ('memory')");
    const tag40 = "a".repeat(40);

    const realDuckWave35: readonly [string, readonly string[], string][] = [
      [
        `SELECT error('"Authorization" = Bearer ' || secret_val || ' rejected') FROM t_iter36`,
        ["Authorization", "Bearer", "memory", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('Authorization=>Bearer ' || secret_val || ' rejected') FROM t_iter36`,
        ["Authorization", "Bearer", "memory", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('Authorization::Bearer ' || secret_val || ' rejected') FROM t_iter36`,
        ["Authorization", "Bearer", "memory"],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('AWS4-HMAC-SHA256 Credential=abc, response = ' || secret_val || ' rejected') FROM t_iter36`,
        ["AWS4", "Credential", "abc", "response", "memory", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('Authorization=Digest username=alice, response= ' || secret_val || ' rejected') FROM t_iter36`,
        ["Authorization", "Digest", "alice", "response", "memory", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('Negotiate 100ms rejected')`,
        ["Negotiate", "100ms"],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('WWW-Authenticate=Bearer realm=a, error = ' || secret_val || ' rejected') FROM t_iter36`,
        ["WWW-Authenticate", "Bearer", "realm", "error", "memory", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('password=' || chr(160) || '42 MB rejected')`,
        ["password", "42", "MB", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('Cookie=a=1; ; b = ' || secret_val || ' rejected') FROM t_iter36`,
        ["Cookie", "memory", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('headers/cookie=sid=a; max-age = 100ms rejected')`,
        ["headers", "cookie", "sid", "max-age", "100ms", "="],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
      [
        `SELECT error('$${tag40}$' || secret_val || ' 42 MB 100ms$${tag40}$ rejected') FROM t_iter36`,
        [tag40, "memory", "42", "MB", "100ms"],
        "Error: Invalid Input Error: [redacted] rejected",
      ],
    ];
    for (const [sql, forbidden, expectedExact] of realDuckWave35) {
      await expect(measureCloudQuery("query.console", () => backend.query(sql))).rejects.toThrow();
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `sql=${sql} got=${got}`).not.toContain(frag);
      }
      expect(got, `sql=${sql}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
    await backend.dispose();
  }
}, 45000);

it("SEC-T5-ITER36-FE-05: maintains strict O(N) performance (<10ms) across 64 KiB Wave 35/36 adversarial inputs including long dollar-quote tags and Unicode whitespace", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    // Also assert correctness on a large >32-char dollar-quoted payload
    const tag500 = "t" + "a".repeat(499);
    const bigDollarErr = new Error(`Runtime Error: $${tag500}$memory 42 MB 100ms$${tag500}$ rejected`);
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw bigDollarErr; })).rejects.toBe(bigDollarErr);
    expect(events.at(-1)?.error).toBe("Error: Runtime Error: [redacted] rejected");

    const perfPayloads64k = [
      "Runtime Error: $" + "a".repeat(64000),
      "Runtime Error: $" + "a".repeat(30000) + "$" + "b".repeat(30000),
      "Runtime Error: " + ("$" + "a".repeat(60) + " ").repeat(1000),
      "Runtime Error: " + "$a$x$a$".repeat(9000),
      "Runtime Error: " + ("$" + "k".repeat(40) + "$v$" + "k".repeat(40) + "$ ").repeat(750),
      "Runtime Error: " + "a:".repeat(32000) + "100ms",
      "Runtime Error: Cookie: a=1" + "; ".repeat(32000) + "b = memory",
      "Runtime Error: Authorization: Digest a=1" + ", ".repeat(32000) + "response = cache",
      "Runtime Error: Authorization:\u00a0" + "Bearer\u00a0".repeat(9000) + "memory",
      "Runtime Error: WWW-Authenticate: Bearer " + "realm=a, ".repeat(7000) + "error = memory",
    ];
    for (const payload of perfPayloads64k) {
      const err = new Error(payload);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const trials: number[] = [];
      for (let k = 0; k < 5; k += 1) {
        const t0 = performance.now();
        await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
        trials.push(performance.now() - t0);
      }
      const sorted = [...trials].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)]!;
      const max = sorted.at(-1)!;
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(10 * V8_COV_SCALE);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(150);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);

it("SEC-T5-ITER37-FE-01: redacts spaced = on quoted first params in Cookie/Auth headers", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        'Runtime Error: "Authorization": Digest "realm" = memory, "response" = cache rejected',
        ["Authorization", "Digest", "realm", "memory", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Cookie": "sid" = memory; mode = server rejected',
        ["Cookie", "sid", "memory", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: \"Authorization\": Digest 'realm' = memory, 'response' = cache rejected",
        ["Authorization", "Digest", "realm", "memory", "response", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Cookie": "sid" = "memory"; mode = server rejected',
        ["Cookie", "sid", "memory", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "WWW-Authenticate": Bearer "realm" = memory, "error" = cache rejected',
        ["WWW-Authenticate", "Bearer", "realm", "memory", "error", "cache", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Cookie": "sid=memory; mode=server" rejected',
        ["Cookie", "sid", "memory", "mode", "server", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER37-FE-02: redacts custom Authorization and Proxy-Authorization schemes not in AUTH_SCHEMES", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        "Authorization: FastlyKey select",
        ["Authorization", "FastlyKey", "select"],
        "Error: [redacted]",
      ],
      [
        "Authorization: SSWS 123ms",
        ["Authorization", "SSWS", "123ms"],
        "Error: [redacted]",
      ],
      [
        "Authorization: CustomScheme 42 MB",
        ["Authorization", "CustomScheme", "42", "MB"],
        "Error: [redacted]",
      ],
      [
        "Proxy-Authorization: CustomAuth default",
        ["Proxy-Authorization", "CustomAuth", "default"],
        "Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: FastlyKey select rejected",
        ["Authorization", "FastlyKey", "select"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: CustomScheme 42 MB rejected",
        ["Authorization", "CustomScheme", "42", "MB"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Authorization": FastlyKey select rejected',
        ["Authorization", "FastlyKey", "select"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Authorization": "FastlyKey select" rejected',
        ["Authorization", "FastlyKey", "select"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: "Proxy-Authorization": CustomAuth default rejected',
        ["Proxy-Authorization", "CustomAuth", "default"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization:FastlyKey select rejected",
        ["Authorization", "FastlyKey", "select"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Proxy-Authorization:CustomAuth default rejected",
        ["Proxy-Authorization", "CustomAuth", "default"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: CustomScheme realm=a, response = memory rejected",
        ["Authorization", "CustomScheme", "realm", "response", "memory", "="],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        "Runtime Error: Authorization: FastlyKey. select rejected",
        ["Authorization", "FastlyKey"],
        "Error: Runtime Error: [redacted] select rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER37-FE-03: handles non-E backslash-escaped single quotes in Stage 2A without exposing inner tokens", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        "Error parsing 'foo\\' table ' and 'bar'",
        ["foo", "table", "bar"],
        "Error: Error parsing [redacted]",
      ],
      [
        "Runtime Error: parsing 'foo\\' table ' rejected",
        ["foo", "table"],
        "Error: Runtime Error: parsing [redacted]",
      ],
      [
        "Runtime Error: parsing 'foo\\\\' and 'bar' rejected",
        ["foo", "bar"],
        "Error: Runtime Error: parsing [redacted] and [redacted] rejected",
      ],
      [
        "Runtime Error: parsing 'foo\\\\\\' table ' and 'bar' rejected",
        ["foo", "table", "bar"],
        "Error: Runtime Error: parsing [redacted]",
      ],
      [
        "Runtime Error: parsing 'foo''bar' and 'baz' rejected",
        ["foo", "bar", "baz"],
        "Error: Runtime Error: parsing [redacted] and [redacted] rejected",
      ],
      [
        "Runtime Error: parsing 'foo\\' table",
        ["foo", "table"],
        "Error: Runtime Error: parsing [redacted]",
      ],
      [
        "Binder Error: invalid pattern LIKE '%\\' for client 'memory'",
        ["memory", "%\\"],
        "Error: Binder Error: invalid pattern LIKE [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER37-FE-04: redacts values for >64-char JSON secret keys including 256+ char keys", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        `{"${"x".repeat(60)}_token": select}`,
        ["token", "select"],
        "Error: [redacted]",
      ],
      [
        `{"${"x".repeat(256)}_secret": 100ms}`,
        ["secret", "100ms"],
        "Error: [redacted]",
      ],
      [
        `{'${"x".repeat(60)}_token': select}`,
        ["token", "select"],
        "Error: [redacted]",
      ],
      [
        `Runtime Error: {\`${"x".repeat(80)}_password\`: 42 MB} rejected`,
        ["password", "42", "MB"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        `Runtime Error: {"${"x".repeat(100)}_authorization": FastlyKey select} rejected`,
        ["authorization", "FastlyKey", "select"],
        "Error: Runtime Error: [redacted] rejected",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER37-FE-05: maintains strict O(N) performance (<10ms median) on 64 KiB Wave 37 adversarial inputs", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const longKey512 = `${"k".repeat(506)}_token`;
    const bigKeyErr = new Error(`Runtime Error: {"${longKey512}": select} rejected`);
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw bigKeyErr; })).rejects.toBe(bigKeyErr);
    expect(events.at(-1)?.error).toBe("Error: Runtime Error: [redacted] rejected");

    const perfPayloads64k = [
      `Runtime Error: {"${"x".repeat(64000)}_token": select}`,
      `Runtime Error: {"${"x".repeat(64000)}": select}`,
      `Runtime Error: '${"\\'".repeat(32000)} table ' and 'bar'`,
      `Runtime Error: "Authorization": Digest ${'"a" = b, '.repeat(7000)}"response" = cache`,
      `Runtime Error: "Cookie": ${'"sid" = a; '.repeat(5800)}mode = server`,
      `Runtime Error: ${"Authorization: CustomScheme ".repeat(2200)}42 MB`,
    ];
    for (const payload of perfPayloads64k) {
      const err = new Error(payload);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const trials: number[] = [];
      for (let k = 0; k < 5; k += 1) {
        const t0 = performance.now();
        await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
        trials.push(performance.now() - t0);
      }
      const sorted = [...trials].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)]!;
      const max = sorted.at(-1)!;
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(10 * V8_COV_SCALE);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(150);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);

it("SEC-T5-ITER39-FE-01: prevents non-E backslash-quote phase inversion with trailing apostrophes or truncated quotes", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        "error: 'a\\'memory' in user's query",
        ["memory", "user", "query"],
        "Error: error: [redacted]",
      ],
      [
        "error: 'a\\'select' for O'Brien",
        ["select", "Brien"],
        "Error: error: [redacted]",
      ],
      [
        "error: 'a\\'memory' 'truncated",
        ["memory", "truncated"],
        "Error: error: [redacted]",
      ],
      [
        "error: 'a\\' memory ' in user's query",
        ["memory", "user", "query"],
        "Error: error: [redacted]",
      ],
      [
        "error: 'a\\' select ' for O'Brien",
        ["select", "Brien"],
        "Error: error: [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER39-FE-02: prevents double-quote and backtick trailing-backslash quote desync while preserving JSON escapes", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        'Parser Error: near "a\\" and "select update delete session 500ms"',
        ["select", "update", "delete", "session", "500ms"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        "Parser Error: near `a\\` and `session 500ms 404 B`",
        ["session", "500ms", "404"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        'Parser Error: near "a\\\\" and "select update delete"',
        ["select", "update", "delete"],
        "Error: Parser Error: near [redacted] and [redacted]",
      ],
      [
        'JSON Error: bad value "foo\\"bar" and "select"',
        ["foo", "bar", "select"],
        "Error: JSON Error: bad value [redacted]",
      ],
      [
        'JSON Error: bad value "foo\\" bar " and "select"',
        ["foo", "bar", "select"],
        "Error: JSON Error: bad value [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER39-FE-03: maintains strict O(N) performance (<10ms median, <150ms max) on 64 KiB Wave 39 adversarial quote payloads", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const perfPayloads64k = [
      `error: '${"a\\'b".repeat(16000)}' in user's query`,
      `Parser Error: near "${'a\\"'.repeat(21000)}" and "select update delete session 500ms"`,
      `Parser Error: near \`${"a\\`".repeat(21000)}\` and \`session 500ms 404 B\``,
      `Parser Error: ${'"a\\" '.repeat(12800)}"select"`,
      `Parser Error: ${"`a\\` ".repeat(12800)}\`session\``,
    ];
    for (const payload of perfPayloads64k) {
      const err = new Error(payload);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const trials: number[] = [];
      for (let k = 0; k < 5; k += 1) {
        const t0 = performance.now();
        await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
        trials.push(performance.now() - t0);
      }
      const sorted = [...trials].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)]!;
      const max = sorted.at(-1)!;
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(10 * V8_COV_SCALE);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(150);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);

it("SEC-T5-ITER40-FE-01: prevents multi-trailing-backslash quote parity inversion across double, backtick, and single quotes", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        'Parser Error: near "a\\" and "b\\" and "select update delete session 500ms"',
        ["select", "update", "delete", "session", "500ms"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        "Parser Error: near `a\\` and `b\\` and `session 500ms 404 B`",
        ["session", "500ms", "404"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        "Parser Error: near 'a\\' and 'b\\' and 'select update delete session 500ms'",
        ["select", "update", "delete", "session", "500ms"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        'Parser Error: near "a\\" and "select update delete" and "b\\"',
        ["select", "update", "delete"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        'Parser Error: near "a\\\\\\" and "b\\\\\\" and "select update delete"',
        ["select", "update", "delete"],
        "Error: Parser Error: near [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER40-FE-02: prevents dot and colon delimiter bypasses after trailing-backslash quotes", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        'Binder Error: column "a\\"."select update delete session 500ms" not found',
        ["select", "update", "delete", "session", "500ms"],
        "Error: Binder Error: column [redacted]",
      ],
      [
        'Parser Error: near "a\\": "select update delete session 500ms"',
        ["select", "update", "delete", "session", "500ms"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        "Parser Error: near 'a\\': 'select update delete session 500ms'",
        ["select", "update", "delete", "session", "500ms"],
        "Error: Parser Error: near [redacted]",
      ],
      [
        "Parser Error: near `a\\`. `session 500ms 404 B`",
        ["session", "500ms", "404"],
        "Error: Parser Error: near [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER40-FE-03: prevents non-contraction apostrophe mismatch and cross-quote parity inversion", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const cases: readonly [string, readonly string[], string][] = [
      [
        "Error: 'a\\' b'c 'HTTP 403 100 MB session valid'",
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: 'a\\' user's '1234ms 500 GiB DB::Exception'",
        ["1234ms", "500", "GiB", "DB::Exception", "user"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: \"a\\\" HTTP 403 100 MB session valid\" 'foo\"bar'",
        ["HTTP", "403", "100", "MB", "session", "valid", "foo", "bar"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: `a\\` HTTP 403 100 MB session valid` 'foo`bar'",
        ["HTTP", "403", "100", "MB", "session", "valid", "foo", "bar"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: 'a\\' can't HTTP 403 100 MB session valid'",
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: 'a\\' doesn't SELECT update delete session 500ms'",
        ["SELECT", "update", "delete", "session", "500ms"],
        "Error: Error: [redacted]",
      ],
      [
        "Binder Error: 'a\\' won't 1234ms 500 GiB table column'",
        ["1234ms", "500", "GiB", "table", "column"],
        "Error: Binder Error: [redacted]",
      ],
      [
        "Error: 'a\\' \\can't HTTP 403 100 MB session valid'",
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Error: [redacted]",
      ],
      [
        "Error: 'a\\' \\\\can't \\doesn't HTTP 403 100 MB session valid'",
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Error: [redacted]",
      ],
      [
        'Runtime Error: "token": "a\\" and " HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "api_key": \'a\\\' and \' HTTP 403 100 MB session valid\'',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "token": `a\\` and ` HTTP 403 100 MB session valid`',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": "sid=a\\" ; " HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer "a\\" and " HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest realm="a\\", response=" HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": sid=("a\\") HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": sid=["a\\"] HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest realm=("a\\") HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest realm={"a\\"} HTTP 403 100 MB session valid"',
        ["HTTP", "403", "100", "MB", "session", "valid"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": sid=123; https://example.com = 100ms; mode = server',
        ["123", "example.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, https://example.com = 100ms, mode = server',
        ["example.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": sid = https://example.com = 100ms; mode = server',
        ["example.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest uri = https://example.com = 100ms, response = cache',
        ["example.com", "100ms", "response", "cache"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": https://example.com = 100ms; mode = server',
        ["example.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest https://example.com = 100ms, mode = server',
        ["example.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: https://example.com = 100ms; mode = server",
        ["example.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest https://example.com = 100ms, mode = server",
        ["example.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": sid = https://example.com = https://b.com = 100ms; mode = server',
        ["example.com", "b.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": sid=123; https://example.com = https://b.com = 100ms; mode = server',
        ["123", "example.com", "b.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": $$secret$$ = 100ms; mode = server',
        ["secret", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest $$secret$$ = 100ms, mode = server',
        ["secret", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": [url] = 100ms; mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest [url] = 100ms, mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": sid = https://a.com == 100ms; mode = server',
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: sid = https://a.com = = 100ms; mode = server",
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: https://a.com == 100ms; mode = server",
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": $$secret$$ == 100ms; mode = server',
        ["secret", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: sid = = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest [url] = = 100ms, mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest user = = 100ms, mode = server',
        ["user", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest https://a.com == 100ms, mode = server",
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest user == 100ms, mode = server',
        ["user", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest user == 100ms, mode = server",
        ["user", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest $$secret$$ == 100ms, mode = server',
        ["secret", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest user = 100ms, param == 100ms, mode = server",
        ["user", "param", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: sid=https://a.com'x' = 100ms; mode = server",
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest uri=https://a.com"x" = 100ms, mode = server',
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: \"Cookie\": 'a''b' = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest "a""b" = 100ms, mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": "" = 100ms; mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": a=1; "opaque"; mode = server',
        ["opaque", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest realm=a, "opaque", response = memory rejected',
        ["opaque", "response", "memory"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        `Runtime Error: Cookie: ${"a".repeat(65)} = 100ms; mode = server`,
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        `Runtime Error: "Cookie": "${"a".repeat(65)}" = 100ms; mode = server`,
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": foo,100ms; mode = server',
        ["foo", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; https://a.com'x' = 100ms; mode = server",
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: \"Cookie\": a=1; https://a.com'x' = 100ms; mode = server",
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest realm=a, https://a.com"x" = 100ms, mode = server',
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest realm=a, https://a.com"x" = 100ms, mode = server',
        ["a.com", "100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: \"x\"'y' = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: \"Cookie\": \"x\"'y' = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a'x' = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: \"Cookie\": a'x' = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; https://a.com'x'; mode = server",
        ["a.com", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: \"Cookie\": a=1; https://a.com'x'; mode = server",
        ["a.com", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest realm=a, https://a.com"x", mode = server',
        ["a.com", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest realm=a, https://a.com"x", mode = server',
        ["a.com", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; \"x\"'y'; mode = server",
        ["mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; a'x'; mode = server",
        ["mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest realm=a, "x"https://a.com, mode = server',
        ["a.com", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; a/b = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; a:b = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; a$b = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; (x) = 100ms; mode = server",
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": (x) = 100ms; mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": [x] = 100ms; mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": {x} = 100ms; mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest (x) = 100ms, mode = server',
        ["100ms", "mode", "server"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": a=1; mode"unclosed',
        ["mode", "unclosed"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, mode"unclosed',
        ["mode", "unclosed"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest a=1, mode"unclosed',
        ["mode", "unclosed"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, mode"closed"',
        ["mode", "closed"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest a=1, "secret"100ms',
        ["secret", "100ms"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, "secret"100ms',
        ["secret", "100ms"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest a=1, "x"mode',
        ["mode"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, "x"mode',
        ["mode"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest a=1, "x"100ms:',
        ["100ms"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, "x"100ms:',
        ["100ms"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: Authorization: Digest a=1, mode"x":',
        ["mode"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, mode"x":',
        ["mode"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": "foo",cache; a = error',
        ["foo", "cache", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": (foo),cache; a = error',
        ["foo", "cache", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": "foo"[url]cache; a = error',
        ["foo", "cache", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest "foo"[url]cache, b = error',
        ["foo", "cache", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        `Runtime Error: Authorization: Digest realm${" ".repeat(17)}=error, b = cache`,
        ["error", "cache"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        `Runtime Error: Cookie: a=1; sid${" ".repeat(17)}=error; path = cache`,
        ["error", "cache"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        `Runtime Error: Authorization: Digest uri=https://a.com${" ".repeat(17)}=error, b = cache`,
        ["error", "cache"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest a=b, error =",
        ["error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=b, error =',
        ["error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 58 / Iter 59: AUTH_SCHEMES followed by ;/, (inline or spaced)
      [
        "Runtime Error: Cookie:bearer; a = error",
        ["bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie:token; a = error",
        ["token", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: bearer ; a = error",
        ["bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": bearer ; a = error',
        ["bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest , a = error",
        ["Digest", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest , a = error',
        ["Digest", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization:Digest, a = error",
        ["Digest", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate:Basic, a = error",
        ["Basic", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Digest , a = error",
        ["Digest", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 58 / Iter 59: Nameless =value continuation item (Stage 2A & 2B)
      [
        "Runtime Error: Cookie: a=1; =error; b = cache",
        ["error", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": a=1; =error; b = cache',
        ["error", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest a=1, =error, b = cache",
        ["error", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, =error, b = cache',
        ["error", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 58 / Iter 59: Comma-folded Set-Cookie headers & JSON member boundaries
      [
        "Runtime Error: Set-Cookie: a=1; HttpOnly, b = error",
        ["HttpOnly", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1, b = error",
        ["error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Wed, 21 Oct 2015 07:28:00 GMT, b = error",
        ["Expires", "GMT", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: {"Set-Cookie": a=1; HttpOnly, "status": 500}',
        ["HttpOnly", "status", "500"],
        "Error: Runtime Error: [redacted], [redacted]: [redacted]",
      ],
      // Wave 58 / Iter 59: Backslash escapes in JSON secret keys
      [
        'Runtime Error: "my\\\\token": error',
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "X\\tCookie": a=1; b = error',
        ["Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "X\\u0020Authorization": Digest a=1, b = error',
        ["Authorization", "Digest", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 59 / Iter 60: Bypass A — Nameless = value as FIRST item in Cookie / Set-Cookie / Multi-Param Auth
      [
        "Runtime Error: Cookie: = error; sid = cache",
        ["Cookie", "error", "sid", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": = error; sid = cache',
        ["Cookie", "error", "sid", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: = error, b = cache",
        ["Set-Cookie", "error", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest = error, b = cache",
        ["Authorization", "Digest", "error", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest = error, b = cache',
        ["Authorization", "Digest", "error", "cache", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: = 1234s; b = 500ms",
        ["Cookie", "1234s", "500ms", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 59 / Iter 60: Bypass B — Comma-folded Set-Cookie / Cookie with no space after , and unknown boolean attr before ,
      [
        "Runtime Error: Set-Cookie: a=1,b = error",
        ["Set-Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Path=/,b = error",
        ["Set-Cookie", "Path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Set-Cookie": a=1,b = error',
        ["Set-Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1,b = error",
        ["Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1,b = 1234s",
        ["Set-Cookie", "1234s", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; CustomFlag, b = error",
        ["Set-Cookie", "CustomFlag", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 59 / Iter 60: Bypass C — AUTH_SCHEMES followed by spaced =
      [
        "Runtime Error: Authorization: Bearer = error",
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Basic = error",
        ["Authorization", "Basic", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer = 256 KiB",
        ["Authorization", "Bearer", "256", "KiB", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token: Bearer = error",
        ["Token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "token": Bearer = error',
        ["token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization:Bearer = error",
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 59 / Iter 60: Bypass D — unescapeJsonKey + hasKeyStemSegment non-alphanumeric separator gap
      [
        'Runtime Error: {"\\"token\\"": error}',
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: {"\\"Cookie\\"": a=1; b = error}',
        ["Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: {"[token]": error}',
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: {"token\\u0000": error}',
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: &token = error",
        ["token", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: <token>: error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 60 / Iter 61: Bypass 1 — AUTH_SCHEMES glued to = or : followed by space + value (Stage 2A & 2B)
      [
        "Runtime Error: X-Token: Bearer= error",
        ["Token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "token": Bearer= error',
        ["token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=Bearer= error",
        ["Token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: token = Bearer= error",
        ["token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token: Basic= 256 KiB",
        ["Token", "Basic", "256", "KiB", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer: error",
        ["Authorization", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer: error',
        ["Authorization", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 60 / Iter 61: Bypass 2 — Set-Cookie Expires= with non-GMT/UTC timezone (PST, Z, +00:00)
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Wed, 21 Oct 2015 07:28:00 PST; sid = error",
        ["Set-Cookie", "Expires", "PST", "sid", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Wed, 21 Oct 2015 07:28:00 +00:00; sid = error",
        ["Set-Cookie", "Expires", "+00:00", "sid", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Wed, 21 Oct 2015 07:28:00 Z, b = error",
        ["Set-Cookie", "Expires", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 60 / Iter 61: Bypass 3 — AWS4-HMAC-SHA256 with = or quoted JSON key
      [
        "Runtime Error: AWS4-HMAC-SHA256= Credential=abc, host = error",
        ["AWS4", "HMAC", "Credential", "abc", "host", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "AWS4-HMAC-SHA256": Credential=abc, host = error',
        ["AWS4", "HMAC", "Credential", "abc", "host", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 60 / Iter 61: Bypass 4 — WWW-Authenticate / Proxy-Authenticate multi-challenge with custom scheme after ,
      [
        "Runtime Error: WWW-Authenticate: Basic realm=host, CustomAuth param = error",
        ["WWW-Authenticate", "Basic", "realm", "host", "CustomAuth", "param", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Proxy-Authenticate: Bearer realm=host, CustomScheme error",
        ["Proxy-Authenticate", "Bearer", "realm", "host", "CustomScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 60 / Iter 61: Bypass 5 — Comma-folded Set-Cookie with quoted cookie name in Stage 2A
      [
        'Runtime Error: "Set-Cookie": a=1, "b" = error',
        ["Set-Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: {"Set-Cookie": a=1, "b" = error}',
        ["Set-Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 61 / Iter 62: Verifier 3 — Glued inline compound separators =>, ::, := and repeated :::
      [
        "Runtime Error: X-Token=Bearer=> error",
        ["Token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=Bearer:: error",
        ["Token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=Bearer:= error",
        ["Token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer::: error",
        ["Authorization", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 61 / Iter 62: Verifier 5 #1 — Set-Cookie Expires= extended date formats
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00 GMT+0000, status = valid",
        ["Set-Cookie", "Expires", "GMT", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00 UTC-05:00, status = valid",
        ["Set-Cookie", "Expires", "UTC", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu Jan 01 2099 00:00:00 GMT, status = valid",
        ["Set-Cookie", "Expires", "GMT", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00.000 GMT, status = valid",
        ["Set-Cookie", "Expires", "GMT", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00 GMT+0000 (UTC); status = valid",
        ["Set-Cookie", "Expires", "GMT", "UTC", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Set-Cookie": a=1; Expires=Thu, 01 Jan 2099 00:00:00 GMT+0000, status = valid',
        ["Set-Cookie", "Expires", "GMT", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 61 / Iter 62: Verifier 5 #2 — HMAC multi-param, glued key=scheme:val/=, Authorization: Bearer realm = local
      [
        "Runtime Error: X-Auth: HMAC id = valid, mode = local",
        ["Auth", "HMAC", "id", "valid", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token: HMAC Credential = valid, mode = local",
        ["Token", "HMAC", "Credential", "valid", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=Digest:error, mode = local",
        ["Token", "Digest", "error", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=AWS4-HMAC-SHA256:Credential=valid, mode = local",
        ["Token", "AWS4", "HMAC", "Credential", "valid", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=OAuth:token=valid, mode = local",
        ["Token", "OAuth", "valid", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=Digest=realm=valid, mode = local",
        ["Token", "Digest", "realm", "valid", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer realm = local",
        ["Authorization", "Bearer", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 61 / Iter 62: Verifier 5 #3 — Custom auth schemes (_/., glued :, grouped (/[] in multi-challenge)
      [
        "Runtime Error: Authorization: MY_CUSTOM_SCHEME error",
        ["Authorization", "MY_CUSTOM_SCHEME", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Custom.Scheme error",
        ["Authorization", "Custom.Scheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, MY_CUSTOM_SCHEME error",
        ["WWW-Authenticate", "Digest", "realm", "host", "MY_CUSTOM_SCHEME", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": MY_CUSTOM_SCHEME error',
        ["Authorization", "MY_CUSTOM_SCHEME", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: MyScheme: error",
        ["Authorization", "MyScheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, MyScheme: error",
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "WWW-Authenticate": Digest realm=host, MyScheme: error',
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": MyScheme: error',
        ["Authorization", "MyScheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, MyScheme(error)",
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, MyScheme[error]",
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "WWW-Authenticate": Digest realm=host, MyScheme(error)',
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 61 / Iter 62: Verifier 5 #4 — Comma-folded quoted cookie name with unquoted suffix in Stage 2A
      [
        'Runtime Error: "Set-Cookie": a=1, "my"status = error',
        ["Set-Cookie", "status", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 62 / Iter 63: Verifier 5 #1 — Multi-challenge custom auth scheme with {...} group
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, MyScheme{error}",
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "WWW-Authenticate": Digest realm=host, MyScheme{error}',
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, MyScheme:{error}",
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 62 / Iter 63: Verifier 5 #2 — Bearer/Basic multi-param continuations & bare Bearer realm = local
      [
        "Runtime Error: Authorization: Bearer realm = local, scope = valid",
        ["Authorization", "Bearer", "realm", "local", "scope", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer realm = local, scope = valid',
        ["Authorization", "Bearer", "realm", "local", "scope", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer realm=local, error = expired",
        ["Authorization", "Bearer", "realm", "local", "error", "expired", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Basic realm = local, mode = valid",
        ["Authorization", "Basic", "realm", "local", "mode", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Bearer realm = local",
        ["Bearer", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 62 / Iter 63: Verifier 5 #3 — Extended Expires= date formats & >64-char TZ labels
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Wednesday, 01-Jan-2099 00:00:00 GMT+0000 (Coordinated Universal Time), status = valid",
        ["Set-Cookie", "Expires", "GMT", "Coordinated", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Wednesday, 01-Jan-2099 00:00:00 GMT+0000 (Coordinated Universal Time); status = valid",
        ["Set-Cookie", "Expires", "GMT", "Coordinated", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00 +0000 (America/New_York), status = valid",
        ["Set-Cookie", "Expires", "America", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00 GMT+00:00 (UTC+00:00), status = valid",
        ["Set-Cookie", "Expires", "GMT", "UTC", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=2099-01-01 00:00:00 UTC, status = valid",
        ["Set-Cookie", "Expires", "UTC", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 January 2099 00:00:00 GMT, status = valid",
        ["Set-Cookie", "Expires", "January", "GMT", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 62 / Iter 63: Verifier 5 #4 — Repeated/compound separators & => / := in KV/auth params
      [
        "Runtime Error: token::: error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "token"::: error',
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: token :=: error",
        ["token", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: token ==> error",
        ["token", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest realm=> local",
        ["Authorization", "Digest", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest realm := local",
        ["Authorization", "Digest", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; status => error",
        ["Set-Cookie", "status", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; status := error",
        ["Set-Cookie", "status", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: MyScheme=> error",
        ["Authorization", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 62 / Iter 63: Verifier 5 #5 — Stage 2A comma-folded compound quoted cookie name without '='
      [
        'Runtime Error: "Set-Cookie": a=1, "my"status; mode = local',
        ["Set-Cookie", "status", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Set-Cookie": a=1, "my"status, mode = local',
        ["Set-Cookie", "status", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Cookie": a=1, "my"status; mode = local',
        ["Cookie", "status", "mode", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Benign preservation checks for Iter 63 fixes
      [
        "Runtime Error: Authorization: Basic YWRtaW46c2VjcmV= rejected",
        ["Authorization", "Basic", "YWRtaW46c2VjcmV"],
        "Error: Runtime Error: [redacted] rejected",
      ],
      [
        'Runtime Error: {"Set-Cookie": a=1, "error": "timeout"}',
        ["Set-Cookie", "a=1"],
        "Error: Runtime Error: [redacted], [redacted]: [redacted]",
      ],
      // Wave 63 / Iter 64: Verifier 5 #1 — Bearer/Basic key= value not misclassified as base64 padding
      [
        "Runtime Error: Authorization: Bearer realm= local, scope = valid",
        ["Authorization", "Bearer", "realm", "local", "scope", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer realm= local, scope = valid',
        ["Authorization", "Bearer", "realm", "local", "scope", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Basic realm= local, mode = valid",
        ["Authorization", "Basic", "realm", "local", "mode", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Bearer realm= local, scope = valid",
        ["Bearer", "realm", "local", "scope", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token=Bearer:realm= local, status = valid",
        ["Token", "Bearer", "realm", "local", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 63 / Iter 64: Verifier 5 #2 — Spaced => / := / =: / : after Bearer/Basic & Bearer = local, scope = valid
      [
        "Runtime Error: Authorization: Bearer => error",
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer := error",
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer =: error",
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer : error",
        ["Authorization", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer => error',
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer := error',
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token: Bearer => error",
        ["Token", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer = local, scope = valid",
        ["Authorization", "Bearer", "local", "scope", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 63 / Iter 64: Verifier 5 #3 — Spaced : after custom auth scheme
      [
        "Runtime Error: Authorization: MyScheme : error",
        ["Authorization", "MyScheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": MyScheme : error',
        ["Authorization", "MyScheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, MyScheme : error",
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "WWW-Authenticate": Digest realm=host, MyScheme : error',
        ["WWW-Authenticate", "Digest", "realm", "host", "MyScheme", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 63 / Iter 64: Verifier 5 #4 — Stage 2A quoted "Digest" / "oauth" / "Basic" JSON keys
      [
        'Runtime Error: "Digest": realm = local, mode = valid',
        ["Digest", "realm", "local", "mode", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "oauth": realm = local, mode = valid',
        ["oauth", "realm", "local", "mode", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Basic": error',
        ["Basic", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 63 / Iter 64: Verifier 5 #5 — Stage 2A comma-folded compound quoted cookie name at EOL / before } or )
      [
        'Runtime Error: "Set-Cookie": a=1, "my"status',
        ["Set-Cookie", "status", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: {"Set-Cookie": a=1, "my"status}',
        ["Set-Cookie", "status", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: ("Set-Cookie": a=1, "my"status)',
        ["Set-Cookie", "status", "="],
        "Error: Runtime Error: ([redacted])",
      ],
      // Wave 63 / Iter 64: Verifier 5 #6 — Set-Cookie Expires= optional seconds, punct in TZ label, >128-char spaces & >64-char TZ
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00 GMT, status = valid",
        ["Set-Cookie", "Expires", "GMT", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00 GMT+0000 (St. Johns), status = valid",
        ["Set-Cookie", "Expires", "GMT", "Johns", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        `Runtime Error: Set-Cookie: a=1; Expires=Mon, ${" ".repeat(125)}01 Jan 2099 00:00:00 GMT; status = valid`,
        ["Set-Cookie", "Expires", "GMT", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        `Runtime Error: Set-Cookie: a=1; Expires=Thu, 01 Jan 2099 00:00:00 GMT+0000 (${"A".repeat(65)}), status = valid`,
        ["Set-Cookie", "Expires", "GMT", "AAA", "status", "valid", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 63 / Iter 64: Verifier 5 #7 — Custom auth scheme >32 chars & SQL E'token':
      [
        "Runtime Error: Authorization: CUSTOM_ENTERPRISE_VEND_SCHEME_V2X error",
        ["Authorization", "CUSTOM_ENTERPRISE_VEND_SCHEME_V2X", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: WWW-Authenticate: Digest realm=host, CUSTOM_ENTERPRISE_VEND_SCHEME_V2X error",
        ["WWW-Authenticate", "Digest", "realm", "host", "CUSTOM_ENTERPRISE_VEND_SCHEME_V2X", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: E'token': error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 64 / Iter 65: Verifier 3 & Verifier 5 #5 — Spaced : in authParamMode, continuation params, and : : : runs
      [
        "Runtime Error: WWW-Authenticate: Bearer : error",
        ["WWW-Authenticate", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "WWW-Authenticate": Bearer : error',
        ["WWW-Authenticate", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest : error",
        ["Authorization", "Digest", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest : error',
        ["Authorization", "Digest", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Digest": : error',
        ["Digest", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Cookie: a=1; : error",
        ["Cookie", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest a=1, : error",
        ["Authorization", "Digest", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: token: : : error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "token": : : error',
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer : : error",
        ["Authorization", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 64 / Iter 65: Verifier 5 #1 — Dot-prefixed secret keys in Stage 2B
      [
        "Runtime Error: config[0].password = error",
        ["password", "error", "="],
        "Error: Runtime Error: config[[redacted]][redacted]",
      ],
      [
        "Runtime Error: headers[0].cookie: path = error; mode = timeout",
        ["cookie", "path", "error", "mode", "timeout", "="],
        "Error: Runtime Error: headers[[redacted]][redacted]",
      ],
      [
        "Runtime Error: headers[0].authorization: CustomScheme error",
        ["authorization", "CustomScheme", "error"],
        "Error: Runtime Error: headers[[redacted]][redacted]",
      ],
      [
        "Runtime Error: .token: error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 64 / Iter 65: Verifier 5 #2 — SQL typed single-quoted secret keys & E'...' / U&'...' escapes
      [
        "Runtime Error: N'token': error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: r'token': error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: R'Authorization': CustomScheme error",
        ["Authorization", "CustomScheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: b'token': error",
        ["token", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: E'tok\\x65n': error",
        ["tok", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: E'\\164oken': error",
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: E'\\U00000074oken': error",
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: U&'\\0074oken': error",
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: U&'tok\\+000065n': error",
        ["tok", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 64 / Iter 65: Verifier 5 #3 — isNonBase64KvParamAfterKey real kv params
      [
        "Runtime Error: Authorization: Bearer accessToken= error",
        ["Authorization", "Bearer", "accessToken", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer clientId123= error, scope = timeout",
        ["Authorization", "Bearer", "clientId123", "error", "scope", "timeout", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer accessToken= error',
        ["Authorization", "Bearer", "accessToken", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 64 / Iter 65: Verifier 5 #4 — scanCookieExpiresValueEnd Go/JS/HTTP date formats
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=2099-01-01 00:00:00 +0000 UTC; path = error",
        ["Set-Cookie", "sid", "Expires", "2099", "UTC", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=Jan 01, 2099 00:00:00 GMT; path = error",
        ["Set-Cookie", "sid", "Expires", "Jan", "2099", "GMT", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=2099-01-01 00:00:00 GMT +0000; path = error",
        ["Set-Cookie", "sid", "Expires", "2099", "GMT", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Set-Cookie": sid=1; Expires=2099-01-01 00:00:00 +0000 UTC; path = error',
        ["Set-Cookie", "sid", "Expires", "2099", "UTC", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 65 / Iter 66: Verifier 3 & Verifier 5 #5 — Dot-prefixed & dotted-path AUTH_SCHEMES
      [
        "Runtime Error: .Basic= error",
        ["Basic", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: config[0].Digest= error",
        ["Digest", "error", "="],
        "Error: Runtime Error: config[[redacted]][redacted]",
      ],
      [
        "Runtime Error: config[0].Digest=a=1, scope=error",
        ["Digest", "scope", "error", "="],
        "Error: Runtime Error: config[[redacted]][redacted]",
      ],
      [
        "Runtime Error: .OAuth= realm=error, scope=timeout",
        ["OAuth", "realm", "error", "scope", "timeout", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: config.Basic: error",
        ["Basic", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: .config.Basic: error",
        ["Basic", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: config.Digest: error",
        ["Digest", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: headers.Digest: realm = error",
        ["Digest", "realm", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: .headers.Digest: realm = error",
        ["Digest", "realm", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 65 / Iter 66: Verifier 5 #1 — Spaced separator runs before/around auth schemes
      [
        "Runtime Error: Authorization: : Bearer error",
        ["Authorization", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization = = Bearer error",
        ["Authorization", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: : CustomScheme error",
        ["Authorization", "CustomScheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: token: : Bearer error",
        ["token", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": : Bearer error',
        ["Authorization", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": : CustomScheme error',
        ["Authorization", "CustomScheme", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "token": : Bearer error',
        ["token", "Bearer", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest a=1, Bearer : : error",
        ["Authorization", "Digest", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Digest a=1, Bearer : : error',
        ["Authorization", "Digest", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Digest a=1, : : Bearer error",
        ["Authorization", "Digest", "Bearer", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 65 / Iter 66: Verifier 5 #2 — scanCookieExpiresValueEnd Go monotonic clock & UnixDate + comma
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=2099-01-01 00:00:00 +0000 UTC m=+0.000000001; path = error",
        ["Set-Cookie", "sid", "Expires", "2099", "UTC", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Set-Cookie": sid=1; Expires=2099-01-01 00:00:00 +0000 UTC m=+0.000000001; path = error',
        ["Set-Cookie", "sid", "Expires", "2099", "UTC", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=Mon Jan 2 15:04:05 MST 2006, sid2 = error",
        ["Set-Cookie", "sid", "Expires", "MST", "2006", "sid2", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 65 / Iter 66: Verifier 5 #3 — SQL typed U&"..." & unescapeJsonKey \u{...} / non-special \c
      [
        'Runtime Error: U&"tok\\0065n": error',
        ["tok", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: U&"\\0074oken": error',
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: U&"\\+000074oken": error',
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "\\u{74}oken": error',
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: E't\\oken': error",
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "t\\oken": error',
        ["oken", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 65 / Iter 66: Verifier 5 #4 — isNonBase64KvParamAfterKey quoted/grouped value & 16-char KV params
      [
        'Runtime Error: "Authorization": Bearer clientId1234567= "hello world", scope = error',
        ["Authorization", "Bearer", "clientId1234567", "hello", "world", "scope", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer clientId1234567= (a b), scope = error",
        ["Authorization", "Bearer", "clientId1234567", "scope", "error", "=", "("],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer clientId1234567= error",
        ["Authorization", "Bearer", "clientId1234567", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 66 / Iter 67: Verifier 5 Bypass A — Spaced separators after Bearer/Basic before KV params
      [
        "Runtime Error: Authorization: Bearer : realm = local",
        ["Authorization", "Bearer", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer: : realm = local",
        ["Authorization", "Bearer", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer => realm = local, scope = error",
        ["Authorization", "Bearer", "realm", "local", "scope", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Bearer := realm = local",
        ["Authorization", "Bearer", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer : realm = local',
        ["Authorization", "Bearer", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Bearer => realm = local, scope = error',
        ["Authorization", "Bearer", "realm", "local", "scope", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Authorization: Basic : realm = local",
        ["Authorization", "Basic", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "Authorization": Basic : realm = local',
        ["Authorization", "Basic", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Token: Bearer : realm = local",
        ["X-Token", "Bearer", "realm", "local", "="],
        "Error: Runtime Error: [redacted]",
      ],
      // Wave 66 / Iter 67: Verifier 5 Bypass B — Prefixed / compound Basic, Digest, and OAuth credential keys
      [
        "Runtime Error: X-Basic: error",
        ["X-Basic", "Basic", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "X-Basic": error',
        ["X-Basic", "Basic", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: http_basic = error",
        ["http_basic", "basic", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-Digest: realm = error, scope = timeout",
        ["X-Digest", "Digest", "realm", "error", "scope", "timeout", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "X-Digest": realm = error, scope = timeout',
        ["X-Digest", "Digest", "realm", "error", "scope", "timeout", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: X-OAuth: realm = error, scope = timeout",
        ["X-OAuth", "OAuth", "realm", "error", "scope", "timeout", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "X-OAuth": realm = error, scope = timeout',
        ["X-OAuth", "OAuth", "realm", "error", "scope", "timeout", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: oauth2: error",
        ["oauth2", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        'Runtime Error: "oauth2": error',
        ["oauth2", "error"],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: config[0].x_basic = error",
        ["x_basic", "basic", "error", "="],
        "Error: Runtime Error: config[[redacted]][redacted]",
      ],
      [
        "Runtime Error: config[0].x_digest: realm = error",
        ["x_digest", "digest", "realm", "error", "="],
        "Error: Runtime Error: config[[redacted]][redacted]",
      ],
      [
        "Runtime Error: config[0].oauth2: error",
        ["oauth2", "error"],
        "Error: Runtime Error: config[[redacted]][redacted]",
      ],
      // Wave 66 / Iter 67: Verifier 5 Bypass C — scanCookieExpiresValueEnd 1-digit TZ offset, 12h AM/PM, slash dates
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=Thu, 01 Jan 2099 00:00:00 UTC+2, path = error",
        ["Set-Cookie", "sid", "Expires", "2099", "UTC", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=Thu, 01 Jan 2099 12:00:00 AM GMT, path = error",
        ["Set-Cookie", "sid", "Expires", "2099", "GMT", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=01/Jan/2099 00:00:00 GMT, path = error",
        ["Set-Cookie", "sid", "Expires", "2099", "GMT", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
      [
        "Runtime Error: Set-Cookie: sid=1; Expires=2099/01/01 00:00:00 GMT, path = error",
        ["Set-Cookie", "sid", "Expires", "2099", "GMT", "path", "error", "="],
        "Error: Runtime Error: [redacted]",
      ],
    ];
    for (const [msg, forbidden, expectedExact] of cases) {
      const err = new Error(msg);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const got = events.at(-1)?.error ?? "";
      for (const frag of forbidden) {
        expect(got, `msg=${msg} got=${got}`).not.toContain(frag);
      }
      expect(got, `msg=${msg}`).toBe(expectedExact);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("SEC-T5-ITER40-FE-04: maintains strict O(N) performance (<10ms median, <150ms max) on 64 KiB Wave 40 adversarial union payloads", async () => {
  setCloudModeRuntime(true);
  const events: { state?: string; error?: string }[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const perfPayloads64k = [
      `Parser Error: near ${'"a\\" and '.repeat(7000)}"select update delete session 500ms"`,
      `Parser Error: near ${"`a\\` and ".repeat(7000)}\`session 500ms 404 B\``,
      `Binder Error: column ${'"a\\".'.repeat(12000)}"select update delete session 500ms" not found`,
      `Error: 'a\\' ${"user's ".repeat(9000)}'1234ms 500 GiB DB::Exception'`,
      `Error: "${"a".repeat(64000)}\\" HTTP 403 100 MB session valid" 'foo"bar'`,
      `Error: "${"\\".repeat(63999)}" and "select"`,
      (`${'"Authorization":a,('.repeat(30)}${"x".repeat(65536)}`).slice(0, 65536),
      (`${'"Authorization":a,('.repeat(30)}${"x".repeat(65536)}`).slice(0, 65535) + ")",
      (`'a\\\\' ${'"Authorization":a,('.repeat(26)}${"[x]".repeat(22000)}`).slice(0, 65535) + ")",
      (`${'"Authorization":a,('.repeat(30)}${"x".repeat(65536)}`).slice(0, 65536 - 29) + ")".repeat(29),
      `Runtime Error: "Set-Cookie": a${":".repeat(65000)}`,
      `Runtime Error: "Authorization": Digest a${":".repeat(65000)}`,
    ];
    for (const payload of perfPayloads64k) {
      const err = new Error(payload);
      await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
      const trials: number[] = [];
      for (let k = 0; k < 5; k += 1) {
        const t0 = performance.now();
        await expect(measureCloudQuery("dashboard.bundle", async () => { throw err; })).rejects.toBe(err);
        trials.push(performance.now() - t0);
      }
      const sorted = [...trials].sort((a, b) => a - b);
      const median = sorted[Math.floor(sorted.length / 2)]!;
      const max = sorted.at(-1)!;
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(10 * V8_COV_SCALE);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(150);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);
