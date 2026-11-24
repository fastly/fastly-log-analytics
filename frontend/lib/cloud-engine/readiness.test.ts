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
      expect(median, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(5);
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(5);
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
      ['Runtime Error: {"password_hash": "val\\"847291", "pin": server} rejected', ["password_hash", "847291", "pin", "server"], "Error: Runtime Error: [redacted], [redacted] rejected"],
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
      const max = sorted.at(-1)!;
      expect(max, `len=${payload.length} trials=${trials.map((v) => v.toFixed(2)).join(",")}`).toBeLessThan(25);
    }
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
}, 30000);
