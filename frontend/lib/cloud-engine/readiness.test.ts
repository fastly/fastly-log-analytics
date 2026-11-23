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
        ["SecDotTok99", "SecSpaceTok88"],
        "Error: Binder Error: d'Artagnan filter [redacted] and [redacted]",
      ],
      [
        "Binder Error: O'Brien filter '?cid=SecQmark77' failed",
        ["SecQmark77"],
        "Error: Binder Error: O'Brien filter [redacted] failed",
      ],
      [
        "Parser Error: syntax error in 'foo'='SecAdjUnclosed33",
        ["SecAdjUnclosed33", "foo"],
        "Error: Parser Error: syntax error in [redacted]=[redacted]",
      ],
      [
        "Runtime Error: DB::Exception at 12:34:56 for client 2001:db8:85a3::8a2e:370:7334 or 2001:db8::42 or ::ffff:198.51.100.42",
        ["2001:db8", "8a2e", "198.51.100.42"],
        "Error: Runtime Error: DB::Exception at 12:34:56 for client [redacted] or [redacted] or [redacted]",
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
        ["users"],
        "Error: Binder Error: N'Djamena [redacted] failed",
      ],
      [
        "Binder Error: ce n'est '.users' ici",
        ["users"],
        "Error: Binder Error: ce n'est [redacted] ici",
      ],
      [
        "Binder Error: B'Elanna and R'lyeh saw 'SecLore77' vs N'SecNat66'",
        ["SecLore77", "SecNat66"],
        "Error: Binder Error: B'Elanna and R'lyeh saw [redacted] vs [redacted]",
      ],
      [
        "Runtime Error: DB::Exception at 12:34:56 for fe80::1%eth0 or [fe80::2%en0]:443 or 2001:db8:85a3:0:0:8a2e:370:7334:443 at 09:15:00",
        ["fe80", "eth0", "en0", "2001:db8", "8a2e", "7334"],
        "Error: Runtime Error: DB::Exception at 12:34:56 for [redacted] or [[redacted]]:443 or [redacted] at 09:15:00",
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
