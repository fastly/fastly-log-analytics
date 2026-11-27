// @vitest-environment node
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { parquetWriteBuffer } from "hyparquet-writer";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { extractApiError } from "@/lib/api";
import { resetCloudMetadataCache } from "@/lib/cloud-engine/metadata-cache";
import { handleCloudAnalyticalRequest, resetCloudPlatformBundleCache } from "@/lib/cloud-engine/adapter";
import { cloudLogAccounting, cloudQuarantine } from "@/lib/cloud-engine/admin-reads";
import { queryAssetsAggregates } from "@/lib/cloud-engine/assets";
import { cloudCustomFields } from "@/lib/cloud-engine/custom-fields";
import { getDuckDBEngine } from "@/lib/cloud-engine/dashboard/duckdb";
import { handleCacheCollapseDetail, handleInsightAvailability } from "@/lib/cloud-engine/insights";
import { cloudLoggingSettings } from "@/lib/cloud-engine/logging-settings";
import { maskIp } from "@/lib/cloud-engine/pii-mask";
import { CloudBudgetError, ensureCloudRange } from "@/lib/cloud-engine/range-loader";
import { cloudRumUsage } from "@/lib/cloud-engine/rum-wire";
import { cloudServiceStatus } from "@/lib/cloud-engine/service-bridges";
import {
  buildScoringSourceSql,
  clearCloudSessionKeys,
  openSessionToken,
  sealSessionToken,
} from "@/lib/cloud-engine/sessions";
import { resetComputeBillingCache } from "@/lib/cloud-engine/usage/computeUsage";
import { fixtureUsageInput } from "@/lib/cloud-engine/usage/fixtures";
import * as usageSummary from "@/lib/cloud-engine/usage/summary";
import type { Expect, StripIndex, WireParity } from "@/types/api";
import type { components } from "@/types/api.generated";

vi.mock("@/lib/cloud-engine/range-loader", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/cloud-engine/range-loader")>()),
  ensureCloudRange: vi.fn(async () => getDuckDBEngine().readContext()),
}));

vi.mock("@/lib/cloud-engine/active-log-fields", () => ({
  cloudActiveLogFieldIds: vi.fn(
    async () =>
      new Set([
        "timestamp",
        "url",
        "status",
        "cache",
        "resp_bytes",
        "elapsed",
        "ttfb",
        "ottfb",
        "ost",
        "ip",
        "oip",
        "country",
        "city",
        "region",
        "pop",
        "metro",
        "asn",
        "lat",
        "lon",
        "tcp_rtt",
        "ploss",
        "ua",
        "referer",
        "method",
        "host",
        "ja3",
        "ja4",
        "transport",
        "ttl",
        "resp_header_content_encoding",
        "cmcd_sid",
        "cmcd_ot",
        "cmcd_bl",
        "cmcd_br",
        "edge",
        "edge_sid",
        "edge_score",
        "edge_score_l1",
        "edge_score_l2",
        "edge_score_reason",
        "edge_cookie_compliance",
        "ngwaf_signals",
        "waf",
        "waf_sig",
        "p_type",
      ]),
  ),
}));

// Compile-time WireParity type checks between Cloud outputs and generated OpenAPI schemas.
type _ParityChecks = [
  Expect<WireParity<StripIndex<components["schemas"]["BundleResponse"]>, components["schemas"]["BundleResponse"]>>,
  Expect<WireParity<StripIndex<components["schemas"]["SecurityAggregatesResponse"]>, components["schemas"]["SecurityAggregatesResponse"]>>,
  Expect<WireParity<StripIndex<components["schemas"]["RumStatusResponse"]>, components["schemas"]["RumStatusResponse"]>>,
  Expect<WireParity<StripIndex<components["schemas"]["PrefillRatesResponse"]>, components["schemas"]["PrefillRatesResponse"]>>,
  Expect<WireParity<StripIndex<components["schemas"]["MetadataStorageResponse"]>, components["schemas"]["MetadataStorageResponse"]>>,
];
const _parityTypeProof: _ParityChecks | null = null;
void _parityTypeProof;

type JsonSchema = {
  $ref?: string;
  type?: string | string[];
  anyOf?: JsonSchema[];
  oneOf?: JsonSchema[];
  allOf?: JsonSchema[];
  enum?: unknown[];
  const?: unknown;
  minimum?: number;
  maximum?: number;
  required?: string[];
  properties?: Record<string, JsonSchema>;
  additionalProperties?: boolean | JsonSchema;
  items?: JsonSchema;
};

type OpenApiSpec = {
  paths: Record<string, Record<string, { responses?: Record<string, { content?: Record<string, { schema?: JsonSchema }> }> }>>;
  components?: { schemas?: Record<string, JsonSchema> };
};

type ParityVectors = {
  pii_ip_masking: Array<{ input: string; expected: string }>;
  asset_classification: Array<{ url: string; expected_asset_type: string }>;
  waf_signal_filtering: {
    raw_tags: string;
    excluded_prefixes: string[];
    excluded_exact: string[];
    expected_attack_signals: string[];
  };
  cookie_compliance_normalization: Array<{ input: string; expected: string }>;
  cache_collapse_detail: {
    url: string;
    rates: {
      w_hits: number;
      w_miss: number;
      w_pass: number;
      w_total: number;
      b_hits: number;
      b_miss: number;
      b_pass: number;
      b_total: number;
    };
    timeline_rows: Array<{ bucket: string; total_requests: number; real_hits: number; misses: number }>;
    miss_rows: Array<{ timestamp: string; cache: string; pop: string; ip: string; status: number }>;
    expected: {
      url: string;
      window_hit_rate: number;
      baseline_hit_rate: number;
      window_pass_rate_approx: number;
      baseline_pass_rate_approx: number;
      breakdown: { hits: number; misses: number; passes: number; other: number };
      timeline_hit_rate: number;
      timeline_expected_hits: number;
      has_data: boolean;
      masked_ip: string;
      unmasked_ip: string;
    };
  };
};

const REPO_ROOT = resolve(__dirname, "../../..");
const OPENAPI: OpenApiSpec = JSON.parse(readFileSync(resolve(REPO_ROOT, "frontend/openapi.json"), "utf8"));
const VECTORS: ParityVectors = JSON.parse(
  readFileSync(resolve(REPO_ROOT, "tests/contract/fixtures/shared_parity_vectors.json"), "utf8"),
);

function resolveRef(spec: OpenApiSpec, ref: string): JsonSchema {
  if (!ref.startsWith("#/components/schemas/")) {
    throw new Error(`Unsupported $ref: ${ref}`);
  }
  const name = ref.slice("#/components/schemas/".length);
  const schema = spec.components?.schemas?.[name];
  if (!schema) throw new Error(`Missing schema ${name}`);
  return schema;
}

function validateValueAgainstSchema(
  spec: OpenApiSpec,
  value: unknown,
  schema: JsonSchema,
  path = "$",
  depth = 0,
): string[] {
  if (depth > 25) return [];
  if (schema.$ref) {
    return validateValueAgainstSchema(spec, value, resolveRef(spec, schema.$ref), path, depth + 1);
  }
  if (schema.allOf) {
    return schema.allOf.flatMap((sub) => validateValueAgainstSchema(spec, value, sub, path, depth + 1));
  }
  if (schema.anyOf) {
    for (const branch of schema.anyOf) {
      if (validateValueAgainstSchema(spec, value, branch, path, depth + 1).length === 0) return [];
    }
    return [`${path}: value did not satisfy anyOf`];
  }
  if (schema.oneOf) {
    for (const branch of schema.oneOf) {
      if (validateValueAgainstSchema(spec, value, branch, path, depth + 1).length === 0) return [];
    }
    return [`${path}: value did not satisfy oneOf`];
  }
  if ("const" in schema && value !== schema.const) {
    return [`${path}: expected const ${JSON.stringify(schema.const)}, got ${JSON.stringify(value)}`];
  }
  if (schema.enum && !schema.enum.includes(value)) {
    return [`${path}: expected one of ${JSON.stringify(schema.enum)}, got ${JSON.stringify(value)}`];
  }

  const allowedTypes = schema.type ? (Array.isArray(schema.type) ? schema.type : [schema.type]) : null;
  if (allowedTypes) {
    const actualType =
      value === null
        ? "null"
        : Array.isArray(value)
          ? "array"
          : typeof value === "number"
            ? Number.isInteger(value)
              ? "integer"
              : "number"
            : typeof value;
    const matches = allowedTypes.some(
      (t) => t === actualType || (t === "number" && actualType === "integer"),
    );
    if (!matches) {
      return [`${path}: expected type ${allowedTypes.join("|")}, got ${actualType}`];
    }
  }

  if (typeof value === "number") {
    if (schema.minimum !== undefined && value < schema.minimum) {
      return [`${path}: ${value} < minimum ${schema.minimum}`];
    }
    if (schema.maximum !== undefined && value > schema.maximum) {
      return [`${path}: ${value} > maximum ${schema.maximum}`];
    }
  }

  if (Array.isArray(value) && schema.items) {
    const errs: string[] = [];
    for (let i = 0; i < Math.min(value.length, 10); i++) {
      errs.push(...validateValueAgainstSchema(spec, value[i], schema.items, `${path}[${i}]`, depth + 1));
    }
    return errs;
  }

  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const errs: string[] = [];
    for (const reqKey of schema.required ?? []) {
      if (!(reqKey in record)) {
        errs.push(`${path}: missing required key '${reqKey}'`);
      }
    }
    const props = schema.properties ?? {};
    for (const [key, val] of Object.entries(record)) {
      if (key in props) {
        errs.push(...validateValueAgainstSchema(spec, val, props[key], `${path}.${key}`, depth + 1));
      } else if (schema.additionalProperties === false) {
        errs.push(`${path}.${key}: unexpected additional property`);
      } else if (schema.additionalProperties && typeof schema.additionalProperties === "object") {
        errs.push(
          ...validateValueAgainstSchema(spec, val, schema.additionalProperties, `${path}.${key}`, depth + 1),
        );
      }
    }
    return errs;
  }

  return [];
}

function getResponseSchema(method: string, openapiPath: string): JsonSchema {
  const op = OPENAPI.paths[openapiPath]?.[method.toLowerCase()];
  if (!op) throw new Error(`Route missing in openapi.json: ${method} ${openapiPath}`);
  const schema = op.responses?.["200"]?.content?.["application/json"]?.schema;
  if (!schema) throw new Error(`200 JSON schema missing in openapi.json: ${method} ${openapiPath}`);
  return schema;
}

function buildPopulatedRequestParquet(): ArrayBuffer {
  const baseMs = Date.parse("2026-11-05T12:00:00Z");
  const count = 8;
  const timestamps = Array.from({ length: count }, (_, i) => new Date(baseMs + i * 60_000));
  const tsMs = Array.from({ length: count }, (_, i) => BigInt(baseMs + i * 60_000));
  const urls = [
    "/static/app.mjs?v=42#top",
    "/hero.WEBP?w=800",
    "/docs/spec.PDF#page=2",
    "/fonts/inter.woff2?subset=latin",
    "/hls/seg.m4s?seq=9",
    "/api/v1/checkout?step=2",
    "/api/v1/checkout?step=2",
    "/api/v1/checkout?step=2",
  ];
  const caches = ["HIT", "MISS", "HIT", "HIT", "PASS", "MISS", "MISS", "MISS"];
  const ips = [
    "198.51.100.42",
    "2001:db8:abcd:1234:5678::1",
    "203.0.113.9",
    "198.51.100.42",
    "198.51.100.42",
    "203.0.113.9",
    "203.0.113.9",
    "203.0.113.9",
  ];
  return parquetWriteBuffer({
    columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: timestamps },
      { name: "ts_ms", type: "INT64", data: tsMs },
      { name: "service_id", type: "STRING", data: Array(count).fill("svc-1") },
      { name: "status", type: "INT32", data: [200, 200, 200, 200, 200, 502, 200, 200] },
      { name: "cache", type: "STRING", data: caches },
      { name: "cache_status", type: "STRING", data: caches },
      { name: "url", type: "STRING", data: urls },
      { name: "host", type: "STRING", data: Array(count).fill("example.com") },
      { name: "method", type: "STRING", data: Array(count).fill("GET") },
      { name: "proto", type: "STRING", data: Array(count).fill("HTTP/2") },
      { name: "transport", type: "STRING", data: Array(count).fill("quic") },
      { name: "resp_bytes", type: "INT64", data: Array(count).fill(BigInt(2048)) },
      { name: "resp_body_size", type: "INT64", data: Array(count).fill(BigInt(1800)) },
      { name: "req_header_bytes", type: "INT64", data: Array(count).fill(BigInt(512)) },
      { name: "elapsed", type: "DOUBLE", data: [12000, 25000, 15000, 10000, 18000, 80000, 30000, 22000] },
      { name: "ttfb", type: "DOUBLE", data: [0.01, 0.02, 0.01, 0.01, 0.015, 0.06, 0.02, 0.015] },
      { name: "ottfb", type: "DOUBLE", data: [null, 40, null, null, 30, 120, 45, 35] },
      { name: "ost", type: "INT32", data: [null, 200, null, null, 200, 502, 200, 200] },
      { name: "ip", type: "STRING", data: ips },
      { name: "oip", type: "STRING", data: Array(count).fill("192.0.2.55") },
      { name: "country", type: "STRING", data: Array(count).fill("US") },
      { name: "city", type: "STRING", data: Array(count).fill("San Jose") },
      { name: "region", type: "STRING", data: Array(count).fill("CA") },
      { name: "pop", type: "STRING", data: Array(count).fill("SJC") },
      { name: "metro", type: "INT64", data: Array(count).fill(BigInt(807)) },
      { name: "asn", type: "INT64", data: Array(count).fill(BigInt(15169)) },
      { name: "lat", type: "DOUBLE", data: Array(count).fill(37.33) },
      { name: "lon", type: "DOUBLE", data: Array(count).fill(-121.88) },
      { name: "tcp_rtt", type: "DOUBLE", data: Array(count).fill(18.5) },
      { name: "ploss", type: "DOUBLE", data: Array(count).fill(0.01) },
      { name: "ua", type: "STRING", data: Array(count).fill("Mozilla/5.0") },
      { name: "referer", type: "STRING", data: Array(count).fill("https://example.com/") },
      { name: "ja3", type: "STRING", data: Array(count).fill("771,4865-4866") },
      { name: "ja4", type: "STRING", data: Array(count).fill("t13d1516h2_8daaf615") },
      { name: "ttl", type: "DOUBLE", data: Array(count).fill(600) },
      { name: "content_encoding", type: "STRING", data: Array(count).fill("br") },
      { name: "resp_header_content_encoding", type: "STRING", data: Array(count).fill("br") },
      { name: "edge", type: "BOOLEAN", data: Array(count).fill(true) },
      { name: "p_type", type: "STRING", data: Array(count).fill("none") },
      { name: "cmcd_sid", type: "STRING", data: Array(count).fill("media-sess-1") },
      { name: "cmcd_ot", type: "STRING", data: Array(count).fill("v") },
      { name: "cmcd_bl", type: "DOUBLE", data: Array(count).fill(4500) },
      { name: "cmcd_br", type: "DOUBLE", data: Array(count).fill(2800) },
      { name: "edge_sid", type: "STRING", data: Array(count).fill("esid-01") },
      { name: "edge_score", type: "DOUBLE", data: [10, 20, 82, 85, 88, 91, 95, 15] },
      { name: "edge_score_l1", type: "DOUBLE", data: [5, 10, 40, 42, 44, 45, 48, 8] },
      { name: "edge_score_l2", type: "DOUBLE", data: [5, 10, 42, 43, 44, 46, 47, 7] },
      { name: "edge_score_reason", type: "STRING", data: Array(count).fill("velocity") },
      { name: "edge_cookie_compliance", type: "STRING", data: Array(count).fill("missin") },
      { name: "ngwaf_signals", type: "STRING", data: Array(count).fill(VECTORS.waf_signal_filtering.raw_tags) },
    ],
    codec: "SNAPPY",
  });
}

function buildRumVitalsParquet(): ArrayBuffer {
  const baseMs = Date.parse("2026-11-05T12:00:00Z");
  return parquetWriteBuffer({
    columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(baseMs)] },
      { name: "ts_ms", type: "INT64", data: [BigInt(baseMs)] },
      { name: "service_id", type: "STRING", data: ["svc-1"] },
      { name: "page_url", type: "STRING", data: ["https://example.com/"] },
      { name: "metric_name", type: "STRING", data: ["LCP"] },
      { name: "metric_value", type: "DOUBLE", data: [1850] },
      { name: "session_id", type: "STRING", data: ["rum-s1"] },
      { name: "client_ip", type: "STRING", data: ["198.51.100.42"] },
      { name: "country", type: "STRING", data: ["US"] },
    ],
    codec: "SNAPPY",
  });
}

function buildRumErrorsParquet(): ArrayBuffer {
  const baseMs = Date.parse("2026-11-05T12:00:00Z");
  return parquetWriteBuffer({
    columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(baseMs)] },
      { name: "ts_ms", type: "INT64", data: [BigInt(baseMs)] },
      { name: "service_id", type: "STRING", data: ["svc-1"] },
      { name: "page_url", type: "STRING", data: ["https://example.com/"] },
      { name: "error_type", type: "STRING", data: ["TypeError"] },
      { name: "error_value", type: "STRING", data: ["boom"] },
      { name: "client_ip", type: "STRING", data: ["198.51.100.42"] },
    ],
    codec: "SNAPPY",
  });
}

function stubCloudFetch(role: "admin" | "analyst-no-pii" = "admin") {
  const startUnix = Math.floor(Date.parse("2026-11-05T00:00:00Z") / 1000);
  const serviceRecord = {
    service_id: "svc-1",
    name: "Primary",
    domain: "example.com",
    ngwaf_configured: false,
    fos_prefix: "logs/",
    log_period: 60,
    sample_rate: 100,
    edge_only: false,
    rum: { enabled: true, sample_rate: 100, faro_version: "1.12.0" },
    cmcd: { enabled: true },
    scoring: { enabled: true, matrix_version: "v1" },
    active_log_field_ids: [
      "timestamp",
      "url",
      "status",
      "cache",
      "resp_bytes",
      "ip",
      "oip",
      "country",
      "pop",
      "edge_sid",
      "edge_score",
      "edge_cookie_compliance",
      "cmcd_sid",
    ],
    log_fields: { custom_fields: [] },
  };
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const rawUrl = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const pathname = new URL(rawUrl, "http://localhost").pathname;
    const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();

    if (pathname === "/api/health") {
      return Response.json({ status: "ok", mode: "cloud" });
    }
    if (pathname === "/api/bootstrap") {
      return Response.json({
        services: [serviceRecord],
        active_service_id: "svc-1",
        share: {
          mode: role === "analyst-no-pii" ? "viewer" : "owner",
          role,
          mask_ips: role === "analyst-no-pii",
        },
        capabilities: { edition: "cloud", supports_metadata_storage: false },
      });
    }
    if (pathname === "/api/services") {
      return Response.json({ services: [serviceRecord] });
    }
    if (pathname === "/api/log-extents") {
      return Response.json({
        earliest_log_at: "2026-11-05T11:00:00Z",
        latest_log_at: "2026-11-05T13:00:00Z",
        total_files: 1,
      });
    }
    if (pathname === "/api/log-fields/catalog") {
      return Response.json({
        groups: [{ id: "core", label: "Core", description: "Core log fields" }],
        fields: [
          { id: "timestamp", label: "Timestamp", description: "", sql_type: "TIMESTAMP", duckdb_type: "TIMESTAMP", group: "core" },
          { id: "url", label: "URL", description: "", sql_type: "VARCHAR", duckdb_type: "VARCHAR", group: "core" },
          { id: "ip", label: "Client IP", description: "", sql_type: "VARCHAR", duckdb_type: "VARCHAR", group: "core" },
          { id: "status", label: "Status", description: "", sql_type: "INTEGER", duckdb_type: "INTEGER", group: "core" },
        ],
        presets: {},
        insights: [],
      });
    }
    if (pathname === "/api/cron-runs") {
      return Response.json({ runs: [], retention_days: 30, latest_run: null, summary_last_hour: {} });
    }
    if (pathname === "/api/services/svc-1/cmcd/status") {
      return Response.json({ enabled: true, configured: true });
    }
    if (pathname === "/api/services/svc-1/rum/status") {
      return Response.json({ enabled: true, supports_storage_purge: false });
    }
    if (pathname === "/api/rum/versions") {
      return Response.json({ versions: ["1.12.0"], default: "1.12.0" });
    }
    if (pathname === "/api/services/svc-1/rum/versions") {
      return Response.json({ current_version: "1.12.0", versions: ["1.12.0"] });
    }
    if (pathname === "/api/services/svc-1/rum/beacon-health") {
      return Response.json({ enabled: true, setup_complete: true, recent_beacons: 1 });
    }
    if (pathname === "/api/services/svc-1/logging-settings") {
      return Response.json({ period: 60, gzip_level: 6, path: "/logs" });
    }
    if (pathname === "/api/admin/services/svc-1") {
      return Response.json({ service: serviceRecord, generation: "gen-1" });
    }
    if (pathname === "/api/admin/services/svc-1/status") {
      return Response.json({ logging_enabled: true, service_id: "svc-1", storage_configured: true });
    }
    if (pathname === "/api/admin/debug-settings") {
      return Response.json({
        enabled: true,
        browser_query_telemetry: false,
        dev_telemetry_enabled: false,
        dev_telemetry_available: false,
        retention_days: 30,
      });
    }
    if (pathname === "/api/admin/share/banner") {
      return Response.json({ banner: null, active: false });
    }
    if (pathname === "/api/admin/share/oauth-providers") {
      return Response.json({ providers: [] });
    }
    if (pathname === "/api/admin/share/invites") {
      return Response.json({ invites: [] });
    }
    if (pathname === "/api/admin/share/sessions") {
      return Response.json({ users: [] });
    }
    if (pathname.includes("logged-traffic") || pathname === "/api/admin/log-accounting" || pathname === "/api/admin/log-accounting/svc-1") {
      return Response.json({
        from: startUnix,
        to: startUnix + 86400,
        days: [{ day: startUnix, expected_rows: 10, ingested_rows: 8 }],
      });
    }
    if (pathname === "/api/admin/quarantine" || pathname.includes("quarantine")) {
      return Response.json({ items: [], total: 0, complete: true, next_cursor: null });
    }
    if (pathname === "/api/admin/usage/service-stats") {
      return Response.json({
        service_id: "svc-1",
        from: startUnix,
        to: startUnix + 7200,
        sample_rate: 100,
        edge_only: false,
        hours: [{ start_time: startUnix, requests: 10, bandwidth_bytes: 16384, hits: 5 }],
      });
    }
    if (pathname === "/api/admin/usage/compute-stats") {
      return Response.json({
        source: "fastly",
        requests: 20,
        billed_time_ms: 100,
        app_requests: 15,
        worker_requests: 5,
        app_vcpu_ms: 60,
        worker_vcpu_ms: 40,
        hours: [{ start_time: startUnix, app_requests: 15, worker_requests: 5, app_vcpu_ms: 60, worker_vcpu_ms: 40, billed_ms: 100 }],
      });
    }
    if (pathname === "/api/usage/geography" || pathname === "/api/admin/usage/geography") {
      return Response.json({
        _is_cached: false,
        source: "fastly",
        group_by: "region",
        data: [
          {
            location: "us-east",
            requests: 10,
            bandwidth_bytes: 2048,
            request_share: 1.0,
            bandwidth_share: 1.0,
          },
        ],
        requests: 10,
        bandwidth_bytes: 2048,
      });
    }
    if (pathname === "/api/usage/platform" || pathname === "/api/admin/usage/platform") {
      return Response.json({
        _is_cached: false,
        source: "fastly",
        storage: { status: "ok", scope: "account", observed_at: "2026-11-05T12:00:00Z", bytes: 4096, buckets: 1, gb_hours: 1.0 },
        compute: { status: "ok", scope: "app_and_worker", requests: 20, billed_time_ms: 100 },
        ngwaf: { status: "not_configured", reason: "no_workspace" },
      });
    }
    if (pathname === "/api/services/svc-1/scoring/status") {
      return Response.json({ enabled: true, scoring_service_id: "score-1", scoring_domain: "score.example.com" });
    }
    if (pathname === "/api/services/svc-1/scoring/exclude-regex") {
      return Response.json({ current: "", exclude_url_regex: "" });
    }
    if (pathname === "/api/services/svc-1/scoring/enforce-status-code") {
      return Response.json({ current: 429, status_code: 429 });
    }
    if (pathname === "/api/services/svc-1/scoring/enforce-threshold") {
      return Response.json({ current: 85, threshold: 85 });
    }
    if (pathname === "/api/services/svc-1/scoring/threshold") {
      return Response.json({ threshold: 80 });
    }
    if (pathname === "/api/services/svc-1/scoring/l2-enforce") {
      return Response.json({ l2_enforce: false });
    }
    if (pathname === "/api/services/svc-1/scoring/labels") {
      return Response.json({ labels: [] });
    }
    if (pathname === "/api/services/svc-1/scoring/audit") {
      return Response.json({ entries: [] });
    }
    if (pathname === "/api/services/svc-1/scoring/config") {
      return Response.json({
        enabled: true,
        threshold: { threshold: 80 },
        l2_enforce: { l2_enforce: false },
        enforce_threshold: { current: 85 },
        enforce_status_code: { current: 429 },
        exclude_regex: { current: "" },
      });
    }
    if (pathname === "/api/services/svc-1/scoring/health") {
      return Response.json({ ok: true, status: "healthy" });
    }
    if (method === "GET" && pathname.includes("/api/services/svc-1/scoring/")) {
      return Response.json({ ok: true });
    }
    if (pathname === "/api/files" || pathname === "/api/catalog" || pathname.endsWith("/files")) {
      return Response.json({ files: [], summary_files: [], next_cursor: null, complete: true, has_more: false });
    }
    return Response.json({
      latest_log_at: "2026-11-05T13:00:00Z",
      files: [],
      summary_files: [],
      rows: [],
      next_cursor: null,
      complete: true,
      has_more: false,
      truncated: false,
    });
  });
}

function setupUsageSpies() {
  const from = Date.parse("2026-11-04T00:00:00Z");
  const to = Date.parse("2026-11-05T14:00:00Z");
  const input = {
    ...fixtureUsageInput,
    now: to,
    rows: {
      fos_usage: [
        { ...fixtureUsageInput.rows.fos_usage[0], timestamp: from + 3_600_000, op_count: 5, bytes_out: 2048 },
      ],
      api_telemetry: [],
      cron_runs: [],
      cron_ticks: [],
    },
  };
  const summary = usageSummary.summarizeUsage(input);
  vi.spyOn(usageSummary, "fetchUsageSettings").mockResolvedValue(input.settings);
  vi.spyOn(usageSummary, "fetchUsageSummary").mockResolvedValue({ ...summary, input, summary, lines: [], totalUsd: 0 });
  vi.spyOn(usageSummary, "fetchUsageKindWithCoverage").mockResolvedValue({ rows: input.rows.fos_usage, complete: true });
  vi.spyOn(usageSummary, "fetchCloudStorageInventory").mockResolvedValue({
    intervals: [],
    complete: true,
    snapshotId: "snap-1",
    observedAt: to,
    provenance: "cloud:inventory",
  });
  vi.spyOn(usageSummary, "fetchIngestDeletedIntervals").mockResolvedValue({
    intervals: [],
    outputs: [],
    deletedParquetKeys: new Set<string>(),
    bytes: 0,
    complete: true,
    provenance: "cloud:ingest_files",
  });
}

describe("Cross-edition OpenAPI 3.1 contract parity (Cloud Edition)", () => {
  beforeAll(async () => {
    const engine = getDuckDBEngine();
    await engine.reset();
    const lease = engine.lease();
    try {
      await lease.ingestFile("fixture:req", "req.parquet", buildPopulatedRequestParquet());
    } finally {
      lease.release();
    }
    await engine.syncRumFiles(
      [{ scopedKey: "fixture:vitals", key: "vitals.parquet", bytes: buildRumVitalsParquet() }],
      [{ scopedKey: "fixture:errors", key: "errors.parquet", bytes: buildRumErrorsParquet() }],
    );
  });

  afterAll(async () => {
    await getDuckDBEngine().reset();
  });

  beforeEach(() => {
    vi.stubGlobal("fetch", stubCloudFetch("admin"));
    setupUsageSpies();
  });

  afterEach(() => {
    clearCloudSessionKeys();
    resetComputeBillingCache();
    resetCloudPlatformBundleCache();
    resetCloudMetadataCache();
    usageSummary.resetUsageSettingsCache({ abortActive: false });
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("validates 60+ shared (method, path) routes against frontend/openapi.json schemas with populated data", async () => {
    const rangeBody = { start_time: "2026-11-05T11:30:00Z", end_time: "2026-11-05T13:30:00Z" };
    const rangeQs = `start_time=${encodeURIComponent(rangeBody.start_time)}&end_time=${encodeURIComponent(rangeBody.end_time)}`;

    type RouteCase = {
      method: "GET" | "POST" | "PATCH";
      openapiPath: string;
      url: string;
      body?: Record<string, unknown>;
      call?: (req: Request) => Promise<Response | null>;
    };

    const routeCases: RouteCase[] = [
      // Analytical Core
      { method: "POST", openapiPath: "/api/dashboard/bundle", url: "/api/dashboard/bundle", body: rangeBody },
      { method: "POST", openapiPath: "/api/dashboard/aggregates", url: "/api/dashboard/aggregates", body: rangeBody },
      { method: "POST", openapiPath: "/api/dashboard/field-values", url: "/api/dashboard/field-values", body: { field: "country", ...rangeBody } },
      // Origin
      { method: "POST", openapiPath: "/api/origin/aggregates", url: "/api/origin/aggregates", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/summary", url: "/api/origin/summary", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/timeseries", url: "/api/origin/timeseries", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/slow-urls", url: "/api/origin/slow-urls", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/status-codes", url: "/api/origin/status-codes", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/path-breakdown", url: "/api/origin/path-breakdown", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/pop-latency", url: "/api/origin/pop-latency", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/ip-health", url: "/api/origin/ip-health", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/shielding-analysis", url: "/api/origin/shielding-analysis", body: rangeBody },
      { method: "POST", openapiPath: "/api/origin/drilldown", url: "/api/origin/drilldown", body: { dimension: "pop", ...rangeBody } },
      // Performance, Assets, Value, Network
      { method: "POST", openapiPath: "/api/performance/aggregates", url: "/api/performance/aggregates", body: rangeBody },
      { method: "POST", openapiPath: "/api/assets/aggregates", url: "/api/assets/aggregates", body: rangeBody },
      { method: "POST", openapiPath: "/api/value/summary", url: "/api/value/summary", body: rangeBody },
      { method: "POST", openapiPath: "/api/network-health", url: "/api/network-health", body: rangeBody },
      { method: "POST", openapiPath: "/api/network-quality", url: "/api/network-quality", body: rangeBody },
      { method: "POST", openapiPath: "/api/network/bundle", url: "/api/network/bundle", body: rangeBody },
      { method: "GET", openapiPath: "/api/network/pop-health", url: `/api/network/pop-health?${rangeQs}` },
      // Security
      { method: "POST", openapiPath: "/api/security/aggregates", url: "/api/security/aggregates", body: rangeBody },
      { method: "POST", openapiPath: "/api/security/bundle", url: "/api/security/bundle", body: rangeBody },
      { method: "GET", openapiPath: "/api/security/threat-intel", url: `/api/security/threat-intel?${rangeQs}` },
      // Insights
      { method: "GET", openapiPath: "/api/insight-availability", url: "/api/insight-availability" },
      { method: "POST", openapiPath: "/api/insights", url: "/api/insights", body: { service_id: "svc-1", window_size_hrs: 24, baseline_hours: 168 } },
      { method: "POST", openapiPath: "/api/insights/cache-collapse-detail", url: "/api/insights/cache-collapse-detail", body: { url: "/api/v1/checkout", window_size_hrs: 24, baseline_hours: 168 } },
      // Sessions & Scoring
      { method: "POST", openapiPath: "/api/sessions", url: "/api/sessions", body: rangeBody },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/analytics", url: "/api/services/svc-1/scoring/analytics?since_hours=24" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/compliance-breakdown", url: "/api/services/svc-1/scoring/compliance-breakdown?since_hours=24" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/evaluation", url: "/api/services/svc-1/scoring/evaluation" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/evaluation/per-reason", url: "/api/services/svc-1/scoring/evaluation/per-reason" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/curves", url: "/api/services/svc-1/scoring/curves" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/latency-timeseries", url: "/api/services/svc-1/scoring/latency-timeseries?since_hours=24" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/score-distribution", url: "/api/services/svc-1/scoring/score-distribution?since_hours=24" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/threshold-preview", url: "/api/services/svc-1/scoring/threshold-preview?threshold=75&since_hours=24" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/top-flagged", url: "/api/services/svc-1/scoring/top-flagged?since_hours=24" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/sessions/{sid}/events", url: "/api/services/svc-1/scoring/sessions/esid-01/events?since_days=7" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/status", url: "/api/services/svc-1/scoring/status" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/exclude-regex", url: "/api/services/svc-1/scoring/exclude-regex" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/enforce-status-code", url: "/api/services/svc-1/scoring/enforce-status-code" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/enforce-threshold", url: "/api/services/svc-1/scoring/enforce-threshold" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/labels", url: "/api/services/svc-1/scoring/labels" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/audit", url: "/api/services/svc-1/scoring/audit" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/config", url: "/api/services/svc-1/scoring/config" },
      { method: "GET", openapiPath: "/api/services/{service_id}/scoring/health", url: "/api/services/svc-1/scoring/health" },
      // CMCD & RUM
      { method: "POST", openapiPath: "/api/cmcd/aggregates", url: "/api/cmcd/aggregates", body: rangeBody },
      { method: "GET", openapiPath: "/api/services/{service_id}/rum/analytics", url: `/api/services/svc-1/rum/analytics?${rangeQs}` },
      { method: "GET", openapiPath: "/api/services/{service_id}/rum/live-events", url: `/api/services/svc-1/rum/live-events?${rangeQs}` },
      { method: "GET", openapiPath: "/api/services/{service_id}/rum/beacon-health", url: "/api/services/svc-1/rum/beacon-health" },
      { method: "GET", openapiPath: "/api/services/{service_id}/rum/request-log", url: `/api/services/svc-1/rum/request-log?${rangeQs}` },
      // Usage
      { method: "GET", openapiPath: "/api/usage/current-storage", url: "/api/usage/current-storage?days=30" },
      { method: "GET", openapiPath: "/api/usage/operations", url: "/api/usage/operations?days=7&by=hour" },
      { method: "GET", openapiPath: "/api/usage/bandwidth", url: "/api/usage/bandwidth?days=7&by=hour" },
      { method: "GET", openapiPath: "/api/usage/log-activity", url: "/api/usage/log-activity?days=7&by=hour" },
      { method: "GET", openapiPath: "/api/usage/prefill", url: "/api/usage/prefill" },
      { method: "GET", openapiPath: "/api/usage/prefill/rates", url: "/api/usage/prefill/rates" },
      { method: "GET", openapiPath: "/api/usage/compute", url: "/api/usage/compute?days=7&by=hour" },
      { method: "GET", openapiPath: "/api/usage/geography", url: `/api/usage/geography?start=${encodeURIComponent(rangeBody.start_time)}&end=${encodeURIComponent(rangeBody.end_time)}&group_by=region` },
      { method: "GET", openapiPath: "/api/usage/platform", url: `/api/usage/platform?start=${encodeURIComponent(rangeBody.start_time)}&end=${encodeURIComponent(rangeBody.end_time)}` },
      { method: "GET", openapiPath: "/api/usage/rum-breakdown", url: `/api/usage/rum-breakdown?${rangeQs}` },
      // Query, Admin, Share, Telemetry Beacons & Cloud Bridges
      { method: "GET", openapiPath: "/api/presets", url: "/api/presets" },
      { method: "GET", openapiPath: "/api/query/schema/{service_id}", url: "/api/query/schema/svc-1" },
      { method: "POST", openapiPath: "/api/query", url: "/api/query", body: { sql: "SELECT status, COUNT(*) AS requests FROM logs GROUP BY status", ...rangeBody } },
      { method: "GET", openapiPath: "/api/admin/metadata-storage", url: "/api/admin/metadata-storage" },
      { method: "GET", openapiPath: "/api/admin/debug-settings", url: "/api/admin/debug-settings" },
      { method: "PATCH", openapiPath: "/api/admin/debug-settings", url: "/api/admin/debug-settings", body: { enabled: true } },
      { method: "GET", openapiPath: "/api/admin/share/banner", url: "/api/admin/share/banner" },
      { method: "GET", openapiPath: "/api/admin/share/oauth-providers", url: "/api/admin/share/oauth-providers" },
      { method: "GET", openapiPath: "/api/admin/share/status", url: "/api/admin/share/status" },
      { method: "GET", openapiPath: "/api/admin/share/audit-logs", url: "/api/admin/share/audit-logs?limit=10" },
      { method: "POST", openapiPath: "/api/web-vitals", url: "/api/web-vitals", body: { name: "LCP", value: 1200 } },
      { method: "POST", openapiPath: "/api/ux-events", url: "/api/ux-events", body: { event: "page_view" } },
      { method: "GET", openapiPath: "/api/admin/log-accounting", url: "/api/admin/log-accounting", call: cloudLogAccounting },
      { method: "GET", openapiPath: "/api/admin/quarantine", url: "/api/admin/quarantine", call: cloudQuarantine },
      { method: "GET", openapiPath: "/api/services/{service_id}/custom-fields", url: "/api/services/svc-1/custom-fields", call: cloudCustomFields },
      { method: "GET", openapiPath: "/api/services/{service_id}/rum/status", url: "/api/services/svc-1/rum/status", call: (req) => cloudServiceStatus(req, { role: "admin", isAnalyst: false }) },
      { method: "GET", openapiPath: "/api/services/{service_id}/rum/versions", url: "/api/services/svc-1/rum/versions", call: (req) => cloudServiceStatus(req, { role: "admin", isAnalyst: false }) },
      { method: "GET", openapiPath: "/api/services/{service_id}/cmcd/status", url: "/api/services/svc-1/cmcd/status", call: (req) => cloudServiceStatus(req, { role: "admin", isAnalyst: false }) },
      { method: "GET", openapiPath: "/api/services/{service_id}/logging-settings", url: "/api/services/svc-1/logging-settings", call: (req) => cloudLoggingSettings(req, () => Response.json({ id: "job-1", status: "complete" })) },
      { method: "GET", openapiPath: "/api/usage/rum-breakdown", url: `/api/usage/rum-breakdown?${rangeQs}`, call: () => cloudRumUsage({ service_id: "svc-1", start_time: rangeBody.start_time, end_time: rangeBody.end_time }, 0.0025).then((d) => Response.json(d)) },
    ];

    expect(routeCases.length).toBeGreaterThanOrEqual(65);

    const failures: string[] = [];
    let sessionToken: string | null = null;

    for (const tc of routeCases) {
      const schema = getResponseSchema(tc.method, tc.openapiPath);
      const req = new Request(`http://localhost${tc.url}`, {
        method: tc.method,
        headers: {
          "content-type": "application/json",
          "x-service-id": "svc-1",
        },
        ...(tc.body ? { body: JSON.stringify(tc.body) } : {}),
      });
      const res = tc.call ? await tc.call(req) : await handleCloudAnalyticalRequest(req);
      if (!res) {
        failures.push(`${tc.method} ${tc.openapiPath}: returned null`);
        continue;
      }
      if (res.status !== 200) {
        failures.push(`${tc.method} ${tc.openapiPath}: HTTP ${res.status} (${await res.text()})`);
        continue;
      }
      const payload = await res.json();
      if (tc.openapiPath === "/api/sessions" && Array.isArray((payload as { sessions?: Array<{ session_token?: string }> }).sessions)) {
        sessionToken = (payload as { sessions: Array<{ session_token?: string }> }).sessions[0]?.session_token ?? null;
      }
      const errs = validateValueAgainstSchema(OPENAPI, payload, schema, `${tc.method} ${tc.openapiPath}`);
      failures.push(...errs);
    }

    // Exercise /api/sessions/detail with the live sealed session_token from /api/sessions
    expect(sessionToken).toBeTruthy();
    const detailRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/sessions/detail", {
        method: "POST",
        headers: { "content-type": "application/json", "x-service-id": "svc-1" },
        body: JSON.stringify({ session_token: sessionToken }),
      }),
    );
    expect(detailRes?.status).toBe(200);
    const detailPayload = await detailRes!.json();
    failures.push(
      ...validateValueAgainstSchema(
        OPENAPI,
        detailPayload,
        getResponseSchema("POST", "/api/sessions/detail"),
        "POST /api/sessions/detail",
      ),
    );

    expect(failures).toEqual([]);
  });

  it("validates 0-row empty dataset responses against OpenAPI 3.1 schemas", async () => {
    await getDuckDBEngine().reset();
    try {
      const rangeBody = { start_time: "2026-11-05T11:30:00Z", end_time: "2026-11-05T13:30:00Z" };
      const emptyCases: Array<{ method: "GET" | "POST"; openapiPath: string; url: string; body?: Record<string, unknown> }> = [
        { method: "POST", openapiPath: "/api/dashboard/bundle", url: "/api/dashboard/bundle", body: rangeBody },
        { method: "POST", openapiPath: "/api/dashboard/aggregates", url: "/api/dashboard/aggregates", body: rangeBody },
        { method: "POST", openapiPath: "/api/origin/aggregates", url: "/api/origin/aggregates", body: rangeBody },
        { method: "POST", openapiPath: "/api/performance/aggregates", url: "/api/performance/aggregates", body: rangeBody },
        { method: "POST", openapiPath: "/api/assets/aggregates", url: "/api/assets/aggregates", body: rangeBody },
        { method: "POST", openapiPath: "/api/value/summary", url: "/api/value/summary", body: rangeBody },
        { method: "POST", openapiPath: "/api/network/bundle", url: "/api/network/bundle", body: rangeBody },
        { method: "POST", openapiPath: "/api/security/aggregates", url: "/api/security/aggregates", body: rangeBody },
        { method: "POST", openapiPath: "/api/cmcd/aggregates", url: "/api/cmcd/aggregates", body: rangeBody },
        { method: "POST", openapiPath: "/api/sessions", url: "/api/sessions", body: rangeBody },
        { method: "POST", openapiPath: "/api/insights", url: "/api/insights", body: { service_id: "svc-1", window_size_hrs: 24, baseline_hours: 168 } },
        { method: "GET", openapiPath: "/api/services/{service_id}/rum/analytics", url: "/api/services/svc-1/rum/analytics" },
      ];
      for (const tc of emptyCases) {
        const res = await handleCloudAnalyticalRequest(
          new Request(`http://localhost${tc.url}`, {
            method: tc.method,
            headers: { "content-type": "application/json", "x-service-id": "svc-1" },
            ...(tc.body ? { body: JSON.stringify(tc.body) } : {}),
          }),
        );
        expect(res?.status, `${tc.method} ${tc.openapiPath}`).toBe(200);
        const json = await res!.json();
        const errs = validateValueAgainstSchema(
          OPENAPI,
          json,
          getResponseSchema(tc.method, tc.openapiPath),
          `EMPTY ${tc.method} ${tc.openapiPath}`,
        );
        expect(errs).toEqual([]);
      }
    } finally {
      const engine = getDuckDBEngine();
      const lease = engine.lease();
      try {
        await lease.ingestFile("fixture:req", "req.parquet", buildPopulatedRequestParquet());
      } finally {
        lease.release();
      }
      await engine.syncRumFiles(
        [{ scopedKey: "fixture:vitals", key: "vitals.parquet", bytes: buildRumVitalsParquet() }],
        [{ scopedKey: "fixture:errors", key: "errors.parquet", bytes: buildRumErrorsParquet() }],
      );
    }
  });

  it("enforces canonical ErrorEnvelope shapes and extractApiError extraction on >= 400 responses", async () => {
    const unknownFilterRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/dashboard/aggregates", {
        method: "POST",
        headers: { "content-type": "application/json", "x-service-id": "svc-1" },
        body: JSON.stringify({ filters: { nonexistent_custom_col: { mode: "include", values: ["x"] } } }),
      }),
    );
    expect(unknownFilterRes?.status).toBe(400);
    const unknownBody = await unknownFilterRes!.json();
    expect(unknownBody.detail).toMatchObject({ error: "unknown_filter_field" });
    expect((extractApiError(unknownBody) ?? "").length).toBeGreaterThan(0);

    const invalidTokenRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/sessions/detail", {
        method: "POST",
        headers: { "content-type": "application/json", "x-service-id": "svc-1" },
        body: JSON.stringify({ session_token: "tampered.token" }),
      }),
    );
    expect(invalidTokenRes?.status).toBe(400);
    const invalidTokenBody = await invalidTokenRes!.json();
    expect(invalidTokenBody.detail).toMatchObject({ error: "session_token_invalid" });
    expect(extractApiError(invalidTokenBody)).toContain("Invalid or malformed");

    const blockedQueryRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/query", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql: "DROP TABLE logs" }),
      }),
    );
    expect(blockedQueryRes?.status).toBe(403);
    expect((extractApiError(await blockedQueryRes!.json()) ?? "").length).toBeGreaterThan(0);

    vi.mocked(ensureCloudRange).mockRejectedValueOnce(
      new CloudBudgetError("over", { unit: "bytes", needed: 128 * 1024 * 1024, limit: 64 * 1024 * 1024 }),
    );
    const budgetRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/query", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql: "SELECT status FROM logs" }),
      }),
    );
    expect(budgetRes?.status).toBe(413);
    const budgetBody = await budgetRes!.json();
    expect(budgetBody.detail).toMatchObject({ error: "cloud_scan_budget_exceeded" });
    expect(extractApiError(budgetBody)).toContain("scan budget");
  });

  it("redacts all sentinel PII IPs when mask_ips=true or analyst-no-pii", async () => {
    vi.stubGlobal("fetch", stubCloudFetch("analyst-no-pii"));
    const rangeBody = {
      start_time: "2026-11-05T11:30:00Z",
      end_time: "2026-11-05T13:30:00Z",
      mask_ips: true,
    };
    const piiRoutes: Array<{ method: "GET" | "POST"; url: string; body?: Record<string, unknown> }> = [
      { method: "POST", url: "/api/dashboard/bundle?mask_ips=true", body: rangeBody },
      { method: "POST", url: "/api/dashboard/aggregates?mask_ips=true", body: rangeBody },
      { method: "POST", url: "/api/security/aggregates?mask_ips=true", body: rangeBody },
      { method: "GET", url: "/api/security/threat-intel?mask_ips=true" },
      { method: "POST", url: "/api/sessions?mask_ips=true", body: rangeBody },
      { method: "POST", url: "/api/insights?mask_ips=true", body: { service_id: "svc-1", window_size_hrs: 24, baseline_hours: 168, mask_ips: true } },
      { method: "POST", url: "/api/insights/cache-collapse-detail?mask_ips=true", body: { url: "/api/v1/checkout?step=2", window_size_hrs: 24, baseline_hours: 168, mask_ips: true } },
      { method: "GET", url: "/api/services/svc-1/rum/live-events?mask_ips=true" },
      { method: "POST", url: "/api/query?mask_ips=true", body: { sql: "SELECT ip, url FROM logs", ...rangeBody } },
    ];
    for (const tc of piiRoutes) {
      const res = await handleCloudAnalyticalRequest(
        new Request(`http://localhost${tc.url}`, {
          method: tc.method,
          headers: {
            "content-type": "application/json",
            "x-service-id": "svc-1",
            "x-share-role": "analyst-no-pii",
          },
          ...(tc.body ? { body: JSON.stringify(tc.body) } : {}),
        }),
      );
      expect(res?.status, `${tc.method} ${tc.url}`).toBe(200);
      const raw = await res!.text();
      expect(raw, `${tc.url} leaked raw IPv4`).not.toContain("198.51.100.42");
      expect(raw, `${tc.url} leaked raw miss IPv4`).not.toContain("203.0.113.9");
      expect(raw, `${tc.url} leaked raw IPv6`).not.toContain("2001:db8:abcd:1234:5678::1");
    }

    const schemaRes = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/query/schema/svc-1?mask_ips=true", {
        method: "GET",
        headers: { "x-share-role": "analyst-no-pii" },
      }),
    );
    expect(schemaRes?.status).toBe(200);
    const schemaJson = (await schemaRes!.json()) as { schema: Array<{ name: string }> };
    const colNames = schemaJson.schema.map((c) => c.name);
    expect(colNames).not.toContain("ip");
    expect(colNames).not.toContain("client_ip");
    expect(colNames).not.toContain("oip");

    const tok = sealSessionToken({
      ip: "198.51.100.42",
      ja4: null,
      s: "2026-11-05T11:30:00Z",
      e: "2026-11-05T13:30:00Z",
      svc: "svc-1",
    });
    expect(openSessionToken(tok, "svc-1")?.svc).toBe("svc-1");
    expect(openSessionToken(tok, "other-svc")).toBeNull();

    const cmcdAnalystRes = await cloudServiceStatus(
      new Request("http://localhost/api/services/svc-1/cmcd/status", { method: "GET" }),
      { isAnalyst: true },
    );
    expect(cmcdAnalystRes?.status).toBe(200);
    expect(await cmcdAnalystRes!.json()).toEqual({ enabled: true });

    const secBundleMaskTrue = await handleCloudAnalyticalRequest(
      new Request("http://localhost/api/security/bundle?service_id=svc-1&mask_ips=true", {
        method: "GET",
      }),
    );
    expect(secBundleMaskTrue?.status).toBe(200);
    const secMaskRaw = await secBundleMaskTrue!.text();
    expect(secMaskRaw).not.toContain("198.51.100.42");
    expect(secMaskRaw).not.toContain("203.0.113.9");
  });

  it("matches every shared parity golden vector across IP masking, asset buckets, WAF exclusions, cookie compliance, and cache-collapse detail", async () => {
    for (const vec of VECTORS.pii_ip_masking) {
      expect(maskIp(vec.input)).toBe(vec.expected);
    }

    const assets = await queryAssetsAggregates({
      service_id: "svc-1",
      start_time: "2026-11-05T11:30:00Z",
      end_time: "2026-11-05T13:30:00Z",
    });
    const breakdownMap = new Map(
      (assets.asset_type_breakdown as Array<{ asset_type: string; requests: number }>).map((b) => [
        b.asset_type,
        b.requests,
      ]),
    );
    for (const expectedBucket of new Set(VECTORS.asset_classification.map((v) => v.expected_asset_type))) {
      expect(breakdownMap.get(expectedBucket) ?? 0, `missing bucket ${expectedBucket}`).toBeGreaterThan(0);
    }

    const avail = handleInsightAvailability("svc-1", ["waf", "waf_sig", "timestamp"]);
    const wafCard = avail.insights.find((i) => i.id === "waf_signal_spikes");
    expect(wafCard?.available).toBe(false);
    expect(wafCard?.reason).toBe("cloud_detector_unsupported");

    const engine = getDuckDBEngine();
    for (const vec of VECTORS.cookie_compliance_normalization) {
      const sourceSql = buildScoringSourceSql("comp_vec", new Set(["edge_cookie_compliance"]));
      await engine.executeSql(`CREATE OR REPLACE TEMP TABLE comp_vec AS SELECT ? AS edge_cookie_compliance`, [vec.input]);
      const rows = await engine.executeSql<{ edge_cookie_compliance: string }>(
        `SELECT edge_cookie_compliance FROM ${sourceSql}`,
      );
      expect(rows[0]?.edge_cookie_compliance).toBe(vec.expected);
    }

    const cc = VECTORS.cache_collapse_detail;
    const maskedDetail = await handleCacheCollapseDetail(
      { url: cc.url },
      { maskIps: true, rates: cc.rates, timelineRows: cc.timeline_rows, missRows: cc.miss_rows },
    );
    expect(maskedDetail.window_hit_rate).toBe(cc.expected.window_hit_rate);
    expect(maskedDetail.baseline_hit_rate).toBe(cc.expected.baseline_hit_rate);
    expect(maskedDetail.breakdown).toEqual(cc.expected.breakdown);
    expect(maskedDetail.has_data).toBe(cc.expected.has_data);
    expect(maskedDetail.timeline[0]?.expected_hits).toBe(cc.expected.timeline_expected_hits);
    expect(maskedDetail.timeline[0]?.hit_rate).toBeCloseTo(cc.expected.timeline_hit_rate, 6);
    expect(maskedDetail.recent_misses[0]?.ip).toBe(cc.expected.masked_ip);

    const unmaskedDetail = await handleCacheCollapseDetail(
      { url: cc.url },
      { maskIps: false, rates: cc.rates, timelineRows: cc.timeline_rows, missRows: cc.miss_rows },
    );
    expect(unmaskedDetail.recent_misses[0]?.ip).toBe(cc.expected.unmasked_ip);
  });
});
