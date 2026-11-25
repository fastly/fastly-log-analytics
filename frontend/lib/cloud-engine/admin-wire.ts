import { currentStoreMetaEpoch } from "./dashboard/file-store";
import { dashboardCacheIdentity } from "./dashboard/sources";
import { adminRequest, deleteLogHistory, purgeQuarantine } from "./telemetry/admin";
import { queryOpsLogs } from "./telemetry/client";
import {
  activeOpsUser,
  assertOpsEpoch,
  currentOpsCacheEpoch,
} from "./telemetry/sources";
import { deriveFosUsageFromCronRuns, fetchUsageSettings, type CronRunRow, type FosUsageRow } from "./usage/summary";

type Row = Record<string, unknown>;

function assertAdminWireEpoch(
  request: Request,
  entryMetaEpoch: number,
  entryOpsEpoch: number,
): void {
  request.signal.throwIfAborted();
  if (entryMetaEpoch !== currentStoreMetaEpoch()) {
    throw new DOMException("The operation was aborted.", "AbortError");
  }
  assertOpsEpoch(entryOpsEpoch, request.signal);
}

function id(value: unknown): number {
  let hash = 2166136261;
  for (const char of String(value)) hash = Math.imul(hash ^ char.charCodeAt(0), 16777619);
  return hash >>> 0;
}

function positive(params: URLSearchParams, name: string, fallback: number, max: number): number {
  const value = Number(params.get(name) ?? fallback);
  return Number.isFinite(value) ? Math.min(max, Math.max(1, Math.floor(value))) : fallback;
}

async function nativePages(
  request: Request,
  path: string,
  field: string,
  entryMetaEpoch: number,
  entryOpsEpoch: number,
): Promise<Row> {
  const url = new URL(path, request.url);
  const original = new URL(request.url);
  for (const [key, value] of original.searchParams) url.searchParams.set(key, value);
  const service = request.headers.get("x-service-id");
  if (service && !url.searchParams.has("service_id")) url.searchParams.set("service_id", service);
  const prefix = url.searchParams.get("prefix");
  if (prefix && !prefix.endsWith("/")) url.searchParams.set("prefix", `${prefix}/`);
  const rows: Row[] = [];
  let first: Row | undefined;
  const cursors = new Set<string>();
  for (let count = 0; count < 32; count++) {
    assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
    const page = await adminRequest<Row>(`${url.pathname}${url.search}`, { signal: request.signal });
    assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
    first ??= page;
    if (!Array.isArray(page[field])) throw new Error("Invalid Cloud administrative listing");
    rows.push(...page[field] as Row[]);
    if (page.complete !== false) return { ...first, [field]: rows, complete: true, next_cursor: null };
    if (typeof page.next_cursor !== "string" || cursors.has(page.next_cursor)) {
      throw new Error("Cloud administrative listing is incomplete");
    }
    cursors.add(page.next_cursor);
    url.searchParams.set("cursor", page.next_cursor);
  }
  throw new Error("Cloud administrative listing exceeds the browser page budget");
}

function paginate(rows: Row[], params: URLSearchParams) {
  const perPage = positive(params, "per_page", 50, 1000);
  const page = positive(params, "page", 1, 100000);
  return { entries: rows.slice((page - 1) * perPage, page * perPage), total: rows.length, page, per_page: perPage };
}

type OpsRead = Awaited<ReturnType<typeof queryOpsLogs>>;

const CRON_WIRE_TTL_MS = 15_000;

interface InFlightCronNative {
  read: Promise<Row>;
  controller: AbortController;
  users: number;
}

interface SettledCronNative {
  at: number;
  page: Row;
}

interface InFlightCronRead {
  scopeKey: string;
  fromMs: number;
  toMs: number;
  read: Promise<OpsRead>;
  controller: AbortController;
  users: number;
}

interface SettledCronRead {
  scopeKey: string;
  fromMs: number;
  toMs: number;
  at: number;
  result: OpsRead;
}

const inFlightCronNative = new Map<string, InFlightCronNative>();
const settledCronNative = new Map<string, SettledCronNative>();
const inFlightCronReads = new Set<InFlightCronRead>();
const settledCronReads = new Map<string, SettledCronRead>();

export function resetCronWireCache(): void {
  settledCronNative.clear();
  settledCronReads.clear();
}

async function nativeCronPage(
  request: Request,
  entryMetaEpoch: number,
  entryOpsEpoch: number,
): Promise<Row> {
  assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
  const original = new URL(request.url);
  const service = original.searchParams.get("service_id") ?? request.headers.get("x-service-id") ?? "";
  const scopeKey = JSON.stringify([
    entryMetaEpoch,
    entryOpsEpoch,
    activeOpsUser(),
    dashboardCacheIdentity(),
    service,
  ]);
  const now = Date.now();
  const settled = settledCronNative.get(scopeKey);
  if (settled) {
    if (now - settled.at < CRON_WIRE_TTL_MS) return settled.page;
    settledCronNative.delete(scopeKey);
  }
  let entry = inFlightCronNative.get(scopeKey);
  if (!entry) {
    const controller = new AbortController();
    const cleanUrl = new URL("/api/cron-runs", request.url);
    if (service) cleanUrl.searchParams.set("service_id", service);
    const cleanRequest = new Request(cleanUrl, {
      method: "GET",
      headers: service ? { "x-service-id": service } : undefined,
      signal: controller.signal,
    });
    const created: InFlightCronNative = {
      read: nativePages(cleanRequest, "/api/cron-runs", "runs", entryMetaEpoch, entryOpsEpoch)
        .then((page) => {
          if (entryMetaEpoch === currentStoreMetaEpoch() && entryOpsEpoch === currentOpsCacheEpoch()) {
            settledCronNative.set(scopeKey, { at: Date.now(), page });
          }
          return page;
        }),
      controller,
      users: 0,
    };
    const forget = () => { if (inFlightCronNative.get(scopeKey) === created) inFlightCronNative.delete(scopeKey); };
    created.read.then(forget, forget);
    inFlightCronNative.set(scopeKey, created);
    entry = created;
  }
  const shared = entry;
  shared.users += 1;
  let onAbort!: () => void;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(request.signal.reason);
    request.signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    const page = await Promise.race([shared.read, aborted]);
    assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
    return page;
  } finally {
    request.signal.removeEventListener("abort", onAbort);
    shared.users -= 1;
    if (shared.users === 0 && request.signal.aborted) {
      shared.controller.abort(request.signal.reason);
      if (inFlightCronNative.get(scopeKey) === shared) inFlightCronNative.delete(scopeKey);
    }
  }
}

function findCoveringSettledCronRead(scopeKey: string, fromMs: number, toMs: number): OpsRead | null {
  const now = Date.now();
  let best: SettledCronRead | null = null;
  let bestSpan = Number.POSITIVE_INFINITY;
  for (const [key, entry] of settledCronReads) {
    if (now - entry.at >= CRON_WIRE_TTL_MS) {
      settledCronReads.delete(key);
      continue;
    }
    if (entry.scopeKey !== scopeKey) continue;
    if (entry.fromMs <= fromMs && entry.toMs >= toMs) {
      const span = entry.toMs - entry.fromMs;
      if (span < bestSpan) {
        best = entry;
        bestSpan = span;
      }
    }
  }
  return best?.result ?? null;
}

function findCoveringInFlightCronRead(scopeKey: string, fromMs: number, toMs: number): InFlightCronRead | null {
  let best: InFlightCronRead | null = null;
  let bestSpan = Number.POSITIVE_INFINITY;
  for (const entry of inFlightCronReads) {
    if (entry.scopeKey !== scopeKey) continue;
    if (entry.fromMs <= fromMs && entry.toMs >= toMs) {
      const span = entry.toMs - entry.fromMs;
      if (span < bestSpan) {
        best = entry;
        bestSpan = span;
      }
    }
  }
  return best;
}

// The logs page asks for runs, recent runs, schedule, and header last-sync at once;
// concurrent and short-TTL callers share any covering cron_runs history read. The
// read is cancelled only when every caller has gone.
async function cronHistory(
  range: { from: string; to: string },
  signal: AbortSignal,
  entryOpsEpoch: number,
): Promise<OpsRead> {
  assertOpsEpoch(entryOpsEpoch, signal);
  const scopeKey = JSON.stringify([
    entryOpsEpoch,
    activeOpsUser(),
    dashboardCacheIdentity(),
  ]);
  const fromMs = Date.parse(range.from);
  const toMs = Date.parse(range.to);
  if (Number.isFinite(fromMs) && Number.isFinite(toMs)) {
    const settled = findCoveringSettledCronRead(scopeKey, fromMs, toMs);
    if (settled) return settled;
  }
  let entry = Number.isFinite(fromMs) && Number.isFinite(toMs)
    ? findCoveringInFlightCronRead(scopeKey, fromMs, toMs)
    : null;
  if (!entry) {
    const controller = new AbortController();
    const created: InFlightCronRead = {
      scopeKey,
      fromMs,
      toMs,
      read: queryOpsLogs("cron_runs", range, { signal: controller.signal })
        .then((res) => {
          if (
            entryOpsEpoch === currentOpsCacheEpoch() &&
            res.liveTruncated !== true &&
            Number.isFinite(fromMs) &&
            Number.isFinite(toMs)
          ) {
            settledCronReads.set(`${scopeKey}|${range.from}|${range.to}`, {
              scopeKey,
              fromMs,
              toMs,
              at: Date.now(),
              result: res,
            });
          }
          return res;
        }),
      controller,
      users: 0,
    };
    const forget = () => { inFlightCronReads.delete(created); };
    created.read.then(forget, forget);
    inFlightCronReads.add(created);
    entry = created;
  }
  const shared = entry;
  shared.users += 1;
  let onAbort!: () => void;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    const res = await Promise.race([shared.read, aborted]);
    assertOpsEpoch(entryOpsEpoch, signal);
    return res;
  } finally {
    signal.removeEventListener("abort", onAbort);
    shared.users -= 1;
    if (shared.users === 0 && signal.aborted) {
      shared.controller.abort(signal.reason);
      inFlightCronReads.delete(shared);
    }
  }
}

// Self-hosted task names the shared UI asks for, and the Cloud stage that plays the same role.
const TASK_ALIASES: Record<string, readonly string[]> = { log_discovery: ["request_ingest"] };

// A default-range request reads recent history first and widens only while
// the page it asked for is not yet full: the header's "last sync" badge and
// the recent-runs strip want one or ten rows, not a week of ten-second ticks.
const DEFAULT_LOOKBACKS_MS = [3600_000, 86400_000, 7 * 86400_000] as const;
const WIDE_PAGE_THRESHOLD = 100;

function matchesTask(row: Row, task: string | null): boolean {
  if (!task) return true;
  return row.task === task || (TASK_ALIASES[task] ?? []).includes(String(row.task));
}

async function cron(
  request: Request,
  entryMetaEpoch: number,
  entryOpsEpoch: number,
): Promise<Response> {
  const url = new URL(request.url);
  const params = url.searchParams;
  const explicitStart = params.get("start") ?? params.get("start_time");
  const explicitEnd = params.get("end") ?? params.get("end_time");
  const defaultRange = explicitStart === null && explicitEnd === null;
  const to = defaultRange ? Math.ceil(Date.now() / 60_000) * 60_000 : Date.parse(explicitEnd ?? new Date().toISOString());
  const end = defaultRange ? new Date(to).toISOString() : (explicitEnd ?? new Date(to).toISOString());
  const perPage = positive(params, "per_page", 50, 1000);
  const pageNumber = positive(params, "page", 1, 100000);
  const isSchedule = url.pathname === "/api/cron-schedule";
  const lookbacks: readonly number[] = explicitStart !== null
    ? [to - Date.parse(explicitStart)]
    : !isSchedule && perPage >= WIDE_PAGE_THRESHOLD
      ? [7 * 86400_000]
      : DEFAULT_LOOKBACKS_MS;
  if (!Number.isFinite(to) || lookbacks.some((span) => !Number.isFinite(span) || span < 0)) {
    return Response.json({ detail: { error: "invalid_time_range" } }, { status: 422 });
  }
  const page = await nativeCronPage(
    request,
    entryMetaEpoch,
    entryOpsEpoch,
  );
  const service = params.get("service_id") ?? request.headers.get("x-service-id") ?? page.service_id;
  if (service && page.service_id !== service) throw new Error("Cloud cron service does not match the selected service");
  const coordinators = (page.runs as Row[]).map((row): Row => ({ ...row, task: "cloud_ingest_clock" }));
  const sinceId = params.has("since_id") ? Number(params.get("since_id")) : null;
  const taskFilter = params.get("task");

  for (const [index, span] of lookbacks.entries()) {
    const start = explicitStart ?? new Date(to - span).toISOString();
    const from = to - span;
    assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
    const evidence = await cronHistory(
      { from: start, to: end },
      request.signal,
      entryOpsEpoch,
    );
    assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
    if (evidence.liveTruncated) throw new Error("Cloud cron history is incomplete; choose a smaller range");
    const stageRows = evidence.rows.filter((row) => typeof row.cron_kind === "string")
      .map((row): Row => ({ ...row, task: row.cron_kind }));
    const entries = [...stageRows, ...coordinators]
      .filter((row) => !service || row.service_id === service)
      .map((row) => {
        const started = typeof row.started_at === "number" ? row.started_at : Date.parse(String(row.started_at));
        const duration = typeof row.duration_ms === "number" ? row.duration_ms : null;
        return {
          ...row,
          task: String(row.task),
          id: started * 1000 + id(`${row.task}:${row.run_id ?? row.event_id ?? row.id}`) % 1000,
          started_at: new Date(started).toISOString(),
          finished_at: row.finished_at ?? (duration === null ? null : new Date(started + duration).toISOString()),
          duration_s: duration === null ? null : duration / 1000,
          error_message: row.error ?? null,
          files_downloaded: row.converted_files ?? row.converted_raw_files ?? null,
          files_deleted_fos: row.deleted_files ?? row.deleted_raw_files ?? null,
          rows_ingested: row.rows_written ?? null,
          corrupt_rows: row.bad_lines ?? null,
          // `partial` is a stage stopping at its execution budget with work left for the next
          // turn, not a partial failure; only rejected lines make a run a Partial Success.
          status: row.error || row.outcome === "error" ? "error" :
            row.outcome !== "done" && row.outcome !== "partial" ? "skipped" :
            Number(row.bad_lines ?? 0) > 0 ? "partial_success" : "success",
          summary: row.error ?? (row.outcome === "partial" ? `${row.task}: more work queued, continues next turn` :
            `${row.task}: ${row.outcome}`),
          result_json: JSON.stringify(row),
        };
      }).filter((row) => Date.parse(row.started_at) >= from && Date.parse(row.started_at) <= to)
      .sort((a, b) => b.id - a.id);
    const widest = index === lookbacks.length - 1;
    const history = { history_from: start, history_to: end };
    if (isSchedule) {
      if (!Array.isArray(page.schedules)) throw new Error("Cloud native task schedules are unavailable");
      const schedules = (page.schedules as Row[]).map((schedule): Row => {
        const observed = entries.find((row) => row.task === schedule.task)?.started_at;
        const recorded = typeof schedule.last_run_time === "string" ? schedule.last_run_time : null;
        return { ...schedule, last_run_time: observed && (!recorded || Date.parse(observed) > Date.parse(recorded)) ? observed : recorded };
      });
      if (widest || schedules.every((schedule) => Boolean(schedule.disabled_reason) || typeof schedule.last_run_time === "string")) {
        return Response.json({ schedules, ...history, clock: page.clock ?? null });
      }
      continue;
    }
    const filtered = entries.filter((row) => matchesTask(row, taskFilter) &&
      (!params.get("status") || row.status === params.get("status")) && (sinceId === null || row.id > sinceId));
    if (widest || sinceId !== null || filtered.length >= pageNumber * perPage) {
      return Response.json({ ...page, ...history, ...paginate(filtered, params) });
    }
  }
  throw new Error("Cloud cron history lookback exhausted");
}

async function usage(
  request: Request,
  entryMetaEpoch: number,
  entryOpsEpoch: number,
): Promise<Response> {
  assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
  const url = new URL(request.url);
  const params = url.searchParams;
  const end = params.get("end") ?? params.get("end_time") ?? new Date().toISOString();
  const start = params.get("start") ?? params.get("start_time") ?? new Date(Date.parse(end) - 7 * 86400000).toISOString();
  if (!Number.isFinite(Date.parse(start)) || !Number.isFinite(Date.parse(end)) || Date.parse(start) > Date.parse(end)) {
    return Response.json({ detail: { error: "invalid_time_range" } }, { status: 422 });
  }
  const range = { from: start, to: end };
  const fos = await queryOpsLogs("fos_usage", range, { signal: request.signal });
  assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
  if (fos.liveTruncated) throw new Error("Cloud usage history is incomplete; choose a smaller range");
  let source = fos.rows as unknown as FosUsageRow[];
  let basis = "native_fos_usage";
  if (!source.length) {
    const cron = await queryOpsLogs("cron_runs", range, { signal: request.signal });
    assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
    if (cron.liveTruncated) throw new Error("Cloud usage history is incomplete; choose a smaller range");
    const settings = await fetchUsageSettings(request.signal);
    assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
    source = deriveFosUsageFromCronRuns(cron.rows as unknown as CronRunRow[], settings);
    basis = "derived_cron_counters";
  }
  const service = params.get("service_id") ?? request.headers.get("x-service-id");
  const rows = source.filter((row) => row.timestamp >= Date.parse(start) && row.timestamp <= Date.parse(end)).map((row) => ({
    id: id(JSON.stringify(row)), timestamp: new Date(row.timestamp).toISOString(),
    service_id: service, operation_class: row.op_class === "CDN_HIT" ? "CDN" : row.op_class,
    operation_type: row.op, url: row.dataset, bytes: row.bytes_in + row.bytes_out,
    duration_ms: null, function_name: row.caller, process_context: row.caller,
    status: null, estimated_cost: row.est_cost_usd, count: row.op_count,
  })).filter((row) => (!params.get("usage_type") ||
    params.get("usage_type") === "FOS" && row.operation_class !== "CDN" ||
    row.operation_class === params.get("usage_type")?.replace("FOS-", "")) &&
    (!params.get("operation_type") || row.operation_type === params.get("operation_type")) &&
    (!params.get("process_context") || row.process_context === params.get("process_context")))
    .sort((a, b) => b.timestamp.localeCompare(a.timestamp));
  if (url.pathname.endsWith("/export")) {
    const columns = ["timestamp", "service_id", "operation_class", "operation_type", "url", "bytes", "function_name", "process_context", "estimated_cost", "count"] as const;
    const plainNumRe = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
    const leadingCtrlOrSpaceRe = /^[\s\x00-\x1f\x7f\u0085\u200b-\u200f\u2028-\u202f\u2060\ufeff]*/u;
    const cell = (value: unknown) => {
      if (value == null) return '""';
      if (typeof value === "number") return Number.isFinite(value) ? String(value) : '""';
      if (typeof value === "boolean") return String(value);
      let text = String(value);
      if (!plainNumRe.test(text)) {
        if (/^[\t\r\n]/.test(text) || /^[=+\-@|%]/.test(text.replace(leadingCtrlOrSpaceRe, ""))) {
          text = `'${text}`;
        }
      }
      return `"${text.replaceAll('"', '""')}"`;
    };
    return new Response([columns.join(","), ...rows.map((row) => columns.map((key) => cell(row[key])).join(","))].join("\r\n"), {
      headers: { "content-type": "text/csv;charset=utf-8", "content-disposition": 'attachment; filename="usage_log.csv"' },
    });
  }
  const aggregate = {
    total_class_a: 0, total_class_b: 0, total_cdn_downloads: 0, total_cdn_bytes: 0, total_fos_bytes: 0,
    estimated_cost_class_a: 0, estimated_cost_class_b: 0, estimated_cost_cdn: 0, estimated_cost_total: 0,
    class_a_breakdown: {} as Record<string, number>, class_b_breakdown: {} as Record<string, number>,
  };
  for (const row of rows) {
    aggregate.estimated_cost_total += row.estimated_cost;
    if (row.operation_class === "CDN") {
      aggregate.total_cdn_downloads += row.count;
      aggregate.total_cdn_bytes += row.bytes;
      aggregate.estimated_cost_cdn += row.estimated_cost;
    } else {
      aggregate.total_fos_bytes += row.bytes;
      const a = row.operation_class === "A";
      if (a) { aggregate.total_class_a += row.count; aggregate.estimated_cost_class_a += row.estimated_cost; }
      else { aggregate.total_class_b += row.count; aggregate.estimated_cost_class_b += row.estimated_cost; }
      const breakdown = a ? aggregate.class_a_breakdown : aggregate.class_b_breakdown;
      breakdown[row.operation_type] = (breakdown[row.operation_type] ?? 0) + row.count;
    }
  }
  const pageSize = positive(params, "page_size", 50, 10000);
  const page = positive(params, "page", 1, 100000);
  return Response.json({ service_id: service, entries: rows.slice((page - 1) * pageSize, page * pageSize), total: rows.length, aggregate, measurement_basis: basis });
}

function ingestRowRank(row: Row): [number, number, string] {
  return [
    Number(row.state_version ?? 0),
    Number(row.timestamp ?? 0),
    String(row.event_id ?? ""),
  ];
}

function isHigherIngestRank(a: [number, number, string], b: [number, number, string]): boolean {
  if (a[0] !== b[0]) return a[0] > b[0];
  if (a[1] !== b[1]) return a[1] > b[1];
  return a[2] > b[2];
}

function isoFromMs(value: unknown): string | null {
  if (typeof value === "string" && Number.isFinite(Date.parse(value))) return value;
  const n = Number(value);
  return value != null && value !== "" && Number.isFinite(n) ? new Date(n).toISOString() : null;
}

async function ingestedFiles(
  request: Request,
  entryMetaEpoch: number,
  entryOpsEpoch: number,
): Promise<Response> {
  assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
  const url = new URL(request.url);
  const params = url.searchParams;
  const end = params.get("end") ?? params.get("end_time") ?? new Date().toISOString();
  const to = Date.parse(end);
  const start = params.get("start") ?? params.get("start_time") ?? new Date(to - 86_400_000).toISOString();
  const from = Date.parse(start);
  if (!Number.isFinite(from) || !Number.isFinite(to) || to < from) {
    return Response.json({ detail: { error: "invalid_time_range" } }, { status: 422 });
  }
  const service = params.get("service_id") ?? request.headers.get("x-service-id");
  const result = await queryOpsLogs(
    "ingest_files",
    { from: new Date(from).toISOString(), to: new Date(to).toISOString() },
    { signal: request.signal },
  );
  assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
  const known = new Map<string, { rows: unknown; size: unknown }>();
  const latest = new Map<string, { rank: [number, number, string]; row: Row }>();
  for (const row of result.rows as Row[]) {
    if (service && row.service_id && row.service_id !== service) continue;
    const key = typeof row.raw_key === "string" && row.raw_key ? row.raw_key : null;
    if (!key) continue;
    const prevKnown = known.get(key) ?? { rows: null, size: null };
    if (prevKnown.rows == null && row.rows != null) prevKnown.rows = row.rows;
    if (prevKnown.size == null && row.raw_size != null) prevKnown.size = row.raw_size;
    known.set(key, prevKnown);
    const rank = ingestRowRank(row);
    const current = latest.get(key);
    if (!current || isHigherIngestRank(rank, current.rank)) {
      latest.set(key, { rank, row });
    }
  }
  const files = [...latest.entries()].map(([key, { row }]) => {
    const inherited = known.get(key);
    const rows = row.rows ?? inherited?.rows ?? null;
    const size = row.raw_size ?? inherited?.size ?? null;
    const at = isoFromMs(row.timestamp);
    return {
      key,
      file_name: key,
      ingested_at: at,
      last_modified: at,
      rows,
      row_count: rows,
      size,
      file_size_bytes: size,
      status: row.status ?? null,
      raw_deleted: Boolean(row.raw_deleted),
      raw_deleted_at: isoFromMs(row.raw_deleted_at),
      dataset: row.dataset ?? null,
    };
  });
  return Response.json({ files, skipped_objects: 0, next_cursor: null, complete: true });
}

export async function cloudAdminWire(
  request: Request,
  entryMetaEpoch: number = currentStoreMetaEpoch(),
  entryOpsEpoch: number = currentOpsCacheEpoch(),
): Promise<Response | null> {
  assertAdminWireEpoch(request, entryMetaEpoch, entryOpsEpoch);
  const url = new URL(request.url);
  const path = url.pathname;
  if (path === "/api/admin/quarantine/purge" && request.method === "POST") {
    if (url.searchParams.has("item_id")) return Response.json({ detail: { error: "cloud_line_evidence_unavailable" } }, { status: 501 });
    return Response.json(await purgeQuarantine({ target: "all" }));
  }
  if (path === "/api/admin/usage-log" && request.method === "DELETE") {
    return Response.json(await deleteLogHistory("fos_usage"));
  }
  if (request.method !== "GET") return null;
  if (path === "/api/admin/usage-log" || path === "/api/admin/usage-log/export") {
    return usage(request, entryMetaEpoch, entryOpsEpoch);
  }
  if (path === "/api/admin/raw-tree" || path === "/api/admin/storage-tree") {
    const page = await nativePages(request, path, "nodes", entryMetaEpoch, entryOpsEpoch);
    const unique = new Map((page.nodes as Row[]).map((node) => [String(node.key ?? node.prefix), node]));
    return Response.json({ ...page, nodes: [...unique.values()] });
  }
  if (path === "/api/admin/ingested-files") {
    return ingestedFiles(request, entryMetaEpoch, entryOpsEpoch);
  }
  if (path === "/api/audit-logs") {
    const page = await nativePages(
      request,
      "/api/admin/audit-logs",
      "entries",
      entryMetaEpoch,
      entryOpsEpoch,
    );
    const entries = (page.entries as Row[]).map((row) => {
      let details: unknown = row.details;
      if (typeof details === "string") {
        try { details = JSON.parse(details); } catch { details = { message: details }; }
      }
      return { ...row, id: id(row.id), timestamp: row.time ?? new Date(Number(row.timestamp)).toISOString(),
        event_type: row.action, details };
    }).filter((row) => !url.searchParams.get("event_type") || row.event_type === url.searchParams.get("event_type"));
    return Response.json(paginate(entries, url.searchParams));
  }
  if (path === "/api/cron-runs" || path === "/api/cron-schedule") {
    return cron(request, entryMetaEpoch, entryOpsEpoch);
  }
  return null;
}
