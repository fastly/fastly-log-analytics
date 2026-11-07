import { adminRequest, deleteLogHistory, purgeQuarantine } from "./telemetry/admin";
import { queryOpsLogs } from "./telemetry/client";
import { deriveFosUsageFromCronRuns, fetchUsageSettings, type CronRunRow, type FosUsageRow } from "./usage/summary";

type Row = Record<string, unknown>;

function id(value: unknown): number {
  let hash = 2166136261;
  for (const char of String(value)) hash = Math.imul(hash ^ char.charCodeAt(0), 16777619);
  return hash >>> 0;
}

function positive(params: URLSearchParams, name: string, fallback: number, max: number): number {
  const value = Number(params.get(name) ?? fallback);
  return Number.isFinite(value) ? Math.min(max, Math.max(1, Math.floor(value))) : fallback;
}

async function nativePages(request: Request, path: string, field: string): Promise<Row> {
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
    request.signal.throwIfAborted();
    const page = await adminRequest<Row>(`${url.pathname}${url.search}`, { signal: request.signal });
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

async function cron(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const params = url.searchParams;
  const end = params.get("end") ?? params.get("end_time") ?? new Date().toISOString();
  const to = Date.parse(end);
  const start = params.get("start") ?? params.get("start_time") ??
    (Number.isFinite(to) ? new Date(to - 7 * 86400000).toISOString() : "");
  const from = Date.parse(start);
  if (!Number.isFinite(from) || !Number.isFinite(to) || from > to) {
    return Response.json({ detail: { error: "invalid_time_range" } }, { status: 422 });
  }
  const page = await nativePages(request, "/api/cron-runs", "runs");
  const service = params.get("service_id") ?? request.headers.get("x-service-id") ?? page.service_id;
  if (service && page.service_id !== service) throw new Error("Cloud cron service does not match the selected service");
  const evidence = await queryOpsLogs("cron_runs", { from: start, to: end }, { signal: request.signal });
  request.signal.throwIfAborted();
  if (evidence.liveTruncated) throw new Error("Cloud cron history is incomplete; choose a smaller range");
  const stageRows = evidence.rows.filter((row) => typeof row.cron_kind === "string")
    .map((row): Row => ({ ...row, task: row.cron_kind }));
  const coordinators = (page.runs as Row[]).map((row): Row => ({ ...row, task: "cloud_ingest_clock" }));
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
        status: row.error || row.outcome === "error" ? "error" : row.outcome === "done" ? "success" :
          row.outcome === "partial" ? "partial_success" : "skipped",
        summary: row.error ?? `${row.task}: ${row.outcome}`,
        result_json: JSON.stringify(row),
      };
    }).filter((row) => Date.parse(row.started_at) >= from && Date.parse(row.started_at) <= to)
    .sort((a, b) => b.id - a.id);
  const history = { history_from: start, history_to: end };
  if (url.pathname === "/api/cron-schedule") {
    if (!Array.isArray(page.schedules)) throw new Error("Cloud native task schedules are unavailable");
    const schedules = (page.schedules as Row[]).map((schedule) => {
      const observed = entries.find((row) => row.task === schedule.task)?.started_at;
      const recorded = typeof schedule.last_run_time === "string" ? schedule.last_run_time : null;
      return { ...schedule, last_run_time: observed && (!recorded || Date.parse(observed) > Date.parse(recorded)) ? observed : recorded };
    });
    return Response.json({ schedules, ...history });
  }
  const sinceId = params.has("since_id") ? Number(params.get("since_id")) : null;
  const filtered = entries.filter((row) => (!params.get("task") || row.task === params.get("task")) &&
    (!params.get("status") || row.status === params.get("status")) && (sinceId === null || row.id > sinceId));
  return Response.json({ ...page, ...history, ...paginate(filtered, params) });
}

async function usage(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const params = url.searchParams;
  const end = params.get("end") ?? params.get("end_time") ?? new Date().toISOString();
  const start = params.get("start") ?? params.get("start_time") ?? new Date(Date.parse(end) - 7 * 86400000).toISOString();
  if (!Number.isFinite(Date.parse(start)) || !Number.isFinite(Date.parse(end)) || Date.parse(start) > Date.parse(end)) {
    return Response.json({ detail: { error: "invalid_time_range" } }, { status: 422 });
  }
  const range = { from: start, to: end };
  const fos = await queryOpsLogs("fos_usage", range, { signal: request.signal });
  if (fos.liveTruncated) throw new Error("Cloud usage history is incomplete; choose a smaller range");
  let source = fos.rows as unknown as FosUsageRow[];
  let basis = "native_fos_usage";
  if (!source.length) {
    const cron = await queryOpsLogs("cron_runs", range, { signal: request.signal });
    if (cron.liveTruncated) throw new Error("Cloud usage history is incomplete; choose a smaller range");
    source = deriveFosUsageFromCronRuns(cron.rows as unknown as CronRunRow[], await fetchUsageSettings(request.signal));
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
    const cell = (value: unknown) => {
      let text = String(value ?? "");
      if (/^[=+\-@\t\r]/.test(text)) text = `'${text}`;
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

export async function cloudAdminWire(request: Request): Promise<Response | null> {
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
  if (path === "/api/admin/usage-log" || path === "/api/admin/usage-log/export") return usage(request);
  if (path === "/api/admin/raw-tree" || path === "/api/admin/storage-tree") {
    const page = await nativePages(request, path, "nodes");
    const unique = new Map((page.nodes as Row[]).map((node) => [String(node.key ?? node.prefix), node]));
    return Response.json({ ...page, nodes: [...unique.values()] });
  }
  if (path === "/api/admin/ingested-files") {
    const page = await nativePages(request, path, "files");
    return Response.json({ ...page, files: (page.files as Row[]).map((file) => ({
      ...file, file_name: file.key, ingested_at: file.last_modified ?? file.end,
      row_count: file.rows, file_size_bytes: file.size,
    })) });
  }
  if (path === "/api/audit-logs") {
    const page = await nativePages(request, "/api/admin/audit-logs", "entries");
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
    return cron(request);
  }
  return null;
}
