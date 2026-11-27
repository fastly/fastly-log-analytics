import { CLOUD_SCORING_SCAN_ROUTES } from "./scoring-reads";
import { retrainCloudScoring } from "./scoring-train";
import type { components } from "@/types/api";

type ScoringStatusResponse = components["schemas"]["ScoringStatusResponse"];
type ScoringConfigCompositeResponse = components["schemas"]["ScoringConfigCompositeResponse"];
type ScoringExcludeRegexState = components["schemas"]["ScoringExcludeRegexState"];
type ScoringExcludeRegexPutResponse = components["schemas"]["ScoringExcludeRegexPutResponse"];
type ScoringExcludeRegexValidateResponse = components["schemas"]["ScoringExcludeRegexValidateResponse"];
type ScoringEnforceStatusCodeState = components["schemas"]["ScoringEnforceStatusCodeState"];
type ScoringEnforceStatusCodePutResponse = components["schemas"]["ScoringEnforceStatusCodePutResponse"];
type ScoringEnforceThresholdResponse = components["schemas"]["ScoringEnforceThresholdResponse"];
type ScoringEnforceThresholdPutResponse = components["schemas"]["ScoringEnforceThresholdPutResponse"];
type ScoringRotateKeyResponse = components["schemas"]["ScoringRotateKeyResponse"];
type ScoringLabelRow = components["schemas"]["ScoringLabelRow"];
type ScoringLabelsListResponse = components["schemas"]["ScoringLabelsListResponse"];
type ScoringLabelDeleteResponse = components["schemas"]["ScoringLabelDeleteResponse"];
type ScoringAuditRow = components["schemas"]["ScoringAuditRow"];
type ScoringAuditListResponse = components["schemas"]["ScoringAuditListResponse"];
type ScoringRetrainResponse = components["schemas"]["ScoringRetrainResponse"];

const DEFAULT_ASSET_EXT_REGEX =
  "^[^?#;]*\\.(aif|aiff|au|avi|bin|bmp|cab|carb|cct|cdf|class|css|dcr|doc|" +
  "dtd|exe|flv|gcf|gff|gif|grv|hdml|hqx|ico|ini|jpeg|jpg|js|mov|" +
  "mp3|mp4|nc|pct|pdf|png|ppc|pws|svg|swa|swf|txt|vbs|w32|wav|" +
  "wbmp|wml|wmlc|wmls|wmlsc|xsd|zip|webp|woff|woff2|ttf|bz2|gz|" +
  "tgz|tar|lzma|rar|war|bz|7z|ts|m3u8)($|\\?|#)";

const STREAM_JOB_ACTIONS = new Set(["enable", "disable", "teardown"]);

const SCORING_FLATTENED_KEYS = [
  "scoring_service_id",
  "scoring_service_name",
  "scoring_domain",
  "matrix_version",
  "enabled_at",
  "scoring_active_version",
  "scoring_activated_at",
] as const;

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function asBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function flattenStatusPayload(raw: Record<string, unknown>): ScoringStatusResponse {
  const scoring = asRecord(raw.scoring);
  const out: Record<string, unknown> = { ...raw };
  for (const key of SCORING_FLATTENED_KEYS) {
    if (out[key] === undefined && scoring[key] !== undefined) {
      out[key] = scoring[key];
    }
  }
  return out as ScoringStatusResponse;
}

function toExcludeRegexState(raw: Record<string, unknown>): ScoringExcludeRegexState {
  const current =
    asString(raw.current) ??
    asString(raw.exclude_url_regex) ??
    "";
  const defaultRegex = asString(raw.default) ?? DEFAULT_ASSET_EXT_REGEX;
  const effective =
    asString(raw.effective) ??
    asString(raw.effective_regex) ??
    (current.trim() ? current : defaultRegex);
  const isDefault =
    asBoolean(raw.is_default) ??
    (!current.trim() || current.trim() === defaultRegex);
  return {
    current,
    default: defaultRegex,
    effective,
    is_default: isDefault,
  };
}

function toEnforceStatusCodeState(raw: Record<string, unknown>): ScoringEnforceStatusCodeState {
  const code = asNumber(raw.current) ?? asNumber(raw.status_code) ?? 429;
  const defaultCode = asNumber(raw.default) ?? 429;
  const effective = asNumber(raw.effective) ?? code;
  const isDefault = asBoolean(raw.is_default) ?? effective === defaultCode;
  return {
    current: code,
    default: defaultCode,
    effective,
    min: asNumber(raw.min) ?? 400,
    max: asNumber(raw.max) ?? 599,
    is_default: isDefault,
  };
}

function toLabelRow(raw: Record<string, unknown>, serviceId: string): ScoringLabelRow {
  const sid = asString(raw.sid) ?? asString(raw.edge_sid) ?? "";
  const flaggedBy = asString(raw.flagged_by) ?? asString(raw.created_by) ?? "admin";
  const rawLabel = asString(raw.label);
  const label: ScoringLabelRow["label"] =
    rawLabel === "good" || rawLabel === "bad" ? rawLabel : "neutral";
  const row: ScoringLabelRow = {
    id: asString(raw.id) ?? "",
    service_id: asString(raw.service_id) ?? serviceId,
    sid,
    label,
    notes: asString(raw.notes) ?? "",
    flagged_by: flaggedBy,
  };
  if (raw.sample_ip !== undefined) row.sample_ip = asString(raw.sample_ip);
  if (raw.sample_ua !== undefined) row.sample_ua = asString(raw.sample_ua);
  if (raw.sample_url !== undefined) row.sample_url = asString(raw.sample_url);
  if (raw.created_at !== undefined) row.created_at = asString(raw.created_at);
  if (raw.updated_at !== undefined) row.updated_at = asString(raw.updated_at);
  return row;
}

async function readJsonBody(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const text = await request.text();
    if (!text.trim()) return {};
    const parsed: unknown = JSON.parse(text);
    return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

async function forwardUpstream(
  request: Request,
  targetUrl: string,
  method: string,
  bodyObj?: Record<string, unknown>,
): Promise<{ ok: true; data: Record<string, unknown>; status: number } | { ok: false; response: Response }> {
  const headers = new Headers();
  if (bodyObj !== undefined) {
    headers.set("content-type", "application/json");
  }
  const upstream = await fetch(targetUrl, {
    method,
    headers: bodyObj !== undefined ? headers : undefined,
    body: bodyObj !== undefined ? JSON.stringify(bodyObj) : undefined,
    signal: request.signal,
    credentials: "include",
  });
  const text = await upstream.text();
  if (!upstream.ok) {
    return {
      ok: false,
      response: new Response(text, {
        status: upstream.status,
        headers: { "content-type": upstream.headers.get("content-type") ?? "application/json" },
      }),
    };
  }
  let parsed: Record<string, unknown> = {};
  if (text.trim()) {
    try {
      parsed = asRecord(JSON.parse(text));
    } catch {
      parsed = {};
    }
  }
  return { ok: true, data: parsed, status: upstream.status };
}

function withConfirmQuery(url: URL): string {
  const target = new URL(url.toString());
  if (!target.searchParams.has("confirm")) {
    target.searchParams.set("confirm", "true");
  }
  return target.toString();
}

function boundedPositiveInt(value: string | null, fallback: number, max: number): number {
  const n = Number(value ?? fallback);
  return Number.isFinite(n) && n >= 1 ? Math.min(max, Math.floor(n)) : fallback;
}

export async function cloudScoringControlBridge(request: Request): Promise<Response | null> {
  const url = new URL(request.url, "http://localhost");
  const match = url.pathname.match(/^\/api\/services\/([^/]+)\/scoring\/(.+)$/);
  if (!match) return null;

  let serviceId = match[1];
  try {
    serviceId = decodeURIComponent(match[1]);
  } catch {
    return null;
  }
  const method = request.method.toUpperCase();
  const subPath = match[2].replace(/^\/+|\/+$/g, "");

  if (
    method === "GET" &&
    (CLOUD_SCORING_SCAN_ROUTES.has(subPath) || /^sessions\/[^/]+\/events$/.test(subPath))
  ) {
    return null;
  }
  if (method === "POST" && STREAM_JOB_ACTIONS.has(subPath)) {
    return null;
  }

  if (method === "GET" && subPath === "status") {
    const res = await forwardUpstream(request, url.toString(), "GET");
    if (!res.ok) return res.response;
    return Response.json(flattenStatusPayload(res.data), { status: res.status });
  }

  if (method === "GET" && subPath === "config") {
    const res = await forwardUpstream(request, url.toString(), "GET");
    if (!res.ok) return res.response;
    const rawThreshold = asRecord(res.data.threshold);
    const rawEnforce = asRecord(res.data.enforce_threshold);
    const thresholdVal = asNumber(rawThreshold.threshold);
    const enforcedVal = asNumber(rawEnforce.enforce_threshold);
    const out: ScoringConfigCompositeResponse = {
      ...res.data,
      status: res.data.status ? flattenStatusPayload(asRecord(res.data.status)) : null,
      exclude_regex: res.data.exclude_regex
        ? toExcludeRegexState(asRecord(res.data.exclude_regex))
        : null,
      enforce_status_code: res.data.enforce_status_code
        ? toEnforceStatusCodeState(asRecord(res.data.enforce_status_code))
        : null,
      threshold: {
        threshold: thresholdVal,
        set_at: asString(rawThreshold.set_at),
        enforced: enforcedVal !== null,
      },
    };
    return Response.json(out, { status: res.status });
  }

  if (method === "GET" && subPath === "exclude-regex") {
    const res = await forwardUpstream(request, url.toString(), "GET");
    if (!res.ok) return res.response;
    return Response.json(toExcludeRegexState(res.data), { status: res.status });
  }

  if (method === "PUT" && subPath === "exclude-regex") {
    const body = (await readJsonBody(request)) ?? {};
    const regex = asString(body.regex) ?? asString(body.exclude_url_regex) ?? "";
    const res = await forwardUpstream(request, withConfirmQuery(url), "PUT", {
      exclude_url_regex: regex,
    });
    if (!res.ok) return res.response;
    const cleaned = asString(res.data.exclude_url_regex) ?? regex;
    const effective =
      asString(res.data.effective_regex) ??
      (cleaned.trim() ? cleaned : DEFAULT_ASSET_EXT_REGEX);
    const out: ScoringExcludeRegexPutResponse = {
      ok: asBoolean(res.data.ok) ?? true,
      message: asString(res.data.message),
      is_default: !cleaned.trim() || cleaned.trim() === DEFAULT_ASSET_EXT_REGEX,
      logging_service_active_version: asNumber(res.data.logging_service_active_version),
      effective_regex: effective,
      lint_warnings: [],
    };
    return Response.json(out, { status: res.status });
  }

  if (method === "POST" && subPath === "exclude-regex/validate") {
    const body = (await readJsonBody(request)) ?? {};
    const regex = asString(body.regex) ?? asString(body.exclude_url_regex) ?? "";
    const res = await forwardUpstream(request, url.toString(), "POST", {
      exclude_url_regex: regex,
    });
    if (!res.ok) return res.response;
    const valid = asBoolean(res.data.valid) ?? asBoolean(res.data.ok) ?? false;
    const out: ScoringExcludeRegexValidateResponse = {
      ok: valid,
      lint_warnings: [],
      error: valid ? null : asString(res.data.error) ?? "Invalid regex",
      reason: valid ? null : asString(res.data.reason) ?? "invalid_regex",
    };
    if (res.data.cleaned !== undefined) out.cleaned = res.data.cleaned;
    if (res.data.effective_regex !== undefined) out.effective_regex = res.data.effective_regex;
    return Response.json(out, { status: res.status });
  }

  if (method === "GET" && subPath === "enforce-status-code") {
    const res = await forwardUpstream(request, url.toString(), "GET");
    if (!res.ok) return res.response;
    return Response.json(toEnforceStatusCodeState(res.data), { status: res.status });
  }

  if (method === "PUT" && subPath === "enforce-status-code") {
    const body = (await readJsonBody(request)) ?? {};
    const requestedCode = body.status_code == null ? 429 : Number(body.status_code);
    const res = await forwardUpstream(request, withConfirmQuery(url), "PUT", {
      status_code: requestedCode,
    });
    if (!res.ok) return res.response;
    const effectiveCode = asNumber(res.data.status_code) ?? requestedCode;
    const out: ScoringEnforceStatusCodePutResponse = {
      ok: asBoolean(res.data.ok) ?? true,
      message: asString(res.data.message),
      is_default: effectiveCode === 429,
      logging_service_active_version: asNumber(res.data.logging_service_active_version),
      effective_status_code: effectiveCode,
    };
    return Response.json(out, { status: res.status });
  }

  if (method === "GET" && subPath === "enforce-threshold") {
    const res = await forwardUpstream(request, url.toString(), "GET");
    if (!res.ok) return res.response;
    const threshold = asNumber(res.data.threshold) ?? asNumber(res.data.enforce_threshold);
    const out: ScoringEnforceThresholdResponse = {
      threshold,
      enforced: threshold !== null,
      key: "enforce_threshold",
    };
    return Response.json(out, { status: res.status });
  }

  if (method === "PUT" && subPath === "enforce-threshold") {
    const body = (await readJsonBody(request)) ?? {};
    const thresholdInput =
      body.threshold !== undefined ? body.threshold : body.enforce_threshold;
    const enforceThreshold = thresholdInput == null ? null : Number(thresholdInput);
    const res = await forwardUpstream(request, withConfirmQuery(url), "PUT", {
      enforce_threshold: enforceThreshold,
    });
    if (!res.ok) return res.response;
    const resolved =
      res.data.enforce_threshold === undefined ? enforceThreshold : asNumber(res.data.enforce_threshold);
    const out: ScoringEnforceThresholdPutResponse = {
      ok: asBoolean(res.data.ok) ?? true,
      message: asString(res.data.message),
      threshold: resolved,
      enforced: resolved !== null,
    };
    return Response.json(out, { status: res.status });
  }

  if (method === "POST" && subPath === "rotate-key") {
    const res = await forwardUpstream(request, withConfirmQuery(url), "POST");
    if (!res.ok) return res.response;
    const out: ScoringRotateKeyResponse = {
      ok: asBoolean(res.data.ok) ?? true,
      message: asString(res.data.message) ?? "AES-256 cookie key rotated.",
      rotated_at: asString(res.data.rotated_at) ?? new Date().toISOString(),
      previous_key_grace: asBoolean(res.data.previous_key_grace) ?? true,
    };
    return Response.json(out, { status: res.status });
  }

  if (method === "GET" && subPath === "labels") {
    const res = await forwardUpstream(request, url.toString(), "GET");
    if (!res.ok) return res.response;
    const rawLabels = Array.isArray(res.data.labels) ? res.data.labels : [];
    const labels = rawLabels.map((item) => toLabelRow(asRecord(item), serviceId));
    let good = 0;
    let bad = 0;
    let neutral = 0;
    for (const row of labels) {
      if (row.label === "good") good += 1;
      else if (row.label === "bad") bad += 1;
      else neutral += 1;
    }
    const out: ScoringLabelsListResponse = {
      labels,
      counts: { good, bad, neutral },
    };
    return Response.json(out, { status: res.status });
  }

  if (method === "POST" && subPath === "labels") {
    const body = (await readJsonBody(request)) ?? {};
    const sid = asString(body.sid) ?? asString(body.edge_sid) ?? "";
    const createdBy = asString(body.flagged_by) ?? asString(body.created_by) ?? "admin";
    const res = await forwardUpstream(request, url.toString(), "POST", {
      ...body,
      sid,
      edge_sid: sid,
      flagged_by: createdBy,
      created_by: createdBy,
    });
    if (!res.ok) return res.response;
    const unwrapped = res.data.label !== undefined ? asRecord(res.data.label) : res.data;
    return Response.json(toLabelRow(unwrapped, serviceId), { status: res.status });
  }

  const labelIdMatch = subPath.match(/^labels\/([^/]+)$/);
  if (labelIdMatch) {
    const labelId = decodeURIComponent(labelIdMatch[1]);
    if (method === "PATCH") {
      const body = (await readJsonBody(request)) ?? {};
      const res = await forwardUpstream(request, url.toString(), "PATCH", body);
      if (!res.ok) return res.response;
      const unwrapped = res.data.label !== undefined ? asRecord(res.data.label) : res.data;
      return Response.json(toLabelRow(unwrapped, serviceId), { status: res.status });
    }
    if (method === "DELETE") {
      const res = await forwardUpstream(request, url.toString(), "DELETE");
      if (!res.ok) return res.response;
      const out: ScoringLabelDeleteResponse = {
        status: asString(res.data.status) ?? (res.data.ok === false ? "error" : "deleted"),
        id: asString(res.data.id) ?? labelId,
      };
      return Response.json(out, { status: res.status });
    }
  }

  if (method === "GET" && subPath === "audit") {
    const limit = boundedPositiveInt(url.searchParams.get("limit"), 200, 500);
    const res = await forwardUpstream(request, url.toString(), "GET");
    if (!res.ok) return res.response;
    const rawEntries = Array.isArray(res.data.entries)
      ? res.data.entries
      : Array.isArray(res.data.audit)
        ? res.data.audit
        : [];
    const audit: ScoringAuditRow[] = rawEntries.slice(0, limit).map((entry, idx) => {
      const row = asRecord(entry);
      return {
        id: asNumber(row.id) ?? idx + 1,
        timestamp: asString(row.timestamp) ?? asString(row.ts),
        action: asString(row.action),
        actor: asString(row.actor),
        details: row.details ?? null,
      };
    });
    const out: ScoringAuditListResponse = { audit, limit };
    return Response.json(out, { status: res.status });
  }

  if (method === "POST" && subPath === "retrain") {
    const body = await readJsonBody(request);
    if (body && typeof body.matrix === "object" && body.matrix !== null) {
      return null;
    }
    const sinceDays = boundedPositiveInt(url.searchParams.get("since_days"), 7, 90);
    const trained = await retrainCloudScoring(serviceId, {
      sinceDays,
      signal: request.signal,
    });
    const defaultMinAuc = 0.85;
    const out: ScoringRetrainResponse = {
      ok: trained.ok,
      matrix_version: trained.version,
      since_days: sinceDays,
      sessions_trained_on: trained.stats.sessions_kept,
      transitions: trained.stats.transitions,
      vocab_size: trained.stats.routes_seen,
      rejected: {
        too_few_events: trained.stats.sessions_dropped_short,
        too_fast: trained.stats.sessions_dropped_fast,
        kept: trained.stats.sessions_kept,
        routes_seen: trained.stats.routes_seen,
      },
      auc_against_labels:
        trained.auc_against_labels !== null
          ? {
              n_good: trained.n_good ?? 0,
              n_bad: trained.n_bad ?? 0,
              auc: trained.auc_against_labels,
              threshold: defaultMinAuc,
              passed: trained.auc_against_labels >= defaultMinAuc,
            }
          : null,
      default_min_auc: defaultMinAuc,
      local_matrix_saved: false,
      fos_matrix_published: Boolean(trained.ok),
      matrix_kv_written: trained.kv_pushed ?? true,
      deploy_hint: null,
    };
    return Response.json(out, { status: 200 });
  }

  return null;
}
