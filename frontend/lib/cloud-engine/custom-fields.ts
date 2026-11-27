import { failure } from "./engine-utils";

type CloudService = {
  log_fields?: {
    custom_fields?: Array<Record<string, unknown>>;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

const ROUTE =
  /^\/api\/services\/([^/]+)\/custom-fields(?:\/(validate-vcl|export|import|[^/]+))?$/;

const SYSTEM_EXACT = new Set(["fastly_req_id"]);
const SYSTEM_PREFIXES = ["cmcd_", "edge_", "rum_", "lt_"];

function isSystemCustomField(name: string): boolean {
  return (
    SYSTEM_EXACT.has(name) ||
    SYSTEM_PREFIXES.some((prefix) => name.startsWith(prefix))
  );
}

function normalizeField(cf: Record<string, unknown>): Record<string, unknown> {
  return {
    ...cf,
    description: cf.description ?? "",
    enabled: cf.enabled !== false,
    filterable: cf.filterable !== false,
    show_in_dashboard: cf.show_in_dashboard === true,
    show_in_logs: cf.show_in_logs !== false,
    collection_stage: cf.collection_stage ?? "deliver",
    sql_type: cf.sql_type ?? cf.duckdb_type ?? "VARCHAR",
    value_type: cf.value_type ?? "string",
    bytes_estimate: cf.bytes_estimate ?? 20,
  };
}

function userCustomFields(service: CloudService): Array<Record<string, unknown>> {
  const all = Array.isArray(service.log_fields?.custom_fields)
    ? service.log_fields!.custom_fields!
    : [];
  return all
    .filter(
      (cf) =>
        cf &&
        typeof cf === "object" &&
        typeof cf.name === "string" &&
        !isSystemCustomField(cf.name),
    )
    .map(normalizeField);
}

async function readService(serviceId: string, signal: AbortSignal) {
  const response = await fetch(
    `/api/admin/services/${encodeURIComponent(serviceId)}`,
    { credentials: "include", signal },
  );
  if (!response.ok) return { response };
  const { service, generation } = (await response.json()) as {
    service?: CloudService;
    generation?: string;
  };
  if (!service || !generation) {
    return { response: failure("Missing native service configuration or generation") };
  }
  return { service, generation };
}

async function patchCustomFields(
  serviceId: string,
  generation: string,
  service: CloudService,
  customFields: Array<Record<string, unknown>>,
  signal: AbortSignal,
) {
  const response = await fetch(
    `/api/admin/services/${encodeURIComponent(serviceId)}`,
    {
      method: "PATCH",
      credentials: "include",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        generation,
        log_fields: {
          ...(service.log_fields ?? {}),
          custom_fields: customFields,
        },
      }),
      signal,
    },
  );
  if (!response.ok) return { response };
  const { service: updated, generation: nextGen } = (await response.json()) as {
    service?: CloudService;
    generation?: string;
  };
  if (!updated || !nextGen) {
    return { response: failure("Missing native service configuration or generation") };
  }
  return { service: updated, generation: nextGen };
}

async function validateCustomFieldRemote(
  field: Record<string, unknown>,
  existingNames: string[],
  signal: AbortSignal,
): Promise<{ valid: boolean; errors: string[] }> {
  const response = await fetch("/api/admin/log-fields/custom/validate", {
    method: "POST",
    credentials: "include",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ field, existing_names: existingNames }),
    signal,
  });
  if (!response.ok) {
    const errBody = (await response.json().catch(() => ({}))) as {
      detail?: unknown;
      error?: string;
    };
    return {
      valid: false,
      errors: [String(errBody.error ?? errBody.detail ?? `Validation failed (${response.status})`)],
    };
  }
  const data = (await response.json()) as { valid?: boolean; errors?: string[] };
  const errors = Array.isArray(data.errors) ? data.errors : [];
  return { valid: data.valid !== false && errors.length === 0, errors };
}

function decodePart(raw: string): string {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

function validationErrorResponse(errors: string[]): Response {
  return Response.json(
    {
      detail: {
        error: "validation_error",
        messages: errors,
        errors,
      },
    },
    { status: 422 },
  );
}

export async function cloudCustomFields(request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  const match = url.pathname.match(ROUTE);
  if (!match) return null;
  const serviceId = decodePart(match[1]);
  const sub = match[2] ? decodePart(match[2]) : undefined;
  const { signal } = request;
  const method = request.method.toUpperCase();

  if (!sub && method === "GET") {
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    return Response.json({ fields: userCustomFields(read.service) });
  }

  if (sub === "export" && method === "GET") {
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    const fields = userCustomFields(read.service);
    return new Response(JSON.stringify({ custom_fields: fields }), {
      status: 200,
      headers: {
        "content-type": "application/json",
        "content-disposition": `attachment; filename=custom_fields_${serviceId}.json`,
      },
    });
  }

  if (sub === "validate-vcl" && method === "POST") {
    let input: {
      vcl_log_expression?: string;
      collection_stage?: string;
      log_fields_config?: Record<string, unknown>;
    };
    try {
      input = (await request.json()) as typeof input;
    } catch {
      return failure("invalid_json", 400);
    }
    const candidate = {
      name: "lint_check",
      label: "Lint Check",
      vcl_log_expression: input?.vcl_log_expression ?? "",
      collection_stage: input?.collection_stage ?? "deliver",
      sql_type: "VARCHAR",
      duckdb_type: "VARCHAR",
      value_type: "string",
      bytes_estimate: 20,
      enabled: true,
    };
    const validation = await validateCustomFieldRemote(candidate, [], signal);
    let formatLength: number | null = null;
    let maxLength = 12288;
    const errors = [...validation.errors];
    const warnings: string[] = [];
    if (errors.length === 0) {
      const svcRes = await fetch(
        `/api/admin/services/${encodeURIComponent(serviceId)}`,
        { credentials: "include", signal },
      ).catch(() => null);
      const svcPayload = svcRes?.ok
        ? ((await svcRes.json().catch(() => null)) as { service?: CloudService } | null)
        : null;
      const service = svcPayload?.service;
      const baseLf =
        input?.log_fields_config && typeof input.log_fields_config === "object"
          ? input.log_fields_config
          : (service?.log_fields ?? {});
      const existingAll = Array.isArray(service?.log_fields?.custom_fields)
        ? service!.log_fields!.custom_fields!
        : [];
      const previewRes = await fetch("/api/admin/log-fields/preview", {
        method: "POST",
        credentials: "include",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          log_fields: { ...baseLf, custom_fields: [...existingAll, candidate] },
          cmcd: service?.cmcd,
          scoring: service?.scoring,
          capture: service?.capture,
        }),
        signal,
      }).catch(() => null);
      if (previewRes?.ok) {
        const preview = (await previewRes.json()) as {
          format_length?: number;
          max_length?: number;
        };
        if (typeof preview.format_length === "number") formatLength = preview.format_length;
        if (typeof preview.max_length === "number") maxLength = preview.max_length;
      }
    }
    return Response.json({
      valid: errors.length === 0,
      errors,
      warnings,
      format_length: formatLength,
      format_length_limit: maxLength,
    });
  }

  if (sub === "import" && method === "POST") {
    let input: { custom_fields?: Array<Record<string, unknown>> };
    try {
      input = (await request.json()) as typeof input;
    } catch {
      return failure("invalid_json", 400);
    }
    if (!Array.isArray(input?.custom_fields)) {
      return failure("invalid_input", 400);
    }
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    const existingAll = Array.isArray(read.service.log_fields?.custom_fields)
      ? [...read.service.log_fields!.custom_fields!]
      : [];
    const byName = new Map<string, number>();
    existingAll.forEach((cf, idx) => {
      if (cf && typeof cf.name === "string") byName.set(cf.name, idx);
    });
    const now = new Date().toISOString();
    let imported = 0;
    let skipped = 0;
    const errors: string[] = [];
    for (const raw of input.custom_fields) {
      if (!raw || typeof raw !== "object" || typeof raw.name !== "string" || !raw.name) {
        errors.push("Custom field 'name' must be a non-empty string");
        skipped += 1;
        continue;
      }
      if (isSystemCustomField(raw.name)) {
        errors.push(`${raw.name}: cannot import system-managed field`);
        skipped += 1;
        continue;
      }
      const existingNames = existingAll
        .map((cf) => (typeof cf.name === "string" ? cf.name : ""))
        .filter((n) => n && n !== raw.name);
      const normalized = normalizeField({
        ...raw,
        duckdb_type: raw.duckdb_type ?? raw.sql_type ?? "VARCHAR",
        updated_at: now,
        created_at: raw.created_at ?? now,
      });
      const check = await validateCustomFieldRemote(normalized, existingNames, signal);
      if (!check.valid) {
        errors.push(...check.errors.map((e) => `${raw.name}: ${e}`));
        skipped += 1;
        continue;
      }
      const idx = byName.get(raw.name);
      if (idx !== undefined) {
        existingAll[idx] = { ...existingAll[idx], ...normalized, updated_at: now };
      } else {
        byName.set(raw.name, existingAll.length);
        existingAll.push(normalized);
      }
      imported += 1;
    }
    if (errors.length > 0 && imported === 0) {
      return validationErrorResponse(errors);
    }
    const patched = await patchCustomFields(
      serviceId,
      read.generation,
      read.service,
      existingAll,
      signal,
    );
    if (patched.response) return patched.response;
    return Response.json({
      ok: true,
      imported,
      skipped,
      errors,
      fields: userCustomFields(patched.service),
    });
  }

  if (!sub && method === "POST") {
    let field: Record<string, unknown>;
    try {
      field = (await request.json()) as Record<string, unknown>;
    } catch {
      return failure("invalid_json", 400);
    }
    const name = typeof field?.name === "string" ? field.name : "";
    if (!name) return validationErrorResponse(["Field name is required"]);
    if (isSystemCustomField(name)) {
      return validationErrorResponse([
        `Field '${name}' is system-managed and cannot be created manually.`,
      ]);
    }
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    const existingAll = Array.isArray(read.service.log_fields?.custom_fields)
      ? [...read.service.log_fields!.custom_fields!]
      : [];
    const existingNames = existingAll
      .map((cf) => (typeof cf.name === "string" ? cf.name : ""))
      .filter(Boolean);
    const now = new Date().toISOString();
    const candidate = normalizeField({
      ...field,
      duckdb_type: field.duckdb_type ?? field.sql_type ?? "VARCHAR",
      created_at: now,
      updated_at: now,
    });
    const check = await validateCustomFieldRemote(candidate, existingNames, signal);
    if (!check.valid) {
      return validationErrorResponse(check.errors);
    }
    const nextAll = [...existingAll, candidate];
    const patched = await patchCustomFields(
      serviceId,
      read.generation,
      read.service,
      nextAll,
      signal,
    );
    if (patched.response) return patched.response;
    return Response.json({
      ok: true,
      field: candidate,
      fields: userCustomFields(patched.service),
    });
  }

  if (sub && method === "PATCH") {
    if (isSystemCustomField(sub)) {
      return validationErrorResponse([
        `Field '${sub}' is system-managed and cannot be edited.`,
      ]);
    }
    let updates: Record<string, unknown>;
    try {
      updates = (await request.json()) as Record<string, unknown>;
    } catch {
      return failure("invalid_json", 400);
    }
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    const existingAll = Array.isArray(read.service.log_fields?.custom_fields)
      ? [...read.service.log_fields!.custom_fields!]
      : [];
    const idx = existingAll.findIndex((cf) => cf?.name === sub);
    if (idx < 0) {
      return Response.json({ detail: { error: `Custom field '${sub}' not found` } }, { status: 404 });
    }
    const now = new Date().toISOString();
    const updated = normalizeField({
      ...existingAll[idx],
      ...Object.fromEntries(Object.entries(updates ?? {}).filter(([, v]) => v !== undefined)),
      name: sub,
      duckdb_type: updates?.sql_type ?? updates?.duckdb_type ?? existingAll[idx].duckdb_type ?? existingAll[idx].sql_type ?? "VARCHAR",
      updated_at: now,
    });
    const otherNames = existingAll
      .filter((_, i) => i !== idx)
      .map((cf) => (typeof cf.name === "string" ? cf.name : ""))
      .filter(Boolean);
    const check = await validateCustomFieldRemote(updated, otherNames, signal);
    if (!check.valid) {
      return validationErrorResponse(check.errors);
    }
    const nextAll = [...existingAll];
    nextAll[idx] = updated;
    const patched = await patchCustomFields(
      serviceId,
      read.generation,
      read.service,
      nextAll,
      signal,
    );
    if (patched.response) return patched.response;
    return Response.json({
      ok: true,
      field: updated,
      fields: userCustomFields(patched.service),
    });
  }

  if (sub && method === "DELETE") {
    if (isSystemCustomField(sub)) {
      return validationErrorResponse([
        `Field '${sub}' is system-managed and cannot be deleted.`,
      ]);
    }
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    const existingAll = Array.isArray(read.service.log_fields?.custom_fields)
      ? [...read.service.log_fields!.custom_fields!]
      : [];
    const target = existingAll.find((cf) => cf?.name === sub);
    if (!target) {
      return Response.json({ detail: { error: `Custom field '${sub}' not found` } }, { status: 404 });
    }
    const nextAll = existingAll.filter((cf) => cf?.name !== sub);
    const patched = await patchCustomFields(
      serviceId,
      read.generation,
      read.service,
      nextAll,
      signal,
    );
    if (patched.response) return patched.response;
    return Response.json({
      ok: true,
      field: normalizeField(target),
      fields: userCustomFields(patched.service),
    });
  }

  return null;
}
