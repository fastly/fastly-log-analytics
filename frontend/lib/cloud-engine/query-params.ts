import type { FiltersPayload } from "@/types/api";
import { compileFiltersToSql, duckSqlString, type DuckDBColumnType } from "./dashboard/duckdb";
import type { DashboardFilter } from "./dashboard/filters";
import { parseEpochMs } from "./engine-utils";

type WireFilterSpec = { mode?: string; values?: unknown[] };
type WireFilters = Record<string, WireFilterSpec | null | undefined>;

export async function readRequestParams(request: Request): Promise<Record<string, unknown>> {
  const urlParams = Object.fromEntries(new URL(request.url).searchParams);
  if (request.method !== "POST") return urlParams;
  const body = await request.clone().json().catch(() => ({}));
  return {
    ...urlParams,
    ...(body && typeof body === "object" && !Array.isArray(body) ? body : {}),
  };
}

export function filtersPayloadToPredicates(
  filters: FiltersPayload | WireFilters | null | undefined,
  customFilters?: FiltersPayload | WireFilters | null,
): DashboardFilter[] {
  const out: DashboardFilter[] = [];
  for (const source of [filters, customFilters]) {
    if (!source || typeof source !== "object" || Array.isArray(source)) continue;
    for (const [rawField, entry] of Object.entries(source)) {
      if (!entry || !Array.isArray(entry.values)) continue;
      const field = rawField.replace(/_\d+$/, "");
      const mode = entry.mode === "exclude" ? "exclude" : "include";
      for (const rawValue of entry.values) {
        const value = String(rawValue);
        if (value) out.push({ field, value, mode });
      }
    }
  }
  return out;
}

export function dashboardFiltersToSql(
  filters: readonly DashboardFilter[],
  customColumnTypes?: ReadonlyMap<string, DuckDBColumnType>,
): string {
  return compileFiltersToSql(filters, false, [], customColumnTypes);
}

export function analyticsFiltersToSql(
  filters: FiltersPayload | WireFilters | null | undefined,
  customColumnTypes?: ReadonlyMap<string, DuckDBColumnType>,
): string {
  return dashboardFiltersToSql(filtersPayloadToPredicates(filters), customColumnTypes);
}

export function sqlString(value: unknown): string {
  return String(value).replaceAll("'", "''");
}

export function rangeServiceSql(
  params: {
    start_time?: string | number;
    end_time?: string | number;
    service_id?: string;
  },
  prefix: readonly string[] = ["1=1"],
): string {
  const clauses = [...prefix];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs !== null) clauses.push(`ts_ms >= ${startMs}`);
  if (endMs !== null) clauses.push(`ts_ms < ${endMs}`);
  if (params.service_id) {
    clauses.push(`(service_id = ${duckSqlString(String(params.service_id))} OR service_id IS NULL)`);
  }
  return clauses.join(" AND ");
}

export function queryRange(
  params: Record<string, unknown>,
  defaultHours: number,
): { start: string; end: string } {
  const end = String(params.end_time ?? params.end ?? params.clamp_end ?? new Date().toISOString());
  const start = String(
    params.start_time ??
    params.start ??
    new Date(Date.parse(end) - defaultHours * 3_600_000).toISOString(),
  );
  return { start, end };
}
