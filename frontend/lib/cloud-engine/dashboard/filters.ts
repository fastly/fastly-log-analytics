import { CARD_FIELDS, CARD_LABELS } from "./summaries";
import { parseFilterToken } from "@/lib/filterToken";

export type FilterMode = "include" | "exclude";

export type DashboardFilter = {
  field: string;
  value: string;
  mode: FilterMode;
};

export const FILTER_FIELDS: { value: string; label: string }[] =
  CARD_FIELDS.map((value) => ({ value, label: CARD_LABELS[value] ?? value }));

/** URL form: `+field:value` includes, `-field:value` excludes. */
export function serializeFilter(filter: DashboardFilter): string {
  return `${filter.mode === "include" ? "+" : "-"}${filter.field}:${filter.value}`;
}

/** `customFields` are the custom card ids that may be filtered on too. */
export function parseFilter(
  text: string,
  customFields: readonly string[] = [],
): DashboardFilter | null {
  const parsed = parseFilterToken(text);
  if (!parsed) return null;
  const { field, value, mode } = parsed;
  if (
    (!CARD_FIELDS.includes(field) && !customFields.includes(field)) ||
    value === ""
  ) {
    return null;
  }
  return { field, value, mode };
}

export function parseFilters(
  texts: readonly string[],
  customFields: readonly string[] = [],
): DashboardFilter[] {
  const seen = new Set<string>();
  const filters: DashboardFilter[] = [];
  for (const text of texts) {
    const filter = parseFilter(text, customFields);
    if (!filter) continue;
    const key = serializeFilter(filter);
    if (seen.has(key)) continue;
    seen.add(key);
    filters.push(filter);
  }
  return filters;
}

export type Matcher = (value: unknown) => boolean;

/**
 * Canonical text of a cell value, shared by every filter path. Numbers use
 * JavaScript's shortest form, so a DOUBLE 3600 is "3600", never "3600.0".
 */
export function filterText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isNaN(value)) return null;
  return String(value);
}

const NUMERIC_PATTERN = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;

/** The number an exact filter value denotes, or null when it is not numeric. */
export function numericFilterValue(pattern: string): number | null {
  if (!NUMERIC_PATTERN.test(pattern)) return null;
  const parsed = Number(pattern);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Exact patterns compare numeric cells numerically; wildcard patterns and
 * non-numeric cells compare against `filterText`.
 */
export function createValueMatcher(pattern: string): Matcher {
  if (!pattern.includes("*")) {
    const numeric = numericFilterValue(pattern);
    return (value) => {
      if (typeof value === "number" || typeof value === "bigint") {
        return numeric !== null && Number(value) === numeric;
      }
      return filterText(value) === pattern;
    };
  }
  const source = pattern
    .split("*")
    .map((part) => part.replace(/[.+?^${}()|[\]\\]/g, "\\$&"))
    .join(".*");
  const regex = new RegExp(`^${source}$`, "s");
  return (value) => {
    const text = filterText(value);
    return text !== null && regex.test(text);
  };
}

export type RowFilter = {
  /** Columns the filter needs; a missing column never matches. */
  fields: string[];
  accepts: (read: (field: string) => unknown) => boolean;
};

export function isEdgeValue(value: unknown): boolean {
  return (
    value === true ||
    value === 1 ||
    (typeof value === "bigint" && value === BigInt(1)) ||
    value === "true" ||
    value === "1"
  );
}

/**
 * Include filters OR within a field and AND across fields; any matching
 * exclude filter drops the row. Null values match nothing, so they are
 * dropped by includes and kept by excludes.
 */
export function compileFilters(
  filters: readonly DashboardFilter[],
): RowFilter | null {
  if (filters.length === 0) return null;
  const includes = new Map<string, Matcher[]>();
  const excludes = new Map<string, Matcher[]>();
  for (const { field, value, mode } of filters) {
    const target = mode === "include" ? includes : excludes;
    const list = target.get(field) ?? [];
    list.push(createValueMatcher(value));
    target.set(field, list);
  }
  return {
    fields: [...new Set(filters.map((f) => f.field))],
    accepts(read) {
      for (const [field, matchers] of includes) {
        const value = read(field);
        if (filterText(value) === null || !matchers.some((m) => m(value)))
          return false;
      }
      for (const [field, matchers] of excludes) {
        const value = read(field);
        if (filterText(value) !== null && matchers.some((m) => m(value)))
          return false;
      }
      return true;
    },
  };
}
