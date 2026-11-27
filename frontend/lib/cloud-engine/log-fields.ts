import type { components } from "@/types/api.generated";
import { isIpFamilyField } from "@/lib/utils";
import { parquetColumns } from "./schema";

type Catalog = components["schemas"]["LogFieldsCatalogResponse"];
type Field = NonNullable<Catalog["fields"]>[number];

function isPiiSchemaColumn(name: string): boolean {
  const raw = name.trim().toLowerCase();
  if (!raw) return false;
  if (isIpFamilyField(raw)) return true;
  let stripped = raw.replace(/^(?:xfilter_|filter_|_cf_)/, "").replace(/_\d+$/, "");
  if (isIpFamilyField(stripped)) return true;
  stripped = stripped.replace(/[^\w]/g, "");
  return isIpFamilyField(stripped);
}

export function cloudLogFieldsCatalog(
  catalog: Catalog,
  logFields?: { custom_fields?: Array<Record<string, unknown>> },
): Catalog {
  const fields: Field[] = [...(catalog.fields ?? [])];
  for (const field of logFields?.custom_fields ?? []) {
    if (field.enabled === false || typeof field.name !== "string") continue;
    const id = field.name;
    const entry: Field = {
      ...field,
      id,
      label: String(field.label ?? id),
      description: String(field.description ?? ""),
      group: "custom",
      duckdb_type: String(field.sql_type ?? field.duckdb_type ?? "VARCHAR"),
      sql_type: String(field.sql_type ?? field.duckdb_type ?? "VARCHAR"),
      is_custom: true,
      filterable: field.filterable !== false,
      show_in_dashboard: field.show_in_dashboard === true,
      show_in_logs: field.show_in_logs !== false,
    };
    const index = fields.findIndex((existing) => existing.id === id);
    if (index < 0) fields.push(entry);
    else fields[index] = entry;
  }
  return {
    ...catalog,
    fields: fields.map((field) => ({
      ...field,
      sql_type: String(field.sql_type ?? field.duckdb_type ?? "VARCHAR"),
    })),
    groups: [
      ...(catalog.groups ?? []).filter((group) => group.id !== "custom"),
      ...(fields.some((field) => field.is_custom) ? [{
        id: "custom", label: "Custom Fields", description: "Active custom log fields",
        locked: false, requires: null, note: null, recommended_with: null,
        total_bytes: 0, fields: fields.filter((field) => field.is_custom).map((field) => field.id),
      }] : []),
    ],
  };
}

export function cloudSchema(
  catalog: Catalog,
  activeFieldIds?: ReadonlySet<string>,
  maskIps = false,
) {
  const physical = (catalog.fields ?? []).filter(
    (field) =>
      field.group !== "METRICS" &&
      (!activeFieldIds || field.id === "timestamp" || activeFieldIds.has(field.id)) &&
      (!maskIps || !isPiiSchemaColumn(field.id)),
  );
  const columns = parquetColumns({
    fields: physical.map((field) => ({
      ...field,
      duckdb_type: String(field.duckdb_type ?? field.sql_type ?? "VARCHAR"),
    })),
  });
  return {
    table_name: "logs",
    schema: columns.map((column) => ({ ...column, column: column.name })),
    columns,
  };
}
