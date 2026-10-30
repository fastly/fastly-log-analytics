import type { components } from "@/types/api.generated";
import type { LogFieldsConfig } from "@/types/api";

// ── Log Field Config Operations ───────────────────────────────────────────────

export type LogFieldsCatalogLike =
  | components["schemas"]["LogFieldsCatalogResponse"]
  | null
  | undefined;

function applyGroupDependencies<T extends string | null>(
  groups: Set<T>,
  catalog: LogFieldsCatalogLike,
): void {
  const catalogGroups = catalog?.groups || [];
  let changed = true;
  while (changed) {
    changed = false;
    catalogGroups.forEach((g) => {
      if (
        g.id != null &&
        groups.has(g.id as T) &&
        g.requires &&
        !groups.has(g.requires as T)
      ) {
        groups.add(g.requires as T);
        changed = true;
      }
    });
  }
}

export function resolveEnabledGroupsWithDeps(
  groups: string[] | undefined,
  catalog: LogFieldsCatalogLike,
): Set<string | null> {
  const enabledGroups = new Set<string | null>([null, ...(groups || [])]);
  applyGroupDependencies(enabledGroups, catalog);
  return enabledGroups;
}

export function toggleLogFieldGroup(
  prev: LogFieldsConfig,
  catalog: LogFieldsCatalogLike,
  groupId: string,
  checked: boolean,
): LogFieldsConfig {
  const nextGroups = new Set<string>(prev.groups || []);
  if (checked) {
    nextGroups.add(groupId);
    applyGroupDependencies(nextGroups, catalog);
  } else {
    nextGroups.delete(groupId);
  }
  return { ...prev, groups: Array.from(nextGroups) };
}

export function toggleLogFieldOverride(
  prev: LogFieldsConfig,
  fieldId: string,
  checked: boolean,
  defaultEnabledByGroup: boolean,
): LogFieldsConfig {
  const overrides = { ...(prev.field_overrides || {}) };
  if (checked === defaultEnabledByGroup) {
    delete overrides[fieldId];
  } else {
    overrides[fieldId] = checked;
  }
  return { ...prev, field_overrides: overrides };
}

export function updateLogFieldLimit(
  prev: LogFieldsConfig,
  fieldId: string,
  limit?: number,
): LogFieldsConfig {
  const field_limits = { ...(prev.field_limits || {}) };
  if (limit === undefined) {
    delete field_limits[fieldId];
  } else {
    field_limits[fieldId] = limit;
  }
  return { ...prev, field_limits };
}

export function isLogFieldPresetActive(
  config: LogFieldsConfig | undefined,
  groups: string[],
): boolean {
  if (!groups.length) return false;
  const currentGroups = new Set(config?.groups || []);
  return groups.every((g) => currentGroups.has(g));
}

export function toggleLogFieldPreset(
  prev: LogFieldsConfig,
  catalog: LogFieldsCatalogLike,
  presetGroups: string[],
  isPresetActiveFn?: (groups: string[]) => boolean,
): LogFieldsConfig {
  const currentGroups = new Set<string>(prev.groups || []);
  const allActive = presetGroups.every((g) => currentGroups.has(g));
  const nextGroups = new Set<string>(prev.groups || []);
  const checkActive =
    isPresetActiveFn ?? ((groups: string[]) => isLogFieldPresetActive(prev, groups));

  if (allActive) {
    const otherActivePresetsGroups = new Set<string>();
    if (catalog?.presets) {
      Object.entries(catalog.presets).forEach(([_key, preset]) => {
        if (
          preset.groups.length !== presetGroups.length ||
          !preset.groups.every((g: string) => presetGroups.includes(g))
        ) {
          if (checkActive(preset.groups)) {
            preset.groups.forEach((g: string) => otherActivePresetsGroups.add(g));
          }
        }
      });
    }

    presetGroups.forEach((g) => {
      if (!otherActivePresetsGroups.has(g)) {
        nextGroups.delete(g);
        catalog?.groups?.forEach((cg) => {
          if (
            cg.id != null &&
            cg.requires === g &&
            !otherActivePresetsGroups.has(cg.id)
          ) {
            nextGroups.delete(cg.id);
          }
        });
      }
    });
  } else {
    presetGroups.forEach((g) => nextGroups.add(g));
    applyGroupDependencies(nextGroups, catalog);
  }

  return { ...prev, groups: Array.from(nextGroups) };
}

export function estimateLogFieldBytesPerLine(
  config: LogFieldsConfig | undefined,
  catalog: LogFieldsCatalogLike,
): number {
  if (!catalog?.fields) return 0;
  let total = 0;
  const enabledGroups = new Set(config?.groups || []);
  const overrides = config?.field_overrides || {};
  for (const field of catalog.fields) {
    const inGroup = field.group === null || enabledGroups.has(field.group);
    const override = overrides[field.id];
    if (override === true) {
      total += field.typical_bytes || 0;
      continue;
    }
    if (override === false) continue;
    if (inGroup) total += field.typical_bytes || 0;
  }
  return total;
}

export function evaluateLogFieldPresetSummary(
  config: LogFieldsConfig | undefined,
  catalog: LogFieldsCatalogLike,
): {
  enabledGroupsSet: Set<string>;
  bestPresetName: string | null;
  disabledCount: number;
} {
  const enabledGroupsSet = new Set(config?.groups || []);
  const overrides = config?.field_overrides || {};
  const hasOverrides = Object.keys(overrides).length > 0;
  let bestPresetName: string | null = null;

  if (catalog?.presets && !hasOverrides) {
    for (const [key, preset] of Object.entries(catalog.presets)) {
      const presetGroups = preset.groups;
      if (
        presetGroups.length === enabledGroupsSet.size &&
        presetGroups.every((g) => enabledGroupsSet.has(g))
      ) {
        bestPresetName = preset.label || key;
        break;
      }
    }
  }

  const disabledCount = (catalog?.groups || []).filter(
    (g) => !(g.locked || (g.id != null && enabledGroupsSet.has(g.id))),
  ).length;

  return { enabledGroupsSet, bestPresetName, disabledCount };
}
