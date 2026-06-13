"use client";

import type { ProvisionConfig } from "./types";

// ── log field helpers (pure transformations on config) ──
export function buildToggleGroup(catalog: any) {
  return (
    prev: ProvisionConfig,
    groupId: string,
    checked: boolean,
  ): ProvisionConfig => {
    const lf = { ...prev.log_fields };
    const nextGroups = new Set<string>(lf.groups || []);
    if (checked) {
      nextGroups.add(groupId);
      let changed = true;
      while (changed) {
        changed = false;
        catalog?.groups.forEach((g: any) => {
          if (
            nextGroups.has(g.id) &&
            g.requires &&
            !nextGroups.has(g.requires)
          ) {
            nextGroups.add(g.requires);
            changed = true;
          }
        });
      }
    } else {
      nextGroups.delete(groupId);
    }
    return { ...prev, log_fields: { ...lf, groups: Array.from(nextGroups) } };
  };
}

export function applyToggleField(
  prev: ProvisionConfig,
  fieldId: string,
  checked: boolean,
  defaultEnabledByGroup: boolean,
): ProvisionConfig {
  const lf = { ...prev.log_fields };
  const overrides = { ...(lf.field_overrides || {}) };
  if (checked === defaultEnabledByGroup) {
    delete overrides[fieldId];
  } else {
    overrides[fieldId] = checked;
  }
  return { ...prev, log_fields: { ...lf, field_overrides: overrides } };
}

export function applyUpdateFieldLimit(
  prev: ProvisionConfig,
  fieldId: string,
  limit?: number,
): ProvisionConfig {
  const lf = { ...prev.log_fields };
  const field_limits = { ...(lf.field_limits || {}) };
  if (limit === undefined) {
    delete field_limits[fieldId];
  } else {
    field_limits[fieldId] = limit;
  }
  return { ...prev, log_fields: { ...lf, field_limits } };
}

export function buildTogglePreset(
  catalog: any,
  isPresetActive: (groups: string[]) => boolean,
) {
  return (
    prev: ProvisionConfig,
    presetGroups: string[],
  ): ProvisionConfig => {
    const lf = { ...prev.log_fields };
    const currentGroups = new Set<string>(lf.groups || []);
    const allActive = presetGroups.every((g) => currentGroups.has(g));

    const nextGroups = new Set<string>(lf.groups || []);

    if (allActive) {
      // Toggle OFF: remove groups in this preset.
      // First, figure out which OTHER presets are currently active.
      const otherActivePresetsGroups = new Set<string>();
      if (catalog?.presets) {
        Object.entries(catalog.presets).forEach(
          ([_key, preset]: [string, any]) => {
            if (
              preset.groups.length !== presetGroups.length ||
              !preset.groups.every((g: string) => presetGroups.includes(g))
            ) {
              if (isPresetActive(preset.groups)) {
                preset.groups.forEach((g: string) =>
                  otherActivePresetsGroups.add(g),
                );
              }
            }
          },
        );
      }

      presetGroups.forEach((g) => {
        if (!otherActivePresetsGroups.has(g)) {
          nextGroups.delete(g);
          catalog?.groups.forEach((cg: any) => {
            if (cg.requires === g && !otherActivePresetsGroups.has(cg.id)) {
              nextGroups.delete(cg.id);
            }
          });
        }
      });
    } else {
      presetGroups.forEach((g) => nextGroups.add(g));

      let changed = true;
      while (changed) {
        changed = false;
        catalog?.groups.forEach((cg: any) => {
          if (
            nextGroups.has(cg.id) &&
            cg.requires &&
            !nextGroups.has(cg.requires)
          ) {
            nextGroups.add(cg.requires);
            changed = true;
          }
        });
      }
    }

    return { ...prev, log_fields: { ...lf, groups: Array.from(nextGroups) } };
  };
}
