"use client";

import {
  toggleLogFieldGroup,
  toggleLogFieldOverride,
  toggleLogFieldPreset,
  updateLogFieldLimit,
} from "@/lib/log-field-config-ops";
import type { LogFieldsCatalog, ProvisionConfig } from "./types";

// ── log field helpers (pure transformations on config) ──
export function buildToggleGroup(catalog: LogFieldsCatalog | null | undefined) {
  return (
    prev: ProvisionConfig,
    groupId: string,
    checked: boolean,
  ): ProvisionConfig => ({
    ...prev,
    log_fields: toggleLogFieldGroup(prev.log_fields, catalog, groupId, checked),
  });
}

export function applyToggleField(
  prev: ProvisionConfig,
  fieldId: string,
  checked: boolean,
  defaultEnabledByGroup: boolean,
): ProvisionConfig {
  return {
    ...prev,
    log_fields: toggleLogFieldOverride(
      prev.log_fields,
      fieldId,
      checked,
      defaultEnabledByGroup,
    ),
  };
}

export function applyUpdateFieldLimit(
  prev: ProvisionConfig,
  fieldId: string,
  limit?: number,
): ProvisionConfig {
  return {
    ...prev,
    log_fields: updateLogFieldLimit(prev.log_fields, fieldId, limit),
  };
}

export function buildTogglePreset(
  catalog: LogFieldsCatalog | null | undefined,
  isPresetActive: (groups: string[]) => boolean,
) {
  return (
    prev: ProvisionConfig,
    presetGroups: string[],
  ): ProvisionConfig => ({
    ...prev,
    log_fields: toggleLogFieldPreset(
      prev.log_fields,
      catalog,
      presetGroups,
      isPresetActive,
    ),
  });
}
