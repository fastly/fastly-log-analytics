"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { CollapsibleGroup } from "@/components/LogSettingsModal/LogSettingsModal";
import { FileJson, Loader2, Shield } from "lucide-react";
import { cn, formatBytes } from "@/lib/utils";
import type { WizardState } from "../useWizardState";

export function FieldsStep({ s }: { s: WizardState }) {
  const { config, setConfig, catalog, isLoadingCatalog } = s;
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-6 max-w-4xl mx-auto">
        <div className="flex items-center justify-between pb-2 border-b">
          <div className="flex items-center gap-2">
            <FileJson className="h-5 w-5 text-primary" />
            <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
              Log Fields
            </h3>
          </div>
          {!isLoadingCatalog && (
            <div className="text-xs font-mono text-muted-foreground bg-muted/50 px-3 py-1 rounded-md border">
              Est. ~{formatBytes(s.estimatedBytes)} / line
            </div>
          )}
        </div>

        {isLoadingCatalog ? (
          <div className="h-[200px] flex items-center justify-center bg-muted/10 rounded-lg border border-dashed">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <div className="space-y-6">
            <div className="space-y-4">
              <p className="text-sm text-muted-foreground">
                Select the data fields to capture at the edge. More fields
                provide richer insights but increase storage and bandwidth
                costs.
              </p>
              <div className="p-3 bg-blue-500/10 border border-blue-500/20 text-blue-700 dark:text-blue-400 rounded-md text-xs">
                <strong>Note:</strong> Custom log fields (e.g. tracking specific
                HTTP headers or application IDs) can be configured from the
                Admin dashboard after initial provisioning is complete.
              </div>
              {catalog?.presets && (
                <div className="flex flex-wrap gap-2 pt-2 items-center">
                  {Object.entries(catalog.presets).map(
                    ([key, preset]: [string, any]) => {
                      const isMinimal = key === "minimal";
                      const active =
                        isMinimal || s.isPresetActive(preset.groups || []);
                      return (
                        <Button
                          key={key}
                          variant={active ? "default" : "outline"}
                          size="sm"
                          className={cn(
                            "h-8 text-xs font-semibold transition-all",
                            active && "ring-2 ring-primary/20",
                            isMinimal && "opacity-80",
                          )}
                          title={preset.description}
                          onClick={() =>
                            !isMinimal && s.togglePreset(preset.groups || [])
                          }
                          disabled={isMinimal}
                        >
                          {preset.label || key}
                        </Button>
                      );
                    },
                  )}
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 text-xs font-semibold text-muted-foreground hover:text-foreground ml-auto"
                    onClick={() =>
                      setConfig((prev) => ({
                        ...prev,
                        log_fields: { groups: [], field_overrides: {} },
                      }))
                    }
                  >
                    Clear All
                  </Button>
                </div>
              )}
            </div>
            {!config.ngwaf_workspace_id && (
              <div className="flex items-center gap-2 text-xs text-muted-foreground bg-muted/30 border border-dashed rounded-lg px-3 py-2">
                <Shield className="h-3.5 w-3.5 shrink-0" />
                WAF / NGWAF fields (group J) are hidden — no NGWAF workspace
                selected.
              </div>
            )}
            <div className="grid grid-cols-1 gap-3 pb-8">
              {(catalog?.groups ?? [])
                .filter(
                  (g: any) => config.ngwaf_workspace_id || g.id !== "J",
                )
                .map((g: any) => (
                  <CollapsibleGroup
                    key={g.id}
                    group={g}
                    catalog={catalog}
                    config={config.log_fields}
                    toggleGroup={s.toggleGroup}
                    toggleField={s.toggleField}
                    updateFieldLimit={s.updateFieldLimit}
                  />
                ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
