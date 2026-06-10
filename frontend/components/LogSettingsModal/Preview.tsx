'use client'

import React from 'react'
import { components } from '@/types/api.generated'
import { AlertTriangle } from 'lucide-react'
import { cn, formatBytes } from '@/lib/utils'

type ServiceConfig = components['schemas']['ServiceConfig']
type LogFieldsConfig = components['schemas']['LogFieldsConfig']

interface ReviewStepProps {
  service: ServiceConfig
  catalog: any
  config: LogFieldsConfig
  period: string
  sampleRate: number
  edgeOnly: boolean
  customCondition: string
  estimatedBytes: number
}

export function ReviewStep({
  service,
  catalog,
  config,
  period,
  sampleRate,
  edgeOnly,
  customCondition,
  estimatedBytes,
}: ReviewStepProps) {
  return (
    <div className="space-y-6">
      <h3 className="text-lg font-semibold border-b pb-2">Review Log Configuration Changes</h3>
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div className="p-4 border rounded-lg bg-muted/20 space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">General Settings</p>
            <p className="text-sm font-medium">Log Period: {period} seconds</p>
            <p className="text-sm font-medium">Sample Rate: {sampleRate}%</p>
            <p className="text-sm font-medium">Edge Only: {edgeOnly ? "Yes" : "No"}</p>
            {customCondition && (
              <p className="text-sm font-medium truncate" title={customCondition}>
                Custom Condition: <code className="text-[10px] bg-background px-1 rounded border">{customCondition}</code>
              </p>
            )}
          </div>

          <div className="p-4 border rounded-lg bg-muted/20 space-y-1">
            <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider flex items-center justify-between">
              Log Configuration
              <span className="font-mono text-[10px] bg-background px-1.5 py-0.5 rounded border normal-case font-medium">
                ~{formatBytes(estimatedBytes)} / line
              </span>
            </p>
            <div className="flex flex-wrap gap-1.5 pt-2">
              {(() => {
                const enabledGroupsSet = new Set(config.groups || []);
                const overrides = config.field_overrides || {};
                const hasOverrides = Object.keys(overrides).length > 0;
                let bestPresetName = null;

                if (catalog?.presets && !hasOverrides) {
                  for (const [key, preset] of Object.entries(catalog.presets)) {
                    const presetGroups = (preset as any).groups || [];
                    if (presetGroups.length === enabledGroupsSet.size && presetGroups.every((g: string) => enabledGroupsSet.has(g))) {
                      bestPresetName = (preset as any).label || key;
                      break;
                    }
                  }
                }

                const disabledCount = (catalog?.groups || []).filter((g: any) => !(g.locked || enabledGroupsSet.has(g.id))).length || 0;

                if (bestPresetName) {
                  return (
                    <>
                      <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                        {bestPresetName} Preset
                      </div>
                      {disabledCount > 0 && (
                        <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                          +{disabledCount} disabled
                        </div>
                      )}
                    </>
                  );
                }

                return (
                  <>
                    <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                      Custom Configuration
                    </div>
                    {(catalog?.groups || []).map((g: any) => {
                      const isEnabled = g.locked || enabledGroupsSet.has(g.id);
                      if (!isEnabled) return null;
                      return (
                        <div key={g.id || "core"} className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-primary/10 text-primary border border-primary/20">
                          {g.label}
                        </div>
                      );
                    })}
                    {disabledCount > 0 && (
                      <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                        +{disabledCount} disabled
                      </div>
                    )}
                  </>
                );
              })()}
            </div>
          </div>
        </div>

        {/* Custom Fields Summary */}
        {(() => {
           const customFields = (catalog?.fields || []).filter((f: any) => f.is_custom);
           if (customFields.length === 0) return null;
           return (
             <div className="p-4 border rounded-lg bg-muted/20 space-y-3">
               <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">Custom Fields ({customFields.length})</p>
               <div className="flex flex-wrap gap-2">
                 {customFields.map((cf: any) => (
                    <div key={cf.id} className="flex items-center gap-1.5 px-2.5 py-1 rounded bg-background border text-xs shadow-sm">
                       <span className="font-medium">{cf.label}</span>
                       <span className="text-[10px] text-muted-foreground font-mono">({cf.id})</span>
                    </div>
                 ))}
               </div>
             </div>
           );
        })()}

        {/* Insights Section */}
        <div className="p-4 border rounded-lg bg-muted/20 space-y-3">
          <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider flex justify-between items-center">
            Automated Insights
            <span className="text-[10px] normal-case font-medium">Derived from logs</span>
          </p>
          <div className="grid grid-cols-2 gap-3">
            {(catalog as any)?.insights?.map((insight: any) => {
              const enabledGroups = new Set([null, ...(config.groups || [])]);
              const catalogGroups = (catalog as any)?.groups || [];
              let changed = true;
              while (changed) {
                changed = false;
                catalogGroups.forEach((g: any) => {
                  if (enabledGroups.has(g.id) && g.requires && !enabledGroups.has(g.requires)) {
                    enabledGroups.add(g.requires);
                    changed = true;
                  }
                });
              }

              const isEnabled = insight.required_groups?.every((rg: any) => enabledGroups.has(rg));
              return (
                <div key={insight.id} className={cn("flex items-start gap-3 border rounded-lg p-2.5 bg-background shadow-sm transition-all", isEnabled ? "border-primary/20 bg-primary/5" : "opacity-50 grayscale bg-muted/50")}>
                  <div className={cn("w-2 h-2 mt-1.5 rounded-full shrink-0", isEnabled ? "bg-primary" : "bg-muted-foreground")} />
                  <div className="space-y-1 overflow-hidden">
                    <h4 className="text-xs font-semibold tracking-tight truncate" title={insight.name}>{insight.name}</h4>
                    <p className="text-[10px] text-muted-foreground line-clamp-2 leading-snug">{insight.description}</p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-900/50 p-4 rounded-lg flex gap-3">
          <AlertTriangle className="h-5 w-5 text-amber-600 shrink-0 mt-0.5" />
          <div className="text-sm text-amber-800 dark:text-amber-300 leading-normal">
            {service.storage_mode === "terraform" ? (
              <>
                <strong>Note:</strong> This service is managed via Terraform. Direct deployment is disabled. Please view and export the updated Terraform code to apply these changes.
              </>
            ) : (
              <>
                <strong>Important:</strong> Deploying this configuration will clone your active Fastly service version, update the logging endpoints, and activate the new version. Data collected before this change will have <code>null</code> for any newly added fields.
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
