"use client";

import React from "react";
import { SSEProgressView } from "@/components/SSEModal";
import {
  ReviewCard,
  ReviewHeader,
  ReviewContent,
  ReviewItem,
} from "@/components/ui/review-card";
import {
  CheckCircle2,
  Cloud,
  Database,
  FileJson,
  Globe,
  Settings,
  Sparkles,
  XCircle,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { formatBytes } from "@/lib/format"
import type { WizardState } from "../useWizardState";
import { REGION_LABELS, SHIELD_LABELS } from "../types";

interface CatalogGroup {
  id: string;
  locked?: boolean;
}

interface CatalogPreset {
  groups?: string[];
  label?: string;
}

interface FieldCatalog {
  groups?: CatalogGroup[];
  presets?: Record<string, CatalogPreset>;
}

export function ExecuteStep({ s }: { s: WizardState }) {
  const { config, catalog, selectedService } = s;
  return (
    <div className="flex-1 overflow-y-auto min-h-0 flex flex-col p-8 items-center text-left">
      <div className="w-full max-w-2xl space-y-8">
        {s.isDeploying ? (
          <div className="space-y-6 w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="text-center space-y-2">
              <h3 className="text-2xl font-semibold tracking-tight">
                Provisioning: {selectedService?.name}
              </h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Setting up Fastly Object Storage, logging endpoints, and CDN
                proxy...
              </p>
            </div>

            <SSEProgressView
              lines={s.lines}
              status={s.status}
              error={s.sseError}
              className="h-[400px]"
              progressLabel="Progress"
              doneMessage="Provisioning completed successfully! You may now close this window."
            />
          </div>
        ) : (
          <>
            <div className="text-center space-y-2">
              <h3 className="text-2xl font-semibold tracking-tight">
                Review & Deploy
              </h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                You are about to provision the following resources.
              </p>
            </div>

            <div className="grid grid-cols-2 gap-4">
              {/* Enabled Features Summary Card */}
              <ReviewCard className="col-span-2 space-y-3">
                <ReviewHeader icon={Sparkles}>Enabled Features</ReviewHeader>
                <div className="flex flex-col sm:flex-row gap-3 pt-1">
                  {config.logging_enabled !== false && (
                    <div className="flex-1 flex items-center gap-3 border rounded-xl p-3 bg-primary/[0.03] border-primary/10 shadow-sm">
                      <div className="h-8 w-8 rounded-lg bg-primary/10 flex items-center justify-center text-primary shrink-0">
                        <Database className="h-4 w-4" />
                      </div>
                      <div className="flex flex-col min-w-0">
                        <span className="text-xs font-semibold text-foreground">Request Logging</span>
                        <span className="text-[10px] text-muted-foreground leading-snug">In-depth HTTP logs, edge metrics, and WAF protection.</span>
                      </div>
                    </div>
                  )}
                  {config.rum_enabled && (
                    <div className="flex-1 flex items-center gap-3 border rounded-xl p-3 bg-primary/[0.03] border-primary/10 shadow-sm">
                      <div className="h-8 w-8 rounded-lg bg-primary/10 flex items-center justify-center text-primary shrink-0">
                        <Globe className="h-4 w-4" />
                      </div>
                      <div className="flex flex-col min-w-0">
                        <span className="text-xs font-semibold text-foreground">Real User Monitoring (RUM)</span>
                        <span className="text-[10px] text-muted-foreground leading-snug">Client-side web vitals, speed insights, and session analytics.</span>
                      </div>
                    </div>
                  )}
                </div>
              </ReviewCard>

              <ReviewCard>
                <ReviewHeader icon={Cloud}>Target Service</ReviewHeader>
                <ReviewContent>
                  <ReviewItem
                    label="Service Name"
                    value={selectedService?.name}
                  />
                  {config.logging_enabled !== false && (
                    <>
                      <ReviewItem
                        label="Log Endpoint"
                        value={config.endpoint_name}
                      />
                      <ReviewItem
                        label="Sampling Rate / Period"
                        value={`${config.sample_rate}% / ${config.log_period}s`}
                      />
                    </>
                  )}
                  {config.logging_enabled !== false && config.custom_condition && (
                    <ReviewItem
                      label="Custom Condition"
                      value={config.custom_condition}
                      className="truncate font-mono text-[10px]"
                    />
                  )}
                </ReviewContent>{" "}
              </ReviewCard>

              <ReviewCard>
                <ReviewHeader icon={Globe}>CDN Edge Proxy</ReviewHeader>
                <ReviewContent>
                  <ReviewItem
                    label="Domain"
                    value={`${config.cdn_prefix}.global.ssl.fastly.net`}
                  />
                  <ReviewItem
                    label="Shield POP"
                    value={SHIELD_LABELS[config.cdn_shield] || "None"}
                  />
                </ReviewContent>
              </ReviewCard>

              <ReviewCard>
                <ReviewHeader icon={Database}>Object Storage</ReviewHeader>
                <ReviewContent>
                  <ReviewItem
                    label="Bucket"
                    value={config.fos_bucket_name}
                  />
                  <ReviewItem
                    label="Region"
                    value={REGION_LABELS[config.fos_region]}
                  />
                  <ReviewItem
                    label="Edge Only"
                    value={config.edge_only ? "Yes" : "No"}
                  />
                </ReviewContent>
              </ReviewCard>

              <ReviewCard>
                <ReviewHeader icon={Settings}>Automation</ReviewHeader>
                <ReviewContent className="gap-2.5">
                  <ReviewItem
                    variant="between"
                    label={`Sync every ${config.log_period >= 120 ? Math.floor(config.log_period / 120) + "m" : Math.floor(config.log_period / 2) + "s"}`}
                    className={cn(
                      !config.enable_cron_sync && "text-muted-foreground",
                    )}
                    value={
                      config.enable_cron_sync ? (
                        <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                      ) : (
                        <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                      )
                    }
                  />
                  <ReviewItem
                    variant="between"
                    label={`Commit to Iceberg every ${config.commit_interval_mins}m`}
                    className={cn(
                      !config.enable_cron_sync && "text-muted-foreground",
                    )}
                    value={
                      config.enable_cron_sync ? (
                        <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                      ) : (
                        <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                      )
                    }
                  />
                  <ReviewItem
                    variant="between"
                    label="Auto-delete Raw Logs"
                    className={cn(
                      (!config.delete_after || !config.enable_cron_sync) &&
                        "text-muted-foreground",
                    )}
                    value={
                      config.delete_after && config.enable_cron_sync ? (
                        <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                      ) : (
                        <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                      )
                    }
                  />
                  <ReviewItem
                    variant="between"
                    label="Daily Iceberg Optimization"
                    className={cn(
                      (!config.enable_cron_compact ||
                        !config.enable_cron_sync) &&
                        "text-muted-foreground",
                    )}
                    value={
                      config.enable_cron_compact && config.enable_cron_sync ? (
                        <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                      ) : (
                        <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                      )
                    }
                  />
                  {config.logging_enabled !== false && (
                    <ReviewItem
                      variant="between"
                      label="Log Retention"
                      className={cn(!config.enable_cron_sync && "text-muted-foreground")}
                      value={`${config.log_retention_days === 0 ? "Forever" : config.log_retention_days + " days"}`}
                    />
                  )}
                  {config.rum_enabled && (
                    <ReviewItem
                      variant="between"
                      label="RUM Retention"
                      className={cn(!config.enable_cron_sync && "text-muted-foreground")}
                      value={`${config.rum_retention_days === 0 ? "Forever" : config.rum_retention_days + " days"}`}
                    />
                  )}
                </ReviewContent>
              </ReviewCard>

              {/* Full Width Log Fields - only if logging is enabled */}
              {config.logging_enabled !== false && (
                <ReviewCard className="col-span-2 space-y-3">
                  <div className="flex justify-between items-center">
                    <ReviewHeader icon={FileJson}>Log Configuration</ReviewHeader>
                    <span className="font-mono text-[10px] bg-muted px-2 py-0.5 rounded text-muted-foreground border">
                      ~{formatBytes(s.estimatedBytes)} / line
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    {(() => {
                      const enabledGroupsSet = new Set(
                        config.log_fields?.groups || [],
                      );
                      const overrides = config.log_fields?.field_overrides || {};
                      const hasOverrides = Object.keys(overrides).length > 0;

                      let bestPresetName = null;
                      if (catalog?.presets && !hasOverrides) {
                        for (const [key, preset] of Object.entries(
                          catalog.presets,
                        )) {
                          const presetGroups = (preset as any).groups || [];
                          if (
                            presetGroups.length === enabledGroupsSet.size &&
                            presetGroups.every((g: string) =>
                              enabledGroupsSet.has(g),
                            )
                          ) {
                            bestPresetName = (preset as any).label || key;
                            break;
                          }
                        }
                      }

                      const disabledCount =
                        catalog?.groups?.filter(
                          (g: CatalogGroup) =>
                            !(g.locked || enabledGroupsSet.has(g.id)),
                        ).length || 0;

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
                          {catalog?.groups.map((g: any) => {
                            const isEnabled =
                              g.locked || enabledGroupsSet.has(g.id);
                            if (!isEnabled) return null;
                            return (
                              <div
                                key={g.id || "core"}
                                className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-primary/10 text-primary border border-primary/20"
                              >
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
                </ReviewCard>
              )}

              {/* Insights Section - only if logging is enabled */}
              {config.logging_enabled !== false && (
                <ReviewCard className="col-span-2 space-y-3">
                  <div className="flex justify-between items-center">
                    <ReviewHeader icon={Sparkles}>
                      Automated Insights
                    </ReviewHeader>
                    <span className="text-[10px] text-muted-foreground">
                      Derived from logs
                    </span>
                  </div>
                  <div className="grid grid-cols-2 gap-3 pt-1">
                    {(catalog as any)?.insights?.map((insight: any) => {
                      const enabledGroups = new Set<any>([
                        null,
                        ...(config.log_fields?.groups || []),
                      ]);
                      // Also include dependencies
                      const catalogGroups = (catalog as any)?.groups || [];
                      let changed = true;
                      while (changed) {
                        changed = false;
                        catalogGroups.forEach((g: any) => {
                          if (
                            enabledGroups.has(g.id) &&
                            g.requires &&
                            !enabledGroups.has(g.requires)
                          ) {
                            enabledGroups.add(g.requires);
                            changed = true;
                          }
                        });
                      }

                      const isEnabled = insight.required_groups?.every(
                        (rg: any) => enabledGroups.has(rg),
                      );
                      return (
                        <div
                          key={insight.id}
                          className={cn(
                            "flex items-start gap-3 border rounded-lg p-2.5 bg-background shadow-sm transition-all",
                            !isEnabled && "opacity-50 grayscale",
                          )}
                        >
                          <div className="mt-0.5 shrink-0">
                            {isEnabled ? (
                              <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                            ) : (
                              <XCircle className="h-4 w-4 text-muted-foreground" />
                            )}
                          </div>
                          <div className="flex flex-col min-w-0">
                            <span
                              className={cn(
                                "text-xs font-semibold truncate",
                                !isEnabled &&
                                  "line-through text-muted-foreground",
                              )}
                            >
                              {insight.title}
                            </span>
                            <span
                              className="text-[10px] text-muted-foreground line-clamp-2 leading-tight mt-0.5"
                              title={insight.description}
                            >
                              {insight.description}
                            </span>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </ReviewCard>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
