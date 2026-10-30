"use client"

import React from 'react'
import { Button } from '@/components/ui/button'
import { ReviewCard, ReviewHeader, ReviewContent, ReviewItem } from '@/components/ui/confirm-dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { SSEProgressView } from '@/components/SSEModal'
import {
  CheckCircle2,
  Cloud,
  Copy,
  Database,
  FileJson,
  FileText,
  Globe,
  Loader2,
  Settings,
  Sparkles,
  XCircle,
  Zap,
} from 'lucide-react'
import { cn, copyToClipboard, formatBytes, REGION_LABELS, SHIELD_LABELS } from '@/lib/utils'
import { evaluateLogFieldPresetSummary, resolveEnabledGroupsWithDeps } from '@/lib/api'
import type { WizardState } from '../types'
import { FeatureSummaryBadge, StatusCheckIcon } from '../WizardChrome'

export function ExecuteStep({ s }: { s: WizardState }) {
  const { config, catalog, selectedService } = s
  const enabledGroups = React.useMemo(
    () => resolveEnabledGroupsWithDeps(config.log_fields?.groups, catalog),
    [config.log_fields?.groups, catalog],
  )
  const presetSummary = React.useMemo(
    () => evaluateLogFieldPresetSummary(config.log_fields, catalog),
    [config.log_fields, catalog],
  )
  const automationChecks = [
    {
      label: `Sync every ${config.log_period >= 120 ? Math.floor(config.log_period / 120) + "m" : Math.floor(config.log_period / 2) + "s"}`,
      enabled: config.enable_cron_sync,
    },
    {
      label: `Commit to DuckLake every ${config.commit_interval_mins}m`,
      enabled: config.enable_cron_sync,
    },
    {
      label: "Auto-delete Raw Logs",
      enabled: config.delete_after && config.enable_cron_sync,
    },
    {
      label: "Daily DuckLake Optimization",
      enabled: config.enable_cron_compact && config.enable_cron_sync,
    },
  ]

  return (
    <div className="flex-1 overflow-y-auto min-h-0 flex flex-col p-8 items-center text-left">
      <div className="w-full max-w-2xl space-y-8">
        {s.isDeploying ? (
          <div className="space-y-6 w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="text-center space-y-2">
              <h3 className="text-2xl font-semibold tracking-tight">Provisioning: {selectedService?.name}</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Setting up Fastly Object Storage, logging endpoints, and CDN proxy...
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
              <h3 className="text-2xl font-semibold tracking-tight">Review & Deploy</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                You are about to provision the following resources.
              </p>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <ReviewCard className="col-span-2 space-y-3">
                <ReviewHeader icon={Sparkles}>Enabled Features</ReviewHeader>
                <div className="flex flex-col sm:flex-row gap-3 pt-1">
                  {config.logging_enabled !== false && (
                    <FeatureSummaryBadge
                      icon={Database}
                      title="Request Logging"
                      subtitle="In-depth HTTP logs, edge metrics, and WAF protection."
                    />
                  )}
                  {config.rum_enabled && (
                    <FeatureSummaryBadge
                      icon={Globe}
                      title="Real User Monitoring (RUM)"
                      subtitle="Client-side web vitals, speed insights, and session analytics."
                    />
                  )}
                </div>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Cloud}>Target Service</ReviewHeader>
                <ReviewContent>
                  <ReviewItem label="Service Name" value={selectedService?.name} />
                  {config.logging_enabled !== false && (
                    <>
                      <ReviewItem label="Log Endpoint" value={config.endpoint_name} />
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
                </ReviewContent>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Globe}>CDN Edge Proxy</ReviewHeader>
                <ReviewContent>
                  <ReviewItem label="Domain" value={`${config.cdn_prefix}.global.ssl.fastly.net`} />
                  <ReviewItem label="Shield POP" value={SHIELD_LABELS[config.cdn_shield] || "None"} />
                </ReviewContent>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Database}>Object Storage</ReviewHeader>
                <ReviewContent>
                  <ReviewItem label="Bucket" value={config.fos_bucket_name} />
                  <ReviewItem label="Region" value={REGION_LABELS[config.fos_region]} />
                  <ReviewItem label="Edge Only" value={config.edge_only ? "Yes" : "No"} />
                </ReviewContent>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Settings}>Automation</ReviewHeader>
                <ReviewContent className="gap-2.5">
                  {automationChecks.map(({ label, enabled }) => (
                    <ReviewItem
                      key={label}
                      variant="between"
                      label={label}
                      className={cn(!enabled && "text-muted-foreground")}
                      value={<StatusCheckIcon enabled={enabled} />}
                    />
                  ))}
                  {config.logging_enabled !== false && (
                    <ReviewItem
                      variant="between"
                      label="Log Retention"
                      className={cn(!config.enable_cron_sync && "text-muted-foreground")}
                      value={config.log_retention_days === 0 ? "Forever" : `${config.log_retention_days} days`}
                    />
                  )}
                  {config.rum_enabled && (
                    <ReviewItem
                      variant="between"
                      label="RUM Retention"
                      className={cn(!config.enable_cron_sync && "text-muted-foreground")}
                      value={config.rum_retention_days === 0 ? "Forever" : `${config.rum_retention_days} days`}
                    />
                  )}
                </ReviewContent>
              </ReviewCard>
              {config.logging_enabled !== false && (
                <ReviewCard className="col-span-2 space-y-3">
                  <div className="flex justify-between items-center">
                    <ReviewHeader icon={FileJson}>Log Configuration</ReviewHeader>
                    <span className="font-mono text-[10px] bg-muted px-2 py-0.5 rounded text-muted-foreground border">
                      ~{formatBytes(s.estimatedBytes)} / line
                    </span>
                  </div>
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                      {presetSummary.bestPresetName
                        ? `${presetSummary.bestPresetName} Preset`
                        : "Custom Configuration"}
                    </div>
                    {!presetSummary.bestPresetName &&
                      catalog?.groups?.map((g) => {
                        const isEnabled =
                          g.locked || (g.id != null && presetSummary.enabledGroupsSet.has(g.id))
                        return isEnabled ? (
                          <div
                            key={g.id || "core"}
                            className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-primary/10 text-primary border border-primary/20"
                          >
                            {g.label}
                          </div>
                        ) : null
                      })}
                    {presetSummary.disabledCount > 0 && (
                      <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                        +{presetSummary.disabledCount} disabled
                      </div>
                    )}
                  </div>
                </ReviewCard>
              )}
              {config.logging_enabled !== false && (
                <ReviewCard className="col-span-2 space-y-3">
                  <div className="flex justify-between items-center">
                    <ReviewHeader icon={Sparkles}>Automated Insights</ReviewHeader>
                    <span className="text-[10px] text-muted-foreground">Derived from logs</span>
                  </div>
                  <div className="grid grid-cols-2 gap-3 pt-1">
                    {catalog?.insights?.map((insight) => {
                      const isEnabled = insight.required_groups.every((rg) => enabledGroups.has(rg))
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
                                !isEnabled && "line-through text-muted-foreground",
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
                      )
                    })}
                  </div>
                </ReviewCard>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

const TERRAFORM_TABS = [
  {
    value: "instructions",
    label: "Instructions",
    icon: FileText,
    defaultFile: "instructions",
    match: (f: string) => f === "instructions",
  },
  {
    value: "logging",
    label: "Logging Service",
    icon: Zap,
    defaultFile: "logging_service.tf",
    match: (f: string) =>
      f === "logging_service.tf" || f === "log_format.vcl" || f.startsWith("capture_snippets/"),
  },
  {
    value: "cdn",
    label: "CDN & Storage",
    icon: Globe,
    defaultFile: "fos.tf",
    match: (f: string) =>
      f === "fos.tf" || f === "cdn_proxy.tf" || f === "cdn_proxy.vcl" || f.startsWith("cdn_snippets/"),
  },
  {
    value: "all",
    label: "All Files",
    icon: FileJson,
    defaultFile: undefined,
    match: () => true,
  },
] as const

export function TerraformStep({ s }: { s: WizardState }) {
  const { terraformFiles, selectedTfFile, setSelectedTfFile } = s
  return (
    <div className="flex-1 overflow-hidden p-8 flex flex-col">
      <div className="w-full max-w-6xl mx-auto flex flex-col h-full space-y-6">
        <div className="flex items-center justify-between pb-4 border-b shrink-0">
          <div className="space-y-1">
            <h3 className="text-lg font-bold tracking-tight flex items-center gap-2">
              <FileJson className="h-5 w-5 text-primary" /> Terraform & VCL Preview
            </h3>
            <p className="text-sm text-muted-foreground">
              Review and export the generated configuration files.
            </p>
          </div>
          <Button onClick={s.handleExportTerraform} className="h-9 font-bold">
            Export as ZIP
          </Button>
        </div>
        {s.isFetchingTerraform ? (
          <div className="flex-1 flex items-center justify-center bg-muted/10 rounded-lg border border-dashed">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <Tabs
            defaultValue="instructions"
            className="flex-1 flex flex-col min-h-0"
            onValueChange={(tab) => {
              const found = TERRAFORM_TABS.find((t) => t.value === tab)
              if (found?.defaultFile) setSelectedTfFile(found.defaultFile)
            }}
          >
            <TabsList className="grid w-full grid-cols-4 shrink-0">
              {TERRAFORM_TABS.map(({ value, label, icon: Icon }) => (
                <TabsTrigger key={value} value={value} className="flex items-center gap-2">
                  <Icon className="w-3.5 h-3.5" /> {label}
                </TabsTrigger>
              ))}
            </TabsList>
            {TERRAFORM_TABS.map(({ value, match }) => (
              <TabsContent key={value} value={value} className="flex-1 flex gap-4 min-h-0 pt-4 mt-0">
                <div className="w-64 shrink-0 flex flex-col gap-1 overflow-y-auto pr-2 custom-scrollbar border-r">
                  {Object.keys(terraformFiles)
                    .filter(match)
                    .sort((a, b) =>
                      a.endsWith(".tf") !== b.endsWith(".tf")
                        ? a.endsWith(".tf")
                          ? -1
                          : 1
                        : a.localeCompare(b),
                    )
                    .map((fileName) => (
                      <button
                        key={fileName}
                        onClick={() => setSelectedTfFile(fileName)}
                        className={cn(
                          "text-left px-3 py-2 rounded-md text-[11px] font-mono transition-colors truncate",
                          selectedTfFile === fileName
                            ? "bg-primary text-primary-foreground font-bold shadow-sm"
                            : "hover:bg-muted text-muted-foreground",
                        )}
                      >
                        {fileName}
                      </button>
                    ))}
                </div>
                <div className="flex-1 bg-muted rounded-lg border overflow-hidden flex flex-col">
                  <div className="px-4 py-2 border-b bg-muted/50 flex items-center justify-between shrink-0">
                    <span className="text-[10px] font-mono text-muted-foreground">{selectedTfFile}</span>
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label={`Copy ${selectedTfFile} to clipboard`}
                      className="h-6 w-6 hover:bg-muted-foreground/10"
                      onClick={() => void copyToClipboard(terraformFiles[selectedTfFile])}
                    >
                      <Copy className="h-3 w-3" />
                    </Button>
                  </div>
                  <div className="flex-1 overflow-auto p-4 custom-scrollbar">
                    <pre className="text-xs font-mono text-muted-foreground whitespace-pre leading-relaxed">
                      {terraformFiles[selectedTfFile] ||
                        "Select a file on the left to preview its content."}
                    </pre>
                  </div>
                </div>
              </TabsContent>
            ))}
          </Tabs>
        )}
      </div>
    </div>
  )
}

export const ReviewStep = ExecuteStep
