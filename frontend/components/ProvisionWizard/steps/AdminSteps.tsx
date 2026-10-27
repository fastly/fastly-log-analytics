"use client"

import React from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SectionHeader, ReviewCard, ReviewHeader, ReviewContent, ReviewItem } from '@/components/ui/confirm-dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { CollapsibleGroup, CmcdConfigSection } from '@/components/LogSettingsModal/LogSettingsModal'
import { SSEProgressView } from '@/components/SSEModal'
import {
  Activity,
  AlertCircle,
  Calendar,
  CheckCircle2,
  ChevronRight,
  Cloud,
  Copy,
  Database,
  Eye,
  FileJson,
  FileText,
  Globe,
  Info,
  Layers,
  Loader2,
  Lock,
  Search,
  Settings,
  Shield,
  Sparkles,
  XCircle,
  Zap,
} from 'lucide-react'
import { cn, copyToClipboard, formatBytes, formatDateTime, REGION_LABELS, SHIELD_LABELS } from '@/lib/utils'
import { evaluateLogFieldPresetSummary, resolveEnabledGroupsWithDeps } from '@/lib/api'
import { ANALYST_PATH_A_UNSUPPORTED_REASON, type WizardState } from '../types'
import { FeatureSummaryBadge, StatusCheckIcon } from '../WizardChrome'

function errorCode(e: unknown): string | undefined {
  return e && typeof e === "object" && "code" in e
    ? String((e as Record<string, unknown>).code)
    : undefined
}

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
      label: `Commit to Iceberg every ${config.commit_interval_mins}m`,
      enabled: config.enable_cron_sync,
    },
    {
      label: "Auto-delete Raw Logs",
      enabled: config.delete_after && config.enable_cron_sync,
    },
    {
      label: "Daily Iceberg Optimization",
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

export function TokenStep({ s }: { s: WizardState }) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-6 text-center">
      <div className="space-y-2 max-w-md">
        <h3 className="text-xl font-semibold tracking-tight">Enter Fastly API Token</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          We need a token with <code>superuser</code> permissions to create and configure your services. An{" "}
          <code>engineer</code> token cannot create new services.
        </p>
        <div className="pt-2">
          <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-amber-500/10 text-amber-600 dark:text-amber-500 border border-amber-500/20 text-[10px] font-bold uppercase tracking-wider">
            <AlertCircle className="h-3 w-3 shrink-0" />
            <a
              href="https://www.fastly.com/documentation/reference/api/auth-tokens/user/"
              target="_blank"
              rel="noreferrer"
              className="hover:underline hover:text-amber-700 dark:hover:text-amber-400 transition-colors"
            >
              Personal API Tokens required for NGWAF
            </a>
          </div>
        </div>
      </div>
      <div className="space-y-4 w-full max-w-sm text-left">
        <div className="space-y-2">
          <Label htmlFor="token" className="flex items-center gap-2 text-sm font-medium">
            <Lock className="h-3.5 w-3.5" /> API Token
          </Label>
          <Input
            id="token"
            type="password"
            value={s.token}
            onChange={(e) => s.setToken(e.target.value.trim())}
            placeholder=""
            className="font-mono text-center"
          />
        </div>
        {s.servicesError && (
          <div className="p-3 bg-destructive/10 text-destructive text-xs rounded-md border border-destructive/20 flex gap-2 animate-in fade-in slide-in-from-top-1 text-left">
            <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
            {errorCode(s.servicesError) === "object_storage_not_enabled" ? (
              <span>
                Object Storage isn&apos;t enabled on this Fastly account, and it&apos;s required to store logs.{" "}
                <a
                  href="https://manage.fastly.com/products/object-storage"
                  target="_blank"
                  rel="noreferrer"
                  className="font-semibold underline underline-offset-2 hover:opacity-80"
                >
                  Enable Object Storage
                </a>{" "}
                for your account, then click Fetch Services again.
              </span>
            ) : s.servicesError instanceof Error ? (
              s.servicesError.message
            ) : (
              "Failed to fetch services"
            )}
          </div>
        )}
      </div>
    </div>
  )
}

export function ServiceStep({ s }: { s: WizardState }) {
  return (
    <div className="flex-1 flex flex-col overflow-hidden p-6 md:p-8 max-w-3xl mx-auto w-full gap-4">
      <div className="flex items-center justify-between shrink-0">
        <div className="p-2 border rounded-lg bg-muted/10 flex items-center gap-3 px-4 flex-1">
          <Search className="h-5 w-5 text-muted-foreground" />
          <Input
            placeholder="Search your services..."
            className="h-10 border-none bg-transparent shadow-none focus-visible:ring-0 text-base"
            value={s.search}
            onChange={(e) => s.setSearch(e.target.value)}
          />
        </div>
        {s.tokenInfo && (
          <div className="ml-4 flex flex-col items-end shrink-0">
            <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
              Authenticated as
            </span>
            <div className="flex items-center gap-1.5">
              <span className="text-xs font-semibold">{s.tokenInfo.name}</span>
              <Badge
                variant={s.tokenInfo.type === "user" ? "default" : "outline"}
                className="text-[9px] h-3.5 px-1 uppercase"
              >
                {s.tokenInfo.type}
              </Badge>
            </div>
          </div>
        )}
      </div>
      <div className="flex-1 overflow-y-auto min-h-0 border rounded-lg shadow-sm">
        <div className="divide-y divide-muted/50 bg-background">
          {s.isLoadingServices ? (
            <div className="py-12 flex flex-col items-center justify-center gap-3 text-muted-foreground text-sm">
              <Loader2 className="h-6 w-6 animate-spin text-primary" />
              <span>Loading services...</span>
            </div>
          ) : s.filteredServices.length > 0 ? (
            s.filteredServices.map((svc) => (
              <button
                key={svc.id}
                type="button"
                disabled={svc.provisioned}
                aria-label={
                  svc.provisioned
                    ? `${svc.name} — already provisioned`
                    : `Select service ${svc.name}, id ${svc.id}`
                }
                className={cn(
                  "w-full text-left p-4 flex items-center justify-between transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
                  svc.provisioned
                    ? "opacity-40 grayscale bg-muted/5 cursor-not-allowed"
                    : "hover:bg-muted/50 cursor-pointer active:bg-muted",
                )}
                onClick={() => !svc.provisioned && s.handleServiceSelect(svc)}
              >
                <div className="space-y-1">
                  <div className="font-semibold text-sm flex items-center gap-2">
                    {svc.name}
                    {svc.provisioned && (
                      <Badge
                        variant="secondary"
                        className="text-[10px] h-4 px-1 leading-none font-bold uppercase tracking-tight"
                      >
                        Active
                      </Badge>
                    )}
                  </div>
                  <div className="text-xs font-mono text-muted-foreground">{svc.id}</div>
                </div>
                {!svc.provisioned && (
                  <div className="flex items-center text-primary">
                    {s.validateMutation.isPending && s.selectedService?.id === svc.id ? (
                      <Loader2 className="h-5 w-5 animate-spin" />
                    ) : (
                      <ChevronRight className="h-5 w-5" />
                    )}
                  </div>
                )}
              </button>
            ))
          ) : (
            <div className="py-12 text-center text-muted-foreground text-sm italic">
              No services found matching &quot;{s.search}&quot;
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export function SettingsStep({ s }: { s: WizardState }) {
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
        <div className="space-y-6">
          <SectionHeader title="Ingestion Settings" icon={Settings} />
          <p className="text-sm text-muted-foreground leading-relaxed">
            Configure how you want to handle ongoing updates from the data lake.
          </p>
          <div className="bg-muted/5 border rounded-xl overflow-hidden divide-y">
            <div className="p-6 flex items-center justify-between gap-8">
              <div className="space-y-1 flex-1">
                <div className="flex items-center gap-2">
                  <Label id="settings-auto-sync-label" className="text-sm font-bold tracking-tight">
                    Auto-Sync New Data
                  </Label>
                  <Badge variant="secondary" className="text-[9px] uppercase h-4">
                    Recommended
                  </Badge>
                </div>
                <p className="text-xs text-muted-foreground leading-relaxed">
                  Automatically poll for and download new processed log files as they are committed to the
                  cloud.
                </p>
              </div>
              <Switch
                aria-labelledby="settings-auto-sync-label"
                checked={s.syncEnabled}
                onCheckedChange={s.setSyncEnabled}
              />
            </div>
            {s.syncEnabled && (
              <div className="p-6 space-y-4 bg-background/30 animate-in fade-in slide-in-from-top-1">
                <div className="flex items-start justify-between gap-8">
                  <div className="space-y-1">
                    <Label htmlFor="settings-sync-interval" className="text-sm font-bold tracking-tight">
                      Cloud Sync Interval
                    </Label>
                    <p className="text-xs text-muted-foreground leading-relaxed">
                      How often to check for new cloud commits. More frequent = fresher data.
                    </p>
                  </div>
                  <Select value={s.syncIntervalMins} onValueChange={(v) => v && s.setSyncIntervalMins(v)}>
                    <SelectTrigger id="settings-sync-interval" className="h-9 w-[180px] shrink-0">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {["1", "2", "5", "15", "30", "60"].map((mins) => (
                        <SelectItem key={mins} value={mins}>
                          Every {mins} {mins === "1" ? "min" : "mins"}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}
          </div>
          {!s.syncEnabled && (
            <div className="p-4 rounded-lg bg-amber-500/5 border border-amber-500/20 flex items-start gap-3">
              <Info className="h-4 w-4 text-amber-500 mt-0.5" />
              <p className="text-[11px] text-amber-700 dark:text-amber-400 leading-normal">
                With auto-sync disabled, your local dashboard will only show the data you import now. You will
                need to manually trigger a sync later to see newer logs.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export function NgwafStep({ s }: { s: WizardState }) {
  const { config, setConfig } = s
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-6 max-w-2xl mx-auto">
        <div className="flex items-center gap-2 pb-2 border-b">
          <Shield className="h-5 w-5 text-primary" />
          <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
            NGWAF Workspace
          </h3>
        </div>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Link this service to an existing Fastly NGWAF workspace to enable WAF signal logging and bot
          detection. This step is optional — skip it if NGWAF is not deployed on this service.
        </p>
        {s.ngwafFetching ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading workspaces…
          </div>
        ) : s.ngwafFetchError ? (
          <div className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4 shrink-0" /> {s.ngwafFetchError}
          </div>
        ) : s.ngwafWorkspaces.length > 0 ? (
          <div className="space-y-2">
            <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Workspace
            </Label>
            <Select
              value={config.ngwaf_workspace_id || "__none__"}
              onValueChange={(v: string | null) =>
                setConfig((prev) => {
                  const workspaceId = !v || v === "__none__" ? "" : v
                  const groups: string[] = prev.log_fields?.groups ?? []
                  return {
                    ...prev,
                    ngwaf_workspace_id: workspaceId,
                    ...(workspaceId && !groups.includes("J")
                      ? { log_fields: { ...prev.log_fields, groups: [...groups, "J"] } }
                      : {}),
                  }
                })
              }
            >
              <SelectTrigger className="h-9 text-sm">
                <SelectValue placeholder="Select a workspace…" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__none__">
                  <span className="text-muted-foreground">No NGWAF (skip)</span>
                </SelectItem>
                {s.ngwafWorkspaces.map((ws) => (
                  <SelectItem key={ws.id} value={ws.id}>
                    {ws.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-sm text-muted-foreground bg-muted/30 p-3 rounded-lg border border-dashed">
              <Info className="h-4 w-4 shrink-0" /> No NGWAF workspaces found in this account.
            </div>
            {s.ngwafDebugRaw && (
              <details className="text-[10px]">
                <summary className="cursor-pointer text-muted-foreground uppercase tracking-wider font-bold">
                  Raw API response (debug)
                </summary>
                <pre className="mt-1 p-2 bg-muted rounded text-xs overflow-auto max-h-32 whitespace-pre-wrap break-all">
                  {s.ngwafDebugRaw}
                </pre>
              </details>
            )}
          </div>
        )}
        <div className="p-4 rounded-xl bg-muted/30 border border-dashed space-y-1">
          <p className="text-xs font-semibold text-muted-foreground">
            WAF / NGWAF log fields (group J) will only be available in the next step if a workspace is
            selected here.
          </p>
        </div>
      </div>
    </div>
  )
}

const FEATURE_OPTIONS = [
  {
    id: "logging" as const,
    icon: Activity,
    title: "Request Logging",
    description:
      "Stream edge requests directly to Object Storage. Power charts for traffic, status codes, and security.",
    activeClass: "border-primary bg-primary/5 shadow-md shadow-primary/5",
    hoverClass: "border-muted hover:border-primary/50 hover:bg-muted/30",
    iconBgClass: "bg-primary/20",
    iconColorClass: "text-primary",
    dotClass: "bg-primary",
  },
  {
    id: "rum" as const,
    icon: Eye,
    title: "RUM only",
    description:
      "Deploy client-side Core Web Vitals, page performance, and error analytics. Skips standard request logs.",
    activeClass: "border-amber-500 bg-amber-500/5 shadow-md shadow-amber-500/5",
    hoverClass: "border-muted hover:border-amber-500/50 hover:bg-muted/30",
    iconBgClass: "bg-amber-500/20",
    iconColorClass: "text-amber-500",
    dotClass: "bg-amber-500",
  },
  {
    id: "both" as const,
    icon: Layers,
    title: "Shared Bundle (Both)",
    description:
      "Enable Request Logging and Real User Monitoring side-by-side, sharing a single FOS storage bucket.",
    activeClass: "border-emerald-500 bg-emerald-500/5 shadow-md shadow-emerald-500/5",
    hoverClass: "border-muted hover:border-emerald-500/50 hover:bg-muted/30",
    iconBgClass: "bg-emerald-500/20",
    iconColorClass: "text-emerald-500",
    dotClass: "bg-emerald-500",
  },
] as const

export function FeaturesStep({ s }: { s: WizardState }) {
  const { config, setConfig } = s
  const currentSelection =
    config.logging_enabled && config.rum_enabled
      ? "both"
      : config.rum_enabled && !config.logging_enabled
        ? "rum"
        : "logging"
  return (
    <div className="flex-1 flex flex-col items-center justify-center p-6 md:p-8 space-y-8 text-center animate-in fade-in slide-in-from-bottom-4 duration-500 max-w-4xl mx-auto w-full">
      <div className="space-y-3 max-w-lg">
        <span className="text-[10px] font-bold uppercase tracking-widest text-primary bg-primary/10 px-2.5 py-1 rounded-full">
          Step 4 of {s.STEPS.length}
        </span>
        <h3 className="text-2xl font-bold tracking-tight">Select features to deploy</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Choose the integration capabilities you want to configure on your Fastly service. You can adjust
          these settings later.
        </p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full">
        {FEATURE_OPTIONS.map((opt) => {
          const active = currentSelection === opt.id
          const Icon = opt.icon
          return (
            <button
              key={opt.id}
              type="button"
              onClick={() =>
                setConfig((prev) => ({
                  ...prev,
                  logging_enabled: opt.id === "logging" || opt.id === "both",
                  rum_enabled: opt.id === "rum" || opt.id === "both",
                }))
              }
              className={cn(
                "group relative flex flex-col items-center gap-5 p-6 border-2 rounded-2xl bg-background transition-all text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                active ? opt.activeClass : opt.hoverClass,
              )}
            >
              <div
                className={cn(
                  "w-12 h-12 rounded-xl flex items-center justify-center transition-transform group-hover:scale-110",
                  active ? opt.iconBgClass : "bg-muted",
                )}
              >
                <Icon className={cn("h-6 w-6", active ? opt.iconColorClass : "text-muted-foreground")} />
              </div>
              <div className="space-y-1.5 text-center">
                <h4 className="font-bold text-base">{opt.title}</h4>
                <p className="text-xs text-muted-foreground leading-relaxed">{opt.description}</p>
              </div>
              {active && <div className={cn("absolute top-3 right-3 w-2 h-2 rounded-full", opt.dotClass)} />}
            </button>
          )
        })}
      </div>
    </div>
  )
}

export function FieldsStep({ s }: { s: WizardState }) {
  const { config, setConfig, catalog, isLoadingCatalog } = s
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-6 max-w-4xl mx-auto">
        <div className="flex items-center justify-between pb-2 border-b">
          <div className="flex items-center gap-2">
            <FileJson className="h-5 w-5 text-primary" />
            <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">Log Fields</h3>
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
                Select the data fields to capture at the edge. More fields provide richer insights but
                increase storage and bandwidth costs.
              </p>
              <div className="p-3 bg-blue-500/10 border border-blue-500/20 text-blue-700 dark:text-blue-400 rounded-md text-xs">
                <strong>Note:</strong> Custom log fields (e.g. tracking specific HTTP headers or application
                IDs) can be configured from the Admin dashboard after initial provisioning is complete.
              </div>
              {catalog?.presets && (
                <div className="flex flex-wrap gap-2 pt-2 items-center">
                  {Object.entries(catalog.presets).map(([key, preset]) => {
                    const isMinimal = key === "minimal"
                    const active = isMinimal || s.isPresetActive(preset.groups)
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
                        onClick={() => !isMinimal && s.togglePreset(preset.groups || [])}
                        disabled={isMinimal}
                      >
                        {preset.label || key}
                      </Button>
                    )
                  })}
                  <Button
                    variant="ghost"
                    size="sm"
                    className="h-8 text-xs font-semibold text-muted-foreground hover:text-foreground ml-auto"
                    onClick={() =>
                      setConfig((prev) => ({
                        ...prev,
                        log_fields: {
                          ...prev.log_fields,
                          groups: [],
                          field_overrides: {},
                          field_limits: prev.log_fields.field_limits,
                        },
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
                WAF / NGWAF fields (group J) are hidden — no NGWAF workspace selected.
              </div>
            )}
            <div className="grid grid-cols-1 gap-3 pb-8">
              {catalog &&
                (catalog.groups ?? [])
                  .filter((g) => config.ngwaf_workspace_id || g.id !== "J")
                  .map((g) => (
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
              <CmcdConfigSection
                enabled={config.cmcd_enabled}
                onEnabledChange={(v) => setConfig((prev) => ({ ...prev, cmcd_enabled: v }))}
                mode={config.cmcd_mode}
                onModeChange={(v) => setConfig((prev) => ({ ...prev, cmcd_mode: v }))}
                version={config.cmcd_version}
                onVersionChange={(v) => setConfig((prev) => ({ ...prev, cmcd_version: v }))}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export function ConfirmStep({ s }: { s: WizardState }) {
  const { config, importMode, importRange, lakeInfo } = s
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-8 pb-12 max-w-4xl mx-auto text-left">
        <div className="text-center space-y-2">
          <h3 className="text-2xl font-bold tracking-tight">Confirm Connection</h3>
          <p className="text-sm text-muted-foreground leading-relaxed">
            Review your connection and import settings before continuing.
          </p>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <ReviewCard>
            <ReviewHeader icon={Cloud}>Target Service</ReviewHeader>
            <ReviewContent>
              <ReviewItem label="Service Name" value={config.endpoint_name} />
              <ReviewItem label="Service ID" value={config.cdn_service_name} />
              <ReviewItem label="Mode" value="Read-Only Analyst" />
            </ReviewContent>
          </ReviewCard>
          <ReviewCard>
            <ReviewHeader icon={Database}>Data Lake</ReviewHeader>
            <ReviewContent>
              <ReviewItem label="Bucket" value={config.fos_bucket_name} />
              <ReviewItem label="Region" value={config.fos_region} />
              <ReviewItem label="Existing Data" value={lakeInfo?.table_exists ? "Available" : "Not Found"} />
            </ReviewContent>
          </ReviewCard>
          <ReviewCard>
            <ReviewHeader icon={Calendar}>Initial Import</ReviewHeader>
            <ReviewContent>
              <ReviewItem label="Strategy" value={importMode === "all" ? "Import All" : "Custom Range"} />
              {importMode === "range" ? (
                <>
                  <ReviewItem label="Start Time" value={formatDateTime(importRange.start, s.timezone)} />
                  <ReviewItem label="End Time" value={formatDateTime(importRange.end, s.timezone)} />
                </>
              ) : (
                <ReviewItem
                  label="Range"
                  value={`${formatDateTime(lakeInfo?.range?.start, s.timezone)} → ${formatDateTime(lakeInfo?.range?.end, s.timezone)}`}
                />
              )}
              <ReviewItem
                label="Est. Download Size"
                value={`~${formatBytes(s.estimatedImportSize)}`}
                className="text-primary font-medium"
              />
            </ReviewContent>
          </ReviewCard>
          <ReviewCard>
            <ReviewHeader icon={Settings}>Automation</ReviewHeader>
            <ReviewContent>
              <ReviewItem
                variant="between"
                label="Background Sync"
                value={<StatusCheckIcon enabled={s.syncEnabled} />}
              />
              {s.syncEnabled && (
                <ReviewItem label="Sync Interval" value={`Every ${s.syncIntervalMins} minutes`} />
              )}
            </ReviewContent>
          </ReviewCard>
        </div>
        <div className="p-4 rounded-xl bg-primary/5 border border-primary/20 space-y-3">
          <div className="flex items-center gap-2 text-primary">
            <Sparkles className="h-4 w-4" />
            <span className="text-xs font-bold uppercase tracking-wider">What to expect</span>
          </div>
          <p className="text-xs text-muted-foreground leading-relaxed">
            After connecting, the system will begin downloading the requested Parquet data files to your local
            cache. This process happens in the background and may take a few minutes depending on the volume
            of data. Your dashboard will begin populating as files arrive.
          </p>
        </div>
      </div>
    </div>
  )
}

export function ModeStep({ s }: { s: WizardState }) {
  const joinDisabled = !s.analystPathASupported
  const analystPathAReason = s.analystPathAReason ?? ANALYST_PATH_A_UNSUPPORTED_REASON
  const modeCards = [
    {
      mode: "provision" as const,
      nextStep: "token" as const,
      icon: Zap,
      iconBg: "bg-primary/10",
      iconColor: "text-primary",
      hoverBorder: "hover:border-primary",
      title: "Admin: Provision",
      description:
        "Deploy new Fastly Object Storage resources, logging endpoints, and a CDN proxy. Best for new projects.",
      disabled: false,
    },
    {
      mode: "ingest" as const,
      nextStep: "token" as const,
      icon: Settings,
      iconBg: "bg-amber-500/10",
      iconColor: "text-amber-500",
      hoverBorder: "hover:border-amber-500",
      title: "Admin: Connect Terraform",
      description:
        "Set up ingestion for a service that is already managed via Terraform. We'll skip creating Fastly resources but set up data sync.",
      disabled: false,
    },
    {
      mode: "join" as const,
      nextStep: "join" as const,
      icon: Search,
      iconBg: "bg-blue-500/10",
      iconColor: "text-blue-500",
      hoverBorder: "hover:border-blue-500",
      title: "Analyst: Join",
      description:
        "Connect to an existing project. We'll only sync the processed data from the cloud. No new resources.",
      disabled: joinDisabled,
      reason: joinDisabled ? analystPathAReason : undefined,
    },
  ]

  return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-10 text-center animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="space-y-3 max-w-lg">
        <h3 className="text-2xl font-bold tracking-tight">Select your role</h3>
        <p className="text-muted-foreground leading-relaxed">Choose how you want to set up this service.</p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full max-w-4xl">
        {modeCards.map((card) => {
          const Icon = card.icon
          return (
            <button
              key={card.mode}
              type="button"
              disabled={card.disabled}
              aria-describedby={card.reason ? "analyst-path-a-reason" : undefined}
              onClick={() => {
                s.setMode(card.mode)
                s.setStep(card.nextStep)
              }}
              className={cn(
                "group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 transition-all text-left",
                card.hoverBorder,
                card.disabled && "cursor-not-allowed opacity-60 hover:bg-background hover:border-border",
              )}
            >
              <div
                className={cn(
                  "w-16 h-16 rounded-2xl flex items-center justify-center group-hover:scale-110 transition-transform",
                  card.iconBg,
                )}
              >
                <Icon className={cn("h-8 w-8", card.iconColor)} />
              </div>
              <div className="space-y-2 text-center">
                <h4 className="font-bold text-lg">{card.title}</h4>
                <p className="text-xs text-muted-foreground leading-relaxed">{card.description}</p>
                {card.reason && (
                  <p
                    id="analyst-path-a-reason"
                    className="text-xs font-medium text-amber-600 dark:text-amber-400 leading-relaxed"
                  >
                    {card.reason}
                  </p>
                )}
              </div>
            </button>
          )
        })}
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
