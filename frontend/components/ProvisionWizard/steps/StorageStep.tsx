"use client"

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { useForm, type PathValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SectionHeader, LabelWithInfo, RetentionSelect } from '@/components/ui/confirm-dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Skeleton } from '@/components/ui/skeleton'
import {
  AlertCircle,
  ArrowRight,
  Calendar,
  CheckCircle2,
  Database,
  Globe,
  RefreshCw,
  Search,
  Settings,
  TriangleAlert,
  Zap,
} from 'lucide-react'
import {
  cn,
  formatBytes,
  formatDateTime,
  formatForInput,
  parseFromInput,
  PERIOD_LABELS,
  REGION_LABELS,
  SHIELD_LABELS,
} from '@/lib/utils'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import type { components } from '@/types/api.generated'
import type { WizardState } from '../types'

type RumVersionsResponse = components["schemas"]["RumVersionsResponse"]

const storageStepSchema = z.object({
  endpoint_name: z.string().min(1),
  fos_region: z.string().min(1),
  fos_bucket_name: z.string().min(1),
  log_period: z.number().int().min(1),
  edge_only: z.boolean(),
  sample_rate: z.number().min(1).max(100),
  custom_condition: z.string(),
  rum_log_period: z.number().int().min(1),
  cdn_prefix: z.string(),
  cdn_shield: z.string().min(1),
  enable_cron_sync: z.boolean(),
  delete_after: z.boolean(),
  commit_interval_mins: z.number().int().min(1),
  log_retention_days: z.number().int().min(0),
  rum_retention_days: z.number().int().min(0),
  faro_version: z.string().nullable(),
  rum_custom_condition: z.string(),
  cache_retention_days: z.number().int().min(0),
  rollup_retention_months: z.number().int().min(0),
  enable_cron_compact: z.boolean(),
})
type StorageStepFormValues = z.infer<typeof storageStepSchema>

const analyzeStepSchema = z.object({
  importMode: z.enum(["all", "range"]),
  start: z.string(),
  end: z.string(),
})
type AnalyzeStepFormValues = z.infer<typeof analyzeStepSchema>

export interface RumVersionPickerProps {
  serviceId: string
  value: string | null
  onChange: (version: string | null) => void
  availableVersions?: string[]
  currentVersion?: string | null
  latestVersion?: string | null
}

const INFO_TEXT =
  "The self-hosted Grafana Faro Web SDK build served from your Object Storage bucket. Pin a version to control exactly which RUM client ships to real users; leave it unpinned and the service self-hosts the default vetted version instead."
const RUM_PERIOD_OPTIONS = [
  ["10", "10 seconds (Ultra-Fast / High-Load)"],
  ["60", "1 minute (Fastest)"],
  ["300", "5 minutes (Recommended)"],
  ["600", "10 minutes"],
  ["900", "15 minutes"],
] as const
const COMMIT_INTERVAL_OPTIONS = [
  ["1", "Every 1 min — most real-time"],
  ["2", "Every 2 min"],
  ["3", "Every 3 min"],
  ["5", "Every 5 min — recommended"],
  ["15", "Every 15 min"],
  ["30", "Every 30 min"],
  ["60", "Every 60 min — fewest snapshots"],
] as const
const ROLLUP_MONTH_OPTIONS = [
  ["1", "1 month"],
  ["3", "3 months"],
  ["6", "6 months"],
  ["12", "12 months"],
  ["24", "24 months"],
  ["0", "Forever"],
] as const

export function RumVersionPicker({
  serviceId,
  value,
  onChange,
  availableVersions,
  currentVersion,
  latestVersion,
}: RumVersionPickerProps) {
  const hasPrefetched = availableVersions !== undefined
  const { data, isLoading, isError, refetch, isFetching } = useQuery<RumVersionsResponse>({
    queryKey: queryKeys.rumVersions(serviceId),
    queryFn: async ({ signal }) =>
      unwrapClient(
        client.GET("/api/services/{service_id}/rum/versions", {
          params: { path: { service_id: serviceId } },
          signal,
        }),
      ),
    enabled: !!serviceId && !hasPrefetched,
    retry: false,
    staleTime: Infinity,
  })
  const available = availableVersions ?? data?.available ?? []
  const latest = latestVersion !== undefined ? latestVersion : (data?.latest ?? null)
  const current = currentVersion !== undefined ? currentVersion : (data?.current ?? null)
  const degraded = !hasPrefetched && (isError || (!isLoading && available.length === 0))
  const seedLatest = React.useEffectEvent(() => {
    if (latest && value === null) onChange(latest)
  })
  React.useEffect(() => {
    seedLatest()
  }, [latest])

  const header = (
    <div className="flex items-center justify-between">
      <LabelWithInfo label="Faro Web SDK Version" info={INFO_TEXT} />
      {!hasPrefetched && isLoading ? (
        <span className="text-xs text-muted-foreground animate-pulse font-semibold">Release Notes &rarr;</span>
      ) : (
        <a
          href="https://github.com/grafana/faro-web-sdk/releases"
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-primary hover:underline flex items-center gap-1 font-semibold"
        >
          Release Notes &rarr;
        </a>
      )}
    </div>
  )
  if (!hasPrefetched && isLoading) {
    return (
      <div className="space-y-1.5">
        {header}
        <Skeleton className="h-8 w-full" />
      </div>
    )
  }
  if (degraded) {
    return (
      <div className="space-y-1.5">
        {header}
        <Alert variant="destructive">
          <TriangleAlert />
          <AlertTitle>{isError ? "Couldn't reach the npm registry" : "No versions available"}</AlertTitle>
          <AlertDescription className="flex items-center justify-between gap-2">
            <span>
              RUM will provision using the default self-hosted version — you can pin a specific one later from
              the service&apos;s RUM settings.
            </span>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-7 text-xs shrink-0"
              onClick={() => refetch()}
              disabled={isFetching}
            >
              <RefreshCw className={cn("h-3 w-3 mr-1", isFetching && "animate-spin")} />
              {isFetching ? "Retrying…" : "Retry"}
            </Button>
          </AlertDescription>
        </Alert>
      </div>
    )
  }
  return (
    <div className="space-y-1.5">
      {header}
      <Select value={value ?? undefined} onValueChange={(v) => onChange(v)}>
        <SelectTrigger className="h-8 text-xs w-full" aria-label="Faro Web SDK Version">
          <SelectValue placeholder="Select a version" />
        </SelectTrigger>
        <SelectContent>
          {available.map((v) => {
            const tags = [v === latest && "latest", v === current && "current"].filter(Boolean)
            return (
              <SelectItem key={v} value={v} className="text-xs">
                {v}
                {tags.length > 0 ? ` (${tags.join(", ")})` : ""}
              </SelectItem>
            )
          })}
        </SelectContent>
      </Select>
    </div>
  )
}

export function StorageStep({ s }: { s: WizardState }) {
  const { config, setConfig, domainStatus, domainMessage, checkDomain } = s
  const { setValue } = useForm<StorageStepFormValues>({
    resolver: zodResolver(storageStepSchema),
    values: config,
  })
  const update = <K extends keyof StorageStepFormValues>(key: K, val: StorageStepFormValues[K]) => {
    setValue(key, val as PathValue<StorageStepFormValues, K>, { shouldDirty: true, shouldValidate: true })
    setConfig({ ...config, [key]: val })
  }
  const isLogging = config.logging_enabled !== false
  const syncDisabledClass = !config.enable_cron_sync ? "opacity-30 pointer-events-none" : undefined

  const fosRegionField = (
    <div className="space-y-1.5">
      <LabelWithInfo
        label="FOS Region"
        info="The geographical region where your Fastly Object Storage bucket will be created. We recommend matching this with your primary user base."
      />
      <Select value={config.fos_region} onValueChange={(v) => v && update("fos_region", v)}>
        <SelectTrigger className="h-9">
          <SelectValue>{(val) => REGION_LABELS[String(val)] || val}</SelectValue>
        </SelectTrigger>
        <SelectContent>
          {Object.entries(REGION_LABELS).map(([k, label]) => (
            <SelectItem key={k} value={k}>
              {label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  )
  const bucketNameField = (
    <div className="space-y-1.5">
      <LabelWithInfo
        label="Bucket Name"
        info="The name of the Fastly Object Storage bucket. Must be unique across all Fastly customers."
      />
      <Input
        value={config.fos_bucket_name}
        onChange={(e) => update("fos_bucket_name", e.target.value.toLowerCase())}
        className="h-9 font-mono text-sm"
      />
    </div>
  )
  const renderRumPeriodField = (infoText: string, compact = false) => (
    <div className="space-y-1.5">
      <LabelWithInfo label="RUM Log Period" info={infoText} />
      <Select value={String(config.rum_log_period || 300)} onValueChange={(v) => update("rum_log_period", Number(v) || 300)}>
        <SelectTrigger className={compact ? "h-8 text-xs" : "h-9"}>
          <SelectValue>{(val) => PERIOD_LABELS[String(val)] || val}</SelectValue>
        </SelectTrigger>
        <SelectContent>
          {RUM_PERIOD_OPTIONS.map(([val, label]) => (
            <SelectItem key={val} value={val} className={compact ? "text-xs" : undefined}>
              {label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  )
  const pollFreqShort = isLogging
    ? config.log_period >= 120
      ? `${Math.floor(config.log_period / 120)}m`
      : config.log_period >= 60
        ? `${Math.floor(config.log_period / 2)}s`
        : `${Math.max(10, config.log_period)}s`
    : "1m"
  const pollFreqLong = isLogging
    ? config.log_period >= 120
      ? `${Math.floor(config.log_period / 120)} min`
      : config.log_period >= 60
        ? `${Math.floor(config.log_period / 2)}s`
        : `${Math.max(10, config.log_period)}s`
    : "1 min"

  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
        <div className="space-y-5">
          {isLogging ? (
            <>
              <SectionHeader title="Logging Setup" icon={Zap} />
              <div className="grid grid-cols-2 gap-6">
                <div className="space-y-1.5">
                  <LabelWithInfo
                    label="Endpoint Name"
                    info="The name of the logging endpoint that will be created on your Fastly service. This is just for your reference."
                  />
                  <Input
                    value={config.endpoint_name}
                    onChange={(e) => update("endpoint_name", e.target.value)}
                    className="h-9"
                  />
                </div>
                {fosRegionField}
              </div>
              <div className="grid grid-cols-2 gap-6">
                {bucketNameField}
                <div className="space-y-1.5">
                  <LabelWithInfo
                    label="Log Period"
                    info="How often Fastly will write log files to the bucket. A shorter period means more real-time data but creates more files."
                  />
                  <Select value={String(config.log_period)} onValueChange={(v) => update("log_period", Number(v) || 60)}>
                    <SelectTrigger className="h-9">
                      <SelectValue>{(val) => PERIOD_LABELS[String(val)] || val}</SelectValue>
                    </SelectTrigger>
                    <SelectContent>
                      {["1", "5", "10", "20", "30", "60", "120", "300"].map((val) => (
                        <SelectItem key={val} value={val}>
                          {PERIOD_LABELS[val]}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-6 items-center">
                <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
                  <div className="space-y-0.5">
                    <LabelWithInfo
                      label="Edge Only"
                      info="When enabled, only edge nodes write logs, skipping shield nodes and cache restarts. This prevents duplicate log entries."
                    />
                    <p className="text-[10px] text-muted-foreground">Skip shield/restart logs</p>
                  </div>
                  <Switch
                    aria-label="Edge Only"
                    checked={config.edge_only}
                    onCheckedChange={(v) => update("edge_only", v)}
                  />
                </div>
                <div className="space-y-1.5">
                  <LabelWithInfo
                    label="Sample Rate (%)"
                    info="The percentage of requests to log. Set to 100% to log everything, or lower it for high-traffic services to save storage."
                  />
                  <Input
                    type="number"
                    min={1}
                    max={100}
                    value={config.sample_rate}
                    onChange={(e) => update("sample_rate", Number(e.target.value))}
                    className="h-9"
                  />
                </div>
              </div>
              <div className="space-y-1.5">
                <LabelWithInfo
                  htmlFor="customCondition"
                  label="Optional Log Condition"
                  info="An additional VCL condition to filter logs (e.g., req.url !~ '\.(jpg|png)$'). The expression will be wrapped in parentheses and added to the logging condition logic."
                />
                <Input
                  id="customCondition"
                  placeholder="e.g. std.tolower(req.url) !~ '\.(jpg|png|css|js)$'"
                  value={config.custom_condition}
                  onChange={(e) => update("custom_condition", e.target.value)}
                  className="h-9 font-mono text-xs"
                />
              </div>
            </>
          ) : (
            <>
              <SectionHeader title="RUM Telemetry Storage" icon={Database} />
              <div className="grid grid-cols-2 gap-6">
                {fosRegionField}
                {bucketNameField}
              </div>
              <div className="grid grid-cols-2 gap-6 pt-1">
                {renderRumPeriodField(
                  "How often Fastly will flush RUM beacon logs to your bucket. 5 minutes is recommended for balanced latency and storage efficiency.",
                )}
              </div>
            </>
          )}
        </div>

        <div className="space-y-5">
          <SectionHeader title="CDN Performance Front" icon={Globe} />
          <p className="text-xs text-muted-foreground leading-relaxed">
            Highly recommended. Provision a secondary Fastly service to front the Object Storage bucket for
            faster dashboard queries and secure access.
          </p>
          <div className="grid grid-cols-2 gap-6 pt-1">
            <div className="space-y-1.5">
              <LabelWithInfo
                label="Domain Prefix"
                info="The domain name for the secondary CDN service that sits in front of your Object Storage bucket."
              />
              <div className="space-y-1.5">
                <div className="flex items-center gap-1.5">
                  <Input
                    value={config.cdn_prefix}
                    onChange={(e) => update("cdn_prefix", e.target.value.toLowerCase())}
                    className={cn(
                      "h-9 font-mono text-sm",
                      domainStatus === "available" && "border-green-500 focus-visible:ring-green-500",
                      domainStatus === "taken" && "border-red-500 focus-visible:ring-red-500",
                    )}
                  />
                  <span className="text-[10px] font-mono text-muted-foreground opacity-70">
                    .global.ssl.fastly.net
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-9 px-3 shrink-0 text-xs"
                    onClick={() => checkDomain(config.cdn_prefix)}
                    disabled={domainStatus === "checking" || !config.cdn_prefix}
                    title="Check Domain Availability"
                  >
                    <Search className="h-4 w-4 mr-1.5" />
                    Check Domain
                  </Button>
                </div>
                {domainStatus === "checking" && (
                  <p className="text-[10px] animate-pulse text-muted-foreground">Checking availability...</p>
                )}
                {domainStatus === "available" && (
                  <p className="text-[10px] text-green-600 font-medium flex items-center gap-1">
                    <CheckCircle2 className="h-3 w-3" /> {domainMessage}
                  </p>
                )}
                {domainStatus === "taken" && (
                  <p className="text-[10px] text-red-600 font-medium flex items-center gap-1">
                    <AlertCircle className="h-3 w-3" /> {domainMessage}
                  </p>
                )}
              </div>
            </div>
            <div className="space-y-1.5">
              <LabelWithInfo
                label="Origin Shield"
                info="The Fastly POP that will act as a shield between the edge nodes and your bucket, reducing direct bucket reads and improving performance."
              />
              <Select value={config.cdn_shield} onValueChange={(v) => v && update("cdn_shield", v)}>
                <SelectTrigger className="h-9">
                  <SelectValue>{(val) => SHIELD_LABELS[String(val)] || val}</SelectValue>
                </SelectTrigger>
                <SelectContent>
                  {Object.entries(SHIELD_LABELS).map(([k, label]) => (
                    <SelectItem key={k} value={k}>
                      {label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>

        <div className="space-y-5">
          <SectionHeader title="Automation" icon={Settings} />
          <div className="grid grid-cols-2 gap-4">
            <div className="flex items-center justify-between p-3 border rounded-md bg-muted/5">
              <div className="space-y-0.5">
                <LabelWithInfo
                  label="Background Sync"
                  info={`Automatically polls FOS for new ${isLogging ? "log" : "RUM telemetry"} files (every ${pollFreqLong}) and writes them into the local buffer. The buffer is then committed to the shared Iceberg table at the Cloud Commit Interval below.`}
                />
                <p className="text-[10px] text-muted-foreground">Polls FOS every {pollFreqShort}</p>
              </div>
              <Switch
                aria-label="Background Sync"
                checked={config.enable_cron_sync}
                onCheckedChange={(v) => update("enable_cron_sync", v)}
              />
            </div>
            <div
              className={cn(
                "flex items-center justify-between p-3 border rounded-md bg-muted/5 transition-opacity",
                syncDisabledClass,
              )}
            >
              <div className="space-y-0.5">
                <LabelWithInfo
                  label="Auto-Delete Raw Logs"
                  info={`Deletes the raw .gz ${isLogging ? "log" : "RUM beacon"} files from FOS after they are ingested into Iceberg. Recommended — the Iceberg table holds the same data in a more efficient format.`}
                />
                <p className="text-[10px] text-muted-foreground">
                  {isLogging ? "Remove .gz files after ingest" : "Remove .gz RUM files after ingest"}
                </p>
              </div>
              <Switch
                aria-label="Auto-Delete Raw Logs"
                checked={config.delete_after}
                onCheckedChange={(v) => update("delete_after", v)}
              />
            </div>
          </div>

          <div className={cn("p-4 border rounded-md bg-muted/5 space-y-3 transition-opacity", syncDisabledClass)}>
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <LabelWithInfo
                  label="Cloud Commit Interval"
                  info="How often the local buffer is flushed to the shared Iceberg table in Fastly Object Storage. This determines how quickly data becomes visible to other users or tools querying the Iceberg table directly. More frequent commits mean fresher data but create more small files — the daily Iceberg optimization consolidates them."
                />
                <p className="text-[10px] text-muted-foreground leading-relaxed">
                  Controls data freshness for shared access. Every commit creates one Iceberg snapshot in FOS.
                </p>
              </div>
              <Select
                value={String(config.commit_interval_mins)}
                onValueChange={(v) => v && update("commit_interval_mins", Number(v))}
              >
                <SelectTrigger className="h-8 w-[220px] shrink-0 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {COMMIT_INTERVAL_OPTIONS.map(([val, label]) => (
                    <SelectItem key={val} value={val} className="text-xs">
                      {label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="text-[10px] text-muted-foreground bg-muted/30 rounded px-3 py-2 leading-relaxed">
              {isLogging
                ? `With a ${config.log_period >= 60 ? `${config.log_period / 60}-minute` : `${config.log_period}-second`} log period`
                : "With a 1-minute RUM poll period"}{" "}
              and a {config.commit_interval_mins}-minute commit interval, the system will create ~
              {Math.round(1440 / config.commit_interval_mins)} Iceberg snapshots per day before the daily
              optimization consolidates them.
            </div>
          </div>

          <div className={cn("p-4 border rounded-md bg-muted/5 space-y-4 transition-opacity", syncDisabledClass)}>
            <div>
              <LabelWithInfo
                label="Data Retention Policy"
                info="Control how long data is stored in your Iceberg table before being deleted to manage storage size and billing. General edge logs are high-volume, while RUM beacons are low-volume."
              />
              <p className="text-[10px] text-muted-foreground leading-relaxed">
                Configure separate retention windows to optimize storage billing and historical trend depth.
              </p>
            </div>
            <div className={cn("grid gap-6", isLogging && config.rum_enabled ? "grid-cols-2" : "grid-cols-1")}>
              {isLogging && (
                <div className="space-y-1.5">
                  <LabelWithInfo
                    label="Request Log Retention"
                    info="Prunes high-volume CDN HTTP logs older than this threshold to minimize your Object Storage charges. RUM records are kept safe."
                  />
                  <RetentionSelect
                    value={String(config.log_retention_days)}
                    onValueChange={(v) => update("log_retention_days", Number(v))}
                    triggerClassName="h-8 text-xs"
                    itemClassName="text-xs"
                  />
                </div>
              )}
              {config.rum_enabled && (
                <div className="space-y-1.5">
                  <LabelWithInfo
                    label="RUM Telemetry Retention"
                    info="Keeps client-side performance beacons (Web Vitals) for a longer period of time. Since RUM volume is very small, a larger retention window has negligible storage costs."
                  />
                  <RetentionSelect
                    value={String(config.rum_retention_days)}
                    onValueChange={(v) => update("rum_retention_days", Number(v))}
                    triggerClassName="h-8 text-xs"
                    itemClassName="text-xs"
                  />
                </div>
              )}
            </div>
            {config.rum_enabled && isLogging && (
              <div className="grid grid-cols-2 gap-6 pt-1">
                {renderRumPeriodField(
                  "Fastly edge rotation period for RUM beacons. Ingestion polls at a derived frequency (e.g. 5m flush polls every 150s). You can configure a longer period than request logs to batch beacons and optimize file writes.",
                  true,
                )}
              </div>
            )}
            {config.rum_enabled && s.selectedService?.id && (
              <div className="pt-1">
                <RumVersionPicker
                  serviceId={s.selectedService.id}
                  value={config.faro_version}
                  onChange={(v) => update("faro_version", v)}
                />
              </div>
            )}
            {config.rum_enabled && (
              <div className="space-y-1.5 pt-2">
                <LabelWithInfo
                  htmlFor="rumCustomCondition"
                  label="Optional RUM Log Condition"
                  info="An additional VCL condition to filter RUM beacons (e.g., req.url.path !~ '^/api/'). The expression will be wrapped in parentheses and added to the RUM beacon condition logic."
                />
                <Input
                  id="rumCustomCondition"
                  placeholder="e.g. req.url.path !~ '^/api/'"
                  value={config.rum_custom_condition}
                  onChange={(e) => update("rum_custom_condition", e.target.value)}
                  className="h-9 font-mono text-xs"
                />
              </div>
            )}
          </div>

          <div className={cn("p-4 border rounded-md bg-muted/5 space-y-4 transition-opacity", syncDisabledClass)}>
            <div>
              <LabelWithInfo
                label="Local Dashboard Cache Retention"
                info="Control how long data is stored on this server's local NVMe disk for instantaneous dashboard querying. Data older than this is seamlessly queried directly from S3 (slower, but saves local disk space)."
              />
              <p className="text-[10px] text-muted-foreground leading-relaxed">
                Configure local hot-cache windows. Rollups are tiny and should be kept long-term; raw logs are
                huge and can be evicted early. If your local disk gets &gt;85% full, the oldest cache is
                automatically evicted regardless of these settings to prevent disk exhaustion.
              </p>
            </div>
            <div className="grid grid-cols-2 gap-6">
              <div className="space-y-1.5">
                <LabelWithInfo
                  label="Local Raw Log Cache"
                  info="How many days of raw logs to keep on the local NVMe disk. Older logs are automatically queried directly from S3 Object Storage."
                />
                <RetentionSelect
                  value={String(config.cache_retention_days)}
                  onValueChange={(v) => update("cache_retention_days", Number(v))}
                  triggerClassName="h-8 text-xs"
                  itemClassName="text-xs"
                />
              </div>
              <div className="space-y-1.5">
                <LabelWithInfo
                  label="Local Rollups & Metrics"
                  info="How many MONTHS of pre-aggregated time-series rollups to keep locally. These are very small and allow instant loading of 90-day dashboard charts."
                />
                <Select
                  value={String(config.rollup_retention_months)}
                  onValueChange={(v) => update("rollup_retention_months", Number(v))}
                >
                  <SelectTrigger className="h-8 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {ROLLUP_MONTH_OPTIONS.map(([val, label]) => (
                      <SelectItem key={val} value={val} className="text-xs">
                        {label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          <div
            className={cn(
              "flex items-center justify-between p-3 border rounded-md bg-muted/5 transition-opacity",
              syncDisabledClass,
            )}
          >
            <div className="space-y-0.5">
              <LabelWithInfo
                label="Daily Iceberg Optimization"
                info="Every night at 04:00 UTC, rewrites many small Iceberg snapshot files into larger, optimized Parquet files in FOS. This is storage-side housekeeping that keeps cloud storage costs down — dashboard query speed is handled separately by always-on local compaction. Strongly recommended when using frequent commit intervals."
              />
              <p className="text-[10px] text-muted-foreground">Runs at 04:00 UTC — consolidates daily snapshots</p>
            </div>
            <Switch
              aria-label="Daily Iceberg Optimization"
              checked={config.enable_cron_compact}
              onCheckedChange={(v) => update("enable_cron_compact", v)}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export function AnalyzeStep({ s }: { s: WizardState }) {
  const { lakeInfo, importMode, setImportMode, importRange, setImportRange } = s
  const { setValue } = useForm<AnalyzeStepFormValues>({
    resolver: zodResolver(analyzeStepSchema),
    values: { importMode, start: importRange.start, end: importRange.end },
  })
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-8 pb-12 max-w-3xl mx-auto">
        <div className="space-y-4">
          <SectionHeader title="Analyze Data Lake" icon={Search} />
          {lakeInfo?.table_exists ? (
            <div className="space-y-6">
              <div className="bg-emerald-500/5 border border-emerald-500/20 rounded-xl p-6 space-y-4">
                <div className="flex items-center gap-3 text-emerald-600 dark:text-emerald-400">
                  <CheckCircle2 className="h-6 w-6" />
                  <h4 className="text-lg font-bold">Found existing Iceberg Table</h4>
                </div>
                <p className="text-sm text-muted-foreground leading-relaxed">
                  We found an active data lake in this bucket with <strong>{lakeInfo.info?.data_files}</strong>{" "}
                  data files and <strong>{lakeInfo.info?.snapshots}</strong> snapshots.
                </p>
                <div className="grid grid-cols-2 gap-4 pt-2">
                  {[
                    ["Available From", lakeInfo.range?.start],
                    ["Available To", lakeInfo.range?.end],
                  ].map(([label, val]) => (
                    <div key={String(label)} className="bg-background/50 border rounded-lg p-4 space-y-1">
                      <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                        {label}
                      </span>
                      <div className="flex flex-col font-mono text-sm font-semibold">
                        <div className="flex items-center gap-2">
                          <Calendar className="h-3.5 w-3.5 text-primary" />
                          {formatDateTime(val, s.timezone)}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <LabelWithInfo
                    label="Data Import Strategy"
                    info="Choose how much historical data you want to sync to your local machine. You can always sync more later."
                  />
                  <Badge variant="secondary" className="font-mono bg-muted/50 border shadow-sm">
                    ~{formatBytes(s.estimatedImportSize)}
                  </Badge>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  {(
                    [
                      ["all", Database, "Import All Data", "Sync every available log file"],
                      ["range", Calendar, "Select Range", "Choose specific dates to import"],
                    ] as const
                  ).map(([modeKey, Icon, title, sub]) => (
                    <button
                      key={modeKey}
                      onClick={() => {
                        setValue("importMode", modeKey, { shouldDirty: true })
                        setImportMode(modeKey)
                      }}
                      className={cn(
                        "flex flex-col items-center gap-3 p-6 border-2 rounded-xl transition-all text-left",
                        importMode === modeKey
                          ? "border-primary bg-primary/5 ring-4 ring-primary/10"
                          : "border-muted hover:bg-muted/50",
                      )}
                    >
                      <Icon className="h-6 w-6 text-primary" />
                      <div className="text-center">
                        <div className="font-bold text-sm">{title}</div>
                        <p className="text-[10px] text-muted-foreground mt-1">{sub}</p>
                      </div>
                    </button>
                  ))}
                </div>
              </div>
              {importMode === "range" && (
                <div className="p-6 border rounded-xl bg-muted/5 space-y-4 animate-in fade-in slide-in-from-top-2">
                  <div className="flex items-center gap-4">
                    <div className="space-y-1.5 flex-1">
                      <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
                        Start Time
                      </Label>
                      <Input
                        type="datetime-local"
                        step="1"
                        value={formatForInput(importRange.start, s.timezone)}
                        min={formatForInput(lakeInfo.range?.start ?? "", s.timezone)}
                        max={formatForInput(importRange.end || lakeInfo.range?.end || "", s.timezone)}
                        onChange={(e) => {
                          const start = parseFromInput(e.target.value, s.timezone) ?? ""
                          setValue("start", start, { shouldDirty: true })
                          setImportRange((prev) => ({ ...prev, start }))
                        }}
                        className="h-9 font-mono"
                      />
                    </div>
                    <ArrowRight className="h-4 w-4 text-muted-foreground mt-6" />
                    <div className="space-y-1.5 flex-1">
                      <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
                        End Time
                      </Label>
                      <Input
                        type="datetime-local"
                        step="1"
                        value={formatForInput(importRange.end, s.timezone)}
                        min={formatForInput(importRange.start || lakeInfo.range?.start || "", s.timezone)}
                        max={formatForInput(lakeInfo.range?.end ?? "", s.timezone)}
                        onChange={(e) => {
                          const end = parseFromInput(e.target.value, s.timezone) ?? ""
                          setValue("end", end, { shouldDirty: true })
                          setImportRange((prev) => ({ ...prev, end }))
                        }}
                        className="h-9 font-mono"
                      />
                    </div>
                  </div>
                  <div className="flex items-center justify-between mt-2 pt-2 border-t border-muted/50">
                    <p className="text-[10px] text-muted-foreground italic">
                      Only data between these times will be downloaded initially.
                    </p>
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="p-12 border border-dashed rounded-xl bg-muted/5 text-center space-y-4">
              <div className="mx-auto w-12 h-12 rounded-full bg-amber-500/10 flex items-center justify-center">
                <AlertCircle className="h-6 w-6 text-amber-500" />
              </div>
              <div className="space-y-1">
                <h4 className="font-bold">No Data Found</h4>
                <p className="text-sm text-muted-foreground max-w-xs mx-auto">
                  We couldn&apos;t find an Iceberg table in this bucket. The admin might not have started the
                  ingestion yet.
                </p>
              </div>
              <p className="text-xs text-muted-foreground">
                You can still connect, but the dashboard will be empty until data is available.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
