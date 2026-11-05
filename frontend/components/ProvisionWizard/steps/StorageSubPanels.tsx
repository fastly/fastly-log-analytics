"use client"

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { SectionHeader, LabelWithInfo, RetentionSelect } from '@/components/ui/confirm-dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Skeleton } from '@/components/ui/skeleton'
import { Database, RefreshCw, Settings, TriangleAlert, Zap } from 'lucide-react'
import { cn, PERIOD_LABELS, REGION_LABELS } from '@/lib/utils'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import type { components } from '@/types/api.generated'
import type { ProvisionConfig } from '../types'
import type { StorageStepFormValues } from '../wizardSchemas'

type RumVersionsResponse = components["schemas"]["RumVersionsResponse"]

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
export const RUM_PERIOD_OPTIONS = [
  ["10", "10 seconds (Ultra-Fast / High-Load)"],
  ["60", "1 minute (Fastest)"],
  ["300", "5 minutes (Recommended)"],
  ["600", "10 minutes"],
  ["900", "15 minutes"],
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

export interface StorageSectionProps {
  config: ProvisionConfig
  selectedServiceId?: string
  update: <K extends keyof StorageStepFormValues>(key: K, val: StorageStepFormValues[K]) => void
}

export function renderRumPeriodField(
  rumLogPeriod: number,
  onChange: (val: number) => void,
  infoText: string,
  compact = false,
) {
  return (
    <div className="space-y-1.5">
      <LabelWithInfo label="RUM Log Period" info={infoText} />
      <Select value={String(rumLogPeriod || 300)} onValueChange={(v) => onChange(Number(v) || 300)}>
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
}

export function LoggingSetupSection({ config, update }: StorageSectionProps) {
  const isLogging = config.logging_enabled !== false
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

  return (
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
              config.rum_log_period,
              (val) => update("rum_log_period", val),
              "How often Fastly will flush RUM beacon logs to your bucket. 5 minutes is recommended for balanced latency and storage efficiency.",
            )}
          </div>
        </>
      )}
    </div>
  )
}

export function AutomationSection({ config, selectedServiceId, update }: StorageSectionProps) {
  const isLogging = config.logging_enabled !== false
  const syncDisabledClass = !config.enable_cron_sync ? "opacity-30 pointer-events-none" : undefined
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
    <div className="space-y-5">
      <SectionHeader title="Automation" icon={Settings} />
      <div className="grid grid-cols-2 gap-4">
        <div className="flex items-center justify-between p-3 border rounded-md bg-muted/5">
          <div className="space-y-0.5">
            <LabelWithInfo
              label="Background Sync"
              info={`Automatically polls FOS for new ${isLogging ? "log" : "RUM telemetry"} files (every ${pollFreqLong}), checkpoints canonical Archive Parquet batches to Fastly Object Storage, and publishes verified batches into ClickHouse.`}
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
              info={`Deletes the raw .zst ${isLogging ? "log" : "RUM beacon"} files from FOS after they are archived to Parquet and published into ClickHouse. Recommended — the Archive Parquet and ClickHouse tables hold the same data in a more efficient format.`}
            />
            <p className="text-[10px] text-muted-foreground">
              {isLogging ? "Remove .zst files after ingest" : "Remove .zst RUM files after ingest"}
            </p>
          </div>
          <Switch
            aria-label="Auto-Delete Raw Logs"
            checked={config.delete_after}
            onCheckedChange={(v) => update("delete_after", v)}
          />
        </div>
      </div>

      <div className={cn("p-4 border rounded-md bg-muted/5 space-y-4 transition-opacity", syncDisabledClass)}>
        <div>
          <LabelWithInfo
            label="Data Retention Policy"
            info="Control how long data is stored in ClickHouse and Fastly Object Storage (FOS) Archive Parquet before being pruned to manage storage size and billing. General edge logs are high-volume, while RUM beacons are low-volume."
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
              config.rum_log_period,
              (val) => update("rum_log_period", val),
              "Fastly edge rotation period for RUM beacons. Ingestion polls at a derived frequency (e.g. 5m flush polls every 150s). You can configure a longer period than request logs to batch beacons and optimize file writes.",
              true,
            )}
          </div>
        )}
        {config.rum_enabled && selectedServiceId && (
          <div className="pt-1">
            <RumVersionPicker
              serviceId={selectedServiceId}
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
    </div>
  )
}
