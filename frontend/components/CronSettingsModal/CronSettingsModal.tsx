'use client'

import React from 'react'
import Link from 'next/link'
import { useForm, useWatch, type PathValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQueryClient } from '@tanstack/react-query'
import { Clock, Loader2, Shield } from 'lucide-react'
import { SSEActionDialogShell, SSEExecutionSection } from '@/components/SSEModal'
import { Button, buttonVariants } from '@/components/ui/button'
import { RetentionSelect } from '@/components/ui/confirm-dialog'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { useOpenGeneration, useSSE } from '@/hooks/useSSE'
import { queryKeys } from "@/lib/api"
import type { ServiceConfig } from '@/types/api'

export const SYNC_INTERVAL_OPTIONS = [
  { value: '1', label: 'Every 1 minute' },
  { value: '5', label: 'Every 5 minutes' },
  { value: '15', label: 'Every 15 minutes' },
  { value: '30', label: 'Every 30 minutes' },
  { value: '60', label: 'Every 1 hour' },
  { value: '360', label: 'Every 6 hours' },
]

export const COMMIT_INTERVAL_OPTIONS = [
  { value: '1', label: 'Every 1 minute' },
  { value: '2', label: 'Every 2 minutes' },
  { value: '5', label: 'Every 5 minutes' },
  { value: '10', label: 'Every 10 minutes' },
  { value: '15', label: 'Every 15 minutes' },
  { value: '30', label: 'Every 30 minutes' },
  { value: '60', label: 'Every 1 hour' },
]

export const NGWAF_INTERVAL_OPTIONS = [
  { value: '60', label: 'Every 1 hour' },
  { value: '180', label: 'Every 3 hours' },
  { value: '360', label: 'Every 6 hours' },
  { value: '720', label: 'Every 12 hours' },
  { value: '1440', label: 'Every 24 hours' },
  { value: '4320', label: 'Every 3 days' },
  { value: '10080', label: 'Every 7 days' },
]

function CronLogRetentionControls({
  switchId, logEnabled, setLogEnabled, retentionId, retention, setRetention,
}: {
  switchId: string; logEnabled: boolean; setLogEnabled: (v: boolean) => void
  retentionId: string; retention: string; setRetention: (v: string) => void
}) {
  return (
    <>
      <div className="flex items-center justify-between">
        <div className="space-y-0.5 pr-4">
          <Label className="text-[11px] font-semibold cursor-pointer" htmlFor={switchId}>Log runs to database</Label>
          <p className="text-[10px] text-muted-foreground leading-tight">Keep historical records of execution statuses.</p>
        </div>
        <Switch id={switchId} checked={logEnabled} onCheckedChange={setLogEnabled} />
      </div>
      <div className="grid gap-1.5 max-w-[200px]">
        <Label htmlFor={retentionId} className="text-[11px] font-medium">Keep cron logs for</Label>
        <RetentionSelect id={retentionId} value={retention} onValueChange={setRetention} />
      </div>
    </>
  )
}

interface AnalystSchedulePanelProps {
  syncEnabled: boolean; setSyncEnabled: (v: boolean) => void
  syncIntervalMins: string; setSyncIntervalMins: (v: string) => void
  cacheRetention: string; setCacheRetention: (v: string) => void
}

export function AnalystSchedulePanel({
  syncEnabled, setSyncEnabled, syncIntervalMins, setSyncIntervalMins, cacheRetention, setCacheRetention,
}: AnalystSchedulePanelProps) {
  return (
    <div className="p-6 space-y-6 text-sm divide-y">
      <div className="space-y-4">
        <div>
          <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80">Cloud Sync Interval</h3>
          <p className="text-[10px] text-muted-foreground mt-1">How often to pull new log files from the cloud bucket to your local cache.</p>
        </div>
        <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
          <div className="space-y-0.5 pr-4">
            <Label className="text-xs font-semibold cursor-pointer" htmlFor="enable-sync-analyst">Auto-Sync New Data</Label>
            <p className="text-[10px] text-muted-foreground">Automatically poll for and download new processed log files.</p>
          </div>
          <Switch id="enable-sync-analyst" checked={syncEnabled} onCheckedChange={setSyncEnabled} />
        </div>
        {syncEnabled && (
          <Select value={syncIntervalMins} onValueChange={v => v && setSyncIntervalMins(v)}>
            <SelectTrigger className="h-9 text-sm"><SelectValue /></SelectTrigger>
            <SelectContent>
              {SYNC_INTERVAL_OPTIONS.map(o => <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>)}
            </SelectContent>
          </Select>
        )}
        <div className="grid gap-1.5 pt-2 border-t border-border/50">
          <Label htmlFor="cache-retention-analyst" className="text-xs font-semibold">Local Cache Retention</Label>
          <p className="text-[10px] text-muted-foreground leading-tight">
            Automatically deletes local data files older than this to save disk space.
          </p>
          <RetentionSelect id="cache-retention-analyst" value={cacheRetention} onValueChange={setCacheRetention} triggerClassName="h-9 text-sm" itemClassName="" />
        </div>
      </div>
    </div>
  )
}

interface LogSyncSectionProps {
  syncEnabled: boolean
  setSyncEnabled: (v: boolean) => void
  pollingMode: 'regular' | 'adaptive'
  setPollingMode: (v: 'regular' | 'adaptive') => void
  deleteAfter: boolean
  setDeleteAfter: (v: boolean) => void
  dataRetention: string
  setDataRetention: (v: string) => void
  cacheRetention: string
  setCacheRetention: (v: string) => void
  rollupRetention: string
  setRollupRetention: (v: string) => void
  commitInterval: string
  setCommitInterval: (v: string) => void
  syncLogEnabled: boolean
  setSyncLogEnabled: (v: boolean) => void
  syncRetention: string
  setSyncRetention: (v: string) => void
  syncFreqLabel: string
  isAnalyst: boolean
  syncIntervalNum: number
  adminSyncSeconds: number
  rumRetention: string
  setRumRetention: (v: string) => void
  rumEnabled: boolean
  rumSyncEnabled?: boolean
  setRumSyncEnabled?: (v: boolean) => void
  rumLogPeriod: string
  setRumLogPeriod: (v: string) => void
  rumDeleteAfter: boolean
  setRumDeleteAfter: (v: boolean) => void
  serviceId?: string
}

export function LogSyncSection({
  syncEnabled, setSyncEnabled, pollingMode, setPollingMode, deleteAfter, setDeleteAfter,
  dataRetention, setDataRetention, cacheRetention, setCacheRetention, rollupRetention, setRollupRetention,
  commitInterval, setCommitInterval, syncLogEnabled, setSyncLogEnabled, syncRetention, setSyncRetention,
  syncFreqLabel, isAnalyst, syncIntervalNum, adminSyncSeconds, rumRetention, setRumRetention,
  rumEnabled, rumSyncEnabled, setRumSyncEnabled, rumLogPeriod, setRumLogPeriod, rumDeleteAfter, setRumDeleteAfter, serviceId,
}: LogSyncSectionProps) {
  const minCommitMins = isAnalyst ? syncIntervalNum : Math.max(1, Math.ceil(adminSyncSeconds / 60))
  const rumPeriodInt = parseInt(rumLogPeriod)
  const derivedRumSecs = rumPeriodInt >= 60 ? Math.max(5, Math.floor(rumPeriodInt / 2)) : Math.max(5, rumPeriodInt)

  return (
    <div className="space-y-4">
      <div>
        <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80">Log Sync</h3>
        <p className="text-[10px] text-muted-foreground">Automated ingestion from Fastly Object Storage.</p>
      </div>

      <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
        <div className="space-y-0.5 pr-4">
          <Label className="text-xs font-semibold cursor-pointer" htmlFor="enable-sync">Enable Cron Sync</Label>
          <p className="text-[10px] text-muted-foreground">
            Polls FOS for new log files {syncFreqLabel} and ingests them into the local buffer.
          </p>
        </div>
        <Switch id="enable-sync" checked={syncEnabled} onCheckedChange={setSyncEnabled} />
      </div>

      <div className="grid gap-1.5 max-w-sm">
        <Label htmlFor="polling-mode" className="text-[11px] font-semibold">Discovery Polling Mode</Label>
        <Select value={pollingMode} onValueChange={v => { if (v === 'regular' || v === 'adaptive') setPollingMode(v) }}>
          <SelectTrigger id="polling-mode" aria-label="Discovery Polling Mode" className="h-7 text-[11px]"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="regular" className="text-[11px]">Regular</SelectItem>
            <SelectItem value="adaptive" className="text-[11px]">Adaptive</SelectItem>
          </SelectContent>
        </Select>
        <p className="text-[10px] text-muted-foreground leading-tight">
          Adaptive makes up to two short follow-up discovery passes after finding logs. It can increase FOS LIST costs.
        </p>
      </div>

      <div className={`space-y-5 pl-4 border-l-2 transition-opacity ${syncEnabled ? 'opacity-100 border-primary' : 'opacity-40 border-muted pointer-events-none'}`}>
        <div className="flex items-center justify-between">
          <div className="space-y-0.5 pr-4">
            <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="auto-delete">Auto-delete Raw .gz Logs</Label>
            <p className="text-[10px] text-muted-foreground leading-tight">Saves FOS storage by removing raw logs once they are ingested into Iceberg.</p>
          </div>
          <Switch id="auto-delete" checked={deleteAfter} onCheckedChange={setDeleteAfter} />
        </div>

        <div className={`grid gap-4 pb-2 ${rumEnabled ? 'grid-cols-3' : 'grid-cols-2'}`}>
          <div className="grid gap-1.5">
            <Label htmlFor="data-retention" className="text-[11px] font-medium">
              {rumEnabled ? 'Request Log Retention' : 'Cloud Data Retention'}
            </Label>
            <p className="text-[10px] text-muted-foreground leading-tight h-6">
              Delete CDN request log data from Iceberg older than this.
            </p>
            <RetentionSelect id="data-retention" value={dataRetention} onValueChange={setDataRetention} />
          </div>

          {rumEnabled && (
            <div className="grid gap-1.5">
              <Label htmlFor="rum-retention" className="text-[11px] font-medium">RUM Data Retention</Label>
              <p className="text-[10px] text-muted-foreground leading-tight h-6">
                Delete RUM beacon telemetry from Iceberg older than this.
              </p>
              <RetentionSelect id="rum-retention" value={rumRetention} onValueChange={setRumRetention} />
            </div>
          )}

          <div className="grid gap-1.5">
            <Label htmlFor="cache-retention" className="text-[11px] font-medium">Local Cache Retention</Label>
            <p className="text-[10px] text-muted-foreground leading-tight h-6">
              Delete local cache files older than this to save disk space.
            </p>
            <RetentionSelect id="cache-retention" value={cacheRetention} onValueChange={setCacheRetention} />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor="rollup-retention" className="text-[11px] font-medium">Rollup Retention</Label>
            <p className="text-[10px] text-muted-foreground leading-tight h-6">
              Keep local monthly rollup files for this many months.
            </p>
            <Select value={rollupRetention} onValueChange={v => v && setRollupRetention(v)}>
              <SelectTrigger id="rollup-retention" className="h-7 text-[11px]">
                <SelectValue>{(val) => val + (val === '1' ? ' month' : ' months')}</SelectValue>
              </SelectTrigger>
              <SelectContent>
                {[1, 2, 3, 6, 12, 24, 36, 60, 120].map(v => (
                  <SelectItem key={v} value={String(v)} className="text-[11px]">{v} month{v === 1 ? '' : 's'}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div className="grid gap-1.5">
            <div className="flex items-center gap-1.5">
              <Label htmlFor="commit-interval" className="text-[11px] font-semibold">Cloud Commit Interval</Label>
            </div>
            <p className="text-[10px] text-muted-foreground leading-tight">
              How often the local buffer is pushed to Iceberg. Cannot be shorter than sync frequency ({syncFreqLabel}).
            </p>
            <Select
              value={commitInterval}
              onValueChange={v => { if (v && parseInt(v) >= minCommitMins) setCommitInterval(v) }}
            >
              <SelectTrigger id="commit-interval" className="h-7 text-[11px]"><SelectValue /></SelectTrigger>
              <SelectContent>
                {COMMIT_INTERVAL_OPTIONS.filter(o => parseInt(o.value) >= minCommitMins).map(o => (
                  <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="grid gap-1.5">
            <Label htmlFor="sync-log-enabled" className="text-[11px] font-semibold">Sync Logs</Label>
            <p className="text-[10px] text-muted-foreground leading-tight">
              Keep historical records of execution statuses.
            </p>
            <div className="flex items-center gap-2 pt-2">
              <Switch id="sync-log-enabled" checked={syncLogEnabled} onCheckedChange={setSyncLogEnabled} />
              <span className="text-[11px] text-muted-foreground">{syncLogEnabled ? 'Enabled' : 'Disabled'}</span>
            </div>
          </div>
        </div>

        {syncLogEnabled && (
          <div className="grid gap-1.5 max-w-[300px]">
            <Label htmlFor="sync-retention" className="text-[11px] font-medium">Keep cron logs for</Label>
            <RetentionSelect id="sync-retention" value={syncRetention} onValueChange={setSyncRetention} />
          </div>
        )}

        <div className="space-y-4 pt-3 border-t border-border/30">
          <div className="text-[10px] font-semibold text-foreground/70 uppercase tracking-widest">RUM Beacon Sync</div>

          {rumEnabled ? (
            <>
              <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
                <div className="space-y-0.5 pr-4">
                  <Label className="text-xs font-semibold cursor-pointer" htmlFor="enable-rum-sync">Enable RUM Beacon Sync</Label>
                  <p className="text-[10px] text-muted-foreground">
                    Polls FOS for new RUM beacon logs and ingests them into DuckLake.
                  </p>
                </div>
                <Switch id="enable-rum-sync" checked={rumSyncEnabled ?? false} onCheckedChange={setRumSyncEnabled} />
              </div>

              <div className={`space-y-4 pl-4 border-l-2 transition-opacity ${rumSyncEnabled ? 'opacity-100 border-primary' : 'opacity-40 border-muted pointer-events-none'}`}>
                <div className="grid grid-cols-2 gap-4">
                  <div className="grid gap-1.5">
                    <Label htmlFor="rum-log-period" className="text-[11px] font-medium">RUM Log Period / Freshness</Label>
                    <p className="text-[10px] text-muted-foreground leading-tight h-6">
                      Fastly edge rotation period for RUM beacons. Ingestion polls at derived frequency.
                    </p>
                    <Select value={rumLogPeriod} onValueChange={v => v && setRumLogPeriod(v)}>
                      <SelectTrigger id="rum-log-period" className="h-7 text-[11px]"><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="10" className="text-[11px]">10 seconds (Ultra-Fast / High-Load)</SelectItem>
                        <SelectItem value="60" className="text-[11px]">1 minute (Fastest)</SelectItem>
                        <SelectItem value="300" className="text-[11px]">5 minutes (Recommended)</SelectItem>
                        <SelectItem value="600" className="text-[11px]">10 minutes</SelectItem>
                        <SelectItem value="900" className="text-[11px]">15 minutes</SelectItem>
                      </SelectContent>
                    </Select>
                    <p className="text-[10px] text-muted-foreground mt-0.5">
                      Automated sync runs every {derivedRumSecs}s to ingest new beacons.
                    </p>
                  </div>

                  <div className="grid gap-1.5">
                    <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="rum-delete-after">Auto-delete RUM Logs</Label>
                    <p className="text-[10px] text-muted-foreground leading-tight">Remove raw .gz files after ingestion.</p>
                    <div className="flex items-center gap-2 pt-2">
                      <Switch id="rum-delete-after" checked={rumDeleteAfter} onCheckedChange={setRumDeleteAfter} />
                      <span className="text-[11px] text-muted-foreground">{rumDeleteAfter ? 'Enabled' : 'Disabled'}</span>
                    </div>
                  </div>
                </div>
              </div>
            </>
          ) : (
            <div className="rounded-md border border-dashed border-border/80 bg-muted/20 p-3 space-y-2">
              <p className="text-xs text-muted-foreground">
                Real User Monitoring is not currently enabled on Fastly edge for this service. Enable RUM on your edge service to collect client vitals and error beacons.
              </p>
              <div>
                <Link
                  href={serviceId ? `/admin/rum?service=${serviceId}` : '/admin/rum'}
                  className={buttonVariants({ variant: 'outline', size: 'sm' })}
                >
                  Configure RUM
                </Link>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export const Schedule = LogSyncSection

interface NgwafSectionProps {
  ngwafInterval: string
  setNgwafInterval: (v: string) => void
  ngwafLogEnabled: boolean
  setNgwafLogEnabled: (v: boolean) => void
  ngwafRetention: string
  setNgwafRetention: (v: string) => void
}

export function NgwafSection({
  ngwafInterval,
  setNgwafInterval,
  ngwafLogEnabled,
  setNgwafLogEnabled,
  ngwafRetention,
  setNgwafRetention,
}: NgwafSectionProps) {
  return (
    <div className="space-y-3">
      <div>
        <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80 flex items-center gap-1.5">
          <Shield className="h-3.5 w-3.5" /> NGWAF Bot Sync
        </h3>
        <p className="text-[10px] text-muted-foreground">Fetches verified bot records from Fastly NGWAF and caches them locally.</p>
      </div>

      <div className="space-y-4 pl-4 border-l-2 border-primary">
        <div className="grid gap-1.5">
          <Label htmlFor="ngwaf-interval" className="text-[11px] font-semibold">Sync Interval</Label>
          <Select value={ngwafInterval} onValueChange={v => v && setNgwafInterval(v)}>
            <SelectTrigger id="ngwaf-interval" className="h-7 text-[11px]"><SelectValue /></SelectTrigger>
            <SelectContent>
              {NGWAF_INTERVAL_OPTIONS.map(o => (
                <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <CronLogRetentionControls
          switchId="ngwaf-log-enabled"
          logEnabled={ngwafLogEnabled}
          setLogEnabled={setNgwafLogEnabled}
          retentionId="ngwaf-retention"
          retention={ngwafRetention}
          setRetention={setNgwafRetention}
        />
      </div>
    </div>
  )
}

interface IcebergOptimizationSectionProps {
  compactEnabled: boolean
  setCompactEnabled: (v: boolean) => void
  compactLogEnabled: boolean
  setCompactLogEnabled: (v: boolean) => void
  compactRetention: string
  setCompactRetention: (v: string) => void
}

export function IcebergOptimizationSection({
  compactEnabled,
  setCompactEnabled,
  compactLogEnabled,
  setCompactLogEnabled,
  compactRetention,
  setCompactRetention,
}: IcebergOptimizationSectionProps) {
  return (
    <div className="space-y-3">
      <div>
        <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80">Iceberg Optimization</h3>
        <p className="text-[10px] text-muted-foreground">Nightly FOS-side housekeeping to keep cloud storage costs down. (Dashboard query speed is handled separately by always-on local compaction.)</p>
      </div>

      <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
        <div className="space-y-0.5 pr-4">
          <Label className="text-xs font-semibold cursor-pointer" htmlFor="enable-compact">Enable Daily Optimization</Label>
          <p className="text-[10px] text-muted-foreground leading-tight">
            Rewrites many small Iceberg snapshot files into larger, optimized Parquet files at 04:00 UTC.
            Strongly recommended when using frequent commit intervals.
          </p>
        </div>
        <Switch id="enable-compact" checked={compactEnabled} onCheckedChange={setCompactEnabled} />
      </div>

      <div className={`space-y-4 pl-4 border-l-2 transition-opacity ${compactEnabled ? 'opacity-100 border-primary' : 'opacity-40 border-muted pointer-events-none'}`}>
        <CronLogRetentionControls
          switchId="compact-log-enabled"
          logEnabled={compactLogEnabled}
          setLogEnabled={setCompactLogEnabled}
          retentionId="compact-retention"
          retention={compactRetention}
          setRetention={setCompactRetention}
        />
      </div>
    </div>
  )
}

interface CronSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

const cronSettingsSchema = z.object({
  syncIntervalMins: z.string(),
  pollingMode: z.enum(['regular', 'adaptive']),
  syncEnabled: z.boolean(),
  deleteAfter: z.boolean(),
  commitInterval: z.string(),
  syncLogEnabled: z.boolean(),
  syncRetention: z.string(),
  dataRetention: z.string(),
  rumRetention: z.string(),
  cacheRetention: z.string(),
  rollupRetention: z.string(),
  compactEnabled: z.boolean(),
  compactLogEnabled: z.boolean(),
  compactRetention: z.string(),
  ngwafInterval: z.string(),
  ngwafLogEnabled: z.boolean(),
  ngwafRetention: z.string(),
  rumSyncEnabled: z.boolean(),
  rumLogPeriod: z.string(),
  rumDeleteAfter: z.boolean(),
})
type CronSettingsFormValues = z.infer<typeof cronSettingsSchema>

function initialSettings(service: ServiceConfig | null): CronSettingsFormValues {
  const rumCfg = service?.rum as Record<string, unknown> | null | undefined
  const rumSyncVal = typeof rumCfg?.enabled === 'boolean' ? rumCfg.enabled : undefined
  const rumPeriodVal = typeof rumCfg?.log_period === 'number' ? rumCfg.log_period : undefined
  const initialRumPeriod = rumPeriodVal ?? service?.rum_log_period ?? service?.log_period ?? 300
  const rumDeleteVal = typeof rumCfg?.delete_after === 'boolean' ? rumCfg.delete_after : undefined

  return {
    syncIntervalMins: String(service?.cron_sync?.interval_mins ?? 2),
    pollingMode: service?.cron_sync?.polling_mode ?? 'adaptive',
    syncEnabled: service?.cron_sync?.enabled ?? false,
    deleteAfter: service?.cron_sync?.delete_after ?? false,
    commitInterval: String(service?.cron_sync?.commit_interval_mins ?? 5),
    syncLogEnabled: service?.cron_sync?.log_enabled !== false,
    syncRetention: String(service?.cron_sync?.log_retention_days ?? 7),
    dataRetention: String(service?.cron_sync?.data_retention_days ?? 30),
    rumRetention: String(service?.cron_sync?.rum_retention_days ?? 30),
    cacheRetention: String(service?.cron_sync?.cache_retention_days ?? 90),
    rollupRetention: String(service?.cron_sync?.rollup_retention_months ?? 12),
    compactEnabled: service?.cron_compact?.enabled ?? false,
    compactLogEnabled: service?.cron_compact?.log_enabled !== false,
    compactRetention: String(service?.cron_compact?.log_retention_days ?? 7),
    ngwafInterval: String(service?.cron_ngwaf?.interval_mins ?? 5),
    ngwafLogEnabled: service?.cron_ngwaf?.log_enabled !== false,
    ngwafRetention: String(service?.cron_ngwaf?.log_retention_days ?? 7),
    rumSyncEnabled: rumSyncVal ?? service?.rum_enabled ?? false,
    rumLogPeriod: String(initialRumPeriod),
    rumDeleteAfter: rumDeleteVal ?? service?.cron_sync?.delete_after ?? false,
  }
}

export function CronSettingsModal(props: CronSettingsModalProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <CronSettingsModalBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function CronSettingsModalBody({ service, open, onOpenChange }: CronSettingsModalProps) {
  const queryClient = useQueryClient()
  const { lines, status, error, start, stop } = useSSE()
  const { control, setValue } = useForm<CronSettingsFormValues>({
    resolver: zodResolver(cronSettingsSchema),
    defaultValues: initialSettings(service),
  })
  const {
    syncEnabled, deleteAfter, commitInterval, syncLogEnabled, syncRetention, dataRetention,
    rumRetention, cacheRetention, rollupRetention, compactEnabled, compactLogEnabled, compactRetention,
    ngwafInterval, ngwafLogEnabled, ngwafRetention, syncIntervalMins, pollingMode,
    rumSyncEnabled, rumLogPeriod, rumDeleteAfter,
  } = useWatch({ control }) as CronSettingsFormValues
  const bind = <K extends keyof CronSettingsFormValues>(key: K) => (v: CronSettingsFormValues[K]) =>
    setValue(key, v as PathValue<CronSettingsFormValues, K>, { shouldDirty: true })

  const isAnalyst = service?.access_level === 'read_only'

  const handleSave = () => {
    if (!service) return
    const intervalMins = parseInt(syncIntervalMins)
    if (isAnalyst) {
      start(`/api/services/${service.service_id}/cron-settings`, {
        cron_sync: {
          enabled: syncEnabled,
          interval_mins: intervalMins,
          cache_retention_days: parseInt(cacheRetention),
          rollup_retention_months: parseInt(rollupRetention),
        },
      })
      return
    }
    const commitMins = Math.max(intervalMins, parseInt(commitInterval))
    start(`/api/services/${service.service_id}/cron-settings`, {
      cron_sync: {
        enabled: syncEnabled,
        delete_after: deleteAfter,
        polling_mode: pollingMode,
        commit_interval_mins: commitMins,
        log_enabled: syncLogEnabled,
        log_retention_days: parseInt(syncRetention),
        data_retention_days: parseInt(dataRetention),
        rum_retention_days: parseInt(rumRetention),
        cache_retention_days: parseInt(cacheRetention),
        rollup_retention_months: parseInt(rollupRetention),
      },
      cron_compact: {
        enabled: compactEnabled,
        log_enabled: compactLogEnabled,
        log_retention_days: parseInt(compactRetention),
      },
      ...(service.ngwaf_workspace_id ? {
        cron_ngwaf: {
          interval_mins: parseInt(ngwafInterval),
          log_enabled: ngwafLogEnabled,
          log_retention_days: parseInt(ngwafRetention),
        },
      } : {}),
      ...(service.rum_enabled ? {
        rum: {
          enabled: rumSyncEnabled,
          log_period: parseInt(rumLogPeriod),
          delete_after: rumDeleteAfter,
        },
      } : {}),
    })
  }

  if (!service) return null

  const isPending = status === 'streaming'
  const isExecuting = status === 'done' || status === 'error' || status === 'streaming'
  const syncIntervalNum = parseInt(syncIntervalMins)
  const adminSyncSeconds = Math.max(10, Math.floor((service?.log_period || 60) / 2))
  const syncFreqLabel = isAnalyst
    ? (syncIntervalNum === 1 ? 'every 1 minute' : `every ${syncIntervalNum} minutes`)
    : (adminSyncSeconds >= 60
        ? `every ${Math.floor(adminSyncSeconds / 60)}m${adminSyncSeconds % 60 > 0 ? ` ${adminSyncSeconds % 60}s` : ''}`
        : `every ${adminSyncSeconds} seconds`)

  return (
    <SSEActionDialogShell
      open={open}
      onOpenChange={onOpenChange}
      onComplete={() => { queryClient.invalidateQueries({ queryKey: queryKeys.services() }) }}
      reloadOnDone={false}
      status={status}
      onStop={stop}
      isExecuting={isExecuting}
      maxWidthClassName="sm:max-w-4xl"
      titleClassName="text-foreground"
      title={<><Clock className="h-5 w-5 text-primary" /> Cron Settings</>}
      headerExtra={
        <div className="text-sm text-muted-foreground mt-1">
          Service: <span className="font-medium text-foreground">{service.name}</span>
        </div>
      }
      executionSection={
        <SSEExecutionSection
          title="Updating Cron Settings"
          subtitle="Applying new background sync configuration..."
          lines={lines}
          status={status}
          error={error}
          heightClassName="h-[300px]"
          progressLabel="Step"
          doneMessage="Settings applied! You may now close this window."
        />
      }
      confirmButton={
        <Button onClick={handleSave} disabled={isPending} className="h-10 px-6 font-bold">
          {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          Save Changes
        </Button>
      }
      idleOrErrorLabel="Close"
    >
      {isAnalyst ? (
        <AnalystSchedulePanel
          syncEnabled={syncEnabled}
          setSyncEnabled={bind('syncEnabled')}
          syncIntervalMins={syncIntervalMins}
          setSyncIntervalMins={bind('syncIntervalMins')}
          cacheRetention={cacheRetention}
          setCacheRetention={bind('cacheRetention')}
        />
      ) : (
        <div className="space-y-8 p-6 text-sm">
          <LogSyncSection
            syncEnabled={syncEnabled}
            setSyncEnabled={bind('syncEnabled')}
            pollingMode={pollingMode}
            setPollingMode={bind('pollingMode')}
            deleteAfter={deleteAfter}
            setDeleteAfter={bind('deleteAfter')}
            dataRetention={dataRetention}
            setDataRetention={bind('dataRetention')}
            cacheRetention={cacheRetention}
            setCacheRetention={bind('cacheRetention')}
            rollupRetention={rollupRetention}
            setRollupRetention={bind('rollupRetention')}
            commitInterval={commitInterval}
            setCommitInterval={bind('commitInterval')}
            syncLogEnabled={syncLogEnabled}
            setSyncLogEnabled={bind('syncLogEnabled')}
            syncRetention={syncRetention}
            setSyncRetention={bind('syncRetention')}
            syncFreqLabel={syncFreqLabel}
            isAnalyst={isAnalyst}
            syncIntervalNum={syncIntervalNum}
            adminSyncSeconds={adminSyncSeconds}
            rumRetention={rumRetention}
            setRumRetention={bind('rumRetention')}
            rumEnabled={service.rum_enabled ?? false}
            rumSyncEnabled={rumSyncEnabled}
            setRumSyncEnabled={bind('rumSyncEnabled')}
            rumLogPeriod={rumLogPeriod}
            setRumLogPeriod={bind('rumLogPeriod')}
            rumDeleteAfter={rumDeleteAfter}
            setRumDeleteAfter={bind('rumDeleteAfter')}
            serviceId={service.service_id}
          />

          {service.ngwaf_workspace_id && (
            <NgwafSection
              ngwafInterval={ngwafInterval}
              setNgwafInterval={bind('ngwafInterval')}
              ngwafLogEnabled={ngwafLogEnabled}
              setNgwafLogEnabled={bind('ngwafLogEnabled')}
              ngwafRetention={ngwafRetention}
              setNgwafRetention={bind('ngwafRetention')}
            />
          )}

          <IcebergOptimizationSection
            compactEnabled={compactEnabled}
            setCompactEnabled={bind('compactEnabled')}
            compactLogEnabled={compactLogEnabled}
            setCompactLogEnabled={bind('compactLogEnabled')}
            compactRetention={compactRetention}
            setCompactRetention={bind('compactRetention')}
          />
        </div>
      )}
    </SSEActionDialogShell>
  )
}
