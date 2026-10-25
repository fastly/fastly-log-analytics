'use client'

import React, { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useOpenGeneration } from '@/hooks/useDerivedState'
import { queryKeys } from '@/lib/query-keys'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { RetentionSelect } from '@/components/ui/retention-select'
import { Loader2, Clock, Shield } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEActionDialogShell, SSEExecutionSection } from '@/components/SSEModal'
import { AnalystSchedulePanel, LogSyncSection, NGWAF_INTERVAL_OPTIONS } from './Schedule'
import type { ServiceConfig } from '@/types/api'

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
            <SelectTrigger id="ngwaf-interval" className="h-7 text-[11px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {NGWAF_INTERVAL_OPTIONS.map(o => (
                <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="flex items-center justify-between">
          <div className="space-y-0.5 pr-4">
            <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="ngwaf-log-enabled">Log runs to database</Label>
            <p className="text-[10px] text-muted-foreground leading-tight">Keep historical records of execution statuses.</p>
          </div>
          <Switch id="ngwaf-log-enabled" checked={ngwafLogEnabled} onCheckedChange={setNgwafLogEnabled} />
        </div>

        <div className="grid gap-1.5 max-w-[200px]">
          <Label htmlFor="ngwaf-retention" className="text-[11px] font-medium">Keep cron logs for</Label>
          <RetentionSelect
            id="ngwaf-retention"
            value={ngwafRetention}
            onValueChange={setNgwafRetention}
          />
        </div>
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
        <div className="flex items-center justify-between">
          <div className="space-y-0.5 pr-4">
            <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="compact-log-enabled">Log runs to database</Label>
            <p className="text-[10px] text-muted-foreground leading-tight">Keep historical records of execution statuses.</p>
          </div>
          <Switch id="compact-log-enabled" checked={compactLogEnabled} onCheckedChange={setCompactLogEnabled} />
        </div>

        <div className="grid gap-1.5 max-w-[200px]">
          <Label htmlFor="compact-retention" className="text-[11px] font-medium">Keep cron logs for</Label>
          <RetentionSelect
            id="compact-retention"
            value={compactRetention}
            onValueChange={setCompactRetention}
          />
        </div>
      </div>
    </div>
  )
}


interface CronSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

function initialSettings(service: ServiceConfig | null) {
  // RUM settings
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

/**
 * The form seeds from `service` when it (re)opens: the body remounts per service and per open,
 * so edits and the SSE preview never leak between sessions.
 */
export function CronSettingsModal(props: CronSettingsModalProps) {
  const { service, open } = props
  const generation = useOpenGeneration(open)
  return <CronSettingsModalBody key={`${service?.service_id ?? ''}|${generation}`} {...props} />
}

function CronSettingsModalBody({ service, open, onOpenChange }: CronSettingsModalProps) {
  const queryClient = useQueryClient()
  const { lines, status, error, start, stop } = useSSE()
  const init = initialSettings(service)

  const [syncEnabled, setSyncEnabled] = useState(init.syncEnabled)
  const [deleteAfter, setDeleteAfter] = useState(init.deleteAfter)
  const [commitInterval, setCommitInterval] = useState(init.commitInterval)
  const [syncLogEnabled, setSyncLogEnabled] = useState(init.syncLogEnabled)
  const [syncRetention, setSyncRetention] = useState(init.syncRetention)

  const [dataRetention, setDataRetention] = useState(init.dataRetention)
  const [rumRetention, setRumRetention] = useState(init.rumRetention)
  const [cacheRetention, setCacheRetention] = useState(init.cacheRetention)
  const [rollupRetention, setRollupRetention] = useState(init.rollupRetention)

  const [compactEnabled, setCompactEnabled] = useState(init.compactEnabled)
  const [compactLogEnabled, setCompactLogEnabled] = useState(init.compactLogEnabled)
  const [compactRetention, setCompactRetention] = useState(init.compactRetention)

  const [ngwafInterval, setNgwafInterval] = useState(init.ngwafInterval)
  const [ngwafLogEnabled, setNgwafLogEnabled] = useState(init.ngwafLogEnabled)
  const [ngwafRetention, setNgwafRetention] = useState(init.ngwafRetention)

  const [syncIntervalMins, setSyncIntervalMins] = useState(init.syncIntervalMins)
  const [pollingMode, setPollingMode] = useState<'regular' | 'adaptive'>(init.pollingMode)
  const [rumSyncEnabled, setRumSyncEnabled] = useState(init.rumSyncEnabled)
  const [rumLogPeriod, setRumLogPeriod] = useState(init.rumLogPeriod)
  const [rumDeleteAfter, setRumDeleteAfter] = useState(init.rumDeleteAfter)

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
        rollup_retention_months: parseInt(rollupRetention)
        },
      })
      return
    }
    const commitMins = Math.max(intervalMins, parseInt(commitInterval))
    const body = {
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
        rollup_retention_months: parseInt(rollupRetention)
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
    }
    start(`/api/services/${service.service_id}/cron-settings`, body)
  }

  if (!service) return null

  const isPending = status === 'streaming'
  const isExecuting = status === 'done' || status === 'error' || status === 'streaming'

  const syncIntervalNum = parseInt(syncIntervalMins)

  // Admins derive sync frequency from log_period. Analysts use the select box.

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
      onComplete={() => {
        queryClient.invalidateQueries({ queryKey: queryKeys.services() })
      }}
      reloadOnDone={false}
      status={status}
      onStop={stop}
      isExecuting={isExecuting}
      maxWidthClassName="sm:max-w-4xl"
      titleClassName="text-foreground"
      title={
        <>
          <Clock className="h-5 w-5 text-primary" />
          Cron Settings
        </>
      }
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
          setSyncEnabled={setSyncEnabled}
          syncIntervalMins={syncIntervalMins}
          setSyncIntervalMins={setSyncIntervalMins}
          cacheRetention={cacheRetention}
          setCacheRetention={setCacheRetention}
        />
      ) : (
        <div className="space-y-8 p-6 text-sm">
          <LogSyncSection
            syncEnabled={syncEnabled}
            setSyncEnabled={setSyncEnabled}
            pollingMode={pollingMode}
            setPollingMode={setPollingMode}
            deleteAfter={deleteAfter}
            setDeleteAfter={setDeleteAfter}
            dataRetention={dataRetention}
            setDataRetention={setDataRetention}
            cacheRetention={cacheRetention}
            setCacheRetention={setCacheRetention}
            rollupRetention={rollupRetention}
            setRollupRetention={setRollupRetention}
            commitInterval={commitInterval}
            setCommitInterval={setCommitInterval}
            syncLogEnabled={syncLogEnabled}
            setSyncLogEnabled={setSyncLogEnabled}
            syncRetention={syncRetention}
            setSyncRetention={setSyncRetention}
            syncFreqLabel={syncFreqLabel}
            isAnalyst={isAnalyst}
            syncIntervalNum={syncIntervalNum}
            adminSyncSeconds={adminSyncSeconds}
            rumRetention={rumRetention}
            setRumRetention={setRumRetention}
            rumEnabled={service.rum_enabled ?? false}
            rumSyncEnabled={rumSyncEnabled}
            setRumSyncEnabled={setRumSyncEnabled}
            rumLogPeriod={rumLogPeriod}
            setRumLogPeriod={setRumLogPeriod}
            rumDeleteAfter={rumDeleteAfter}
            setRumDeleteAfter={setRumDeleteAfter}
            serviceId={service.service_id}
          />

          {service.ngwaf_workspace_id && (
            <NgwafSection
              ngwafInterval={ngwafInterval}
              setNgwafInterval={setNgwafInterval}
              ngwafLogEnabled={ngwafLogEnabled}
              setNgwafLogEnabled={setNgwafLogEnabled}
              ngwafRetention={ngwafRetention}
              setNgwafRetention={setNgwafRetention}
            />
          )}

          <IcebergOptimizationSection
            compactEnabled={compactEnabled}
            setCompactEnabled={setCompactEnabled}
            compactLogEnabled={compactLogEnabled}
            setCompactLogEnabled={setCompactLogEnabled}
            compactRetention={compactRetention}
            setCompactRetention={setCompactRetention}
          />
        </div>
      )}
    </SSEActionDialogShell>
  )
}
