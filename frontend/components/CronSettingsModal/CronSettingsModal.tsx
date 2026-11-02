'use client'

import React from 'react'
import { useForm, useWatch, type PathValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useQueryClient } from '@tanstack/react-query'
import { Clock, Loader2 } from 'lucide-react'
import { SSEActionDialogShell, SSEExecutionSection } from '@/components/SSEModal'
import { Button } from '@/components/ui/button'
import { useOpenGeneration, useSSE } from '@/hooks/useSSE'
import { queryKeys } from "@/lib/api"
import type { ServiceConfig } from '@/types/api'
import {
  SYNC_INTERVAL_OPTIONS,
  COMMIT_INTERVAL_OPTIONS,
  NGWAF_INTERVAL_OPTIONS,
  LogSyncSection,
  Schedule,
  NgwafSection,
  StorageOptimizationSection,
} from './CronSettingsSections'

export {
  SYNC_INTERVAL_OPTIONS,
  COMMIT_INTERVAL_OPTIONS,
  NGWAF_INTERVAL_OPTIONS,
  LogSyncSection,
  Schedule,
  NgwafSection,
  StorageOptimizationSection,
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

  const handleSave = () => {
    if (!service) return
    const intervalMins = parseInt(syncIntervalMins)
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
  const adminSyncSeconds = Math.max(10, Math.floor((service?.log_period || 60) / 2))
  const syncFreqLabel = adminSyncSeconds >= 60
    ? `every ${Math.floor(adminSyncSeconds / 60)}m${adminSyncSeconds % 60 > 0 ? ` ${adminSyncSeconds % 60}s` : ''}`
    : `every ${adminSyncSeconds} seconds`

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

          <StorageOptimizationSection
            compactEnabled={compactEnabled}
            setCompactEnabled={bind('compactEnabled')}
            compactLogEnabled={compactLogEnabled}
            setCompactLogEnabled={bind('compactLogEnabled')}
            compactRetention={compactRetention}
            setCompactRetention={bind('compactRetention')}
          />
        </div>
    </SSEActionDialogShell>
  )
}
