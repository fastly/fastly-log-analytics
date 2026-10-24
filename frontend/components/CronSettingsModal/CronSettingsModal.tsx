'use client'

import React, { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { queryKeys } from '@/lib/query-keys'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Loader2, Clock } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEDialogFooter, SSEExecutionSection } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderMuted,
} from '@/lib/panel-dialog'
import { AnalystSchedulePanel, LogSyncSection } from './Schedule'
import { NgwafSection, IcebergOptimizationSection } from './Triggers'
import type { ServiceConfig } from '@/types/api'


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
  const [generation, setGeneration] = useState(0)
  const [wasOpen, setWasOpen] = useState(open)
  if (open !== wasOpen) {
    setWasOpen(open)
    if (open) setGeneration((g) => g + 1)
  }
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

  const handleOpenChange = (newOpen: boolean) => {
    if (status === 'streaming') return
    if (!newOpen && status === 'done') {
      queryClient.invalidateQueries({ queryKey: queryKeys.services() })
    }
    onOpenChange(newOpen)
  }

  if (!service) return null

  const isPending = status === 'streaming'
  const isSuccess = status === 'done' || status === 'error' || status === 'streaming'

  const syncIntervalNum = parseInt(syncIntervalMins)

  // Admins derive sync frequency from log_period. Analysts use the select box.

  const adminSyncSeconds = Math.max(10, Math.floor((service?.log_period || 60) / 2))
  const syncFreqLabel = isAnalyst
    ? (syncIntervalNum === 1 ? 'every 1 minute' : `every ${syncIntervalNum} minutes`)
    : (adminSyncSeconds >= 60
        ? `every ${Math.floor(adminSyncSeconds / 60)}m${adminSyncSeconds % 60 > 0 ? ` ${adminSyncSeconds % 60}s` : ''}`
        : `every ${adminSyncSeconds} seconds`)

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className={cn("sm:max-w-4xl", panelDialogContent)} showCloseButton={status !== 'streaming'}>
        <DialogHeader className={panelDialogHeaderMuted}>
          <div className="flex items-center justify-between">
            <DialogTitle className="flex items-center gap-2">
              <Clock className="h-5 w-5 text-primary" />
              Cron Settings
            </DialogTitle>
          </div>
          <div className="text-sm text-muted-foreground mt-1">
            Service: <span className="font-medium text-foreground">{service.name}</span>
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto min-h-0">
          {isSuccess ? (
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
          ) : isAnalyst ? (
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
        </div>

        <DialogFooter className={panelDialogFooter}>
          {!isSuccess ? (
            <>
              <Button variant="outline" onClick={() => onOpenChange(false)} className="h-10 px-6">Cancel</Button>
              <Button onClick={handleSave} disabled={isPending} className="h-10 px-6 font-bold">
                {isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Save Changes
              </Button>
            </>
          ) : (
            <SSEDialogFooter
              status={status}
              onClose={() => onOpenChange(false)}
              onStop={stop}
              idleOrErrorLabel="Close"
            />
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
