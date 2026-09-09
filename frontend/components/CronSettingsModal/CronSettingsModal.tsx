'use client'

import React, { useState, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
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
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderMuted,
} from '@/lib/panel-dialog'
import type { components } from '@/types/api.generated'
import { AnalystSchedulePanel, LogSyncSection } from './Schedule'
import { NgwafSection, IcebergOptimizationSection } from './Triggers'
import { Preview } from './Preview'

type ServiceConfig = components["schemas"]["ServiceConfig"]

interface CronSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function CronSettingsModal({ service, open, onOpenChange }: CronSettingsModalProps) {
  const queryClient = useQueryClient()
  const { lines, status, error, start, stop, reset } = useSSE()

  const [syncEnabled, setSyncEnabled] = useState(false)
  const [deleteAfter, setDeleteAfter] = useState(false)
  const [commitInterval, setCommitInterval] = useState('5')
  const [syncLogEnabled, setSyncLogEnabled] = useState(true)
  const [syncRetention, setSyncRetention] = useState('7')

  const [dataRetention, setDataRetention] = useState('30')
  const [rumRetention, setRumRetention] = useState('90')
  const [cacheRetention, setCacheRetention] = useState('90')
  const [rollupRetention, setRollupRetention] = useState('12')

  const [compactEnabled, setCompactEnabled] = useState(false)
  const [compactLogEnabled, setCompactLogEnabled] = useState(true)
  const [compactRetention, setCompactRetention] = useState('7')

  const [ngwafInterval, setNgwafInterval] = useState('5')
  const [ngwafLogEnabled, setNgwafLogEnabled] = useState(true)
  const [ngwafRetention, setNgwafRetention] = useState('7')

  const [syncIntervalMins, setSyncIntervalMins] = useState('2')
  const [rumSyncIntervalSeconds, setRumSyncIntervalSeconds] = useState('60')
  const [rumDeleteAfter, setRumDeleteAfter] = useState(false)

  const isAnalyst = service?.access_level === 'read_only'

  useEffect(() => {
    if (service && open) {
      setSyncIntervalMins(String(service.cron_sync?.interval_mins ?? 2))
      setSyncEnabled(service.cron_sync?.enabled ?? false)
      setDeleteAfter(service.cron_sync?.delete_after ?? false)
      setCommitInterval(String(service.cron_sync?.commit_interval_mins ?? 5))
      setSyncLogEnabled(service.cron_sync?.log_enabled !== false)
      setSyncRetention(String(service.cron_sync?.log_retention_days ?? 7))
      setDataRetention(String(service.cron_sync?.data_retention_days ?? 30))
      setRumRetention(String(service.cron_sync?.rum_retention_days ?? 30))
      setCacheRetention(String(service.cron_sync?.cache_retention_days ?? 90))
      setRollupRetention(String(service.cron_sync?.rollup_retention_months ?? 12))

      setCompactEnabled(service.cron_compact?.enabled ?? false)
      setCompactLogEnabled(service.cron_compact?.log_enabled !== false)
      setCompactRetention(String(service.cron_compact?.log_retention_days ?? 7))

      setNgwafInterval(String(service.cron_ngwaf?.interval_mins ?? 5))
      setNgwafLogEnabled(service.cron_ngwaf?.log_enabled !== false)
      setNgwafRetention(String(service.cron_ngwaf?.log_retention_days ?? 7))

      // RUM settings
      const syncIntervalsecs = service.cron_sync?.interval_mins ? service.cron_sync.interval_mins * 60 : 60
      setRumSyncIntervalSeconds(String(syncIntervalsecs))
      setRumDeleteAfter(service.cron_sync?.delete_after ?? false)
    }
    reset()
  }, [service, open, reset, isAnalyst])

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
          sync_interval_seconds: parseInt(rumSyncIntervalSeconds),
          delete_after: rumDeleteAfter,
        },
      } : {}),
    }
    start(`/api/services/${service.service_id}/cron-settings`, body)
  }

  const handleOpenChange = (newOpen: boolean) => {
    if (status === 'streaming') return
    if (!newOpen && status === 'done') {
      queryClient.invalidateQueries({ queryKey: ['services'] })
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
            <Preview lines={lines} status={status} error={error} />
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
                rumSyncIntervalSeconds={rumSyncIntervalSeconds}
                setRumSyncIntervalSeconds={setRumSyncIntervalSeconds}
                rumDeleteAfter={rumDeleteAfter}
                setRumDeleteAfter={setRumDeleteAfter}
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
            <>
              {status !== 'streaming' && (
                <Button variant="outline" onClick={() => onOpenChange(false)} className="h-10 px-6">Close</Button>
              )}
              {status === 'streaming' && (
                <Button variant="outline" onClick={stop} className="h-10 px-6">Stop</Button>
              )}
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
