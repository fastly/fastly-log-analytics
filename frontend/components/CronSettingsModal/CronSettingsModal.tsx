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
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Input } from '@/components/ui/input'
import { Loader2, Clock, Shield } from 'lucide-react'
import { useSSE } from '@/hooks/useSSE'
import { SSEProgressView } from '@/components/SSEModal'
import { cn } from '@/lib/utils'
import {
  panelDialogContent,
  panelDialogFooter,
  panelDialogHeaderMuted,
} from '@/lib/panel-dialog'
import type { components } from '@/types/api.generated'

type ServiceConfig = components["schemas"]["ServiceConfig"]

interface CronSettingsModalProps {
  service: ServiceConfig | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

const RETENTION_OPTIONS = [
  { value: '1', label: '1 day' },
  { value: '3', label: '3 days' },
  { value: '7', label: '7 days' },
  { value: '14', label: '14 days' },
  { value: '30', label: '30 days' },
  { value: '90', label: '90 days' },
  { value: '0', label: 'Forever' },
]

const RETENTION_LABELS: Record<string, string> = Object.fromEntries(
  RETENTION_OPTIONS.map(o => [o.value, o.label])
)

const COMMIT_INTERVAL_OPTIONS = [
  { value: '1',  label: 'Every 1 min  — most fresh, most snapshots' },
  { value: '2',  label: 'Every 2 min' },
  { value: '3',  label: 'Every 3 min' },
  { value: '5',  label: 'Every 5 min  — recommended' },
  { value: '15', label: 'Every 15 min' },
  { value: '30', label: 'Every 30 min' },
  { value: '60', label: 'Every 60 min — fewest snapshots' },
]

const SYNC_INTERVAL_OPTIONS = [
  { value: '1',  label: 'Every 1 minute' },
  { value: '2',  label: 'Every 2 minutes' },
  { value: '5',  label: 'Every 5 minutes' },
  { value: '10', label: 'Every 10 minutes' },
  { value: '15', label: 'Every 15 minutes' },
  { value: '30', label: 'Every 30 minutes' },
  { value: '60', label: 'Every 60 minutes' },
]

const NGWAF_INTERVAL_OPTIONS = [
  { value: '1',  label: 'Every 1 minute' },
  { value: '2',  label: 'Every 2 minutes' },
  { value: '5',  label: 'Every 5 minutes — recommended' },
  { value: '10', label: 'Every 10 minutes' },
  { value: '15', label: 'Every 15 minutes' },
  { value: '30', label: 'Every 30 minutes' },
  { value: '60', label: 'Every 60 minutes' },
]

export function CronSettingsModal({ service, open, onOpenChange }: CronSettingsModalProps) {
  const queryClient = useQueryClient()
  const { lines, status, error, start, stop, reset } = useSSE()

  const [syncEnabled, setSyncEnabled] = useState(false)
  const [deleteAfter, setDeleteAfter] = useState(false)
  const [commitInterval, setCommitInterval] = useState('5')
  const [syncLogEnabled, setSyncLogEnabled] = useState(true)
  const [syncRetention, setSyncRetention] = useState('7')

  const [dataRetention, setDataRetention] = useState('30')
  const [cacheRetention, setCacheRetention] = useState('90')

  const [compactEnabled, setCompactEnabled] = useState(false)
  const [compactLogEnabled, setCompactLogEnabled] = useState(true)
  const [compactRetention, setCompactRetention] = useState('7')

  const [ngwafInterval, setNgwafInterval] = useState('5')
  const [ngwafLogEnabled, setNgwafLogEnabled] = useState(true)
  const [ngwafRetention, setNgwafRetention] = useState('7')

  const [syncIntervalMins, setSyncIntervalMins] = useState('2')
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
      setCacheRetention(String(service.cron_sync?.cache_retention_days ?? 90))

      setCompactEnabled(service.cron_compact?.enabled ?? false)
      setCompactLogEnabled(service.cron_compact?.log_enabled !== false)
      setCompactRetention(String(service.cron_compact?.log_retention_days ?? 7))

      setNgwafInterval(String(service.cron_ngwaf?.interval_mins ?? 5))
      setNgwafLogEnabled(service.cron_ngwaf?.log_enabled !== false)
      setNgwafRetention(String(service.cron_ngwaf?.log_retention_days ?? 7))
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
          cache_retention_days: parseInt(cacheRetention)
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
        cache_retention_days: parseInt(cacheRetention)
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
      <DialogContent className={cn("sm:max-w-xl", panelDialogContent)} showCloseButton={status !== 'streaming'}>
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
            <div className="p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
               <div className="text-center space-y-2">
                  <h3 className="text-lg font-semibold tracking-tight">Updating Cron Settings</h3>
                  <p className="text-sm text-muted-foreground">Applying new background sync configuration...</p>
               </div>
               <SSEProgressView
                 lines={lines}
                 status={status}
                 error={error}
                 className="h-[300px]"
                 progressLabel="Step"
                 doneMessage="Settings applied! You may now close this window."
               />
            </div>
          ) : isAnalyst ? (
            <div className="p-6 space-y-6 text-sm divide-y">
              <div className="space-y-4">
                <div>
                  <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80">Cloud Sync Interval</h3>
                  <p className="text-[10px] text-muted-foreground mt-1">How often to pull new log files from the cloud bucket to your local cache.</p>
                </div>
                <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
                  <div className="space-y-0.5 pr-4">
                    <Label className="text-xs font-semibold cursor-pointer" htmlFor="enable-sync-analyst">Auto-Sync New Data</Label>
                    <p className="text-[10px] text-muted-foreground">
                      Automatically poll for and download new processed log files.
                    </p>
                  </div>
                  <Switch id="enable-sync-analyst" checked={syncEnabled} onCheckedChange={setSyncEnabled} />
                </div>
                {syncEnabled && (
                  <Select value={syncIntervalMins} onValueChange={v => v && setSyncIntervalMins(v)}>
                    <SelectTrigger className="h-9 text-sm">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {SYNC_INTERVAL_OPTIONS.map(o => (
                        <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                )}
                <div className="grid gap-1.5 pt-2 border-t border-border/50">
                  <Label htmlFor="cache-retention-analyst" className="text-xs font-semibold">Local Cache Retention</Label>
                  <p className="text-[10px] text-muted-foreground leading-tight">
                    Automatically deletes local data files older than this to save disk space.
                  </p>
                  <Select value={cacheRetention} onValueChange={v => v && setCacheRetention(v)}>
                    <SelectTrigger id="cache-retention-analyst" className="h-9 text-sm">
                      <SelectValue>{(val) => RETENTION_LABELS[String(val)] || val}</SelectValue>
                    </SelectTrigger>
                    <SelectContent>
                      {RETENTION_OPTIONS.map(o => (
                        <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-8 p-6 text-sm">
              {/* ── Log Sync ── */}
              <div className="space-y-3">
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

                <div className={`space-y-4 pl-4 border-l-2 transition-opacity ${syncEnabled ? 'opacity-100 border-primary' : 'opacity-40 border-muted pointer-events-none'}`}>
                  <div className="flex items-center justify-between">
                    <div className="space-y-0.5 pr-4">
                      <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="auto-delete">Auto-delete Raw .gz Logs</Label>
                      <p className="text-[10px] text-muted-foreground leading-tight">Saves FOS storage by removing raw logs once they are ingested into Iceberg.</p>
                    </div>
                    <Switch id="auto-delete" checked={deleteAfter} onCheckedChange={setDeleteAfter} />
                  </div>

                  <div className="grid grid-cols-2 gap-4 pt-2 pb-2">
                    <div className="grid gap-1.5">
                      <Label htmlFor="data-retention" className="text-[11px] font-medium">Cloud Data Retention</Label>
                      <p className="text-[10px] text-muted-foreground leading-tight h-6">
                        Delete log data from Iceberg table older than this.
                      </p>
                      <Select value={dataRetention} onValueChange={v => v && setDataRetention(v)}>
                        <SelectTrigger id="data-retention" className="h-7 text-[11px]">
                          <SelectValue>{(val) => RETENTION_LABELS[String(val)] || val}</SelectValue>
                        </SelectTrigger>
                        <SelectContent>
                          {RETENTION_OPTIONS.map(o => (
                            <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="grid gap-1.5">
                      <Label htmlFor="cache-retention" className="text-[11px] font-medium">Local Cache Retention</Label>
                      <p className="text-[10px] text-muted-foreground leading-tight h-6">
                        Delete local cache files older than this to save disk space.
                      </p>
                      <Select value={cacheRetention} onValueChange={v => v && setCacheRetention(v)}>
                        <SelectTrigger id="cache-retention" className="h-7 text-[11px]">
                          <SelectValue>{(val) => RETENTION_LABELS[String(val)] || val}</SelectValue>
                        </SelectTrigger>
                        <SelectContent>
                          {RETENTION_OPTIONS.map(o => (
                            <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  {/* Cloud commit interval */}
                  <div className="grid gap-1.5">
                    <div className="flex items-center gap-1.5">
                      <Label htmlFor="commit-interval" className="text-[11px] font-semibold">Cloud Commit Interval</Label>
                    </div>
                    <p className="text-[10px] text-muted-foreground leading-tight">
                      How often the local buffer is pushed to the shared Iceberg table in FOS.
                      More frequent = fresher data for all users, more small files before daily optimization.
                      Cannot be shorter than the sync frequency ({syncFreqLabel}).
                    </p>
                    <Select
                      value={commitInterval}
                      onValueChange={v => {
                        const minCommitMins = isAnalyst ? syncIntervalNum : Math.max(1, Math.ceil(adminSyncSeconds / 60))
                        if (v && parseInt(v) >= minCommitMins) setCommitInterval(v)
                      }}
                    >
                      <SelectTrigger id="commit-interval" className="h-7 text-[11px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {COMMIT_INTERVAL_OPTIONS.filter(o => {
                          const minCommitMins = isAnalyst ? syncIntervalNum : Math.max(1, Math.ceil(adminSyncSeconds / 60))
                          return parseInt(o.value) >= minCommitMins
                        }).map(o => (
                          <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  <div className="flex items-center justify-between">
                    <div className="space-y-0.5 pr-4">
                      <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="sync-log-enabled">Log runs to database</Label>
                      <p className="text-[10px] text-muted-foreground leading-tight">Keep historical records of execution statuses.</p>
                    </div>
                    <Switch id="sync-log-enabled" checked={syncLogEnabled} onCheckedChange={setSyncLogEnabled} />
                  </div>

                  <div className="grid gap-1.5 max-w-[200px]">
                    <Label htmlFor="sync-retention" className="text-[11px] font-medium">Keep cron logs for</Label>
                    <Select value={syncRetention} onValueChange={v => v && setSyncRetention(v)}>
                      <SelectTrigger id="sync-retention" className="h-7 text-[11px]">
                        <SelectValue>{(val) => RETENTION_LABELS[String(val)] || val}</SelectValue>
                      </SelectTrigger>
                      <SelectContent>
                        {RETENTION_OPTIONS.map(o => (
                          <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
              </div>

              {/* ── NGWAF Bot Sync ── */}
              {service.ngwaf_workspace_id && (
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
                      <Select value={ngwafRetention} onValueChange={v => v && setNgwafRetention(v)}>
                        <SelectTrigger id="ngwaf-retention" className="h-7 text-[11px]">
                          <SelectValue>{(val) => RETENTION_LABELS[String(val)] || val}</SelectValue>
                        </SelectTrigger>
                        <SelectContent>
                          {RETENTION_OPTIONS.map(o => (
                            <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </div>
              )}

              {/* ── Iceberg Optimization ── */}
              <div className="space-y-3">
                <div>
                  <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80">Iceberg Optimization</h3>
                  <p className="text-[10px] text-muted-foreground">Daily table maintenance to keep query performance fast.</p>
                </div>

                <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
                  <div className="space-y-0.5 pr-4">
                    <Label className="text-xs font-semibold cursor-pointer" htmlFor="enable-compact">Enable Daily Optimization</Label>
                    <p className="text-[10px] text-muted-foreground leading-tight">
                      Rewrites many small Iceberg snapshot files into larger, optimized Parquet files at 03:00 UTC.
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
                    <Select value={compactRetention} onValueChange={v => v && setCompactRetention(v)}>
                      <SelectTrigger id="compact-retention" className="h-7 text-[11px]">
                        <SelectValue>{(val) => RETENTION_LABELS[String(val)] || val}</SelectValue>
                      </SelectTrigger>
                      <SelectContent>
                        {RETENTION_OPTIONS.map(o => (
                          <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
              </div>
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
