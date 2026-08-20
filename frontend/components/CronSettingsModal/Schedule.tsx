'use client'

import React from 'react'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  RETENTION_OPTIONS,
  RETENTION_LABELS,
  COMMIT_INTERVAL_OPTIONS,
  SYNC_INTERVAL_OPTIONS,
} from './constants'

interface AnalystSchedulePanelProps {
  syncEnabled: boolean
  setSyncEnabled: (v: boolean) => void
  syncIntervalMins: string
  setSyncIntervalMins: (v: string) => void
  cacheRetention: string
  setCacheRetention: (v: string) => void
}

export function AnalystSchedulePanel({
  syncEnabled,
  setSyncEnabled,
  syncIntervalMins,
  setSyncIntervalMins,
  cacheRetention,
  setCacheRetention,
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
  )
}

interface LogSyncSectionProps {
  syncEnabled: boolean
  setSyncEnabled: (v: boolean) => void
  deleteAfter: boolean
  setDeleteAfter: (v: boolean) => void
  dataRetention: string
  setDataRetention: (v: string) => void
  cacheRetention: string
  setCacheRetention: (v: string) => void
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
  rumSyncIntervalSeconds: string
  setRumSyncIntervalSeconds: (v: string) => void
  rumDeleteAfter: boolean
  setRumDeleteAfter: (v: boolean) => void
}

export function LogSyncSection({
  syncEnabled,
  setSyncEnabled,
  deleteAfter,
  setDeleteAfter,
  dataRetention,
  setDataRetention,
  cacheRetention,
  setCacheRetention,
  commitInterval,
  setCommitInterval,
  syncLogEnabled,
  setSyncLogEnabled,
  syncRetention,
  setSyncRetention,
  syncFreqLabel,
  isAnalyst,
  syncIntervalNum,
  adminSyncSeconds,
  rumRetention,
  setRumRetention,
  rumEnabled,
  rumSyncIntervalSeconds,
  setRumSyncIntervalSeconds,
  rumDeleteAfter,
  setRumDeleteAfter,
}: LogSyncSectionProps) {
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

      <div className={`space-y-5 pl-4 border-l-2 transition-opacity ${syncEnabled ? 'opacity-100 border-primary' : 'opacity-40 border-muted pointer-events-none'}`}>
        <div className="flex items-center justify-between">
          <div className="space-y-0.5 pr-4">
            <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="auto-delete">Auto-delete Raw .gz Logs</Label>
            <p className="text-[10px] text-muted-foreground leading-tight">Saves FOS storage by removing raw logs once they are ingested into Iceberg.</p>
          </div>
          <Switch id="auto-delete" checked={deleteAfter} onCheckedChange={setDeleteAfter} />
        </div>

        <div className={`grid gap-4 pb-2 ${rumEnabled ? "grid-cols-3" : "grid-cols-2"}`}>
          <div className="grid gap-1.5">
            <Label htmlFor="data-retention" className="text-[11px] font-medium">
              {rumEnabled ? "Request Log Retention" : "Cloud Data Retention"}
            </Label>
            <p className="text-[10px] text-muted-foreground leading-tight h-6">
              Delete CDN request log data from Iceberg older than this.
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

          {rumEnabled && (
            <div className="grid gap-1.5">
              <Label htmlFor="rum-retention" className="text-[11px] font-medium">RUM Data Retention</Label>
              <p className="text-[10px] text-muted-foreground leading-tight h-6">
                Delete RUM beacon telemetry from Iceberg older than this.
              </p>
              <Select value={rumRetention} onValueChange={v => v && setRumRetention(v)}>
                <SelectTrigger id="rum-retention" className="h-7 text-[11px]">
                  <SelectValue>{(val) => RETENTION_LABELS[String(val)] || val}</SelectValue>
                </SelectTrigger>
                <SelectContent>
                  {RETENTION_OPTIONS.map(o => (
                    <SelectItem key={o.value} value={o.value} className="text-[11px]">{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          )}

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

        <div className="grid grid-cols-2 gap-4">
          <div className="grid gap-1.5">
            <div className="flex items-center gap-1.5">
              <Label htmlFor="commit-interval" className="text-[11px] font-semibold">Cloud Commit Interval</Label>
            </div>
            <p className="text-[10px] text-muted-foreground leading-tight">
              How often the local buffer is pushed to Iceberg.
              Cannot be shorter than sync frequency ({syncFreqLabel}).
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
        )}

        {rumEnabled && (
          <div className="space-y-4 pt-3 border-t border-border/30">
            <div className="text-[10px] font-semibold text-foreground/70 uppercase tracking-widest">RUM Beacon Sync</div>
            <div className="grid grid-cols-2 gap-4">
              <div className="grid gap-1.5">
                <Label htmlFor="rum-sync-interval" className="text-[11px] font-medium">RUM Sync Frequency</Label>
                <p className="text-[10px] text-muted-foreground leading-tight h-6">
                  How often to sync RUM beacon logs from FOS.
                </p>
                <Select value={rumSyncIntervalSeconds} onValueChange={v => v && setRumSyncIntervalSeconds(v)}>
                  <SelectTrigger id="rum-sync-interval" className="h-7 text-[11px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="30" className="text-[11px]">every 30 seconds</SelectItem>
                    <SelectItem value="60" className="text-[11px]">every 60 seconds</SelectItem>
                    <SelectItem value="120" className="text-[11px]">every 2 minutes</SelectItem>
                    <SelectItem value="300" className="text-[11px]">every 5 minutes</SelectItem>
                  </SelectContent>
                </Select>
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
        )}
      </div>
    </div>
  )
}
