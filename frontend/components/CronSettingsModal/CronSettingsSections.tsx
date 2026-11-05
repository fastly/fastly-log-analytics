'use client'

import React from 'react'
import Link from 'next/link'
import { Shield } from 'lucide-react'
import { buttonVariants } from '@/components/ui/button'
import { RetentionSelect } from '@/components/ui/confirm-dialog'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'

export const SYNC_INTERVAL_OPTIONS = [
  { value: '1', label: 'Every 1 minute' },
  { value: '5', label: 'Every 5 minutes' },
  { value: '15', label: 'Every 15 minutes' },
  { value: '30', label: 'Every 30 minutes' },
  { value: '60', label: 'Every 1 hour' },
  { value: '360', label: 'Every 6 hours' },
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

interface LogSyncSectionProps {
  syncEnabled: boolean
  setSyncEnabled: (v: boolean) => void
  pollingMode: 'regular' | 'adaptive'
  setPollingMode: (v: 'regular' | 'adaptive') => void
  deleteAfter: boolean
  setDeleteAfter: (v: boolean) => void
  dataRetention: string
  setDataRetention: (v: string) => void
  syncLogEnabled: boolean
  setSyncLogEnabled: (v: boolean) => void
  syncRetention: string
  setSyncRetention: (v: string) => void
  syncFreqLabel: string
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
  dataRetention, setDataRetention, syncLogEnabled, setSyncLogEnabled, syncRetention, setSyncRetention,
  syncFreqLabel, rumRetention, setRumRetention,
  rumEnabled, rumSyncEnabled, setRumSyncEnabled, rumLogPeriod, setRumLogPeriod, rumDeleteAfter, setRumDeleteAfter, serviceId,
}: LogSyncSectionProps) {
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
            Polls FOS for new log files {syncFreqLabel} and ingests them into ClickHouse.
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
            <Label className="text-[11px] font-semibold cursor-pointer" htmlFor="auto-delete">Auto-delete Raw .zst Logs</Label>
            <p className="text-[10px] text-muted-foreground leading-tight">Saves FOS storage by removing raw logs once they are archived to Parquet and ingested into ClickHouse.</p>
          </div>
          <Switch id="auto-delete" checked={deleteAfter} onCheckedChange={setDeleteAfter} />
        </div>

        <div className={`grid gap-4 pb-2 ${rumEnabled ? 'grid-cols-2' : 'grid-cols-1'}`}>
          <div className="grid gap-1.5">
            <Label htmlFor="data-retention" className="text-[11px] font-medium">
              {rumEnabled ? 'Request Log Retention' : 'Cloud Data Retention'}
            </Label>
            <p className="text-[10px] text-muted-foreground leading-tight h-6">
              Delete CDN request log data from ClickHouse and FOS Archive Parquet older than this.
            </p>
            <RetentionSelect id="data-retention" value={dataRetention} onValueChange={setDataRetention} />
          </div>

          {rumEnabled && (
            <div className="grid gap-1.5">
              <Label htmlFor="rum-retention" className="text-[11px] font-medium">RUM Data Retention</Label>
              <p className="text-[10px] text-muted-foreground leading-tight h-6">
                Delete RUM beacon telemetry from ClickHouse older than this.
              </p>
              <RetentionSelect id="rum-retention" value={rumRetention} onValueChange={setRumRetention} />
            </div>
          )}
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
                    Polls FOS for new RUM beacon logs and ingests them into ClickHouse.
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
                    <p className="text-[10px] text-muted-foreground leading-tight">Remove raw .zst files after ingestion.</p>
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
