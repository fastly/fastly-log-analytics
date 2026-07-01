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
import { Shield } from 'lucide-react'
import {
  RETENTION_OPTIONS,
  RETENTION_LABELS,
  NGWAF_INTERVAL_OPTIONS,
} from './constants'

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
  )
}
