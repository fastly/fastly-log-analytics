'use client'

import React from 'react'
import { Switch } from '@/components/ui/switch'
import { Row, NumInput, ReadOnlyValue } from './parts'
import { fmtN } from './calc'
import type { CalcState, CalcResults } from './calc'

interface InputsProps {
  s: CalcState
  r: CalcResults
  set: (key: keyof CalcState) => (value: number | boolean) => void
}

export function Inputs({ s, r, set }: InputsProps) {
  return (
    <section>
      <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3'>Your Traffic &amp; Config</h3>
      <div className='space-y-0'>
        <Row label='Total requests per day' tooltip="Total requests your Fastly service handles per day (including both Edge and Shield).">
          <NumInput value={s.reqDay} onChange={set('reqDay')} wide />
        </Row>
        <Row label='Log sample rate (%)' tooltip="Percentage of requests logged. 100 = all requests, 1 = 1% of requests.">
          <NumInput value={s.sampleRate} onChange={set('sampleRate')} min={1} max={100} />
        </Row>
        <Row label='Edge-only logging' tooltip="Only log requests handled by the Edge, omitting Shield requests. This uses the 'Edge requests per day' volume for calculation.">
          <Switch checked={s.edgeOnly} onCheckedChange={set('edgeOnly')} />
        </Row>
        {s.edgeOnly && (
          <Row label='Edge requests per day' tooltip="Total requests handled directly by the Edge (excluding Shield requests). If 'Edge only logging' is enabled, this lower volume is used to calculate costs.">
            <NumInput value={s.edgeReqDay} onChange={set('edgeReqDay')} wide />
          </Row>
        )}
        <Row label='Log file period (seconds)' tooltip="How often Fastly closes and uploads a log file. Controlled by the Fastly logging configuration.">
          <NumInput value={s.logPeriod} onChange={set('logPeriod')} min={1} />
        </Row>
        <Row label='Cloud Commit Interval (minutes)' tooltip="How often the local buffer is flushed to the shared Iceberg table in FOS, creating new snapshots (e.g., 5).">
          <NumInput value={s.commitMins} onChange={set('commitMins')} min={1} />
        </Row>
        <Row label='Average bytes per log line' tooltip="The average uncompressed bytes per request based on your selected fields. Used to calculate storage sizes.">
          <NumInput value={s.bytesPerLine} onChange={set('bytesPerLine')} min={1} />
        </Row>
        <Row label='Parquet target size (MB)' tooltip="Target file size for compacted Parquet files. Larger files optimize query performance.">
          <NumInput value={s.parquetMB} onChange={set('parquetMB')} min={1} />
        </Row>              <Row label='Log nodes / fan-out' tooltip="Estimated number of Fastly aggregators/cache nodes flushing per period. Higher traffic = more nodes.">
          <NumInput value={s.logNodes} onChange={set('logNodes')} min={1} max={72} />
        </Row>
        <Row label='Local parquet cache enabled' tooltip="Download and cache Parquet files locally to avoid paying Class B operations on every query.">
          <Switch checked={s.cacheEnabled} onCheckedChange={set('cacheEnabled')} />
        </Row>
        <Row label='Dashboard page loads per day' tooltip="Page loads across dashboard, charts, etc. Costs FOS reads if local cache is disabled.">
          <NumInput value={s.queriesDay} onChange={set('queriesDay')} />
        </Row>
        <Row label='Manual log checks per day' tooltip="Each click of 'Refresh' on the Ingestion tab performs 1 Class A list operation against FOS.">
          <NumInput value={s.logsChecksPerDay} onChange={set('logsChecksPerDay')} />
        </Row>
        <Row label='CDN fronting FOS reads' tooltip="Use a Fastly CDN service to cache reads from Fastly Object Storage, reducing Class B operations.">
          <Switch checked={s.cdnEnabled} onCheckedChange={set('cdnEnabled')} />
        </Row>
        <Row label='Data retention (days)' tooltip="How many days to keep data in Object Storage before deleting.">
          <NumInput value={s.retentionDays} onChange={set('retentionDays')} min={1} />
        </Row>
        <Row label='Auto-delete raw .gz logs after ingest' tooltip="Delete raw .gz log files immediately after ingesting them into Iceberg. They will still be billed for the FOS minimum 30 days, but doing this prevents redundant long-term storage since Iceberg writes its own optimized Parquet files.">
          <Switch checked={s.deleteLogs} onCheckedChange={set('deleteLogs')} />
        </Row>
        <Row label='Iceberg table optimization enabled' tooltip="Periodically rewrite and merge small Parquet files into larger ones. This happens automatically but you can model the cost impact here.">
          <Switch checked={s.icebergOptimizeEnabled} onCheckedChange={set('icebergOptimizeEnabled')} />
        </Row>
        <Row label='Active remote analysts' tooltip="How many other team members have this service open on their computer simultaneously. Each analyst syncs metadata every 2 minutes.">
          <NumInput value={s.activeAnalysts} onChange={set('activeAnalysts')} min={0} />
        </Row>
        <Row label='Analyst full syncs / month' tooltip="How many times per month an analyst completely resets their local cache and re-downloads the entire historical Iceberg table.">
          <NumInput value={s.analystFullSyncsPerMonth} onChange={set('analystFullSyncsPerMonth')} min={0} />
        </Row>
      </div>

      {/* Minimum billing box */}
      <div className='mt-4 p-3 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-md space-y-2'>
        <div className='text-xs font-semibold text-amber-700 dark:text-amber-400'>Minimum Charge per Object</div>
        <Row label='Min. days billed per object (even if deleted early)'>
          <ReadOnlyValue value={s.minDays} />
        </Row>
        <div className='text-[11px] text-amber-700/80 dark:text-amber-500/80'>
          Edit on the <a href='/admin' className='underline font-medium hover:text-amber-900 dark:hover:text-amber-300'>admin page</a>.
        </div>
        <div className='text-xs text-amber-600 dark:text-amber-500 space-y-0.5'>
          <div>Objects created/day: <strong>{fmtN(Math.round(r.objectsPerDay))}</strong></div>
          <div>Sustained billed footprint (30d): <strong>{fmtN(Math.round(r.objectsBilled))}</strong> objects</div>
        </div>
      </div>
    </section>
  )
}
