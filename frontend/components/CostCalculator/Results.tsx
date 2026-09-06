'use client'

import React from 'react'
import { formatBytes } from '@/lib/format'
import { ResultRow } from './parts'
import { fmtN, fmtUSD } from './calc'
import type { CalcState, CalcResults } from './calc'

interface ResultsProps {
  s: CalcState
  r: CalcResults
}

export function Results({ s, r }: ResultsProps) {
  return (
    <div className='bg-muted/30 border rounded-lg p-5'>
      <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-4'>Monthly Estimate Breakdown</h3>
      <div className='space-y-0'>
        <ResultRow
          label='Class A Operations (writes, lists)'
          detail={`${fmtN(Math.round(r.classAPerMonth))} ops @ $${s.rateA}/1k`}
          cost={fmtUSD(r.costA)}
        />
        <ResultRow
          label='Class B Operations (reads)'
          detail={`${fmtN(Math.round(r.classBPerMonth))} ops @ $${s.rateB}/1k`}
          cost={fmtUSD(r.costB)}
        />
        <ResultRow
          label='Storage (GB-months billed)'
          detail={r.storageTiers.map(t => `${t.label}: ${Math.max(0.001, t.gbMonths).toFixed(3)} GB-mo${t.flagged ? '*' : ''}`).join(' · ')}
          cost={fmtUSD(r.costStorage)}
        />
        <ResultRow
          label='CDN Egress'
          detail={`${r.cdnEgressGB.toFixed(3)} GB`}
          cost={fmtUSD(r.costEgress)}
        />
        {s.rumBeaconsDay > 0 && (
          <ResultRow
            label='Real User Monitoring (RUM) Cost Portion'
            detail={`${fmtN(Math.round(r.rumBeaconsMonth))} beacons · ${Math.max(0.001, r.rumParquetGBMonths).toFixed(3)} GB RUM storage (included in Class A & Storage above)`}
            cost={fmtUSD(r.costRum)}
          />
        )}
        <ResultRow
          label='Total Estimated Monthly Cost'
          cost={fmtUSD(r.totalCost)}
          highlight
        />
      </div>

      {/* Volume estimates */}
      <div className='mt-5 pt-4 border-t border-border/40'>
        <h4 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3'>Volume Estimates</h4>
        <div className='grid grid-cols-2 gap-x-6 gap-y-1'>
          {[
            ['Est. log line size (uncompressed)', s.bytesPerLine + ' B'],
            ['Logged requests / month', fmtN(Math.round(r.reqDayEffective * 30))],
            ['Raw volume / month (uncompressed)', formatBytes(r.totalBytesPerMonth)],
            ['Est. volume / month (.gz compressed)', formatBytes(r.totalGzBytesPerMonth)],
            ['Log files written / month', fmtN(Math.round(r.logFilesPerMonth))],
            ['Iceberg data files created / month', fmtN(Math.round(r.parquetFilesPerMonth))],
            ['Billed footprint (30d min)', fmtN(Math.round(r.objectsBilled)) + ' objects'],
            ['Syncs / month', fmtN(Math.round(r.syncsPerMonth))],
            ...(s.rumBeaconsDay > 0 ? [
              ['RUM beacons / month', fmtN(Math.round(r.rumBeaconsMonth))],
              ['RUM footprint / month', Math.max(0.001, r.rumParquetGBMonths).toFixed(3) + ' GB-mo']
            ] : [])
          ].map(([label, value]) => (
            <div key={label} className='flex justify-between text-xs py-0.5'>
              <span className='text-muted-foreground'>{label}</span>
              <span className='font-medium tabular-nums'>{value}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
