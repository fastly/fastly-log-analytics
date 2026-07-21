'use client'

import React from 'react'
import { useRouter } from 'next/navigation'
import { Button } from '@/components/ui/button'
import { useServiceStore } from '@/stores/serviceStore'
import { buildServiceHref } from '@/lib/navigation'
import { Row, ReadOnlyValue } from './parts'
import type { CalcState } from './calc'

interface PricingProps {
  s: CalcState
}

export function Pricing({ s }: PricingProps) {
  const router = useRouter()
  const activeServiceId = useServiceStore((st) => st.activeServiceId)
  return (
    <>
      <section>
        <div className="flex items-center justify-between mb-3">
          <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider'>Pricing (per 1,000 ops)</h3>
          <Button
            variant="link"
            size="sm"
            className="h-7 p-0 text-[10px] uppercase font-bold text-primary"
            onClick={() => router.push(buildServiceHref('/admin', activeServiceId))}
          >
            Edit in Admin
          </Button>
        </div>
        <div className='space-y-0'>
          <Row label='Class A rate (writes, lists)'>
            <ReadOnlyValue value={s.rateA} />
          </Row>
          <Row label='Class B rate (reads/downloads)'>
            <ReadOnlyValue value={s.rateB} />
          </Row>
          <Row label='Storage rate (per GB/month)'>
            <ReadOnlyValue value={s.rateStorage} />
          </Row>
          <Row label='CDN egress rate (per GB)'>
            <ReadOnlyValue value={s.rateEgress} />
          </Row>
        </div>
        <div className='mt-2 text-[11px] text-muted-foreground'>
          Rates are global defaults. Update them on the <a href={buildServiceHref('/admin', activeServiceId)} className='underline font-medium hover:text-foreground'>admin page</a>.
        </div>
      </section>

      {/* What generates ops reference table */}
      <section>
        <h3 className='text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3'>What generates operations</h3>
        {/* M-6 (audit, mobile UX): wrap the ops-reference table so its
            two-column labels (some long, e.g. "List during sync (per
            1,000 objects)") scroll horizontally instead of pushing the
            entire cost-calculator card off-screen on phones. */}
        <div className='overflow-x-auto'>
        <table className='w-full text-xs text-muted-foreground'>
          <tbody>
            <tr><td colSpan={2} className='py-1 font-semibold text-blue-600 dark:text-blue-400'>Class A (writes &amp; lists)</td></tr>
            {[
              ['Fastly writes each log file', '1 op/file'],
              ['Raw Parquet upload per sync', '1 op/file'],
              ['List during sync (per 1,000 objects)', '1 op/page'],
              ['Manual log checks (LIST)', '1 op/check'],
              ['Admin sync state to FOS', '1 op/sync'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5'>{l}</td><td className='text-right'>{r}</td></tr>
            ))}
            <tr><td colSpan={2} className='py-1 font-semibold text-blue-500 dark:text-blue-300 italic'>Iceberg Maintenance (if enabled)</td></tr>
            {[
              ['Iceberg commit (append data)', '1 op/sync'],
              ['Table optimization (rewrite)', '1 op/file'],
              ['Weekly snapshot expiry', '1 op/week'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5 text-muted-foreground/70'>{l}</td><td className='text-right text-muted-foreground/70'>{r}</td></tr>
            ))}
            <tr><td colSpan={2} className='py-1 mt-2 font-semibold text-amber-600 dark:text-amber-400'>Class B (reads)</td></tr>
            {[
              ['Read each .gz during ingest', '1 op/file'],
              ['Analyst metadata pull (cached)', '1 op/min'],
              ['Query Parquet (no local cache)', '1 op/file/query'],
              ['CDN-cached reads', '0 ops'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5'>{l}</td><td className='text-right'>{r}</td></tr>
            ))}
            <tr><td colSpan={2} className='py-1 mt-2 font-semibold text-orange-600 dark:text-orange-400'>Egress (Transfer Out)</td></tr>
            {[
              ['Dashboard queries (uncached)', 'MBs'],
              ['Analyst metadata sync (cached)', '~10KB/min'],
            ].map(([l, r]) => (
              <tr key={l}><td className='pl-3 py-0.5'>{l}</td><td className='text-right'>{r}</td></tr>
            ))}
          </tbody>
        </table>
        </div>
      </section>
    </>
  )
}
