'use client'

import React from 'react'
import type { components } from '@/types/api.generated'
import {
  INPROCESS_SCHEDULE_COLUMNS,
  INPROCESS_ACTIVE_COLUMNS,
  InProcessSchedulesSection,
  type InProcessScheduleEntry,
  type InProcessActiveRun,
} from './QueueWorkerAndSchedulePanels'

export type LedgerEntry = components['schemas']['IngestLedgerEntry']
export type LedgerStatusFilter =
  | 'problem'
  | 'all'
  | 'dead_letter'
  | 'quarantined'
  | 'discovered'
  | 'claimed'
  | 'committed'

export const VALID_LEDGER_STATUSES = [
  'discovered',
  'claimed',
  'committed',
  'quarantined',
  'dead_letter',
] as const

export {
  INPROCESS_SCHEDULE_COLUMNS,
  INPROCESS_ACTIVE_COLUMNS,
  InProcessSchedulesSection,
  type InProcessScheduleEntry,
  type InProcessActiveRun,
}

export function SummaryStatTile({
  label,
  value,
  sublabel,
  valueClassName = '',
}: {
  label: string
  value: React.ReactNode
  sublabel: React.ReactNode
  valueClassName?: string
}) {
  return (
    <div className="rounded-lg border border-border/60 bg-muted/20 p-2.5">
      <div className="text-xs text-muted-foreground uppercase">{label}</div>
      <div className={`text-2xl font-semibold tabular-nums mt-0.5 ${valueClassName}`}>
        {value}
      </div>
      <div className="text-[11px] text-muted-foreground mt-0.5 truncate">{sublabel}</div>
    </div>
  )
}
