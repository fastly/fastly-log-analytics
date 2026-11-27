'use client'

import React from 'react'
import { buildServiceHref, cn } from '@/lib/utils'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  BILLING_ZONE_DESCRIPTIONS,
  BILLING_ZONE_RATES,
  type BillingZone,
  fmtN,
} from './costCalculatorModel'

export function BillingZoneSelector({
  selectedZone,
  egressRate,
  onSelectZone,
  autoDerivedLabel,
}: {
  selectedZone: BillingZone
  egressRate: number
  onSelectZone: (zone: BillingZone) => void
  autoDerivedLabel?: string | null
}) {
  return (
    <div className="mb-4 space-y-2 p-3 bg-muted/40 rounded-lg border border-border/50">
      <div className="flex items-center justify-between">
        <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
          Delivery Billing Zone
          {autoDerivedLabel ? (
            <span className="ml-1.5 font-normal normal-case text-muted-foreground/80">
              {autoDerivedLabel}
            </span>
          ) : null}
        </span>
        <span className="text-xs font-semibold text-primary">
          ${egressRate.toFixed(2)} / GB
        </span>
      </div>
      <div className="grid grid-cols-3 gap-1.5 p-1 bg-muted/80 rounded-md">
        {(['MBZ100', 'MBZ200', 'Global'] as const).map((zone) => {
          const isSelected = selectedZone === zone
          return (
            <TooltipProvider key={zone}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <button
                    type="button"
                    onClick={() => onSelectZone(zone)}
                    className={cn(
                      'px-2.5 py-1.5 text-xs font-medium rounded transition-all text-center',
                      isSelected
                        ? 'bg-background text-foreground shadow-sm font-semibold'
                        : 'text-muted-foreground hover:text-foreground hover:bg-background/50',
                    )}
                    aria-pressed={isSelected}
                  >
                    {zone}
                  </button>
                </TooltipTrigger>
                <TooltipContent side="top" className="max-w-[260px] text-xs">
                  <p className="font-semibold mb-1">{zone} (${BILLING_ZONE_RATES[zone].toFixed(2)}/GB)</p>
                  <p>{BILLING_ZONE_DESCRIPTIONS[zone]}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          )
        })}
      </div>
      <p className="text-[11px] text-muted-foreground leading-normal">
        {BILLING_ZONE_DESCRIPTIONS[selectedZone]}
      </p>
    </div>
  )
}

export function MinimumChargeNotice({
  minDays,
  objectsPerDay,
  objectsBilled,
  activeServiceId,
  billedDaysLabel = 30,
}: {
  minDays: number
  objectsPerDay: number
  objectsBilled: number
  activeServiceId: string | null
  billedDaysLabel?: number
}) {
  return (
    <div className="mt-4 p-3 bg-amber-50 dark:bg-amber-950/30 border border-amber-200 dark:border-amber-800 rounded-md space-y-2">
      <div className="text-xs font-semibold text-amber-700 dark:text-amber-400">Minimum Charge per Object</div>
      <div className="flex items-center justify-between py-1.5 border-b border-border/40 last:border-0 gap-4">
        <LabelWithInfo
          label="Min. days billed per object (even if deleted early)"
          className="gap-1.5 text-sm text-muted-foreground flex-1 leading-tight"
          labelClassName="text-sm font-normal leading-tight"
        />
        <div className="flex-shrink-0">
          <div className="text-right h-7 text-sm flex items-center justify-end px-3 rounded-md bg-muted/40 border border-transparent font-mono tabular-nums text-muted-foreground w-24">
            {minDays}
          </div>
        </div>
      </div>
      <div className="text-[11px] text-amber-700/80 dark:text-amber-500/80">
        Edit on the{' '}
        <a
          href={buildServiceHref('/admin', activeServiceId)}
          className="underline font-medium hover:text-amber-900 dark:hover:text-amber-300"
        >
          admin page
        </a>
        .
      </div>
      <div className="text-xs text-amber-600 dark:text-amber-500 space-y-0.5">
        <div>Objects created/day: <strong>{fmtN(Math.round(objectsPerDay))}</strong></div>
        <div>Sustained billed footprint ({billedDaysLabel}d): <strong>{fmtN(Math.round(objectsBilled))}</strong> objects</div>
      </div>
    </div>
  )
}
