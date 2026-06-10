'use client'

import React from 'react'
import { cn } from '@/lib/utils'
import { Input } from '@/components/ui/input'
import { Info } from 'lucide-react'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'

export function Row({ label, children, muted, tooltip }: { label: string; children: React.ReactNode; muted?: boolean; tooltip?: string }) {
  return (
    <div className={cn('flex items-center justify-between py-1.5 border-b border-border/40 last:border-0 gap-4', muted && 'opacity-60')}>
      <div className='flex items-center gap-1.5 text-sm text-muted-foreground flex-1 leading-tight'>
        <span>{label}</span>
        {tooltip && (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="h-3.5 w-3.5" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[250px] text-xs">
                {tooltip}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        )}
      </div>
      <div className='flex-shrink-0'>{children}</div>
    </div>
  )
}

export function NumInput({ id, value, onChange, step, min, max, wide }: {
  id?: string; value: number; onChange: (v: number) => void
  step?: number; min?: number; max?: number; wide?: boolean
}) {
  return (
    <Input
      id={id}
      type='number'
      value={value}
      step={step ?? 1}
      min={min ?? 0}
      max={max}
      onChange={(e) => { const v = parseFloat(e.target.value); if (!isNaN(v)) onChange(v) }}
      className={cn('text-right h-7 text-sm', wide ? 'w-32' : 'w-24')}
    />
  )
}

export function ReadOnlyValue({ value, wide }: { value: string | number; wide?: boolean }) {
  return (
    <div className={cn('text-right h-7 text-sm flex items-center justify-end px-3 rounded-md bg-muted/40 border border-transparent font-mono tabular-nums text-muted-foreground', wide ? 'w-32' : 'w-24')}>
      {value}
    </div>
  )
}

export function ResultRow({ label, detail, cost, highlight }: {
  label: string; detail?: string; cost: string; highlight?: boolean
}) {
  return (
    <div className={cn('flex items-center justify-between py-2 border-b border-border/40 last:border-0', highlight && 'border-t-2 border-border pt-3 mt-2')}>
      <div>
        <div className={cn('text-sm font-medium', highlight && 'text-base')}>{label}</div>
        {detail && <div className='text-xs text-muted-foreground mt-0.5'>{detail}</div>}
      </div>
      <div className={cn('font-bold tabular-nums', highlight ? 'text-xl text-emerald-500' : 'text-sm')}>{cost}</div>
    </div>
  )
}
