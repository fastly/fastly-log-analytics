'use client'

import React from 'react'
import { cn } from '@/lib/utils'
import { Input } from '@/components/ui/input'
import { LabelWithInfo } from '@/components/ui/label-with-info'

function hasAccessibleName(el: React.ReactElement): boolean {
  const p = el.props as { 'aria-label'?: unknown; 'aria-labelledby'?: unknown }
  return p['aria-label'] != null || p['aria-labelledby'] != null
}

export function Row({ label, children, muted, tooltip }: { label: string; children: React.ReactNode; muted?: boolean; tooltip?: string }) {
  // A11y (WCAG 4.1.2 "label"): the control lives in a separate flex cell from
  // its visible <span> label, so there's no implicit <label> association. Inject
  // the row label as the control's accessible name unless the child already
  // supplies one — covers NumInput (forwards aria-label) and Switch (Base UI
  // honors it, and this is more descriptive than its default "Toggle"); a no-op
  // on the read-only display cells.
  const labelledChild =
    React.isValidElement(children) && !hasAccessibleName(children)
      ? React.cloneElement(children as React.ReactElement<{ 'aria-label'?: string }>, { 'aria-label': label })
      : children
  return (
    <div className={cn('flex items-center justify-between py-1.5 border-b border-border/40 last:border-0 gap-4', muted && 'opacity-60')}>
      <LabelWithInfo
        label={label}
        info={tooltip}
        className="gap-1.5 text-sm text-muted-foreground flex-1 leading-tight"
        labelClassName="text-sm font-normal leading-tight"
      />
      <div className='flex-shrink-0'>{labelledChild}</div>
    </div>
  )
}

export function NumInput({ id, value, onChange, step, min, max, wide, 'aria-label': ariaLabel }: {
  id?: string; value: number; onChange: (v: number) => void
  step?: number; min?: number; max?: number; wide?: boolean
  'aria-label'?: string
}) {
  return (
    <Input
      id={id}
      aria-label={ariaLabel}
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
