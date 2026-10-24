'use client'

import React, { useEffect, useMemo, useReducer } from 'react'
import { reducer, DEFAULTS, calculate } from './calc'
import type { CalcState, PrefillResponse } from './calc'
import { Inputs } from './Inputs'
import { Pricing } from './Pricing'
import { Results } from './Results'

// ─── Main Component ───────────────────────────────────────────────────────────

interface CostCalculatorProps {
  prefillData?: PrefillResponse
  prefillNote?: string
}

export function CostCalculator({ prefillData, prefillNote }: CostCalculatorProps) {
  const [s, dispatch] = useReducer(reducer, DEFAULTS)

  useEffect(() => {
    if (prefillData) {
      dispatch({ type: 'PREFILL', prefill: prefillData })
    }
  }, [prefillData])

  const set = (key: keyof CalcState) => (value: number | boolean) => {
    dispatch({ type: 'SET', key, value })
  }

  const setZone = (zone: import('./calc').BillingZone) => {
    dispatch({ type: 'SET_ZONE', zone })
  }

  const r = useMemo(() => calculate(s), [s])

  return (
    <div className='space-y-6'>
      {prefillNote && (
        <div className='text-xs text-emerald-700 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/30 border border-emerald-200 dark:border-emerald-800 rounded-md px-3 py-2'>
          {prefillNote}
        </div>
      )}

      <div className='grid grid-cols-1 lg:grid-cols-2 gap-8'>
        {/* ── Left: Inputs ── */}
        <div className='space-y-6'>
          <Inputs s={s} r={r} set={set} />
        </div>

        {/* ── Right: Pricing + What generates ops ── */}
        <div className='space-y-6'>
          <Pricing s={s} setZone={setZone} />
        </div>
      </div>

      {/* ── Results ── */}
      <Results s={s} r={r} />

      <p className='text-xs text-muted-foreground leading-relaxed'>
        * Estimates assume ~4× compression of log JSON to Parquet (ZSTD). Storage is billed in GB-hours;
        the calculator converts to GB-months (1 month = 720 hours). Each object is billed for a minimum
        of {s.minDays} days regardless of when it is deleted. CDN caching eliminates Class B ops for cached
        parquet reads; local cache eliminates them entirely for query reads. Actual usage may vary.
      </p>
    </div>
  )
}
