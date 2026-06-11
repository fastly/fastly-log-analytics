'use client'

import React, { useState } from 'react'
import { DashboardTableData } from '@/types/api'
import { FieldSearchDialog } from './FieldSearchDialog'
import { Copy, Check, EyeOff } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { formatValue, calculateDelta } from '@/lib/format'
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'

interface TopTenTableProps {
  title: string
  icon?: React.ReactNode
  field?: string
  data?: DashboardTableData
  compareData?: DashboardTableData
  onRowClick?: (field: string, value: string | number) => void
  inActiveFormat?: boolean
}

function NotLoggedIndicator() {
  return (
    <Tooltip>
      <TooltipTrigger render={<span className="inline-flex items-center" />}>
        <EyeOff className="h-3 w-3 text-muted-foreground/70" />
      </TooltipTrigger>
      <TooltipContent>
        <p className="text-[10px]">Not currently being logged — showing historical data only.</p>
      </TooltipContent>
    </Tooltip>
  )
}

export const TopTenTable = React.memo(function TopTenTable({ title, icon, field, data, compareData, onRowClick, inActiveFormat = true }: TopTenTableProps) {
  const [copied, setCopied] = useState(false)
  const { data: catalog } = useLogFieldsCatalog()

  if (!data || !data.top || data.top.length === 0) {
    let requiredGroupMessage = ''
    if (field && catalog?.fields) {
      const fieldMeta = catalog.fields.find(f => f.id === field)
      const groupId = fieldMeta?.group

      if (groupId) {
        // Only surface "Requires X to be enabled" when the field's group is
        // genuinely not in the active log format. If the group IS enabled
        // and the empty state is just due to no rows for this dimension,
        // the message would be misleading (the field IS being logged).
        if (!inActiveFormat) {
          const groupMeta = catalog.groups?.find(g => g.id === groupId)
          if (groupMeta) {
            requiredGroupMessage = `Requires ${groupMeta.label} fields to be enabled in Fastly logging.`
          }
        }
      } else if (field === '_ngwaf_bot_name' || field === 'waf_sig_ind') {
        // Virtual fields are FORCE_VISIBLE — inActiveFormat doesn't reflect
        // whether the underlying NGWAF fields are enabled, so the message
        // is the only signal we can give the user.
        requiredGroupMessage = `Requires NGWAF fields to be enabled in Fastly logging.`
      } else if (field === '_bot_name') {
        requiredGroupMessage = `Requires User-Agent field to be enabled in Fastly logging.`
      }
    }

    return (
      <div className="flex flex-col border rounded-lg p-4 h-full bg-card [content-visibility:auto] [contain-intrinsic-size:300px]">
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-sm font-medium tracking-tight flex items-center gap-1.5">
            {icon} {title}
            {!inActiveFormat && <NotLoggedIndicator />}
          </h3>
          {field && <FieldSearchDialog field={field} title={title} />}
        </div>
        <div className="flex flex-col flex-1 items-center justify-center text-muted-foreground text-sm border-t border-dashed mt-1 pt-4 text-center">
          <span className="mb-1">No data available</span>
          {requiredGroupMessage && (
            <span className="text-[10px] opacity-70 px-4">
              {requiredGroupMessage}
            </span>
          )}
        </div>
      </div>
    )
  }

  const maxCount = Math.max(...data.top.map(item => item.count))
  const compareMap = new Map(compareData?.top?.map(item => [item.value, item.count]) || [])

  const handleCopyCSV = (e: React.MouseEvent) => {
    e.stopPropagation()
    const header = `${field},count\n`
    const rows = data.top.map(item => `"${item.label || item.value}",${item.count}`).join('\n')
    navigator.clipboard.writeText(header + rows)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className="flex flex-col border rounded-lg p-4 h-full bg-card [content-visibility:auto] [contain-intrinsic-size:300px]">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium tracking-tight flex items-center gap-1.5">
          {icon} {title} <span className="text-muted-foreground font-normal text-xs ml-1">(Top 10)</span>
          {!inActiveFormat && <NotLoggedIndicator />}
        </h3>
        <div className="flex items-center gap-1">
          <Tooltip>
            <TooltipTrigger render={<span className="inline-block" />}>
              <Button
                variant="ghost"
                size="icon"
                aria-label={copied ? 'Copied!' : 'Copy table as CSV'}
                className="h-7 w-7 text-muted-foreground hover:text-primary"
                onClick={handleCopyCSV}
              >
                {copied ? <Check className="h-3.5 w-3.5 text-green-500" /> : <Copy className="h-3.5 w-3.5" />}
              </Button>
            </TooltipTrigger>
            <TooltipContent>
              <p className="text-[10px]">{copied ? 'Copied!' : 'Copy as CSV'}</p>
            </TooltipContent>
          </Tooltip>          {field && <FieldSearchDialog field={field} title={title} />}
        </div>
      </div>
      <div className="flex flex-col gap-[2px] flex-1">
        {data.top.map((item, i) => {
          const displayVal = item.label || formatValue(field, item.value as string | number)
          const compCount = compareMap.get(item.value)
          const delta = calculateDelta(item.count, compCount)

          return (
            <div
              key={i}
              className="group flex items-center justify-between py-1.5 px-2 -mx-2 rounded-sm cursor-pointer hover:bg-muted/50 text-sm relative overflow-hidden"
              onClick={() => onRowClick?.(field ?? '', item.value as string | number)}
              title={String(displayVal)}
            >
              <div
                className="absolute inset-y-0 left-0 bg-primary/10 transition-all duration-300"
                style={{ width: `${(item.count / maxCount) * 100}%` }}
              />
              <span className="relative z-10 truncate pr-4 max-w-[65%]">
                {displayVal}
              </span>
              <div className="relative z-10 flex items-center gap-2">
                {delta !== null && (
                  <span className={cn(
                    "text-[10px] font-bold tabular-nums",
                    delta > 0 ? "text-red-500" : delta < 0 ? "text-green-500" : "text-muted-foreground"
                  )}>
                    {delta > 0 ? '+' : ''}{delta.toFixed(0)}%
                  </span>
                )}
                <span className="text-xs font-mono tabular-nums text-muted-foreground">
                  {item.count.toLocaleString()}
                </span>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
})
