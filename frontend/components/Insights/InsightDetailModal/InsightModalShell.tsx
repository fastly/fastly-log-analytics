'use client'

import React from 'react'
import { ScanSearch } from 'lucide-react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { cn } from '@/lib/utils'
import { InsightStatCard } from '../shared'

export interface InsightModalShellProps {
  isOpen: boolean
  onOpenChange: (open: boolean) => void
  maxWidthClass?: string
  scrollable?: boolean
  icon: React.ReactNode
  title: React.ReactNode
  description: React.ReactNode
  footer?: React.ReactNode
  children: React.ReactNode
}

export function InsightModalShell({
  isOpen,
  onOpenChange,
  maxWidthClass = 'max-w-3xl',
  scrollable = true,
  icon,
  title,
  description,
  footer,
  children,
}: InsightModalShellProps) {
  return (
    <Dialog open={isOpen} onOpenChange={onOpenChange}>
      <DialogContent className={cn(maxWidthClass, scrollable ? 'max-h-[90vh] flex flex-col p-6' : 'p-6')}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg font-bold">
            {icon}
            {title}
          </DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        {scrollable ? (
          <div className="flex-1 min-h-0 flex flex-col gap-6 py-4 overflow-y-auto">{children}</div>
        ) : (
          children
        )}
        {footer ?? <DialogFooter showCloseButton />}
      </DialogContent>
    </Dialog>
  )
}

export interface FindingStatDef {
  icon: React.ReactNode
  label: string
  value: string
  valueClass?: string
  hint?: string
}

export interface FindingEvidenceItem {
  lead: string
  body: React.ReactNode
}

export function BotFindingSection({
  summary,
  stats,
  cols = 3,
  evidence,
}: {
  summary: React.ReactNode
  stats: FindingStatDef[]
  cols?: 3 | 4
  evidence: FindingEvidenceItem[]
}) {
  return (
    <>
      <div className="p-4 rounded-lg bg-muted/30 border border-border">
        <p className="text-sm leading-relaxed">{summary}</p>
      </div>
      <div className={cn('grid grid-cols-2 gap-4', cols === 3 ? 'md:grid-cols-3' : 'md:grid-cols-4')}>
        {stats.map((s) => (
          <InsightStatCard
            key={s.label}
            icon={s.icon}
            label={s.label}
            value={s.value}
            valueClass={s.valueClass}
            hint={s.hint}
          />
        ))}
      </div>
      <div className="rounded-lg bg-muted/10 border border-border p-4">
        <h4 className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-3 flex items-center gap-1.5">
          <ScanSearch className="h-3.5 w-3.5" /> How we read the evidence
        </h4>
        <ul className="space-y-2.5 text-xs leading-relaxed">
          {evidence.map((ev) => (
            <li key={ev.lead}>
              <span className="font-semibold">{ev.lead}</span> {ev.body}
            </li>
          ))}
        </ul>
      </div>
    </>
  )
}
