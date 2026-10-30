'use client'

import React from 'react'
import {
  CheckCircle,
  Filter,
  Info,
  Loader2,
  RefreshCw,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import {
  BASELINE_OPTIONS,
  cn,
  normalizeTabKey,
  SECTION_LOADING_MESSAGES,
  summarizeSeverities,
  WINDOW_OPTIONS,
  type InsightSectionGroup,
  type SectionableInsight,
  type SeveritySummary,
} from '@/lib/utils'
import { SEVERITY_BADGE_CLASS } from './shared'

export const STATUS_OPTIONS = [
  { label: 'All Statuses', value: 'all' },
  { label: 'Critical', value: 'critical' },
  { label: 'Warning', value: 'warning' },
  { label: 'Info', value: 'info' },
  { label: 'Clean', value: 'clean' },
  // `error` severity insights (the insight computation itself failed) were
  // silently dropped from every non-"all" filter without this option.
  { label: 'Error', value: 'error' },
]

// Full per-severity rollup chips (e.g. "2 CRITICAL" "3 WARNING" "4 CLEAN"),
// reusing the exact card palette (SEVERITY_BADGE_CLASS) so the panel header and
// the cards read the same. Shown in the ACTIVE panel header — the whole
// breakdown, not the compact critical/warning-only badges on the tab triggers.
// Renders nothing when there are no known severities (skeleton entries have
// none), so the loading panel header shows only the count.
export function SeverityChips({ severities }: { severities: SeveritySummary[] }) {
  if (severities.length === 0) return null
  return (
    <span className="ml-1 flex flex-wrap items-center gap-1">
      {severities.map(({ severity, count }) => (
        <Badge
          key={severity}
          variant="outline"
          className={cn(
            'border px-1.5 py-0 text-[10px] font-medium uppercase tracking-wide tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count} {severity}
        </Badge>
      ))}
    </span>
  )
}

// Compact per-tab severity badges: colored count-only pills for the high-signal
// severities (critical=red, warning=amber). clean/info/error are folded away
// here to keep the tab row scannable — the FULL breakdown lives in the active
// panel header (SeverityChips). Screen readers still get the severity name via
// the sr-only span so a bare "2" isn't ambiguous. Colors reuse the AA-cleared
// SEVERITY_BADGE_CLASS palette (axe color-contrast gate).
const TAB_BADGE_SEVERITIES = new Set(['critical', 'warning'])

export function TabSeverityBadges({ severities }: { severities: SeveritySummary[] }) {
  const compact = severities.filter((s) => TAB_BADGE_SEVERITIES.has(s.severity))
  // Always reserve two pills' width + pill height: the skeleton tab bar (no severities) and the
  // loaded one (up to 2 pills: critical + warning) must have identical dimensions or every tab
  // shifts when /api/insights lands (CLS).
  if (compact.length === 0) return <span aria-hidden="true" className="inline-block h-[18px] min-w-[2.5rem]" />
  return (
    <span className="flex h-[18px] min-w-[2.5rem] items-center gap-1">
      {compact.map(({ severity, count }) => (
        <span
          key={severity}
          className={cn(
            'inline-flex min-w-[1.125rem] items-center justify-center rounded-full border px-1 text-[10px] font-semibold leading-4 tabular-nums',
            SEVERITY_BADGE_CLASS[severity as keyof typeof SEVERITY_BADGE_CLASS] ?? '',
          )}
        >
          {count}
          <span className="sr-only"> {severity}</span>
        </span>
      ))}
    </span>
  )
}

// The tabs UI. One tab per section in fixed INSIGHT_SECTIONS order
// (All Categories first, then security, origin, edge, network, traffic, + Other last),
// the tab bar rendered from `groups` (skeleton groups during
// load, real groups after, so the row never pops in — CLS). base-ui Tabs give
// roving-tabindex + arrow-key nav for free, and Tabs.Panel UNMOUNTS inactive
// panels by default (keepMounted defaults false) — so a section's cards only
// render when its tab is active (the "load when I click it" behaviour the user
// asked for, even though /api/insights returns everything in one call).
//
// The active tab is controlled by the parent so the selection survives the
// loading→loaded swap and status-filter changes; `activeKey` falls back to the
// first group whenever the current selection isn't among the rendered groups.
export function InsightTabs<T extends SectionableInsight>({
  groups,
  activeTab,
  onTabChange,
  isLoading,
  renderCards,
}: {
  groups: InsightSectionGroup<T>[]
  activeTab: string | null
  onTabChange: (key: string) => void
  isLoading?: boolean
  renderCards: (group: InsightSectionGroup<T>) => React.ReactNode
}) {
  const normalizedActive = normalizeTabKey(activeTab)
  const activeKey =
    groups.some((g) => g.section.key === normalizedActive)
      ? normalizedActive
      : groups[0]?.section.key ?? 'all'

  return (
    <Tabs value={activeKey} onValueChange={(value) => onTabChange(normalizeTabKey(String(value)))}>
      {/* h-auto + flex-wrap so the richer triggers (icon + label + count +
          severity pills) aren't clipped by the primitive's default h-8 and
          wrap instead of overflowing on narrow screens. */}
      <TabsList className="group-data-horizontal/tabs:h-auto w-full flex-wrap justify-start gap-1 p-1">
        {groups.map(({ section, cards }) => {
          const Icon = section.icon
          return (
            <TabsTrigger
              key={section.key}
              value={section.key}
              data-tab={section.key}
              data-category={section.key}
              data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
              className="h-auto flex-none gap-2 px-3 py-2 data-active:border-border data-active:font-semibold"
            >
              <Icon className="size-4 shrink-0" aria-hidden="true" />
              <span>{section.label}</span>
              <span className="min-w-[2ch] text-right text-xs tabular-nums">{cards.length}</span>
              <TabSeverityBadges severities={summarizeSeverities(cards)} />
            </TabsTrigger>
          )
        })}
      </TabsList>

      {groups.map((group) => {
        const { section, cards } = group
        const Icon = section.icon
        return (
          <TabsContent
            key={section.key}
            value={section.key}
            data-tab-content={section.key}
            data-alias={section.key === 'edge' ? 'performance' : section.key === 'network' ? 'routing' : undefined}
            className="mt-4 space-y-4"
          >
            <div>
              <h2 className="flex flex-wrap items-center gap-2 text-lg font-semibold tracking-tight">
                <Icon className="h-5 w-5 shrink-0 text-muted-foreground" aria-hidden="true" />
                <span>{section.label}</span>
                <span className="text-xs font-normal text-muted-foreground tabular-nums">
                  ({cards.length})
                </span>
                <SeverityChips severities={summarizeSeverities(cards)} />
              </h2>
              <p className="mt-1 text-sm text-muted-foreground">{section.description}</p>
            </div>
            {cards.length === 0 ? (
              isLoading ? (
                <div
                  className="text-center py-20 text-sm text-muted-foreground min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-loading"
                >
                  <Loader2 className="inline-block animate-spin h-5 w-5 mb-2 text-primary" />
                  <span>{SECTION_LOADING_MESSAGES[section.key] ?? 'Loading insights…'}</span>
                </div>
              ) : (
                <div
                  className="py-16 text-center border rounded-xl border-dashed bg-muted/10 min-h-[300px] flex flex-col items-center justify-center [contain-intrinsic-size:300px] [content-visibility:auto]"
                  data-testid="insights-empty-pass"
                >
                  <CheckCircle className="h-10 w-10 text-emerald-500 mb-3" />
                  <h3 className="text-base font-semibold">
                    {section.key === 'all'
                      ? 'No anomalies detected'
                      : `No anomalies detected in ${section.label}`}
                  </h3>
                  <p className="text-xs text-muted-foreground mt-1 max-w-sm">
                    {section.key === 'all'
                      ? 'All monitored metrics across all categories are operating within normal baseline parameters.'
                      : 'All monitored metrics in this category are operating within normal baseline parameters.'}
                  </p>
                </div>
              )
            ) : (
              renderCards(group)
            )}
          </TabsContent>
        )
      })}
    </Tabs>
  )
}

export interface InsightsHeaderControlsProps {
  statusFilter: string
  setStatusFilter: (val: string) => void
  windowHours: string
  setWindowHours: (val: string) => void
  baselineHours: string
  setBaselineHours: (val: string) => void
  isRemoteAnalyst: boolean
  isRescanning: boolean
  activeServiceId: string | null | undefined
  onRescan: () => void
}

export function InsightsHeaderControls({
  statusFilter,
  setStatusFilter,
  windowHours,
  setWindowHours,
  baselineHours,
  setBaselineHours,
  isRemoteAnalyst,
  isRescanning,
  activeServiceId,
  onRescan,
}: InsightsHeaderControlsProps) {
  return (
    <div className="flex items-end gap-2">
      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Status Filter</span>
          <span className="w-3 h-3 shrink-0" />
        </div>
        <Select value={statusFilter} onValueChange={(val) => val && setStatusFilter(val)}>
          <SelectTrigger className="w-[140px]">
            <Filter className="w-3 h-3 mr-2 opacity-50" />
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            {STATUS_OPTIONS.map((o) => (
              <SelectItem key={o.value} value={o.value}>
                {o.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="flex flex-col gap-1 ml-2">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Window</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The current time period you want to check for anomalies
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={windowHours} onValueChange={(val) => val && setWindowHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Window" />
          </SelectTrigger>
          <SelectContent>
            {WINDOW_OPTIONS.map((o) => (
              <SelectItem key={o.value} value={o.value}>
                {o.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <span className="text-xs font-bold text-muted-foreground uppercase tracking-widest pb-2">vs</span>

      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-1 ml-1 text-muted-foreground">
          <span className="text-[10px] uppercase font-bold">Baseline</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={<span className=" hover:text-foreground transition-colors shrink-0" />}>
                <Info className="inline-block w-3 h-3 opacity-50" />
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-[200px] text-xs">
                The historical period to compare against (acts as the &apos;normal&apos; baseline)
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <Select value={baselineHours} onValueChange={(val) => val && setBaselineHours(val)}>
          <SelectTrigger className="w-[160px]">
            <SelectValue placeholder="Baseline" />
          </SelectTrigger>
          <SelectContent>
            {BASELINE_OPTIONS.map((o) => (
              <SelectItem key={o.value} value={o.value}>
                {o.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {!isRemoteAnalyst && (
        <div className="flex flex-col gap-1 ml-2 justify-end">
          <Button
            variant="outline"
            size="sm"
            onClick={onRescan}
            disabled={isRescanning || !activeServiceId}
            className="h-9 gap-1.5 px-3 text-xs font-medium"
            title="Force re-evaluation of all anomaly detectors against recent logs"
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isRescanning && 'animate-spin')} />
            <span>{isRescanning ? 'Scanning…' : 'Re-scan'}</span>
          </Button>
        </div>
      )}
    </div>
  )
}
