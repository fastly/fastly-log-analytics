'use client'

import React from 'react'
import { parseAsString, useQueryState } from 'nuqs'
import {
  Activity,
  Ban,
  Check,
  ChevronsUpDown,
  Globe,
  KeyRound,
  Link2,
  List,
  Loader2,
  MoreHorizontal,
  Network,
  Server,
  ShieldBan,
  ShieldCheck,
  ShieldQuestion,
  Undo2,
  VenetianMask,
  X,
} from 'lucide-react'
import { useIsAnalyst } from '@/hooks/useIsAnalyst'
import { formatDate } from '@/lib/date'
import { formatBytes, resolveCountryName } from '@/lib/format'
import { cn } from '@/lib/utils'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { ChartEmptyState } from '@/components/ChartEmptyState'
import { FilterValueCell } from '@/components/FilterValueCell'
import { TimeSeriesChart } from '@/components/charts/TimeSeriesChart'
import { Button } from '@/components/ui/button'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command'
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import type { components } from '@/types/api.generated'
import type { StreamingTabProps } from './QualityTab'
import { SubscriberIpsDialog } from './SubscriberIpsDialog'
import { BlockDialog, BlockMark, useLiveRuleActions, type BlockCondition, type BlockOption } from './blocklist'
import { PolicySyncNotice } from './PolicySyncNotice'
import {
  BLOCK_CATEGORY_LABELS,
  buildRuleLookup,
  listPolicyRules,
  removeLabel,
  useEffectivePolicy,
  usePolicies,
  type BlockCategory,
  type EffectivePolicy,
  type FlatRule,
  type RuleLookup,
  type RuleMark,
  modeLabel,
} from './policyApi'
import { TOP_N, useContentSecurityData } from './securityData'

type TopRow = components['schemas']['ContentSecurityTopRow']
type ContentIdOption = components['schemas']['ContentSecurityContentId']
type SubscriberRow = components['schemas']['ContentSecuritySubscriberRow']
type EnforcementRow = components['schemas']['ContentSecurityEnforcementRow']
type EnforcementReasonRow = components['schemas']['ContentSecurityEnforcementReasonRow']

interface SecurityTabProps extends StreamingTabProps {
  bucketSeconds: number
}

// ?content=<cs_content_id> narrows the whole tab to one piece of content.
const contentParser = parseAsString.withOptions({ history: 'replace', shallow: true })
// ?csview=policies&policy=<id|new> deep-links to the Policies view (and its editor); StreamingClient owns the view.
const viewParser = parseAsString.withOptions({ history: 'replace', shallow: true })

/**
 * The Referers leaderboard shows domains, but the `referer` column holds full URLs, so a filter on a
 * row matches any URL on that host (wildcards are ILIKE-matched server-side).
 */
const referrerHostFilter = (host: string) => `*://${host}/*`

// Log column → the category the blocklist function receives.
const FIELD_BLOCK_CATEGORY: Record<string, BlockCategory> = {
  country: 'country',
  asn: 'asn',
  referer: 'referer',
  host: 'host',
  p_type: 'proxy_type',
  p_desc: 'proxy_description',
}


const BANDWIDTH_LAYOUT = {
  yaxis: { title: { text: 'Bandwidth' }, tickformat: '.3s', ticksuffix: 'bps', rangemode: 'tozero' as const },
}

/**
 * Outcomes the operator's VCL stamps on each request (cs_status). Listed in stack order,
 * most severe first. `stops` marks the ones that only take effect in ENFORCE mode, so
 * the table can tell "was stopped" from "would have been stopped" (WARN).
 */
const ENFORCEMENT_STATUSES: Record<string, { label: string; color: string; stops: boolean }> = {
  blocked: { label: 'Blocked', color: '#ef4444', stops: true },
  rate_limited: { label: 'Rate limited', color: '#f97316', stops: true },
  throttled: { label: 'Throttled', color: '#f59e0b', stops: true },
  check_failed: { label: 'Check failed', color: '#a855f7', stops: false },
  counted: { label: 'Counted', color: '#6366f1', stops: false },
  allowed: { label: 'Allowed', color: '#22c55e', stops: false },
}
const ENFORCEMENT_ORDER = Object.keys(ENFORCEMENT_STATUSES)
const enforcementLabel = (status: string) => ENFORCEMENT_STATUSES[status]?.label ?? status

const ENFORCEMENT_LAYOUT = {
  barmode: 'stack' as const,
  yaxis: { title: { text: 'Requests' }, rangemode: 'tozero' as const },
}

/**
 * Requests per enforcement outcome. "Enforced" counts only requests whose mode was ENFORCE;
 * "Warned" is the WARN remainder, which was served normally.
 */
export function EnforcementTable({ rows }: { rows: EnforcementRow[] }) {
  const showUsers = rows.some((r) => r.users != null)
  const showBytes = rows.some((r) => r.bytes != null)
  const sorted = [...rows].sort(
    (a, b) =>
      (ENFORCEMENT_ORDER.indexOf(a.status) + 1 || ENFORCEMENT_ORDER.length + 1) -
      (ENFORCEMENT_ORDER.indexOf(b.status) + 1 || ENFORCEMENT_ORDER.length + 1),
  )
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Outcome</TableHead>
          <TableHead className="text-right">Requests</TableHead>
          <TableHead className="text-right">Enforced</TableHead>
          <TableHead className="text-right">Warned</TableHead>
          {showUsers && <TableHead className="text-right">Users</TableHead>}
          {showBytes && <TableHead className="text-right">Bandwidth</TableHead>}
        </TableRow>
      </TableHeader>
      <TableBody>
        {sorted.map((row) => {
          const meta = ENFORCEMENT_STATUSES[row.status]
          const stops = meta?.stops ?? false
          return (
            <TableRow key={row.status}>
              <TableCell>
                <span className="flex items-center gap-2">
                  <span
                    className="inline-block h-2.5 w-2.5 shrink-0 rounded-full"
                    style={{ backgroundColor: meta?.color ?? '#9ca3af' }}
                    aria-hidden="true"
                  />
                  <FilterValueCell
                    filters={[{ column: 'cs_status', value: row.status }]}
                    display={enforcementLabel(row.status)}
                  />
                </span>
              </TableCell>
              <TableCell className="text-right tabular-nums">{row.requests.toLocaleString()}</TableCell>
              <TableCell className="text-right tabular-nums font-medium">
                {stops ? row.enforced.toLocaleString() : '—'}
              </TableCell>
              <TableCell className="text-right tabular-nums text-muted-foreground">
                {stops ? (row.requests - row.enforced).toLocaleString() : '—'}
              </TableCell>
              {showUsers && (
                <TableCell className="text-right tabular-nums">{row.users?.toLocaleString() ?? '—'}</TableCell>
              )}
              {showBytes && (
                <TableCell className="text-right tabular-nums">
                  {row.bytes == null ? '—' : formatBytes(row.bytes)}
                </TableCell>
              )}
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}

/** Round bucket sizes (seconds) the Enforcement chart snaps to. */
const ENFORCEMENT_BUCKETS = [60, 300, 600, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400, 172800, 604800]
/** Aim for about this many bars across the range; more and they are too thin to read. */
const ENFORCEMENT_TARGET_BARS = 48

const utcMs = (bucket: string) => Date.parse(`${bucket.replace(' ', 'T')}Z`)

/**
 * Re-bucket the per-status counts into coarser buckets for the stacked bars. The API buckets
 * by the page's interval, which is fine for lines but leaves hairline bars on a wide range
 * with bursty traffic. Picks the smallest round multiple of `bucketSeconds` that gives about
 * ENFORCEMENT_TARGET_BARS bars over the range (or over the data, when the range is open).
 */
export function coarsenEnforcement(
  ts: { bucket: string; status: string; requests: number }[],
  bucketSeconds: number,
  startTime?: string | null,
  endTime?: string | null,
): { bucketSeconds: number; points: { bucketMs: number; status: string; requests: number }[] } {
  const times = ts.map((d) => utcMs(d.bucket))
  const start = startTime ? Date.parse(startTime) : Math.min(...times)
  const end = endTime ? Date.parse(endTime) : Math.max(...times) + bucketSeconds * 1000
  const spanSeconds = Math.max((end - start) / 1000, bucketSeconds)
  const wanted = spanSeconds / ENFORCEMENT_TARGET_BARS
  const size =
    ENFORCEMENT_BUCKETS.find((b) => b >= wanted && b >= bucketSeconds && b % bucketSeconds === 0) ??
    Math.max(Math.ceil(wanted / bucketSeconds) * bucketSeconds, bucketSeconds)

  const sums = new Map<string, { bucketMs: number; status: string; requests: number }>()
  ts.forEach((d, i) => {
    const bucketMs = Math.floor(times[i] / 1000 / size) * size * 1000
    const key = `${d.status}\u0000${bucketMs}`
    const row = sums.get(key)
    if (row) row.requests += d.requests
    else sums.set(key, { bucketMs, status: d.status, requests: d.requests })
  })
  return { bucketSeconds: size, points: [...sums.values()] }
}

/** Subscriber ids ranked server-side by distinct client IPs (then requests). */
export function SubscriberTable({
  rows,
  showCountries,
  timezone,
  onSelect,
  onBlock,
  onUnblock,
  findRule,
}: {
  rows: SubscriberRow[]
  showCountries: boolean
  timezone: string
  /** Opens the per-IP drill-down; omitted when ids are masked (analysts). */
  onSelect?: (subscriberId: string) => void
  /** Admin-only blocklist actions; omitted for analysts. */
  onBlock?: (subscriberId: string) => void
  onUnblock?: (category: BlockCategory, value: string, rule: RuleMark) => void
  findRule?: RuleLookup
}) {
  const showBytes = rows.some((r) => r.bytes != null)
  const showActions = onSelect != null || onBlock != null
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-8 text-right">#</TableHead>
          <TableHead>Subscriber ID</TableHead>
          <TableHead className="text-right">Distinct IPs</TableHead>
          {showCountries && <TableHead className="text-right">Countries</TableHead>}
          <TableHead className="text-right">Requests</TableHead>
          {showBytes && <TableHead className="text-right">Bandwidth</TableHead>}
          <TableHead className="text-right">Last seen</TableHead>
          {showActions && (
            <TableHead className="w-8">
              <span className="sr-only">Actions</span>
            </TableHead>
          )}
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row, i) => {
          const rule = findRule?.('subscriber_id', row.subscriber_id)
          return (
          <TableRow
            key={row.subscriber_id}
            className={cn(onSelect && 'cursor-pointer')}
            onClick={onSelect ? () => onSelect(row.subscriber_id) : undefined}
          >
            <TableCell className="text-right tabular-nums text-muted-foreground">{i + 1}</TableCell>
            <TableCell className="max-w-[20rem] truncate font-mono text-xs" title={row.subscriber_id}>
              <span className="flex min-w-0 items-center gap-1.5">
              {onSelect ? (
                // The row is the click target; this button is its keyboard / screen-reader handle.
                <button
                  type="button"
                  className="max-w-full truncate text-left hover:underline focus-visible:underline focus-visible:outline-none"
                  aria-label={`Show IP addresses for ${row.subscriber_id}`}
                  onClick={(e) => {
                    e.stopPropagation()
                    onSelect(row.subscriber_id)
                  }}
                >
                  {row.subscriber_id}
                </button>
              ) : (
                row.subscriber_id
              )}
              {rule && <BlockMark rule={rule} />}
              </span>
            </TableCell>
            <TableCell className="text-right tabular-nums font-medium">{row.distinct_ips.toLocaleString()}</TableCell>
            {showCountries && (
              <TableCell className="text-right tabular-nums">{row.distinct_countries?.toLocaleString() ?? '—'}</TableCell>
            )}
            <TableCell className="text-right tabular-nums">{row.requests.toLocaleString()}</TableCell>
            {showBytes && (
              <TableCell className="text-right tabular-nums">{row.bytes == null ? '—' : formatBytes(row.bytes)}</TableCell>
            )}
            <TableCell className="text-right tabular-nums whitespace-nowrap">
              {row.last_seen ? formatDate(row.last_seen, timezone, 'yyyy-MM-dd HH:mm:ss') : '—'}
            </TableCell>
            {showActions && (
              // The row click opens the drill-down; keep clicks in the menu from bubbling to it.
              <TableCell className="py-0" onClick={(e) => e.stopPropagation()}>
                <DropdownMenu>
                  <DropdownMenuTrigger
                    render={
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-7 w-7"
                        aria-label={`Actions for ${row.subscriber_id}`}
                      />
                    }
                  >
                    <MoreHorizontal className="h-4 w-4" aria-hidden="true" />
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end" className="min-w-[180px]">
                    {onSelect && (
                      <DropdownMenuItem onClick={() => onSelect(row.subscriber_id)}>
                        <List className="h-3.5 w-3.5" aria-hidden="true" />
                        <span>Show IP addresses</span>
                      </DropdownMenuItem>
                    )}
                    {rule && onUnblock ? (
                      <DropdownMenuItem onClick={() => onUnblock('subscriber_id', row.subscriber_id, rule)}>
                        <Undo2 className="h-3.5 w-3.5" aria-hidden="true" />
                        <span>{removeLabel(rule)}</span>
                      </DropdownMenuItem>
                    ) : (
                      onBlock && (
                        <DropdownMenuItem onClick={() => onBlock(row.subscriber_id)}>
                          <Ban className="h-3.5 w-3.5" aria-hidden="true" />
                          <span>Block…</span>
                        </DropdownMenuItem>
                      )
                    )}
                  </DropdownMenuContent>
                </DropdownMenu>
              </TableCell>
            )}
          </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}

function TopTable({
  rows,
  column,
  valueHeader,
  formatValue = (v) => v,
  blockCategory,
  onBlock,
  onUnblock,
  findRule,
}: {
  rows: TopRow[]
  /** Log field the values come from — clicking a value offers it as a filter on that column. */
  column: string
  valueHeader: string
  formatValue?: (value: string) => string
  /** Admin-only blocklist actions; omitted for analysts. */
  blockCategory?: BlockCategory
  onBlock?: (value: string) => void
  onUnblock?: (category: BlockCategory, value: string, rule: RuleMark) => void
  findRule?: RuleLookup
}) {
  // Users = distinct (subscriber id, IP) pairs, IP alone without a subscriber id.
  const showUsers = rows.some((r) => r.users != null)
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>{valueHeader}</TableHead>
          {showUsers && <TableHead className="text-right">Users</TableHead>}
          <TableHead className="text-right">Requests</TableHead>
          <TableHead className="text-right">Bandwidth</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((row) => {
          const rule = blockCategory ? findRule?.(blockCategory, row.value) : undefined
          let extraItems: React.ReactNode = null
          if (rule && blockCategory && onUnblock) {
            extraItems = (
              <DropdownMenuItem onClick={() => onUnblock(blockCategory, row.value, rule)}>
                <Undo2 className="h-3.5 w-3.5" aria-hidden="true" />
                <span>{removeLabel(rule)}</span>
              </DropdownMenuItem>
            )
          } else if (onBlock) {
            extraItems = (
              <DropdownMenuItem onClick={() => onBlock(row.value)}>
                <Ban className="h-3.5 w-3.5" aria-hidden="true" />
                <span>Block, throttle or allow…</span>
              </DropdownMenuItem>
            )
          }
          return (
          <TableRow key={row.value}>
            <TableCell className="max-w-[16rem]" title={row.value}>
              <span className="flex items-center gap-1.5">
                <span className="min-w-0 flex-1">
                  <FilterValueCell
                    filters={[{ column, value: column === 'referer' ? referrerHostFilter(row.value) : row.value }]}
                    copyValue={column === 'referer' ? row.value : undefined}
                    display={row.label ?? formatValue(row.value)}
                    extraItems={extraItems}
                  />
                </span>
                {rule && <BlockMark rule={rule} />}
              </span>
            </TableCell>
            {showUsers && (
              <TableCell className="text-right tabular-nums font-medium">{row.users?.toLocaleString() ?? '—'}</TableCell>
            )}
            <TableCell className="text-right tabular-nums">{row.requests.toLocaleString()}</TableCell>
            <TableCell className="text-right tabular-nums">{row.bytes == null ? '—' : formatBytes(row.bytes)}</TableCell>
          </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}

/** Admin-only: which policy is in effect for the content being viewed, and whether it is live. */
function PolicyBanner({
  effective,
  contentId,
  timezone,
  activeServiceId,
  onEdit,
}: {
  effective: EffectivePolicy | undefined
  contentId: string | null
  timezone: string
  activeServiceId: string | null
  onEdit: (policyId: string) => void
}) {
  if (!effective) return null
  const { policy, assignment } = effective
  const scope = contentId ?? 'all content'
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 rounded-md border bg-muted/40 px-3 py-2 text-sm">
        {policy && assignment ? (
          <>
            <span>
              In effect for <strong>{scope}</strong>: <strong>{policy.name}</strong> ({modeLabel(assignment.mode)})
              {assignment.scope_type === 'default' && contentId ? (
                <span className="text-muted-foreground"> · all-content policy; this content has no schedule of its own</span>
              ) : null}
            </span>
            <Button variant="link" size="sm" className="h-auto p-0 text-xs" onClick={() => onEdit(policy.id)}>
              Edit policy
            </Button>
          </>
        ) : (
          <span className="text-muted-foreground">
            No policy is in effect for {scope}. Blocking a value creates a Default policy for all content.
          </span>
        )}
      </div>
      <PolicySyncNotice activeServiceId={activeServiceId} timezone={timezone} compact />
    </div>
  )
}

const ruleActionLabel = (r: FlatRule) =>
  r.action === 'block' ? 'Block' : r.action === 'pace' ? `Throttle to ${r.kbps} kbps` : 'Allow'

/**
 * Admin-only: every rule of the policy in effect for the viewed content (blocks, throttles and
 * exemptions), each with an Unblock / Remove button.
 */
function PolicyRulesCard({
  effective,
  contentId,
  isLoading,
  onRemove,
}: {
  effective: EffectivePolicy | undefined
  contentId: string | null
  isLoading: boolean
  onRemove: (policyId: string, rule: FlatRule) => Promise<void>
}) {
  const [pending, setPending] = React.useState<string | null>(null)
  const policy = effective?.policy
  const rules = React.useMemo(() => listPolicyRules(policy), [policy])
  const remove = async (rule: FlatRule) => {
    if (!policy) return
    setPending(`${rule.category}\u0000${rule.value}`)
    try {
      await onRemove(policy.id, rule)
    } catch {
      // The api client already toasts DELETE failures.
    } finally {
      setPending(null)
    }
  }

  let body: React.ReactNode
  if (effective?.configured === false) {
    body = (
      <p className="px-2 py-6 text-center text-sm text-muted-foreground">
        Blocking and throttling are off. Set a blocklist endpoint in Admin → Overall Settings → Content Security.
      </p>
    )
  } else if (!policy) {
    body = (
      <p className="px-2 py-6 text-center text-sm text-muted-foreground">
        No policy is in effect for {contentId ?? 'all content'}, so nothing is blocked or throttled.
      </p>
    )
  } else if (!rules.length && !policy.country_regex) {
    body = <p className="px-2 py-6 text-center text-sm text-muted-foreground">No values are blocked or throttled.</p>
  } else {
    body = (
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Rule</TableHead>
            <TableHead>Action</TableHead>
            <TableHead className="w-36">
              <span className="sr-only">Actions</span>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {policy.country_regex && (
            <TableRow>
              <TableCell>
                <span className="flex min-w-0 items-baseline gap-1.5">
                  <span className="shrink-0 text-muted-foreground">Country regex</span>
                  <span className="truncate font-mono text-xs" title={policy.country_regex}>
                    {policy.country_regex}
                  </span>
                </span>
              </TableCell>
              <TableCell className="whitespace-nowrap">
                <span className="inline-flex items-center gap-1.5">
                  <BlockMark rule={{ action: 'block', kbps: 0 }} />
                  Block
                </span>
              </TableCell>
              <TableCell className="text-right text-xs text-muted-foreground">Edit the policy to change</TableCell>
            </TableRow>
          )}
          {rules.map((r) => (
            <TableRow key={`${r.category}\u0000${r.value}`}>
              <TableCell className="max-w-[28rem]">
                <span className="flex min-w-0 items-baseline gap-1.5">
                  <span className="shrink-0 text-muted-foreground">{BLOCK_CATEGORY_LABELS[r.category]}</span>
                  <span className="truncate font-mono text-xs" title={r.value}>
                    {r.category === 'country' ? formatCountry(r.value) : r.category === 'asn' ? (r.label ?? `AS${r.value}`) : r.value}
                  </span>
                </span>
              </TableCell>
              <TableCell className="whitespace-nowrap">
                <span className="inline-flex items-center gap-1.5">
                  <BlockMark rule={r} />
                  {ruleActionLabel(r)}
                </span>
              </TableCell>
              <TableCell className="text-right">
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  disabled={pending != null}
                  onClick={() => void remove(r)}
                  aria-label={`${removeLabel(r)} ${BLOCK_CATEGORY_LABELS[r.category]} ${r.value}`}
                >
                  {pending === `${r.category}\u0000${r.value}` ? (
                    <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                  ) : (
                    <Undo2 className="h-3 w-3 mr-1" />
                  )}
                  {pending === `${r.category}\u0000${r.value}` ? 'Removing…' : removeLabel(r)}
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    )
  }

  return (
    <AnalyticsCard
      title="Blocked & Throttled"
      description={
        policy
          ? `Rules in ${policy.name}, the policy in effect for ${contentId ?? 'all content'}.`
          : 'Rules of the policy in effect for the selected content.'
      }
      icon={<ShieldBan className="h-4 w-4" />}
      isLoading={isLoading}
      className="min-h-[160px]"
    >
      {body}
    </AnalyticsCard>
  )
}

/** "country_regex" → "Country regex"; "exempt:asn" → "Allowed: ASN". */
function reasonLabel(reason: string): string {
  const exempt = reason.startsWith('exempt:')
  const key = exempt ? reason.slice('exempt:'.length) : reason
  const label =
    key === 'country_regex'
      ? 'Country regex'
      : (BLOCK_CATEGORY_LABELS as Record<string, string>)[key] ?? key.replace(/_/g, ' ')
  return exempt ? `Allowed: ${label}` : label
}

/**
 * Enforcement per policy and rule. Only the `enforced` count is what the edge actually stopped;
 * the remainder of a stopping outcome was WARN-only and was served normally.
 */
export function EnforcementByPolicy({
  rows,
  policyNames,
}: {
  rows: EnforcementReasonRow[]
  /** policy id → name, when the policies list is available (admin). */
  policyNames?: Map<string, string>
}) {
  const sorted = [...rows].sort(
    (a, b) =>
      (policyNames?.get(a.policy_id) ?? a.policy_id).localeCompare(policyNames?.get(b.policy_id) ?? b.policy_id) ||
      b.requests - a.requests,
  )
  return (
    <div className="space-y-1.5">
      <h3 className="text-xs font-medium text-muted-foreground">By policy and rule</h3>
      <div className="max-h-64 overflow-auto">
        <Table aria-label="Enforcement by policy and rule">
          <TableHeader>
            <TableRow>
              <TableHead>Policy</TableHead>
              <TableHead>Rule</TableHead>
              <TableHead>Outcome</TableHead>
              <TableHead className="text-right">Requests</TableHead>
              <TableHead className="text-right">Enforced</TableHead>
              <TableHead className="text-right">Warned</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {sorted.map((r) => {
              const stops = ENFORCEMENT_STATUSES[r.status]?.stops ?? false
              const name = policyNames?.get(r.policy_id)
              return (
                <TableRow key={`${r.policy_id}\u0000${r.reason}\u0000${r.status}`}>
                  <TableCell className="max-w-[10rem] truncate" title={r.policy_id}>
                    {name ?? <span className="font-mono text-xs">{r.policy_id}</span>}
                  </TableCell>
                  <TableCell>{reasonLabel(r.reason)}</TableCell>
                  <TableCell>{enforcementLabel(r.status)}</TableCell>
                  <TableCell className="text-right tabular-nums">{r.requests.toLocaleString()}</TableCell>
                  <TableCell className="text-right tabular-nums font-medium">
                    {stops ? r.enforced.toLocaleString() : '—'}
                  </TableCell>
                  <TableCell className="text-right tabular-nums text-muted-foreground">
                    {stops ? (r.requests - r.enforced).toLocaleString() : '—'}
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}

const formatCountry = (code: string) => `${resolveCountryName(code)} (${code})`

function ContentPicker({
  value,
  options,
  disabled,
  onChange,
}: {
  value: string | null
  options: ContentIdOption[]
  disabled: boolean
  onChange: (contentId: string | null) => void
}) {
  const [open, setOpen] = React.useState(false)
  const select = (contentId: string | null) => {
    onChange(contentId)
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      {/* aria-label on PopoverTrigger, not the Button render-prop — see ViewSelector. */}
      <PopoverTrigger
        aria-label="Content ID"
        disabled={disabled}
        render={
          <Button
            variant="outline"
            role="combobox"
            aria-expanded={open}
            className="h-8 min-w-[220px] max-w-[360px] justify-between text-xs"
          >
            <span className="truncate">{value ?? 'All content'}</span>
            <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
          </Button>
        }
      />
      <PopoverContent className="w-[360px] p-0" align="start">
        <Command>
          <CommandInput placeholder="Search content IDs..." />
          <CommandEmpty>No content found.</CommandEmpty>
          <CommandList>
            <CommandGroup>
              <CommandItem value="__all__" onSelect={() => select(null)}>
                <Check className={cn('h-4 w-4', value === null ? 'opacity-100' : 'opacity-0')} />
                All content
              </CommandItem>
              {options.map((opt) => (
                <CommandItem key={opt.content_id} value={opt.content_id} onSelect={() => select(opt.content_id)}>
                  <Check className={cn('h-4 w-4', value === opt.content_id ? 'opacity-100' : 'opacity-0')} />
                  <span className="flex-1 truncate" title={opt.content_id}>{opt.content_id}</span>
                  <span className="text-muted-foreground tabular-nums">{opt.requests.toLocaleString()}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}

export default function SecurityTab({
  activeServiceId,
  filterPayload,
  startTime,
  endTime,
  relativeRange,
  isAutoRange,
  anchor,
  timezone,
  bucketSeconds,
}: SecurityTabProps) {
  const [contentId, setContentId] = useQueryState('content', contentParser)
  const [, setView] = useQueryState('csview', viewParser)
  const [, setPolicyParam] = useQueryState('policy', viewParser)
  const [selectedSubscriber, setSelectedSubscriber] = React.useState<string | null>(null)

  // Policy rules are admin-only: analysts never see the actions (and the endpoints 403 them).
  const isAdmin = !useIsAnalyst()
  const effective = useEffectivePolicy(activeServiceId, contentId, isAdmin)
  const policies = usePolicies(activeServiceId, isAdmin)
  const { addRule, unblock } = useLiveRuleActions()
  const [blockRequest, setBlockRequest] = React.useState<{
    options: BlockOption[]
    initial: BlockCondition
  } | null>(null)
  const effectivePolicy = effective.data?.policy
  const findRule = React.useMemo<RuleLookup>(() => buildRuleLookup(effectivePolicy), [effectivePolicy])
  const policyNames = React.useMemo(
    () => new Map((policies.data?.policies ?? []).map((p) => [p.id, p.name])),
    [policies.data?.policies],
  )
  const unblockValue = React.useCallback(
    (category: BlockCategory, value: string, rule: RuleMark) => {
      if (effectivePolicy) void unblock(effectivePolicy.id, category, value, rule).catch(() => {})
    },
    [effectivePolicy, unblock],
  )
  const editPolicy = React.useCallback(
    (policyId: string) => {
      void setView('policies')
      void setPolicyParam(policyId)
    },
    [setView, setPolicyParam],
  )

  const query = useContentSecurityData(
    { activeServiceId, filterPayload, startTime, endTime, relativeRange, isAutoRange, anchor, bucketSeconds },
    contentId,
    { refetchInterval: 30_000 },
  )

  const data = query.data
  const fields = data?.fields ?? {}
  // Only claim a field is missing once the backend has told us so.
  const missing = (field: string) => data != null && fields[field] === false

  const bandwidthData = React.useMemo(() => {
    const ts = data?.bandwidth_ts
    if (!ts?.length) return []
    const x = ts.map((d) => formatDate(d.bucket, timezone, 'yyyy-MM-dd HH:mm:ss'))
    const toBps = (bytes: number) => (bytes * 8) / bucketSeconds
    const traces: Record<string, unknown>[] = [
      {
        x,
        y: ts.map((d) => toBps(d.edge_bytes)),
        name: 'Edge (to viewers)',
        type: 'scatter' as const,
        line: { color: '#6366f1' },
        hovertemplate: '%{y:.3s}bps<extra>Edge</extra>',
      },
    ]
    if (data?.has_shield_split) {
      traces.push({
        x,
        y: ts.map((d) => toBps(d.shield_bytes ?? 0)),
        name: 'Shield (to edge)',
        type: 'scatter' as const,
        line: { color: '#f59e0b', dash: 'dot' },
        hovertemplate: '%{y:.3s}bps<extra>Shield</extra>',
      })
    }
    return traces
  }, [data?.bandwidth_ts, data?.has_shield_split, timezone, bucketSeconds])

  // One stacked bar series per outcome, re-bucketed so the bars are wide enough to read and
  // zero-filled across every bucket that has any outcome.
  const enforcementData = React.useMemo(() => {
    const ts = data?.enforcement_ts
    if (!ts?.length) return []
    const { bucketSeconds: size, points } = coarsenEnforcement(ts, bucketSeconds, startTime, endTime)
    const buckets = [...new Set(points.map((d) => d.bucketMs))].sort((a, b) => a - b)
    const x = buckets.map((ms) => formatDate(new Date(ms), timezone, 'yyyy-MM-dd HH:mm:ss'))
    const byKey = new Map(points.map((d) => [`${d.status}\u0000${d.bucketMs}`, d.requests]))
    const present = new Set(points.map((d) => d.status))
    const statuses = [
      ...ENFORCEMENT_ORDER.filter((st) => present.has(st)),
      ...[...present].filter((st) => !ENFORCEMENT_ORDER.includes(st)).sort(),
    ]
    return statuses.map((status) => ({
      x,
      y: buckets.map((ms) => byKey.get(`${status}\u0000${ms}`) ?? 0),
      name: enforcementLabel(status),
      type: 'bar' as const,
      // Each bar spans its whole bucket instead of being a sliver centred on its start.
      xperiod: size * 1000,
      xperiodalignment: 'start' as const,
      marker: { color: ENFORCEMENT_STATUSES[status]?.color },
      hovertemplate: `%{y:,} requests<extra>${enforcementLabel(status)}</extra>`,
    }))
  }, [data?.enforcement_ts, timezone, bucketSeconds, startTime, endTime])


  if (data && !data.available) {
    return (
      <div className="py-16 text-center text-sm text-muted-foreground">
        {data.reason ?? 'Content security metrics are not available for this service.'}
      </div>
    )
  }

  const cardState = {
    isLoading: query.isLoading,
    isFetching: query.isFetching,
    error: query.error as AnalyticsCardError | null,
  }

  const topCard = (
    title: string,
    icon: React.ReactNode,
    rows: TopRow[] | undefined,
    valueHeader: string,
    formatValue: ((value: string) => string) | undefined,
    field: string,
    requires: string,
    capped = true,
  ) => (
    <AnalyticsCard
      title={
        capped ? (
          <>
            {title} <span className="ml-1 text-xs font-normal text-muted-foreground">(Top {TOP_N})</span>
          </>
        ) : (
          title
        )
      }
      icon={icon}
      {...cardState}
      className="min-h-[200px]"
    >
      {rows?.length ? (
        <TopTable
          rows={rows}
          column={field}
          valueHeader={valueHeader}
          formatValue={formatValue}
          {...(isAdmin && {
            blockCategory: FIELD_BLOCK_CATEGORY[field],
            findRule,
            onUnblock: unblockValue,
            onBlock: (value: string) => {
              const category = FIELD_BLOCK_CATEGORY[field]
              setBlockRequest({
                options: rows.map((r) => ({ category, value: r.value, display: r.label ?? formatValue?.(r.value) })),
                initial: { category, value },
              })
            },
          })}
        />
      ) : (
        <ChartEmptyState requires={missing(field) ? requires : undefined} />
      )}
    </AnalyticsCard>
  )

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <span className="text-sm font-medium">Content</span>
        <ContentPicker
          value={contentId}
          options={data?.content_ids ?? []}
          disabled={missing('cs_content_id')}
          onChange={(id) => void setContentId(id)}
        />
        {contentId && (
          <Button variant="ghost" size="sm" className="h-8 text-xs" onClick={() => void setContentId(null)}>
            <X className="h-3.5 w-3.5" />
            Clear
          </Button>
        )}
        {missing('cs_content_id') && (
          <span className="text-xs text-muted-foreground">
            Requires the Streaming → Token option with Content ID configured in Log Settings.
          </span>
        )}
      </div>

      {isAdmin && (
        <PolicyBanner
          effective={effective.data}
          contentId={contentId}
          timezone={timezone}
          activeServiceId={activeServiceId}
          onEdit={editPolicy}
        />
      )}

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {topCard('Countries', <Globe className="h-4 w-4" />, data?.top_countries, 'Country', formatCountry, 'country', 'Geolocation Basic (Group D) fields to be enabled in Fastly logging.')}
        {topCard('ASNs', <Network className="h-4 w-4" />, data?.top_asns, 'ASN', undefined, 'asn', 'the ASN field (Group F) to be enabled in Fastly logging.')}
        {topCard('Referers', <Link2 className="h-4 w-4" />, data?.top_referers, 'Referer', undefined, 'referer', 'the Referer field (Group A) to be enabled in Fastly logging.')}
      </div>

      {/* Hosts is capped; the proxy cards list every proxy type / description (client.geo.proxy_type and proxy_description have only ~10 values each). */}
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        {topCard('Hosts', <Server className="h-4 w-4" />, data?.top_hosts, 'Host', undefined, 'host', 'the Host field (Group A) to be enabled in Fastly logging.')}
        {topCard('Proxy Types', <ShieldQuestion className="h-4 w-4" />, data?.proxy_types, 'Proxy type', undefined, 'p_type', 'the Proxy Type field (Group I) to be enabled in Fastly logging.', false)}
        {topCard('Proxy Descriptions', <VenetianMask className="h-4 w-4" />, data?.proxy_descriptions, 'Proxy description', undefined, 'p_desc', 'the Proxy Description field (Group I) to be enabled in Fastly logging.', false)}
      </div>

      <AnalyticsCard
        title={
          <>
            Subscriber Usage{' '}
            <span className="ml-1 text-xs font-normal text-muted-foreground">(Top {TOP_N})</span>
          </>
        }
        icon={<KeyRound className="h-4 w-4" />}
        {...cardState}
        className="min-h-[200px]"
      >
        {data?.top_subscribers?.length ? (
          <>
            <SubscriberTable
              rows={data.top_subscribers}
              showCountries={!missing('country')}
              timezone={timezone}
              onSelect={data.subscribers_masked ? undefined : setSelectedSubscriber}
              {...(isAdmin &&
                !data.subscribers_masked && {
                  findRule,
                  onUnblock: unblockValue,
                  onBlock: (subscriberId: string) =>
                    setBlockRequest({
                      options: data.top_subscribers!.map((r) => ({ category: 'subscriber_id' as const, value: r.subscriber_id })),
                      initial: { category: 'subscriber_id', value: subscriberId },
                    }),
                })}
            />
            {data.subscribers_masked && (
              <p className="px-2 pt-2 text-[11px] text-muted-foreground">
                Subscriber IDs are hidden for shared sessions; ranks and counts are exact.
              </p>
            )}
          </>
        ) : (
          <ChartEmptyState
            requires={
              missing('cs_sub_id')
                ? 'the Streaming → Token option (Subscriber ID) to be enabled in Log Settings.'
                : undefined
            }
          />
        )}
      </AnalyticsCard>

      {isAdmin && (
        <PolicyRulesCard
          effective={effective.data}
          contentId={contentId}
          isLoading={effective.isLoading}
          onRemove={(policyId, rule) => unblock(policyId, rule.category, rule.value, rule)}
        />
      )}

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <AnalyticsCard
          title="Enforcement"
          icon={<ShieldCheck className="h-4 w-4" />}
          {...cardState}
          className="min-h-[200px]"
        >
          {data?.enforcement?.length ? (
            <div className="space-y-4">
              <EnforcementTable rows={data.enforcement} />
              {data.enforcement_reasons?.length ? (
                <EnforcementByPolicy rows={data.enforcement_reasons} policyNames={isAdmin ? policyNames : undefined} />
              ) : null}
            </div>
          ) : (
            <ChartEmptyState
              requires={
                missing('cs_status')
                  ? 'the Streaming → Content Security → Log enforcement outcome option to be enabled in Log Settings.'
                  : undefined
              }
            />
          )}
        </AnalyticsCard>
        <AnalyticsCard
          title="Enforcement over time"
          icon={<Activity className="h-4 w-4" />}
          {...cardState}
          className="h-[340px]"
          contentClassName="p-2"
        >
          {enforcementData.length ? (
            <TimeSeriesChart
              data={enforcementData}
              layout={ENFORCEMENT_LAYOUT}
              startTime={startTime}
              endTime={endTime}
              timezone={timezone}
              height="100%"
            />
          ) : (
            <ChartEmptyState />
          )}
        </AnalyticsCard>
      </div>

      <AnalyticsCard
        title="Bandwidth: Edge vs Shield"
        icon={<Activity className="h-4 w-4" />}
        {...cardState}
        className="h-[340px]"
        contentClassName="p-2"
      >
        {bandwidthData.length ? (
          <TimeSeriesChart
            data={bandwidthData}
            layout={BANDWIDTH_LAYOUT}
            startTime={startTime}
            endTime={endTime}
            timezone={timezone}
            height="100%"
          />
        ) : (
          <ChartEmptyState />
        )}
      </AnalyticsCard>

      <SubscriberIpsDialog
        subscriberId={selectedSubscriber}
        contentId={contentId}
        onClose={() => setSelectedSubscriber(null)}
        activeServiceId={activeServiceId}
        filterPayload={filterPayload}
        startTime={startTime}
        endTime={endTime}
        relativeRange={relativeRange}
        isAutoRange={isAutoRange}
        anchor={anchor}
        timezone={timezone}
        onBlock={isAdmin ? (options, initial) => setBlockRequest({ options, initial }) : undefined}
        findRule={findRule}
      />

      {isAdmin && (
        <BlockDialog
          open={blockRequest != null}
          onOpenChange={(open) => !open && setBlockRequest(null)}
          options={blockRequest?.options ?? []}
          initial={blockRequest?.initial ?? null}
          effective={effective.data}
          activeServiceId={activeServiceId}
          contentId={contentId}
          onConfirm={addRule}
        />
      )}
    </div>
  )
}
