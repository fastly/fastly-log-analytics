'use client'

import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { History, Loader2, Plus, X } from 'lucide-react'
import { client, extractApiError } from '@/lib/api'
import { useDebounce } from '@/hooks/useDebounce'
import { formatDate } from '@/lib/date'
import { resolveCountryName } from '@/lib/format'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { NativeSelect } from '@/components/ui/native-select'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Textarea } from '@/components/ui/textarea'
import { ValuePicker, type PickerOption } from '@/components/ui/value-picker'
import { cn } from '@/lib/utils'
import {
  RULE_CATEGORIES,
  THROTTLE_KBPS,
  describeScope,
  modeLabel,
  normalizeValue,
  summarizePolicy,
  type Policy,
  type PolicyBody,
  type RuleAction,
  type RuleCategory,
  type RuleItem,
} from './policyApi'
import type { ValueSuggestion } from './securityData'

const TAB_LABELS: Record<RuleCategory | 'individuals', string> = {
  country: 'Countries',
  referer: 'Referers',
  asn: 'ASNs',
  host: 'Hosts',
  proxy_type: 'Proxy types',
  proxy_description: 'Proxy descriptions',
  individuals: 'Individuals',
}

const SINGULAR: Record<RuleCategory, string> = {
  country: 'country',
  referer: 'referer',
  asn: 'ASN',
  host: 'host',
  proxy_type: 'proxy type',
  proxy_description: 'proxy description',
}

type RuleSet = Record<RuleCategory, RuleItem[]>

interface Draft {
  name: string
  regex: string
  rules: RuleSet
  /** One per line. */
  subscribers: string
  ips: string
}

const emptyRules = (): RuleSet => ({ country: [], referer: [], asn: [], host: [], proxy_type: [], proxy_description: [] })

function draftFrom(policy: Policy | null): Draft {
  const rules = emptyRules()
  if (policy) for (const c of RULE_CATEGORIES) rules[c] = (policy.rules[c] ?? []).map((r) => ({ ...r }))
  return {
    name: policy?.name ?? '',
    regex: policy?.country_regex ?? '',
    rules,
    subscribers: (policy?.individual.subscriber_id ?? []).join('\n'),
    ips: (policy?.individual.ip ?? []).join('\n'),
  }
}

/** Non-empty trimmed lines, de-duplicated, order kept. */
export function parseLines(text: string): string[] {
  return [...new Set(text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean))]
}

/** A deliberately light check; the server does the real IPv4/IPv6 validation. */
export function looksLikeIp(value: string): boolean {
  const v4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(value)
  if (v4) return v4.slice(1).every((o) => Number(o) <= 255)
  return value.includes(':') && /^[0-9a-fA-F:.]+$/.test(value)
}

function toBody(draft: Draft): PolicyBody {
  return {
    name: draft.name.trim(),
    country_regex: draft.regex.trim() || null,
    rules: draft.rules,
    individual: { subscriber_id: parseLines(draft.subscribers), ip: parseLines(draft.ips) },
  }
}

const ACTION_LABELS: Record<RuleAction, string> = { block: 'Block', pace: 'Throttle', exempt: 'Allow' }

/** Throttle rate for a rule: 128, 256 or 512 kbps. */
function ThrottleSelect({ label, value, onChange }: { label: string; value: number; onChange: (kbps: number) => void }) {
  return (
    <NativeSelect aria-label={label} value={value > 0 ? String(value) : String(THROTTLE_KBPS[1])} onChange={(e) => onChange(Number(e.target.value))}>
      {THROTTLE_KBPS.map((k) => (
        <option key={k} value={k}>
          {`${k} kbps`}
        </option>
      ))}
    </NativeSelect>
  )
}

/** A suggestion as a two-line option: the name on top; the value (when the name differs) and request count below. */
function suggestionOption(category: RuleCategory, sg: ValueSuggestion): PickerOption {
  const shownValue = category === 'asn' ? `AS${sg.value}` : sg.value
  const label = sg.label ?? shownValue
  const detail = [label !== shownValue ? shownValue : null, sg.requests ? `${sg.requests.toLocaleString()} requests` : null]
    .filter(Boolean)
    .join(' · ')
  return {
    value: sg.value,
    label,
    detail: detail || undefined,
    selectedText: category === 'country' ? `${label} (${sg.value})` : undefined,
  }
}

function CategoryPanel({
  category,
  rows,
  suggestions,
  onChange,
}: {
  category: RuleCategory
  rows: RuleItem[]
  suggestions: ValueSuggestion[]
  onChange: (rows: RuleItem[]) => void
}) {
  const [value, setValue] = React.useState('')
  const [action, setAction] = React.useState<RuleAction>('block')
  const [kbps, setKbps] = React.useState<number>(THROTTLE_KBPS[1])
  const [error, setError] = React.useState<string | null>(null)
  const options = React.useMemo(() => suggestions.map((sg) => suggestionOption(category, sg)), [category, suggestions])

  const add = () => {
    const normalized = normalizeValue(category, value)
    if (!normalized) return
    if (category === 'country' && !/^[A-Z]{2}$/.test(normalized)) {
      setError('Use a two-letter country code, such as US.')
      return
    }
    if (category === 'asn' && !/^\d+$/.test(normalized)) {
      setError('Use the ASN number, such as 7922.')
      return
    }
    if (action === 'pace' && kbps <= 0) {
      setError('Enter a rate in kbps.')
      return
    }
    const label = category === 'asn' ? suggestions.find((sg) => sg.value === normalized)?.label : undefined
    const item: RuleItem = { value: normalized, action, kbps: action === 'pace' ? kbps : 0, ...(label ? { label } : {}) }
    // Adding an existing value replaces its rule.
    onChange([...rows.filter((r) => r.value !== normalized), item])
    setValue('')
    setError(null)
  }

  const patch = (index: number, next: Partial<RuleItem>) =>
    onChange(rows.map((r, i) => (i === index ? { ...r, ...next } : r)))

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-start gap-2">
        <ValuePicker
          ariaLabel={`Add ${SINGULAR[category]}`}
          value={value}
          options={options}
          allowCustom={category !== 'country'}
          mono={category !== 'country'}
          className="w-56"
          placeholder={category === 'country' ? 'Select a country…' : `Select or enter a ${SINGULAR[category]}…`}
          searchPlaceholder={category === 'country' ? 'Search countries…' : `Search or type a ${SINGULAR[category]}…`}
          emptyText={category === 'country' ? 'No country found.' : 'Type a value to use it.'}
          onChange={(next) => {
            setValue(next)
            setError(null)
          }}
        />
        <NativeSelect
          aria-label={`Action for new ${SINGULAR[category]}`}
          value={action}
          onChange={(e) => setAction(e.target.value as RuleAction)}
        >
          {(Object.keys(ACTION_LABELS) as RuleAction[]).map((a) => (
            <option key={a} value={a}>
              {ACTION_LABELS[a]}
            </option>
          ))}
        </NativeSelect>
        {action === 'pace' && <ThrottleSelect label="Throttle rate for the new value in kbps" value={kbps} onChange={setKbps} />}
        <Button variant="outline" size="sm" className="h-8" onClick={add} disabled={!value.trim()}>
          <Plus className="h-3.5 w-3.5 mr-1" aria-hidden="true" />
          Add
        </Button>
      </div>
      {error && (
        <p role="alert" className="text-xs text-destructive">
          {error}
        </p>
      )}
      {rows.length ? (
        <ul className="divide-y rounded-md border" aria-label={`${TAB_LABELS[category]} rules`}>
          {rows.map((r, i) => (
            <li key={r.value} className="flex flex-wrap items-center gap-2 px-2.5 py-1.5">
              <span className="min-w-0 flex-1 truncate font-mono text-xs" title={r.value}>
                {category === 'country'
                  ? `${resolveCountryName(r.value)} (${r.value})`
                  : category === 'asn'
                    ? (r.label ?? `AS${r.value}`)
                    : r.value}
              </span>
              <NativeSelect
                aria-label={`Action for ${r.value}`}
                value={r.action}
                onChange={(e) => {
                  const next = e.target.value as RuleAction
                  patch(i, { action: next, kbps: next === 'pace' ? (r.kbps > 0 ? r.kbps : THROTTLE_KBPS[1]) : 0 })
                }}
              >
                {(Object.keys(ACTION_LABELS) as RuleAction[]).map((a) => (
                  <option key={a} value={a}>
                    {ACTION_LABELS[a]}
                  </option>
                ))}
              </NativeSelect>
              {r.action === 'pace' && (
                <ThrottleSelect label={`Throttle rate for ${r.value} in kbps`} value={r.kbps} onChange={(k) => patch(i, { kbps: k })} />
              )}
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                aria-label={`Remove ${r.value}`}
                onClick={() => onChange(rows.filter((_, j) => j !== i))}
              >
                <X className="h-3.5 w-3.5" aria-hidden="true" />
              </Button>
            </li>
          ))}
        </ul>
      ) : (
        <p className="rounded-md border border-dashed px-3 py-4 text-center text-xs text-muted-foreground">
          No {TAB_LABELS[category].toLowerCase()} yet.
        </p>
      )}
    </div>
  )
}

/** Debounced "matches these countries" preview for the mass-block regex. */
function RegexPreview({ pattern }: { pattern: string }) {
  const debounced = useDebounce(pattern.trim(), 300)
  const query = useQuery({
    queryKey: ['content-security', 'regex-preview', debounced],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/country-regex/preview', {
        signal,
        params: { query: { pattern: debounced } },
      })
      if (error) throw error
      return data
    },
    enabled: debounced !== '',
    staleTime: 60_000,
  })

  let body: React.ReactNode
  if (pattern.trim() === '') {
    body = 'Optional. Blocks every country whose two-letter code matches, for example ^(CN|RU)$.'
  } else if (debounced !== pattern.trim() || query.isLoading) {
    body = 'Checking…'
  } else if (query.error) {
    body = <span className="text-destructive">Could not preview this pattern.</span>
  } else if (query.data && !query.data.valid) {
    body = <span className="text-destructive">Invalid pattern: {query.data.error ?? 'it cannot be used'}</span>
  } else if (query.data) {
    const matches = query.data.matches
    body = matches.length ? (
      <>
        Matches {matches.length.toLocaleString()} {matches.length === 1 ? 'country' : 'countries'}:{' '}
        {matches.map((code) => (
          <span key={code} className="mr-1 inline-block font-mono" title={resolveCountryName(code)}>
            {code}
          </span>
        ))}
      </>
    ) : (
      'Matches no countries.'
    )
  }
  return (
    <p id="cs-regex-preview" aria-live="polite" className="text-xs text-muted-foreground">
      {body}
    </p>
  )
}

function RevisionHistory({ policyId, timezone }: { policyId: string; timezone: string }) {
  const [shown, setShown] = React.useState(false)
  const query = useQuery({
    queryKey: ['content-security', 'revisions', policyId],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/policies/{policy_id}/revisions', {
        signal,
        params: { path: { policy_id: policyId } },
      })
      if (error) throw error
      return data
    },
    enabled: shown,
  })
  return (
    <section className="space-y-2">
      <Button variant="ghost" size="sm" className="h-7 px-1 text-xs" aria-expanded={shown} onClick={() => setShown((s) => !s)}>
        <History className="h-3.5 w-3.5 mr-1" aria-hidden="true" />
        {shown ? 'Hide history' : 'Show history'}
      </Button>
      {shown &&
        (query.isLoading ? (
          <p className="text-xs text-muted-foreground">Loading history…</p>
        ) : query.error ? (
          <p role="alert" className="text-xs text-destructive">
            Could not load the revision history.
          </p>
        ) : query.data?.revisions.length ? (
          <ol aria-label="Revision history" className="divide-y rounded-md border text-xs">
            {query.data.revisions.map((r) => (
              <li key={r.rev} className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 px-2.5 py-1.5">
                <span className="font-medium tabular-nums">Rev {r.rev}</span>
                <span>{r.changed_by}</span>
                <span className="tabular-nums text-muted-foreground">
                  {formatDate(r.changed_at, timezone, 'yyyy-MM-dd HH:mm:ss')}
                </span>
                <span className="min-w-0 truncate text-muted-foreground">{summarizePolicy(r.policy)}</span>
              </li>
            ))}
          </ol>
        ) : (
          <p className="text-xs text-muted-foreground">No revisions yet.</p>
        ))}
    </section>
  )
}

const STATUS_VARIANT = { active: 'success', scheduled: 'secondary', ended: 'ghost' } as const

/**
 * Create or edit a policy: name, country regex, per-category rules (block / throttle / allow) and
 * individual subscriber / IP blocks. Editing applies in place everywhere the policy is scheduled.
 */
export function PolicyEditorDialog({
  policy,
  suggestions,
  timezone,
  onSave,
  onClose,
}: {
  /** The policy being edited; null creates a new one. */
  policy: Policy | null
  suggestions: Record<RuleCategory, ValueSuggestion[]>
  timezone: string
  onSave: (policyId: string | null, body: PolicyBody) => Promise<unknown>
  /** Closes the editor (parent clears its state). */
  onClose: () => void
}) {
  const [initial] = React.useState(() => draftFrom(policy))
  const [draft, setDraft] = React.useState<Draft>(initial)
  const [tab, setTab] = React.useState<RuleCategory | 'individuals'>('country')
  const [busy, setBusy] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const [confirmDiscard, setConfirmDiscard] = React.useState(false)

  const body = toBody(draft)
  const dirty = JSON.stringify(body) !== JSON.stringify(toBody(initial))
  const badIps = body.individual.ip.filter((ip) => !looksLikeIp(ip))
  const badPace = RULE_CATEGORIES.some((c) => draft.rules[c].some((r) => r.action === 'pace' && r.kbps <= 0))
  const canSave = !busy && body.name !== '' && badIps.length === 0 && !badPace && (dirty || policy == null)

  // Browser-level guard while there are unsaved edits.
  React.useEffect(() => {
    if (!dirty) return
    const handler = (e: BeforeUnloadEvent) => {
      e.preventDefault()
    }
    window.addEventListener('beforeunload', handler)
    return () => window.removeEventListener('beforeunload', handler)
  }, [dirty])

  const requestClose = () => {
    if (busy) return
    if (dirty) setConfirmDiscard(true)
    else onClose()
  }

  const save = async () => {
    if (!canSave) return
    setBusy(true)
    setError(null)
    try {
      await onSave(policy?.id ?? null, body)
      onClose()
    } catch (e) {
      setError(extractApiError(e))
    } finally {
      setBusy(false)
    }
  }

  const setRules = (category: RuleCategory) => (rows: RuleItem[]) =>
    setDraft((d) => ({ ...d, rules: { ...d.rules, [category]: rows } }))
  const individualCount = body.individual.subscriber_id.length + body.individual.ip.length
  const usedBy = policy?.used_by ?? []

  return (
    <>
      <Dialog open onOpenChange={(next) => !next && requestClose()}>
        <DialogContent className="max-h-[90vh] max-w-3xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="text-base">{policy ? `Edit policy: ${policy.name}` : 'New policy'}</DialogTitle>
            <DialogDescription>
              A policy is a named set of rules for this service. It only takes effect once it is assigned to content.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-5 pt-3">
            <div className="space-y-2">
              <Label htmlFor="cs-policy-name">Name</Label>
              <Input
                id="cs-policy-name"
                className="max-w-sm"
                value={draft.name}
                onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value }))}
                maxLength={120}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="cs-policy-regex">Mass-block countries by regex</Label>
              <Input
                id="cs-policy-regex"
                aria-describedby="cs-regex-preview"
                className="max-w-sm font-mono text-xs"
                placeholder="^(CN|RU|KP)$"
                spellCheck={false}
                value={draft.regex}
                onChange={(e) => setDraft((d) => ({ ...d, regex: e.target.value }))}
              />
              <RegexPreview pattern={draft.regex} />
            </div>

            {usedBy.length > 0 && (
              <div role="note" className="space-y-1.5 rounded-md border border-amber-500/40 bg-amber-500/10 p-2.5 text-xs">
                <p className="text-amber-900 dark:text-amber-300">
                  <strong>Used by {usedBy.length}</strong> {usedBy.length === 1 ? 'schedule entry' : 'schedule entries'}. Editing applies to
                  all of them immediately.
                </p>
                <ul aria-label="Used by" className="flex flex-wrap gap-1.5">
                  {usedBy.map((u) => (
                    <li key={u.assignment_id}>
                      <Badge variant={STATUS_VARIANT[u.status]}>
                        {describeScope(u.scope_type, u.content_id)} · {modeLabel(u.mode)} · {u.status}
                      </Badge>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            <Tabs value={tab} onValueChange={(v) => setTab(v as RuleCategory | 'individuals')}>
              <TabsList className="h-auto flex-wrap group-data-horizontal/tabs:h-auto">
                {RULE_CATEGORIES.map((c) => (
                  <TabsTrigger key={c} value={c}>
                    {TAB_LABELS[c]}
                    {draft.rules[c].length > 0 && (
                      <span className="ml-1 rounded-full bg-background/70 px-1.5 text-[11px] tabular-nums">
                        {draft.rules[c].length}
                        <span className="sr-only"> rules</span>
                      </span>
                    )}
                  </TabsTrigger>
                ))}
                <TabsTrigger value="individuals">
                  {TAB_LABELS.individuals}
                  {individualCount > 0 && (
                    <span className="ml-1 rounded-full bg-background/70 px-1.5 text-[11px] tabular-nums">
                      {individualCount}
                      <span className="sr-only"> rules</span>
                    </span>
                  )}
                </TabsTrigger>
              </TabsList>
              {RULE_CATEGORIES.map((c) => (
                <TabsContent key={c} value={c} className="pt-2">
                  <CategoryPanel category={c} rows={draft.rules[c]} suggestions={suggestions[c]} onChange={setRules(c)} />
                </TabsContent>
              ))}
              <TabsContent value="individuals" className="space-y-4 pt-2">
                <p className="text-xs text-muted-foreground">
                  Individual blocks only block: they cannot be throttled or allowed, and they take precedence over Allow rules.
                  Subscriber IDs are case-sensitive.
                </p>
                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="space-y-1.5">
                    <Label htmlFor="cs-policy-subscribers">Subscriber IDs (one per line)</Label>
                    <Textarea
                      id="cs-policy-subscribers"
                      className="min-h-32 font-mono text-xs"
                      spellCheck={false}
                      value={draft.subscribers}
                      onChange={(e) => setDraft((d) => ({ ...d, subscribers: e.target.value }))}
                    />
                    <p className="text-[11px] text-muted-foreground">{parseLines(draft.subscribers).length.toLocaleString()} blocked</p>
                  </div>
                  <div className="space-y-1.5">
                    <Label htmlFor="cs-policy-ips">IP addresses (one per line)</Label>
                    <Textarea
                      id="cs-policy-ips"
                      className={cn('min-h-32 font-mono text-xs')}
                      spellCheck={false}
                      aria-invalid={badIps.length > 0 || undefined}
                      aria-describedby={badIps.length ? 'cs-policy-ips-error' : undefined}
                      value={draft.ips}
                      onChange={(e) => setDraft((d) => ({ ...d, ips: e.target.value }))}
                    />
                    <p className="text-[11px] text-muted-foreground">{parseLines(draft.ips).length.toLocaleString()} blocked</p>
                    {badIps.length > 0 && (
                      <p id="cs-policy-ips-error" role="alert" className="text-xs text-destructive">
                        Not an IPv4 or IPv6 address: {badIps.slice(0, 3).join(', ')}
                        {badIps.length > 3 ? ` and ${badIps.length - 3} more` : ''}
                      </p>
                    )}
                  </div>
                </div>
              </TabsContent>
            </Tabs>
            <p className="text-[11px] text-muted-foreground">
              Throttle rates are 128, 256 or 512 kbps.
            </p>

            {policy && <RevisionHistory policyId={policy.id} timezone={timezone} />}

            {error && (
              <p role="alert" className="text-xs text-destructive">
                {error}
              </p>
            )}
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={requestClose} disabled={busy}>
              Cancel
            </Button>
            <Button onClick={() => void save()} disabled={!canSave}>
              {busy && <Loader2 className="h-3 w-3 mr-1 animate-spin" />}
              {busy ? 'Saving…' : policy ? 'Save policy' : 'Create policy'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <ConfirmDialog
        open={confirmDiscard}
        onOpenChange={setConfirmDiscard}
        title="Discard unsaved changes?"
        description="Your edits to this policy have not been saved."
        confirmLabel="Discard changes"
        cancelLabel="Keep editing"
        isDangerous
        onConfirm={() => {
          setConfirmDiscard(false)
          onClose()
        }}
      />
    </>
  )
}
