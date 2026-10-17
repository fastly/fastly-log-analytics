'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { showToast, showToastWithAction } from '@/lib/toast'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import type { components } from '@/types/api.generated'

export type Policy = components['schemas']['ContentSecurityPolicy']
export type Assignment = components['schemas']['ContentSecurityAssignment']
export type PolicyBody = components['schemas']['PolicyBody']
export type AssignmentBody = components['schemas']['AssignmentBody']
export type RuleItem = components['schemas']['PolicyRuleItem']
export type PublishResult = components['schemas']['PublishResult']
export type EffectivePolicy = components['schemas']['EffectivePolicyResponse']
export type SyncStatus = components['schemas']['ContentSecuritySyncStatusResponse']
export type LiveRuleBody = components['schemas']['LiveRuleBody']
export type LiveRuleResult = components['schemas']['LiveRuleResult']
export type RuleAction = RuleItem['action']
export type BlockCategory = components['schemas']['RuleBody']['category']

/** Categories whose values can be blocked, throttled or allowed. */
export const RULE_CATEGORIES = ['country', 'referer', 'asn', 'host', 'proxy_type', 'proxy_description'] as const
export type RuleCategory = (typeof RULE_CATEGORIES)[number]
/** Individual categories are block-only and cannot be allowed. */
export const INDIVIDUAL_CATEGORIES = ['subscriber_id', 'ip'] as const
export type IndividualCategory = (typeof INDIVIDUAL_CATEGORIES)[number]

export const isRuleCategory = (c: BlockCategory): c is RuleCategory => (RULE_CATEGORIES as readonly string[]).includes(c)

export const BLOCK_CATEGORY_LABELS: Record<BlockCategory, string> = {
  country: 'Country',
  asn: 'ASN',
  referer: 'Referer',
  host: 'Host',
  proxy_type: 'Proxy type',
  proxy_description: 'Proxy description',
  subscriber_id: 'Subscriber ID',
  ip: 'IP address',
}

/** Throttle rates offered in the pickers, in kbps. */
export const THROTTLE_KBPS = [128, 256, 512] as const

/** Mirror of the server's value normalisation, so a leaderboard value finds its rule. */
export function normalizeValue(category: BlockCategory, value: string): string {
  const v = value.trim()
  switch (category) {
    case 'country':
      return v.toUpperCase()
    case 'asn':
      return v.replace(/^as/i, '')
    case 'subscriber_id':
      return v
    default:
      return v.toLowerCase()
  }
}

export interface RuleMark {
  action: RuleAction
  kbps: number
}

export type RuleLookup = (category: BlockCategory, value: string) => RuleMark | undefined

const ruleKey = (category: BlockCategory, value: string) => `${category}\u0000${value}`

/** One row of a policy, flattened across categories (individuals are block rules). */
export interface FlatRule {
  category: BlockCategory
  value: string
  action: RuleAction
  kbps: number
  /** ASN rules: the owner name, "Comcast Cable (7922)" (absent for other categories). */
  label?: string | null
}

export function listPolicyRules(policy: Policy | null | undefined): FlatRule[] {
  if (!policy) return []
  const out: FlatRule[] = []
  for (const category of RULE_CATEGORIES) {
    for (const r of policy.rules[category] ?? []) out.push({ category, value: r.value, action: r.action, kbps: r.kbps, label: r.label })
  }
  for (const category of INDIVIDUAL_CATEGORIES) {
    for (const value of policy.individual[category] ?? []) out.push({ category, value, action: 'block', kbps: 0 })
  }
  return out
}

/** Index a policy's rules so leaderboard rows can look up their mark. */
export function buildRuleLookup(policy: Policy | null | undefined): RuleLookup {
  const index = new Map(listPolicyRules(policy).map((r) => [ruleKey(r.category, r.value), { action: r.action, kbps: r.kbps }]))
  return (category, value) => index.get(ruleKey(category, normalizeValue(category, value)))
}

export function describeRule(rule: RuleMark): string {
  if (rule.action === 'exempt') return 'Allowed'
  return rule.action === 'block' ? 'Blocked' : `Throttled to ${rule.kbps} kbps`
}

/** Label for reverting a rule. */
export function removeLabel(rule: RuleMark): string {
  if (rule.action === 'exempt') return 'Remove allow'
  return rule.action === 'block' ? 'Unblock' : 'Remove throttle'
}

const plural = (n: number, one: string, many: string) => `${n.toLocaleString()} ${n === 1 ? one : many}`

/** "regex ^(CN)$, 3 countries, 2 ASNs, 1 allowed": a one-line digest of a policy. */
export function summarizePolicy(policy: Policy): string {
  const parts: string[] = []
  if (policy.country_regex) parts.push(`regex ${policy.country_regex}`)
  const nouns: Record<RuleCategory, [string, string]> = {
    country: ['country', 'countries'],
    referer: ['referer', 'referers'],
    asn: ['ASN', 'ASNs'],
    host: ['host', 'hosts'],
    proxy_type: ['proxy type', 'proxy types'],
    proxy_description: ['proxy description', 'proxy descriptions'],
  }
  let exempt = 0
  for (const category of RULE_CATEGORIES) {
    const rows = policy.rules[category] ?? []
    exempt += rows.filter((r) => r.action === 'exempt').length
    const active = rows.length - rows.filter((r) => r.action === 'exempt').length
    if (active) parts.push(plural(active, nouns[category][0], nouns[category][1]))
  }
  if (exempt) parts.push(`${exempt.toLocaleString()} allowed`)
  const subs = policy.individual.subscriber_id?.length ?? 0
  const ips = policy.individual.ip?.length ?? 0
  if (subs) parts.push(plural(subs, 'subscriber', 'subscribers'))
  if (ips) parts.push(plural(ips, 'IP', 'IPs'))
  return parts.length ? parts.join(', ') : 'No rules'
}

/** "All content" or "Content: ev-123": what a schedule entry (or policy use) applies to. */
export function describeScope(scopeType: 'default' | 'content_id', contentId?: string | null): string {
  return scopeType === 'default' ? 'All content' : `Content: ${contentId ?? ''}`
}

/** Plain-language names for the WARN / ENFORCE modes. */
export const MODE_LABELS = { WARN: 'Test only', ENFORCE: 'Enforce' } as const

export function modeLabel(mode: 'WARN' | 'ENFORCE'): string {
  return MODE_LABELS[mode]
}

// ── Queries ──────────────────────────────────────────────────────────────────
// Every key is ['content-security', <kind>, <serviceId>, ...]: useServiceQuery
// keys its placeholder data on index 2 (the service).

export const CS_KEY = ['content-security'] as const

export function usePolicies(activeServiceId: string | null, enabled = true) {
  return useServiceQuery(
    ['content-security', 'policies', activeServiceId],
    async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/policies', { signal })
      if (error) throw error
      return data
    },
    { enabled },
  )
}

export function useAssignments(activeServiceId: string | null, enabled = true) {
  return useServiceQuery(
    ['content-security', 'assignments', activeServiceId],
    async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/assignments', { signal })
      if (error) throw error
      return data
    },
    { enabled },
  )
}

/** The policy in effect now for `contentId` (its own schedule entry, else the all-content one). */
export function useEffectivePolicy(activeServiceId: string | null, contentId: string | null, enabled = true) {
  return useServiceQuery(
    ['content-security', 'effective', activeServiceId, contentId],
    async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/effective', {
        signal,
        params: { query: contentId ? { content_id: contentId } : {} },
      })
      if (error) throw error
      return data
    },
    { enabled },
  )
}

export function useSyncStatus(activeServiceId: string | null, enabled = true) {
  return useServiceQuery(
    ['content-security', 'sync-status', activeServiceId],
    async ({ signal }) => {
      const { data, error } = await client.GET('/api/admin/content-security/sync-status', { signal })
      if (error) throw error
      return data
    },
    { enabled },
  )
}

// ── Mutations ────────────────────────────────────────────────────────────────

/**
 * Write-side helpers. Every write returns `{..., publish}`: `failed` means the change is saved
 * but not yet live at the blocker, so the toast offers Retry (POST /publish).
 */
export function usePolicyActions() {
  const queryClient = useQueryClient()
  const invalidate = React.useCallback(() => queryClient.invalidateQueries({ queryKey: CS_KEY }), [queryClient])

  const publishNow = React.useCallback(async (): Promise<PublishResult> => {
    const { data, error } = await client.POST('/api/admin/content-security/publish')
    if (error) throw error
    await invalidate()
    return data
  }, [invalidate])

  const retry = React.useCallback(async () => {
    try {
      const result = await publishNow()
      if (result.status === 'published') showToast('Synced with the blocker. Changes are live.', 'success')
      else if (result.status === 'not_configured') showToast('No blocklist endpoint is configured.', 'warn')
      else showToast(`Still not live: ${result.error ?? 'the blocker rejected the update'}`, 'warn')
    } catch {
      // The api client already surfaced the failure.
    }
  }, [publishNow])

  /** Refresh queries, then tell the user whether the change is live. */
  const announce = React.useCallback(
    async (publish: PublishResult, message: string) => {
      await invalidate()
      if (publish.status === 'failed') {
        showToastWithAction(`${message}. Saved but not yet live: ${publish.error ?? 'the blocker rejected the update'}`, {
          actionLabel: 'Retry',
          kind: 'warn',
          durationMs: 10_000,
          onAction: () => void retry(),
        })
      } else if (publish.status === 'not_configured') {
        showToast(`${message}. No blocklist endpoint is configured, so it is not enforced yet. Set one in Admin → Overall Settings.`, 'warn')
      } else {
        showToast(message, 'success')
      }
    },
    [invalidate, retry],
  )

  const savePolicy = React.useCallback(
    async (policyId: string | null, body: PolicyBody) => {
      const res = policyId
        ? await client.PUT('/api/admin/content-security/policies/{policy_id}', { params: { path: { policy_id: policyId } }, body })
        : await client.POST('/api/admin/content-security/policies', { body })
      if (res.error) throw res.error
      await announce(res.data.publish, policyId ? `Saved policy ${res.data.policy.name}` : `Created policy ${res.data.policy.name}`)
      return res.data
    },
    [announce],
  )

  const deletePolicy = React.useCallback(
    async (policy: Policy) => {
      const { data, error } = await client.DELETE('/api/admin/content-security/policies/{policy_id}', {
        params: { path: { policy_id: policy.id } },
      })
      if (error) throw error
      await announce(data.publish, `Deleted policy ${policy.name}`)
      return data
    },
    [announce],
  )

  const saveAssignment = React.useCallback(
    async (assignmentId: string | null, body: AssignmentBody) => {
      const res = assignmentId
        ? await client.PUT('/api/admin/content-security/assignments/{assignment_id}', {
            params: { path: { assignment_id: assignmentId } },
            body,
          })
        : await client.POST('/api/admin/content-security/assignments', { body })
      if (res.error) throw res.error
      await announce(res.data.publish, assignmentId ? 'Saved scheduled policy' : 'Scheduled policy')
      return res.data
    },
    [announce],
  )

  const deleteAssignment = React.useCallback(
    async (assignment: Assignment) => {
      const { data, error } = await client.DELETE('/api/admin/content-security/assignments/{assignment_id}', {
        params: { path: { assignment_id: assignment.id } },
      })
      if (error) throw error
      await announce(data.publish, 'Removed from schedule')
      return data
    },
    [announce],
  )

  /** Remove one rule from a policy (also the Undo for a live rule). */
  const removeRule = React.useCallback(
    async (policyId: string, category: BlockCategory, value: string, message?: string) => {
      const { data, error } = await client.DELETE('/api/admin/content-security/policies/{policy_id}/rules', {
        params: { path: { policy_id: policyId }, query: { category, value } },
      })
      if (error) throw error
      await announce(data.publish, message ?? `Removed ${BLOCK_CATEGORY_LABELS[category]} ${value}`)
      return data
    },
    [announce],
  )

  return { invalidate, publishNow, retry, announce, savePolicy, deletePolicy, saveAssignment, deleteAssignment, removeRule }
}
