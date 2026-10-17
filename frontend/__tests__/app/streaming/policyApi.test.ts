import { expect, test } from 'vitest'
import { isoToLocalInput, localInputToIso } from '@/app/streaming/_sections/tabs/AssignmentDialog'
import { looksLikeIp, parseLines } from '@/app/streaming/_sections/tabs/PolicyEditorDialog'
import {
  buildRuleLookup,
  describeRule,
  listPolicyRules,
  normalizeValue,
  removeLabel,
  summarizePolicy,
  type Policy,
} from '@/app/streaming/_sections/tabs/policyApi'

const EMPTY = { country: [], asn: [], referer: [], host: [], proxy_type: [], proxy_description: [] }

function policy(rules: Partial<Policy['rules']> = {}, individual: Partial<Policy['individual']> = {}, country_regex: string | null = null): Policy {
  return {
    id: 'p1',
    name: 'P',
    country_regex,
    rev: 1,
    rules: { ...EMPTY, ...rules },
    individual: { subscriber_id: [], ip: [], ...individual },
    created_at: '',
    created_by: '',
    updated_at: '',
    updated_by: '',
    used_by: [],
  }
}

test('normalizeValue mirrors the server: country upper, ASN bare number, others lower, subscriber ID untouched', () => {
  expect(normalizeValue('country', ' us ')).toBe('US')
  expect(normalizeValue('asn', 'AS7922')).toBe('7922')
  expect(normalizeValue('referer', 'Pirate.Example')).toBe('pirate.example')
  expect(normalizeValue('proxy_type', 'Hosting')).toBe('hosting')
  expect(normalizeValue('subscriber_id', ' Acct-1 ')).toBe('Acct-1')
})

test('buildRuleLookup finds block, pace, exempt and individual rules by normalised value', () => {
  const lookup = buildRuleLookup(
    policy(
      {
        country: [
          { value: 'US', action: 'exempt', kbps: 0 },
          { value: 'BR', action: 'pace', kbps: 256 },
        ],
        referer: [{ value: 'pirate.example', action: 'block', kbps: 0 }],
      },
      { subscriber_id: ['Acct-1'], ip: ['203.0.113.9'] },
    ),
  )
  expect(lookup('country', 'us')).toEqual({ action: 'exempt', kbps: 0 })
  expect(lookup('country', 'BR')).toEqual({ action: 'pace', kbps: 256 })
  expect(lookup('referer', 'Pirate.Example')).toEqual({ action: 'block', kbps: 0 })
  expect(lookup('subscriber_id', 'Acct-1')).toEqual({ action: 'block', kbps: 0 })
  expect(lookup('subscriber_id', 'acct-1')).toBeUndefined()
  expect(lookup('ip', '203.0.113.9')).toEqual({ action: 'block', kbps: 0 })
  expect(lookup('asn', '7922')).toBeUndefined()
  expect(buildRuleLookup(null)('country', 'US')).toBeUndefined()
})

test('listPolicyRules flattens every category and the individual lists', () => {
  const rules = listPolicyRules(policy({ asn: [{ value: '7922', action: 'block', kbps: 0 }] }, { ip: ['203.0.113.9'] }))
  expect(rules).toEqual([
    { category: 'asn', value: '7922', action: 'block', kbps: 0 },
    { category: 'ip', value: '203.0.113.9', action: 'block', kbps: 0 },
  ])
  expect(listPolicyRules(undefined)).toEqual([])
})

test("listPolicyRules keeps an ASN rule's owner name", () => {
  const rules = listPolicyRules(policy({ asn: [{ value: '7922', action: 'block', kbps: 0, label: 'Comcast Cable (7922)' }] }))
  expect(rules[0]).toMatchObject({ category: 'asn', value: '7922', label: 'Comcast Cable (7922)' })
})

test('describeRule and removeLabel name each action', () => {
  expect(describeRule({ action: 'block', kbps: 0 })).toBe('Blocked')
  expect(describeRule({ action: 'pace', kbps: 128 })).toBe('Throttled to 128 kbps')
  expect(describeRule({ action: 'exempt', kbps: 0 })).toBe('Allowed')
  expect(removeLabel({ action: 'block', kbps: 0 })).toBe('Unblock')
  expect(removeLabel({ action: 'pace', kbps: 128 })).toBe('Remove throttle')
  expect(removeLabel({ action: 'exempt', kbps: 0 })).toBe('Remove allow')
})

test('summarizePolicy gives a one-line digest', () => {
  expect(summarizePolicy(policy())).toBe('No rules')
  expect(
    summarizePolicy(
      policy(
        {
          country: [
            { value: 'BR', action: 'block', kbps: 0 },
            { value: 'CN', action: 'pace', kbps: 128 },
            { value: 'US', action: 'exempt', kbps: 0 },
          ],
          asn: [{ value: '7922', action: 'block', kbps: 0 }],
        },
        { subscriber_id: ['a', 'b'], ip: ['203.0.113.9'] },
        '^(CN)$',
      ),
    ),
  ).toBe('regex ^(CN)$, 2 countries, 1 ASN, 1 allowed, 2 subscribers, 1 IP')
})

test('parseLines trims, drops blanks and duplicates, keeps case', () => {
  expect(parseLines(' A \r\n\nA\nb\n  \nB')).toEqual(['A', 'b', 'B'])
})

test('looksLikeIp accepts IPv4 and IPv6 shapes and rejects the rest', () => {
  for (const ok of ['203.0.113.9', '0.0.0.0', '2001:db8::1', '::1', '::ffff:1.2.3.4']) expect(looksLikeIp(ok)).toBe(true)
  for (const bad of ['256.1.1.1', '1.2.3', 'not-an-ip', 'example.com', '1.2.3.4/24']) expect(looksLikeIp(bad)).toBe(false)
})

test('datetime-local values round-trip through ISO UTC in the browser timezone', () => {
  const iso = localInputToIso('2026-10-20T18:00')
  expect(iso).toBe(new Date('2026-10-20T18:00').toISOString())
  expect(isoToLocalInput(iso)).toBe('2026-10-20T18:00')
  expect(localInputToIso('')).toBeNull()
  expect(localInputToIso('garbage')).toBeNull()
  expect(isoToLocalInput(null)).toBe('')
})
