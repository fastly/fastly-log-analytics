'use client'

import { client } from '@/lib/api'
import { useServiceQuery } from '@/hooks/useServiceQuery'
import { ISO_COUNTRY_CODES } from '@/lib/iso-countries'
import { resolveCountryName } from '@/lib/format'
import { resolveRangeWire } from '@/lib/range-wire'
import type { components } from '@/types/api.generated'
import type { StreamingTabProps } from './QualityTab'
import type { RuleCategory } from './policyApi'

export type ContentSecurityData = components['schemas']['ContentSecurityResponse']

export const TOP_N = 10

/** One value offered while adding a rule; `label` is a friendlier name (an ASN owner, say). */
export interface ValueSuggestion {
  value: string
  label?: string
  /** Requests seen for the value in the selected range; absent when none were seen. */
  requests?: number
}

type DataProps = Pick<
  StreamingTabProps,
  'activeServiceId' | 'filterPayload' | 'startTime' | 'endTime' | 'relativeRange' | 'isAutoRange' | 'anchor'
> & { bucketSeconds: number }

/**
 * The Content Security tab's analytics payload. Shared by the Monitor view (which narrows it to
 * one content id and polls) and the Policies view (which only borrows the top lists and observed
 * content ids as suggestions).
 */
export function useContentSecurityData(
  props: DataProps,
  contentId: string | null,
  options?: { enabled?: boolean; refetchInterval?: number; topN?: number },
) {
  const topN = options?.topN ?? TOP_N
  const { activeServiceId, filterPayload, startTime, endTime, relativeRange, isAutoRange, anchor, bucketSeconds } = props
  const { rangeKey, rangeBody } = resolveRangeWire({ relativeRange, isAutoRange, startTime, endTime, anchor })
  return useServiceQuery<ContentSecurityData | undefined>(
    ['cmcd', 'content-security', activeServiceId, rangeKey, anchor, filterPayload, contentId, bucketSeconds, topN],
    async ({ signal }) => {
      const { data, error } = await client.POST('/api/cmcd/content-security', {
        signal,
        body: {
          filters: filterPayload,
          content_id: contentId,
          bucket_seconds: bucketSeconds,
          top_n: topN,
          ...rangeBody,
        },
      })
      if (error) throw error
      return data
    },
    { enabled: options?.enabled ?? true, refetchInterval: options?.refetchInterval },
  )
}

/** Every country, alphabetically by name, each with its request count when it had traffic in range. */
function countrySuggestions(seen: { value: string; requests: number }[] | undefined): ValueSuggestion[] {
  const requests = new Map<string, number>()
  for (const r of seen ?? []) requests.set(r.value.toUpperCase(), r.requests)
  // An observed code outside the ISO list (a reserved or user-assigned one) is still worth offering.
  const codes = new Set([...ISO_COUNTRY_CODES, ...requests.keys()].filter((c) => /^[A-Z]{2}$/.test(c)))
  return [...codes]
    .map((value) => ({ value, label: resolveCountryName(value), requests: requests.get(value) }))
    .sort((a, b) => a.label.localeCompare(b.label, 'en'))
}

/** Values already seen in the Security tab's top lists, per rule category (countries: all of them). */
export function suggestionsFromData(data: ContentSecurityData | undefined): Record<RuleCategory, ValueSuggestion[]> {
  const rows = (r: { value: string; label?: string | null; requests: number }[] | undefined): ValueSuggestion[] =>
    (r ?? []).map((x) => ({ value: x.value, label: x.label ?? undefined, requests: x.requests }))
  return {
    country: countrySuggestions(data?.top_countries),
    asn: rows(data?.top_asns),
    referer: rows(data?.top_referers),
    host: rows(data?.top_hosts),
    proxy_type: rows(data?.proxy_types),
    proxy_description: rows(data?.proxy_descriptions),
  }
}
