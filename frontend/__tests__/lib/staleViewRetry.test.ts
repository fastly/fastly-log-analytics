import { describe, expect, it } from 'vitest'

import {
  throwIfStaleAggregates,
  isStaleDashboardViewError,
  STALE_VIEW_RETRY_OPTIONS,
  STALE_VIEW_POLL_MS,
} from '@/lib/staleViewRetry'

// Helpers ---------------------------------------------------------------

const EMPTY_FIELDS = {
  status: { total: 0, top: [] },
  country: { total: 0, top: [] },
}

const NONEMPTY_FIELDS = {
  status: { total: 12, top: [{ value: '200', count: 12 }] },
}

/** Build an aggregates-shaped response. */
function aggregates(partial: Record<string, unknown>): Record<string, unknown> {
  return {
    data: EMPTY_FIELDS,
    time_series: [],
    map_data: [],
    ...partial,
  }
}

// The discriminator is exercised through throwIfStaleAggregates: it throws
// the StaleDashboardViewError when the response looks like the stale-view
// symptom, and returns the data otherwise.
function isFlaggedStale(data: unknown, window?: { startTime?: string | null; endTime?: string | null }): boolean {
  try {
    throwIfStaleAggregates(data, window)
    return false
  } catch (err) {
    expect(isStaleDashboardViewError(err)).toBe(true)
    return true
  }
}

// A window that comfortably contains "now"-ish data.
const WIDE_WINDOW = { startTime: '2026-06-21T00:00:00.000Z', endTime: '2026-06-22T23:59:59.999Z' }

describe('throwIfStaleAggregates discriminator', () => {
  it('does not flag a response with no latest_log_at (service has no data)', () => {
    expect(isFlaggedStale(aggregates({ latest_log_at: null }), WIDE_WINDOW)).toBe(false)
    expect(isFlaggedStale(aggregates({}), WIDE_WINDOW)).toBe(false)
  })

  it('does not flag a non-empty response', () => {
    const data = aggregates({
      latest_log_at: '2026-06-22T12:00:00.000Z',
      data: NONEMPTY_FIELDS,
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(false)
  })

  it('does not flag when time_series has points even if fields are empty', () => {
    const data = aggregates({
      latest_log_at: '2026-06-22T12:00:00.000Z',
      time_series: [{ time: '2026-06-22T12:00:00.000Z', value: 5 }],
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(false)
  })

  it('flags an empty response when latest_log_at falls INSIDE the queried window', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-21T06:00:00.000Z',
      latest_log_at: '2026-06-22T12:00:00.000Z',
    })
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(true)
  })

  it('does NOT flag an empty response when the data span is entirely OUTSIDE the window (fresh-install false positive)', () => {
    // Data is from a few days ago; the default last-24h window predates it.
    const data = aggregates({
      earliest_log_at: '2026-06-18T00:00:00.000Z',
      latest_log_at: '2026-06-19T00:00:00.000Z',
    })
    const defaultWindow = { startTime: '2026-06-21T00:00:00.000Z', endTime: '2026-06-22T00:00:00.000Z' }
    expect(isFlaggedStale(data, defaultWindow)).toBe(false)
  })

  it('does NOT flag when data is newer than the queried window (window ends before the data)', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-22T10:00:00.000Z',
      latest_log_at: '2026-06-22T12:00:00.000Z',
    })
    const pastWindow = { startTime: '2026-06-20T00:00:00.000Z', endTime: '2026-06-21T00:00:00.000Z' }
    expect(isFlaggedStale(data, pastWindow)).toBe(false)
  })

  it('handles date-only ("YYYY-MM-DD") extents — overlapping window flags, non-overlapping does not', () => {
    const data = aggregates({
      earliest_log_at: '2026-06-22',
      latest_log_at: '2026-06-22',
    })
    // 2026-06-22 (whole UTC day) overlaps WIDE_WINDOW → stale symptom.
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(true)
    // A window two days earlier does not overlap the 06-22 day → legitimate empty.
    const earlierWindow = { startTime: '2026-06-19T00:00:00.000Z', endTime: '2026-06-20T00:00:00.000Z' }
    expect(isFlaggedStale(data, earlierWindow)).toBe(false)
  })

  it('falls back to the window-agnostic emptiness check when no window is supplied', () => {
    const data = aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' })
    expect(isFlaggedStale(data)).toBe(true)
  })

  it('does NOT flag an empty result when a filter is active (filtered → legitimate empty)', () => {
    // Exact shape that DOES flag stale when unfiltered: latest_log_at inside
    // the window, every field empty. With hasFilters=true it's a legitimate
    // "no rows match the filter", not the stale-view symptom — so it must NOT
    // throw (the fix for the masked-IP-filter infinite-retry / "Preparing your
    // data" banner).
    const data = aggregates({
      earliest_log_at: '2026-06-21T06:00:00.000Z',
      latest_log_at: '2026-06-22T12:00:00.000Z',
    })
    // sanity: unfiltered, this same response IS flagged as stale.
    expect(isFlaggedStale(data, WIDE_WINDOW)).toBe(true)
    // filtered: not flagged, returns the data object unchanged.
    expect(throwIfStaleAggregates(data, WIDE_WINDOW, true)).toBe(data)
  })

  it('returns the original data object unchanged when not stale', () => {
    const data = aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z', data: NONEMPTY_FIELDS })
    expect(throwIfStaleAggregates(data, WIDE_WINDOW)).toBe(data)
  })
})

describe('isStaleDashboardViewError', () => {
  it('recognises the stale-view error and rejects others', () => {
    let caught: unknown
    try {
      throwIfStaleAggregates(aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' }), WIDE_WINDOW)
    } catch (err) {
      caught = err
    }
    expect(isStaleDashboardViewError(caught)).toBe(true)
    expect(isStaleDashboardViewError(new Error('boom'))).toBe(false)
    expect(isStaleDashboardViewError(null)).toBe(false)
    expect(isStaleDashboardViewError('nope')).toBe(false)
  })
})

describe('STALE_VIEW_RETRY_OPTIONS', () => {
  const staleErr = (() => {
    try {
      throwIfStaleAggregates(aggregates({ latest_log_at: '2026-06-22T12:00:00.000Z' }), WIDE_WINDOW)
    } catch (e) {
      return e as Error
    }
    throw new Error('expected a stale error')
  })()

  it('retries the stale-view error up to twice, then stops', () => {
    expect(STALE_VIEW_RETRY_OPTIONS.retry(0, staleErr)).toBe(true)
    expect(STALE_VIEW_RETRY_OPTIONS.retry(1, staleErr)).toBe(true)
    expect(STALE_VIEW_RETRY_OPTIONS.retry(2, staleErr)).toBe(false)
  })

  it('does not retry non-stale errors', () => {
    expect(STALE_VIEW_RETRY_OPTIONS.retry(0, new Error('500'))).toBe(false)
  })

  it('slow-polls only while parked on a stale-view error', () => {
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: staleErr } })).toBe(STALE_VIEW_POLL_MS)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: new Error('500') } })).toBe(false)
    expect(STALE_VIEW_RETRY_OPTIONS.refetchInterval({ state: { error: null } })).toBe(false)
  })
})
