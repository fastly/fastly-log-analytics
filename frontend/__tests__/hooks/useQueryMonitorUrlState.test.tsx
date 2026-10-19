/**
 * @vitest-environment jsdom
 *
 * Deep-link contract for /admin/queries: shared ops links written before the
 * nuqs migration (`?q=…&kind=…&view=…&slow=…&db=…&nogroup=1`) must keep
 * hydrating the same state, and writes must strip defaults.
 */
import { act, renderHook, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { withNuqsTestingAdapter, type OnUrlUpdateFunction } from 'nuqs/adapters/testing'

import { useQueryMonitorUrlState } from '@/app/admin/queries/_hooks/useQueryMonitorUrlState'

const DEFAULT_SLOW = 500

function setup(search: string) {
  const onUrlUpdate = vi.fn<OnUrlUpdateFunction>()
  const hook = renderHook(() => useQueryMonitorUrlState(DEFAULT_SLOW), {
    wrapper: withNuqsTestingAdapter({ searchParams: search, onUrlUpdate, hasMemory: true }),
  })
  return { ...hook, onUrlUpdate }
}

describe('useQueryMonitorUrlState: legacy URLs parse', () => {
  it('a clean URL yields the defaults', () => {
    const { result } = setup('')
    expect(result.current).toMatchObject({
      search: '',
      kindFilter: 'all',
      dbFilter: 'all',
      viewMode: 'all',
      slowThresholdMs: DEFAULT_SLOW,
      groupCrons: true,
    })
  })

  it('hydrates every param from a fully-specified shared link', () => {
    const { result } = setup('?q=COMPACT&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1')
    expect(result.current).toMatchObject({
      search: 'COMPACT',
      kindFilter: 'cron',
      viewMode: 'live',
      slowThresholdMs: 250,
      dbFilter: 'DuckDB',
      groupCrons: false,
    })
  })

  it('ignores unknown or malformed values like the old hydration did', () => {
    const { result } = setup('?kind=nope&view=weird&slow=-5&db=Oracle&nogroup=0')
    expect(result.current).toMatchObject({
      kindFilter: 'all',
      viewMode: 'all',
      slowThresholdMs: DEFAULT_SLOW,
      dbFilter: 'all',
      groupCrons: true,
    })
  })

  it('parses slow with parseInt semantics (trailing junk tolerated)', () => {
    expect(setup('?slow=250ms').result.current.slowThresholdMs).toBe(250)
    expect(setup('?slow=abc').result.current.slowThresholdMs).toBe(DEFAULT_SLOW)
  })
})

describe('useQueryMonitorUrlState: writes', () => {
  it('writes non-default values with the legacy param names and formats', async () => {
    const { result, onUrlUpdate } = setup('')
    act(() => result.current.setSearch('COMPACT'))
    act(() => result.current.setKindFilter('cron'))
    act(() => result.current.setViewMode('past'))
    act(() => result.current.setSlowThresholdMs(250))
    act(() => result.current.setDbFilter('Postgres'))
    act(() => result.current.setGroupCrons(false))
    await waitFor(() => {
      expect(Object.fromEntries(onUrlUpdate.mock.calls.at(-1)![0].searchParams)).toEqual({
        q: 'COMPACT',
        kind: 'cron',
        view: 'past',
        slow: '250',
        db: 'Postgres',
        nogroup: '1',
      })
    })
    expect(onUrlUpdate.mock.calls.at(-1)![0].options.history).toBe('replace')
  })

  it('strips params that return to their defaults', async () => {
    const { result, onUrlUpdate } = setup('?q=x&kind=cron&view=live&slow=250&db=DuckDB&nogroup=1')
    act(() => result.current.setSearch(''))
    act(() => result.current.setKindFilter('all'))
    act(() => result.current.setViewMode('all'))
    act(() => result.current.setSlowThresholdMs(DEFAULT_SLOW))
    act(() => result.current.setDbFilter('all'))
    act(() => result.current.setGroupCrons(true))
    await waitFor(() => expect(onUrlUpdate.mock.calls.at(-1)![0].queryString).toBe(''))
  })

  it('setGroupCrons accepts an updater function', () => {
    const { result } = setup('')
    act(() => result.current.setGroupCrons((v) => !v))
    expect(result.current.groupCrons).toBe(false)
    act(() => result.current.setGroupCrons((v) => !v))
    expect(result.current.groupCrons).toBe(true)
  })
})
