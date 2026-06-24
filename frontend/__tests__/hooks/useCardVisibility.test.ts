/**
 * Persistence + migration semantics for the localStorage-backed
 * card-visibility hook. Several admin/analyst surfaces (raw-logs columns,
 * dashboard cards) depend on the synchronous initializer correctly
 * hydrating from localStorage so the first paint doesn't flash a wrong
 * set of cards.
 *
 * @vitest-environment jsdom
 */
import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, beforeEach } from 'vitest'
import { useCardVisibility } from '@/hooks/useCardVisibility'

const STORAGE_KEY = 'test-card-visibility'
const MIGRATION_KEY = `${STORAGE_KEY}_mv`
const ALL_IDS = ['a', 'b', 'c', 'd']

describe('useCardVisibility', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('cold mount with no stored value returns the default set (all ids when no default passed)', () => {
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    expect([...result.current.visibleCards].sort()).toEqual([...ALL_IDS].sort())
  })

  it('cold mount with no stored value honours the explicit defaultIds', () => {
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS, ['a', 'b']))
    expect([...result.current.visibleCards].sort()).toEqual(['a', 'b'])
  })

  it('cold mount reads the persisted set from localStorage', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['b', 'c']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    expect([...result.current.visibleCards].sort()).toEqual(['b', 'c'])
  })

  it('toggleCard adds a missing id and persists', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    act(() => result.current.toggleCard('b'))
    expect(result.current.visibleCards.has('b')).toBe(true)
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted.sort()).toEqual(['a', 'b'])
  })

  it('toggleCard removes an existing id and persists', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    act(() => result.current.toggleCard('a'))
    expect(result.current.visibleCards.has('a')).toBe(false)
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted).toEqual(['b'])
  })

  it('showAll resets visibility to every id and persists', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS))
    act(() => result.current.showAll())
    expect([...result.current.visibleCards].sort()).toEqual([...ALL_IDS].sort())
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted.sort()).toEqual([...ALL_IDS].sort())
  })

  it('reset reverts to the default set and persists it', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b', 'c', 'd']))
    const { result } = renderHook(() => useCardVisibility(STORAGE_KEY, ALL_IDS, ['a']))
    act(() => result.current.reset())
    expect([...result.current.visibleCards]).toEqual(['a'])
    const persisted = JSON.parse(localStorage.getItem(STORAGE_KEY)!)
    expect(persisted).toEqual(['a'])
  })

  it('migration strips removeIds when storedVersion < migration.version', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b', 'c']))
    localStorage.setItem(MIGRATION_KEY, '0')
    const { result } = renderHook(() =>
      useCardVisibility(STORAGE_KEY, ALL_IDS, undefined, {
        version: 2,
        removeIds: ['b'],
      }),
    )
    // The initial render returns the raw persisted set; the post-mount
    // useEffect re-runs load() and applies the migration. Reading the
    // hook value here reflects the post-effect state.
    expect(result.current.visibleCards.has('b')).toBe(false)
    expect(result.current.visibleCards.has('a')).toBe(true)
    expect(localStorage.getItem(MIGRATION_KEY)).toBe('2')
  })

  it('migration adds addIds when storedVersion < migration.version', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a']))
    localStorage.setItem(MIGRATION_KEY, '0')
    const { result } = renderHook(() =>
      useCardVisibility(STORAGE_KEY, ALL_IDS, undefined, {
        version: 3,
        addIds: ['d'],
      }),
    )
    expect(result.current.visibleCards.has('d')).toBe(true)
    expect(result.current.visibleCards.has('a')).toBe(true)
    expect(localStorage.getItem(MIGRATION_KEY)).toBe('3')
  })

  it('migration is a no-op when storedVersion already at target', () => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(['a', 'b']))
    localStorage.setItem(MIGRATION_KEY, '5')
    const { result } = renderHook(() =>
      useCardVisibility(STORAGE_KEY, ALL_IDS, undefined, {
        version: 5,
        removeIds: ['a'],
      }),
    )
    // 'a' should still be present — migration version matches, no prune.
    expect(result.current.visibleCards.has('a')).toBe(true)
    expect(result.current.visibleCards.has('b')).toBe(true)
  })
})
