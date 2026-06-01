import { describe, it, expect, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useCardVisibility } from '../hooks/useCardVisibility'

describe('useCardVisibility', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('uses defaultIds when no saved set exists', () => {
    const { result } = renderHook(() =>
      useCardVisibility('test_k', ['a', 'b', 'c'], ['a', 'b'])
    )
    expect([...result.current.visibleCards].sort()).toEqual(['a', 'b'])
  })

  it('reads from saved localStorage set, ignoring defaults', () => {
    localStorage.setItem('test_k', JSON.stringify(['c']))
    const { result } = renderHook(() =>
      useCardVisibility('test_k', ['a', 'b', 'c'], ['a', 'b'])
    )
    expect([...result.current.visibleCards]).toEqual(['c'])
  })

  describe('migration', () => {
    it('strips removeIds from saved set when version bumps from 0', () => {
      localStorage.setItem('test_k', JSON.stringify(['a', 'b', 'c', 'd']))
      renderHook(() =>
        useCardVisibility(
          'test_k', ['a', 'b', 'c', 'd'], undefined,
          { version: 1, removeIds: ['b', 'd'] }
        )
      )
      expect(JSON.parse(localStorage.getItem('test_k')!).sort()).toEqual(['a', 'c'])
      expect(localStorage.getItem('test_k_mv')).toBe('1')
    })

    it('adds addIds into saved set when version bumps from 0', () => {
      localStorage.setItem('test_k', JSON.stringify(['a', 'b']))
      renderHook(() =>
        useCardVisibility(
          'test_k', ['a', 'b', 'x', 'y'], undefined,
          { version: 1, addIds: ['x', 'y'] }
        )
      )
      expect(JSON.parse(localStorage.getItem('test_k')!).sort()).toEqual(['a', 'b', 'x', 'y'])
      expect(localStorage.getItem('test_k_mv')).toBe('1')
    })

    it('applies both removeIds and addIds in one migration pass', () => {
      localStorage.setItem(
        'test_k',
        JSON.stringify(['method', 'status', 'bw', 'rid', 'prid', 'waf_sig', 'waf_req_id'])
      )
      renderHook(() =>
        useCardVisibility(
          'test_k',
          ['method', 'status', 'bw', 'rid', 'prid', 'waf_sig', 'waf_req_id', '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'],
          undefined,
          {
            version: 2,
            removeIds: ['bw', 'rid', 'prid', 'waf_sig', 'waf_req_id'],
            addIds: ['_bot_name', '_ngwaf_bot_name', 'waf_sig_ind'],
          }
        )
      )
      const saved = JSON.parse(localStorage.getItem('test_k')!).sort()
      expect(saved).toEqual(['_bot_name', '_ngwaf_bot_name', 'method', 'status', 'waf_sig_ind'])
      expect(localStorage.getItem('test_k_mv')).toBe('2')
    })

    it('does not re-run migration when version matches', () => {
      localStorage.setItem('test_k', JSON.stringify(['a', 'b']))
      localStorage.setItem('test_k_mv', '1')

      // User toggled 'x' on AFTER the migration ran — must not be undone
      localStorage.setItem('test_k', JSON.stringify(['a', 'b', 'x']))

      renderHook(() =>
        useCardVisibility(
          'test_k', ['a', 'b', 'x'], undefined,
          { version: 1, addIds: ['x'], removeIds: ['b'] }
        )
      )
      // 'b' should still be present (migration did NOT re-run)
      expect(JSON.parse(localStorage.getItem('test_k')!).sort()).toEqual(['a', 'b', 'x'])
    })

    it('re-runs when version bumps even if user toggled things since', () => {
      localStorage.setItem('test_k', JSON.stringify(['a', 'b', 'x']))
      localStorage.setItem('test_k_mv', '1')

      renderHook(() =>
        useCardVisibility(
          'test_k', ['a', 'b', 'x'], undefined,
          { version: 2, removeIds: ['x'] }
        )
      )
      expect(JSON.parse(localStorage.getItem('test_k')!).sort()).toEqual(['a', 'b'])
      expect(localStorage.getItem('test_k_mv')).toBe('2')
    })

    it('does not run migration when localStorage has no saved set yet (fresh browser)', () => {
      // Fresh browser: defaultIds wins, no migration needed
      renderHook(() =>
        useCardVisibility(
          'test_k', ['a', 'b'], ['a'],
          { version: 1, addIds: ['b'] }
        )
      )
      // No saved set written; _mv not set either
      expect(localStorage.getItem('test_k')).toBeNull()
      expect(localStorage.getItem('test_k_mv')).toBeNull()
    })
  })

  it('toggleCard persists to localStorage', () => {
    const { result } = renderHook(() =>
      useCardVisibility('test_k', ['a', 'b'], ['a'])
    )
    act(() => result.current.toggleCard('b'))
    expect(JSON.parse(localStorage.getItem('test_k')!).sort()).toEqual(['a', 'b'])
  })
})
