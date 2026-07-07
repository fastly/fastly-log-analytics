import { act, renderHook } from '@testing-library/react'
import { describe, expect, test } from 'vitest'

import { useTableSort, type SortAccessors } from '@/components/share-dashboard/sortable'

type Row = { name: string; n: number | null }

const rows: Row[] = [
  { name: 'bravo', n: 2 },
  { name: 'alpha', n: null },
  { name: 'charlie', n: 1 },
]

const accessors: SortAccessors<Row> = {
  name: (r) => r.name,
  n: (r) => r.n,
}

const names = (rs: Row[]) => rs.map((r) => r.name)

describe('useTableSort', () => {
  test('default sorts by the given key/direction', () => {
    const { result } = renderHook(() =>
      useTableSort(rows, accessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    expect(names(result.current.sorted)).toEqual(['alpha', 'bravo', 'charlie'])
    expect(result.current.sortKey).toBe('name')
    expect(result.current.sortDir).toBe('asc')
  })

  test('toggling the active key flips asc <-> desc', () => {
    const { result } = renderHook(() =>
      useTableSort(rows, accessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    act(() => result.current.toggle('name'))
    expect(result.current.sortDir).toBe('desc')
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
  })

  test('nulls always sort last, regardless of direction', () => {
    const { result } = renderHook(() =>
      useTableSort(rows, accessors, { defaultKey: 'n', defaultDir: 'desc' }),
    )
    // desc: 2, 1, then null last
    expect(names(result.current.sorted)).toEqual(['bravo', 'charlie', 'alpha'])
    act(() => result.current.toggle('n')) // -> asc
    // asc: 1, 2, then null STILL last
    expect(names(result.current.sorted)).toEqual(['charlie', 'bravo', 'alpha'])
  })

  test('switching to a new key resets direction to desc', () => {
    const { result } = renderHook(() =>
      useTableSort(rows, accessors, { defaultKey: 'name', defaultDir: 'asc' }),
    )
    act(() => result.current.toggle('n'))
    expect(result.current.sortKey).toBe('n')
    expect(result.current.sortDir).toBe('desc')
  })
})
