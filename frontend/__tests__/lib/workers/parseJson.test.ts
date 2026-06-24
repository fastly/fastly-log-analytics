/**
 * @vitest-environment jsdom
 *
 * Tests for the [parseJsonAsync](../../../lib/workers/parseJson.ts) helper.
 *
 * The helper has two paths:
 *  1. Non-browser / test environment → synchronous JSON.parse wrapped in
 *     a Promise. This is the only path that gets exercised in jsdom; the
 *     Worker path requires a real browser (jsdom's Worker is a stub that
 *     throws on import.meta.url URL construction).
 *  2. Browser → spins up a Worker, ferries the string across postMessage,
 *     resolves with the parsed payload or rejects on parse error.
 *
 * Since this module routes test runs through the synchronous fallback
 * (the `process.env.NODE_ENV === 'test'` guard), the meaningful coverage
 * here is: success returns the parsed value, failure rejects with the
 * underlying SyntaxError. The Worker path is exercised end-to-end by the
 * pages that consume large JSON in production.
 */
import { describe, it, expect } from 'vitest'

import { parseJsonAsync } from '@/lib/workers/parseJson'

describe('parseJsonAsync', () => {
  it('resolves to the parsed value on valid JSON', async () => {
    const result = await parseJsonAsync<{ a: number; b: string[] }>(
      '{"a": 1, "b": ["x", "y"]}',
    )
    expect(result).toEqual({ a: 1, b: ['x', 'y'] })
  })

  it('handles primitives and arrays at the top level', async () => {
    expect(await parseJsonAsync<number>('42')).toBe(42)
    expect(await parseJsonAsync<null>('null')).toBeNull()
    expect(await parseJsonAsync<boolean>('true')).toBe(true)
    expect(await parseJsonAsync<number[]>('[1, 2, 3]')).toEqual([1, 2, 3])
  })

  it('handles an empty object/array', async () => {
    expect(await parseJsonAsync<Record<string, never>>('{}')).toEqual({})
    expect(await parseJsonAsync<unknown[]>('[]')).toEqual([])
  })

  it('rejects with the underlying SyntaxError on invalid JSON', async () => {
    await expect(parseJsonAsync('{ not: valid json }')).rejects.toThrow(SyntaxError)
  })

  it('rejects on truncated input', async () => {
    await expect(parseJsonAsync('{"a":')).rejects.toThrow(SyntaxError)
  })

  it('rejects on the empty string', async () => {
    await expect(parseJsonAsync('')).rejects.toThrow(SyntaxError)
  })

  it('returns a Promise (not a sync throw) even on bad input', () => {
    // Regression guard: the helper must always be awaitable, never throw
    // synchronously, otherwise callers wrapping it in a Promise.all would
    // see an unhandled exception instead of a rejected promise.
    const p = parseJsonAsync('garbage')
    expect(p).toBeInstanceOf(Promise)
    // Swallow the rejection so vitest doesn't flag an unhandled promise.
    p.catch(() => {})
  })

  it('preserves nested structure (deep clone via JSON, not a reference)', async () => {
    const source = '{"a":{"b":{"c":[1,{"d":"deep"}]}}}'
    const parsed = await parseJsonAsync<{ a: { b: { c: [number, { d: string }] } } }>(source)
    expect(parsed.a.b.c[1].d).toBe('deep')
  })
})
