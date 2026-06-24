/**
 * C-4 (testing_suite_audit_2026-06-14.md). `ChoroplethMap` is a
 * maplibre-gl wrapper that builds a name→country-code lookup from
 * the incoming data so click events can resolve back to a filter
 * value. Jsdom can't actually render maplibre, but we can verify
 * the wrapper component mounts, accepts the data prop, and re-renders
 * cleanly when data changes — the rest is exercised by the
 * `maplibre-country-filter` Playwright journey in Phase 3.
 *
 * The MockMap shape + load-event microtask + ResizeObserver polyfill
 * live in `__tests__/helpers/maplibre-mock` so other map-component
 * tests (NetworkMap, …) share the same surface.
 *
 * @vitest-environment jsdom
 */
import { render } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

import {
  mapInstances,
  maplibreMockFactory,
  installMaplibreSideEffects,
} from '../helpers/maplibre-mock'

vi.mock('next-themes', () => ({
  useTheme: vi.fn(() => ({ theme: 'light' })),
}))

vi.mock('maplibre-gl', () => maplibreMockFactory())
// CSS import — vitest ignores it but Vite's transform throws if unmocked.
vi.mock('maplibre-gl/dist/maplibre-gl.css', () => ({}))

installMaplibreSideEffects()

describe('ChoroplethMap', () => {
  beforeEach(() => {
    mapInstances.length = 0
  })

  it('mounts a map instance into the container ref', async () => {
    const { ChoroplethMap } = await import('@/components/Map/ChoroplethMap')
    const { container } = render(
      <ChoroplethMap data={[{ country: 'US', count: 100 } as any]} />,
    )
    // Container div is rendered
    expect(container.querySelector('div')).toBeInTheDocument()
    // wait one microtask so the constructor fires
    await Promise.resolve()
    expect(mapInstances.length).toBe(1)
  })

  it('renders an empty container when given an empty data array', async () => {
    const { ChoroplethMap } = await import('@/components/Map/ChoroplethMap')
    const { container } = render(<ChoroplethMap data={[]} />)
    expect(container.firstChild).not.toBeNull()
  })

  it('re-renders without crashing when the data prop changes', async () => {
    const { ChoroplethMap } = await import('@/components/Map/ChoroplethMap')
    const { rerender, container } = render(
      <ChoroplethMap data={[{ country: 'US', count: 10 } as any]} />,
    )
    rerender(
      <ChoroplethMap
        data={[
          { country: 'US', count: 50 } as any,
          { country: 'GB', count: 5 } as any,
        ]}
      />,
    )
    expect(container.firstChild).not.toBeNull()
  })

  it('accepts an onCountryClick handler without invoking it on mount', async () => {
    const handler = vi.fn()
    const { ChoroplethMap } = await import('@/components/Map/ChoroplethMap')
    render(<ChoroplethMap data={[{ country: 'US', count: 1 } as any]} onCountryClick={handler} />)
    await Promise.resolve()
    expect(handler).not.toHaveBeenCalled()
  })
})
