import { useEffect, type RefObject } from 'react'

export const NORMALIZE_COUNTRY: Record<string, string> = {
  'United States': 'United States of America',
  'Russia': 'Russia',
  'South Korea': 'South Korea',
  'Vietnam': 'Vietnam',
  'Taiwan': 'Taiwan',
}

/**
 * rAF-throttle a function so it fires at most once per animation frame.
 * Wrapping MapLibre `mousemove` handlers caps the per-frame re-render
 * cost to display refresh rate while preserving the latest position.
 */
export function rafThrottle<TArgs extends unknown[]>(fn: (...args: TArgs) => void) {
  let queued = false
  let lastArgs: TArgs | null = null
  return (...args: TArgs) => {
    lastArgs = args
    if (queued) return
    queued = true
    requestAnimationFrame(() => {
      queued = false
      if (lastArgs) fn(...lastArgs)
      lastArgs = null
    })
  }
}

const COUNTRY_FILL_DARK = '#27272a'
const COUNTRY_FILL_LIGHT = '#e4e4e7'
const COUNTRY_OUTLINE_DARK = '#3f3f46'
const COUNTRY_OUTLINE_LIGHT = '#d4d4d8'

export function countryFill(isDark: boolean): string {
  return isDark ? COUNTRY_FILL_DARK : COUNTRY_FILL_LIGHT
}

export function countryOutline(isDark: boolean): string {
  return isDark ? COUNTRY_OUTLINE_DARK : COUNTRY_OUTLINE_LIGHT
}

type Resizable = { resize: () => unknown }

/**
 * Re-measure a MapLibre map when its container changes size. MapLibre captures
 * the container size at construction and does not re-measure when a flex
 * parent grows, a card collapses, or a modal finishes animating in.
 */
export function observeMapResize(container: Element, getMap: () => Resizable | null | undefined): () => void {
  const ro = new ResizeObserver(() => {
    getMap()?.resize()
  })
  ro.observe(container)
  return () => ro.disconnect()
}

export function useMapResize(
  mapRef: RefObject<Resizable | null>,
  containerRef: RefObject<Element | null>,
): void {
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    return observeMapResize(el, () => mapRef.current)
  }, [mapRef, containerRef])
}
