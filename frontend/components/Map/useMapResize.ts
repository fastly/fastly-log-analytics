import { useEffect, type RefObject } from 'react'

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
