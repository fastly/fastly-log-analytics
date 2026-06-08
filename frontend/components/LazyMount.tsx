'use client'

import React, { useEffect, useRef, useState } from 'react'

interface LazyMountProps {
  children: React.ReactNode
  /** Min height of the placeholder while not yet mounted. Pick the
   *  expected steady-state height of the child so scroll position
   *  doesn't jump as cards come into view. */
  minHeight?: number | string
  /** Pre-mount this many pixels BEFORE the element enters the viewport
   *  so the actual render finishes by the time the user scrolls to it.
   *  Default 600 = mount one screen ahead. */
  rootMargin?: string
  className?: string
}

/**
 * Render `children` only after the placeholder div has scrolled into
 * (or near) the viewport. Once mounted, stays mounted — IntersectionObserver
 * is disconnected so subsequent scroll-out doesn't unmount and lose state.
 *
 * Why this exists: the main dashboard renders 86 TopTenTable cards on
 * load — even with React.memo skipping re-renders, the FIRST render
 * still mounts every one (~860 DOM nodes just for the per-card rows
 * plus headers/footers). Wrapping each card in LazyMount means the
 * initial paint only mounts the ~5-10 cards above the fold; the rest
 * land as the user scrolls.
 *
 * Server-render note: since `IntersectionObserver` doesn't exist on the
 * server, the initial SSR markup is just the empty placeholder. That's
 * fine for client-side-rendered surfaces (the dashboard is one) but
 * means LazyMount should NOT wrap content that needs to be visible to
 * crawlers / non-JS clients.
 */
export function LazyMount({
  children,
  minHeight = 300,
  rootMargin = '600px',
  className,
}: LazyMountProps) {
  const ref = useRef<HTMLDivElement>(null)
  // Default visible=true when IntersectionObserver isn't available
  // (older browsers, test renderers) so we degrade to eager-mount
  // rather than never rendering anything.
  const [visible, setVisible] = useState(() => typeof IntersectionObserver === 'undefined')

  useEffect(() => {
    if (visible || !ref.current || typeof IntersectionObserver === 'undefined') return
    const node = ref.current
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setVisible(true)
          observer.disconnect()
        }
      },
      { rootMargin },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [visible, rootMargin])

  return (
    <div ref={ref} className={className} style={visible ? undefined : { minHeight }}>
      {visible ? children : null}
    </div>
  )
}
