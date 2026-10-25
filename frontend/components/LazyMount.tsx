'use client'

import React, { useEffect, useRef, useState, useSyncExternalStore, type RefObject } from 'react'

const subscribeNever = () => () => {}

interface UseInViewOnceOptions<T extends Element> {
  /** IntersectionObserver rootMargin; defaults to `0px`. */
  rootMargin?: string
  /** Resolve a scroll container to observe against instead of the viewport. */
  findRoot?: (node: T) => Element | null
}

/**
 * True once `ref`'s element has entered the viewport (within `rootMargin`), and stays true.
 *
 * Environments without IntersectionObserver report true straight away. That is derived through
 * `useSyncExternalStore` — the server snapshot assumes support, so the first client render
 * matches the server (an unobserved placeholder, no React #418) and an unsupported browser
 * flips after hydration without an effect that sets state.
 */
export function useInViewOnce<T extends Element>(
  ref: RefObject<T | null>,
  { rootMargin = '0px', findRoot }: UseInViewOnceOptions<T> = {},
): boolean {
  const [seen, setSeen] = useState(false)
  const supported = useSyncExternalStore(
    subscribeNever,
    () => typeof IntersectionObserver !== 'undefined',
    () => true,
  )

  useEffect(() => {
    const node = ref.current
    if (seen || !supported || !node) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setSeen(true)
          observer.disconnect()
        }
      },
      { root: findRoot ? findRoot(node) : null, rootMargin },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [ref, seen, supported, rootMargin, findRoot])

  return seen || !supported
}

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
  // `useInViewOnce` starts false on the server and the first client render, so both emit the
  // same empty min-height placeholder (starting true on the server caused hydration mismatches
  // when this component sat above the fold — React 418); it flips once the viewport observer
  // fires, or immediately where IntersectionObserver is unavailable.
  const visible = useInViewOnce(ref, { rootMargin })

  return (
    <div ref={ref} className={className} style={visible ? undefined : { minHeight }}>
      {visible ? children : null}
    </div>
  )
}
