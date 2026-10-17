'use client'

import { useEffect, useState, useSyncExternalStore, type RefObject } from 'react'

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
