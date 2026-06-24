'use client'

import { useEffect, useState } from 'react'

/**
 * Returns false on the server and on the client's FIRST (hydration) render,
 * then true after mount.
 *
 * Use it to gate rendering of values that legitimately differ between the
 * server and the client — wall-clock relative times ("X ago"), locale/
 * timezone-formatted stamps, anything derived from localStorage — so the
 * server HTML and the first client render agree and React doesn't throw a
 * hydration mismatch (#418). Render a stable placeholder while !mounted, the
 * real value once mounted.
 */
export function useMounted(): boolean {
  const [mounted, setMounted] = useState(false)
  useEffect(() => setMounted(true), [])
  return mounted
}
