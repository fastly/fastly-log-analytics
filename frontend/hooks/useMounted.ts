'use client'

import { useSyncExternalStore } from 'react'

const subscribeNever = () => () => {}

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
 *
 * Implemented as an external-store read (server snapshot false, client snapshot true):
 * React renders hydration with the server snapshot and then re-renders with the client
 * one, with no effect that sets state. A client-only render has no server HTML to match,
 * so it reads true straight away.
 */
export function useMounted(): boolean {
  return useSyncExternalStore(
    subscribeNever,
    () => true,
    () => false,
  )
}
