'use client'

import { useCallback, useSyncExternalStore } from 'react'

const listeners = new Set<() => void>()

function subscribe(onChange: () => void): () => void {
  listeners.add(onChange)
  window.addEventListener('storage', onChange)
  return () => {
    listeners.delete(onChange)
    window.removeEventListener('storage', onChange)
  }
}

function read(key: string): string | null {
  try {
    return window.localStorage.getItem(key)
  } catch {
    return null
  }
}

/**
 * A localStorage string as external state. The server snapshot is `null`, so the first client
 * render matches the server (no hydration mismatch) and the stored value lands right after
 * hydration. Writes notify every subscriber in this tab; other tabs arrive via `storage`.
 * Unavailable or over-quota storage degrades to `null` / a no-op write.
 */
export function useLocalStorageString(key: string): readonly [string | null, (value: string) => void] {
  const value = useSyncExternalStore(
    subscribe,
    () => read(key),
    () => null,
  )
  const write = useCallback(
    (next: string) => {
      try {
        window.localStorage.setItem(key, next)
      } catch {
        /* ignore quota / private-mode errors */
      }
      listeners.forEach((notify) => notify())
    },
    [key],
  )
  return [value, write] as const
}
