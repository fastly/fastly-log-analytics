'use client'

import { useCallback, useState, useSyncExternalStore, type Dispatch, type SetStateAction } from 'react'
import type { VisibilityState } from '@tanstack/react-table'

/**
 * State that starts from `source` and is reset to it whenever `source` changes (compared with
 * `Object.is`). The reset happens during render — React's sanctioned way to derive state from
 * props — so there is no extra committed render and no effect that sets state. Between changes
 * the setter edits freely (a draft that follows an upstream value until the user types).
 */
export function useDerivedState<T>(source: T): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState(source)
  const [seen, setSeen] = useState(source)
  if (!Object.is(seen, source)) {
    setSeen(source)
    setState(source)
  }
  return [state, setState]
}

/**
 * Increments a counter each time `open` transitions to `true`, so a dialog wrapper can pass
 * `key={generation}` (or `key={`${id}|${generation}`}`) to remount its body with fresh state.
 */
export function useOpenGeneration(open: boolean): number {
  const [generation, setGeneration] = useState(0)
  const [wasOpen, setWasOpen] = useState(open)
  if (open !== wasOpen) {
    setWasOpen(open)
    if (open) setGeneration((g) => g + 1)
  }
  return generation
}

export function useColumnVisibility(initial?: VisibilityState) {
  const [visibility, setVisibility] = useState<VisibilityState>(initial ?? {})
  const handleChange = useCallback(
    (id: string, vis: boolean) => setVisibility(prev => ({ ...prev, [id]: vis })),
    []
  )
  return [visibility, setVisibility, handleChange] as const
}

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
