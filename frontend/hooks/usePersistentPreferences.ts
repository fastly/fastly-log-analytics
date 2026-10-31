'use client'

import {
  useState,
  useCallback,
  useMemo,
  useSyncExternalStore,
  type Dispatch,
  type SetStateAction,
} from 'react'
import type { VisibilityState } from '@tanstack/react-table'
import { formatDate, formatRelative, formatTimeAgo, getTimezoneAbbr } from '@/lib/utils'
import { useTimezoneStore } from '@/stores/filterStore'

export function useTimezone(): string {
  return useTimezoneStore((s) => s.timezone)
}

export function useDateFormat() {
  const timezone = useTimezoneStore((state) => state.timezone)
  const short = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm a'), [timezone])
  const full = useCallback((date: string | Date) => formatDate(date, timezone, 'yyyy-MM-dd h:mm:ss a'), [timezone])
  const time = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm a'), [timezone])
  const timeMs = useCallback((date: string | Date) => formatDate(date, timezone, 'h:mm:ss.SSS a'), [timezone])
  const abbr = useCallback(() => getTimezoneAbbr(new Date(), timezone), [timezone])
  const relative = useCallback((date: string | Date) => formatRelative(date), [])
  const timeAgo = useCallback(
    (date: string | Date) => formatTimeAgo(typeof date === 'string' ? date : date.toISOString()),
    [],
  )
  const format = useCallback((date: string | Date, pattern: string) => formatDate(date, timezone, pattern), [timezone])
  return useMemo(
    () => ({ short, full, time, timeMs, abbr, relative, timeAgo, format }),
    [short, full, time, timeMs, abbr, relative, timeAgo, format],
  )
}

let _now = Date.now()
const _listeners = new Set<() => void>()
let _interval: ReturnType<typeof setInterval> | null = null

function _subscribe(cb: () => void): () => void {
  if (_listeners.size === 0) _now = Date.now()
  _listeners.add(cb)
  if (_interval === null) {
    _interval = setInterval(() => {
      _now = Date.now()
      for (const l of _listeners) l()
    }, 1000)
  }
  return () => {
    _listeners.delete(cb)
    if (_listeners.size === 0 && _interval !== null) {
      clearInterval(_interval)
      _interval = null
    }
  }
}

const _getSnapshot = (): number => _now
const _getServerSnapshot = (): number => 0
const _noopUnsubscribe = () => {}
const _noopSubscribe = (): (() => void) => _noopUnsubscribe

export function useNowMs(enabled = true): number {
  return useSyncExternalStore(enabled ? _subscribe : _noopSubscribe, _getSnapshot, _getServerSnapshot)
}

export function useElapsedTime(startedAt: string | null | undefined): number {
  const nowMs = useNowMs(Boolean(startedAt))
  if (!startedAt) return 0
  return Math.max(0, (nowMs - new Date(startedAt).getTime()) / 1000)
}

export function useDerivedState<T>(source: T): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState(source)
  const [seen, setSeen] = useState(source)
  if (!Object.is(seen, source)) {
    setSeen(source)
    setState(source)
  }
  return [state, setState]
}

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
    (id: string, vis: boolean) => setVisibility((prev) => ({ ...prev, [id]: vis })),
    [],
  )
  return [visibility, setVisibility, handleChange] as const
}

const storageListeners = new Set<() => void>()
function subscribeStorage(onChange: () => void): () => void {
  storageListeners.add(onChange)
  window.addEventListener('storage', onChange)
  return () => {
    storageListeners.delete(onChange)
    window.removeEventListener('storage', onChange)
  }
}
function readStorage(key: string): string | null {
  try {
    return window.localStorage.getItem(key)
  } catch {
    return null
  }
}

export function useLocalStorageString(key: string): readonly [string | null, (value: string) => void] {
  const value = useSyncExternalStore(subscribeStorage, () => readStorage(key), () => null)
  const write = useCallback(
    (next: string) => {
      try {
        window.localStorage.setItem(key, next)
      } catch {
        /* ignore */
      }
      storageListeners.forEach((notify) => notify())
    },
    [key],
  )
  return [value, write] as const
}
