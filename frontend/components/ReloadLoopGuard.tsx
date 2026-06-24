'use client'

import * as React from 'react'

// Reload-loop breaker (defense-in-depth).
//
// A stale SPA tab left open across a deploy can, in rare cases, fall into a
// hard-reload loop: Next.js's deployment-skew / chunk-load recovery keeps
// calling window.location.reload() to re-sync the client to the new build.
// The `private, no-cache` HTML header (next.config.ts) normally makes that
// self-heal in a single reload, but this is belt-and-suspenders for any case
// where it does NOT (a real occurrence flooded RUM with ~9.5k samples over
// hours).
//
// How it breaks the loop: it counts hard document loads of the SAME path in a
// short window (a loop hammers one URL; ordinary client-side navigation across
// pages doesn't trigger a document load, and address-bar / harness navigation
// lands on different paths). Once the count crosses the threshold it renders a
// manual-recovery prompt INSTEAD of the app subtree — not mounting the app is
// what actually stops the loop, since the unmounted tree no longer prefetches
// routes or runs the navigation that makes Next reload.
//
// Implemented with useSyncExternalStore so it's SSR/hydration-safe
// (getServerSnapshot → false, so server and client-initial render both produce
// children) and free of the setState-in-effect / ref-in-render foot-guns.

const STORAGE_KEY = '__reload_loop_history'
const WINDOW_MS = 60_000
// Same-path hard loads within WINDOW_MS before we treat it as a loop. A 6–11s
// reload cadence reaches this in ~40–65s; a human almost never hard-reloads one
// page 6× in a minute, and the fallback is dismissible if they do.
const THRESHOLD = 6

type Entry = { t: number; path: string }

function readHistory(): Entry[] {
  try {
    const parsed = JSON.parse(sessionStorage.getItem(STORAGE_KEY) ?? '[]')
    return Array.isArray(parsed) ? parsed : []
  } catch {
    return []
  }
}

function clearHistory() {
  try {
    sessionStorage.removeItem(STORAGE_KEY)
  } catch {
    /* sessionStorage unavailable (private mode / disabled) — nothing to clear */
  }
}

// Record one document load. Called from subscribe() (once per mount); the
// snapshot below re-reads sessionStorage after subscription, so the freshly
// recorded load is reflected without a manual notification.
function recordLoad() {
  try {
    const now = Date.now()
    const path = window.location.pathname
    const recent = readHistory().filter((e) => now - e.t < WINDOW_MS)
    recent.push({ t: now, path })
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(recent))
  } catch {
    /* can't persist — detection simply won't trip */
  }
}

function subscribe() {
  recordLoad()
  return () => {}
}

// Pure read: is the current path over the same-path load threshold?
function getClientSnapshot(): boolean {
  try {
    const now = Date.now()
    const path = window.location.pathname
    const recent = readHistory().filter((e) => now - e.t < WINDOW_MS)
    return recent.filter((e) => e.path === path).length >= THRESHOLD
  } catch {
    return false
  }
}

function getServerSnapshot(): boolean {
  return false
}

export function ReloadLoopGuard({ children }: { children: React.ReactNode }) {
  const tripped = React.useSyncExternalStore(subscribe, getClientSnapshot, getServerSnapshot)
  const [dismissed, setDismissed] = React.useState(false)

  if (tripped && !dismissed) {
    return (
      <ReloadLoopFallback
        onRefresh={() => {
          clearHistory()
          window.location.reload()
        }}
        onContinue={() => {
          clearHistory()
          setDismissed(true)
        }}
      />
    )
  }

  return <>{children}</>
}

function ReloadLoopFallback({
  onRefresh,
  onContinue,
}: {
  onRefresh: () => void
  onContinue: () => void
}) {
  return (
    <div role="alert" aria-live="assertive" className="flex min-h-screen items-center justify-center p-6">
      <div className="flex w-full max-w-md flex-col items-start gap-3 rounded-lg border border-amber-500/30 bg-amber-500/10 p-6">
        <h1 className="text-base font-semibold text-foreground">This page kept reloading</h1>
        <p className="text-sm text-muted-foreground">
          It reloaded several times in a row — usually because a new version was
          deployed while this tab was open. Refresh to load the latest version.
        </p>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={onRefresh}
            className="rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground hover:opacity-90"
          >
            Refresh now
          </button>
          <button
            type="button"
            onClick={onContinue}
            className="rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium hover:bg-accent hover:text-accent-foreground"
          >
            Continue anyway
          </button>
        </div>
      </div>
    </div>
  )
}
