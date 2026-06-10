/**
 * Dependency-free toast helper.
 *
 * The app didn't pull in a toast library (no sonner/react-hot-toast) and the
 * one "Background Sync Completed" toast on /logs is hand-rolled per-page.
 * The 2026-06-10 audit surfaced two analyst-facing actions that silently
 * fail without any UI signal (M-1 Alerts modal, N-6 Save View modal); both
 * needed a global toast. Rather than add a 5-KB dep for one call-site, this
 * helper appends a styled div to ``document.body`` and removes it after a
 * timeout. Call ``showToast`` from anywhere — including non-React code such
 * as the openapi-fetch response middleware.
 */

export type ToastKind = 'info' | 'success' | 'error' | 'warn'

interface ToastOptions {
  durationMs?: number
}

const PALETTE: Record<ToastKind, { bg: string; border: string; fg: string }> = {
  info: { bg: '#0f172a', border: '#1e293b', fg: '#e2e8f0' },
  success: { bg: '#064e3b', border: '#065f46', fg: '#ecfdf5' },
  error: { bg: '#7f1d1d', border: '#991b1b', fg: '#fef2f2' },
  warn: { bg: '#78350f', border: '#92400e', fg: '#fffbeb' },
}

let stackContainer: HTMLDivElement | null = null
const recentMessages = new Map<string, number>()
const RECENT_DEDUP_MS = 1500

function ensureContainer(): HTMLDivElement | null {
  if (typeof document === 'undefined') return null
  if (stackContainer && document.body.contains(stackContainer)) return stackContainer
  const el = document.createElement('div')
  el.setAttribute('role', 'region')
  el.setAttribute('aria-label', 'Notifications')
  el.style.cssText = [
    'position:fixed',
    'top:16px',
    'right:16px',
    'z-index:2147483647',
    'display:flex',
    'flex-direction:column',
    'gap:8px',
    'pointer-events:none',
    'max-width:380px',
  ].join(';')
  document.body.appendChild(el)
  stackContainer = el
  return el
}

export function showToast(message: string, kind: ToastKind = 'info', opts: ToastOptions = {}): void {
  if (typeof document === 'undefined') return
  // Dedupe rapid repeats — the API middleware may fire on multiple parallel
  // 403s for the same action; a single toast per second is enough.
  const now = Date.now()
  const last = recentMessages.get(message) || 0
  if (now - last < RECENT_DEDUP_MS) return
  recentMessages.set(message, now)
  // Trim stale dedup entries opportunistically.
  if (recentMessages.size > 32) {
    for (const [k, t] of recentMessages) {
      if (now - t > RECENT_DEDUP_MS * 4) recentMessages.delete(k)
    }
  }

  const root = ensureContainer()
  if (!root) return
  const palette = PALETTE[kind]
  const node = document.createElement('div')
  node.setAttribute('role', kind === 'error' || kind === 'warn' ? 'alert' : 'status')
  node.setAttribute('aria-live', kind === 'error' || kind === 'warn' ? 'assertive' : 'polite')
  node.style.cssText = [
    `background:${palette.bg}`,
    `color:${palette.fg}`,
    `border:1px solid ${palette.border}`,
    'padding:10px 14px',
    'border-radius:8px',
    'font:13px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif',
    'box-shadow:0 8px 24px rgba(0,0,0,.18)',
    'pointer-events:auto',
    'transition:opacity .18s ease,transform .18s ease',
    'opacity:0',
    'transform:translateY(-4px)',
  ].join(';')
  node.textContent = message
  root.appendChild(node)
  // Trigger transition.
  requestAnimationFrame(() => {
    node.style.opacity = '1'
    node.style.transform = 'translateY(0)'
  })
  const duration = opts.durationMs ?? (kind === 'error' ? 5500 : 3500)
  const remove = () => {
    node.style.opacity = '0'
    node.style.transform = 'translateY(-4px)'
    setTimeout(() => {
      if (node.parentNode) node.parentNode.removeChild(node)
      if (root.childElementCount === 0 && stackContainer) {
        stackContainer.remove()
        stackContainer = null
      }
    }, 200)
  }
  setTimeout(remove, duration)
  node.addEventListener('click', remove)
}

/** Convenience shorthand for the read-only analyst case. */
export function showReadOnlyToast(): void {
  showToast(
    'Read-only access — that action is unavailable for shared sessions.',
    'warn',
  )
}
