/**
 * Dependency-free DOM toast helper. Appends a styled live-region toast to
 * `document.body` and auto-removes it after a timeout. Safe to call from
 * non-React code (e.g., openapi-fetch middleware).
 */

export type ToastKind = 'info' | 'success' | 'error' | 'warn'

interface ToastOptions {
  durationMs?: number
}

interface ToastActionOptions extends ToastOptions {
  actionLabel: string
  onAction: () => void
  kind?: ToastKind
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
  el.style.cssText =
    'position:fixed;top:16px;right:16px;z-index:2147483647;display:flex;flex-direction:column;gap:8px;pointer-events:none;max-width:380px'
  document.body.appendChild(el)
  stackContainer = el
  return el
}

function mountToastNode(
  kind: ToastKind,
  durationMs: number,
  extraCss: string[] = [],
  setup?: (node: HTMLDivElement, dismiss: () => boolean) => void,
): void {
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
    ...extraCss,
  ].join(';')

  let dismissed = false
  const dismiss = (): boolean => {
    if (dismissed) return false
    dismissed = true
    node.style.opacity = '0'
    node.style.transform = 'translateY(-4px)'
    setTimeout(() => {
      node.remove()
      if (root.childElementCount === 0 && stackContainer) {
        stackContainer.remove()
        stackContainer = null
      }
    }, 200)
    return true
  }

  setup?.(node, dismiss)
  root.appendChild(node)
  requestAnimationFrame(() => {
    node.style.opacity = '1'
    node.style.transform = 'translateY(0)'
  })
  setTimeout(dismiss, durationMs)
}

export function showToast(message: string, kind: ToastKind = 'info', opts: ToastOptions = {}): void {
  if (typeof document === 'undefined') return
  const now = Date.now()
  if (now - (recentMessages.get(message) || 0) < RECENT_DEDUP_MS) return
  recentMessages.set(message, now)
  if (recentMessages.size > 32) {
    for (const [k, t] of recentMessages) {
      if (now - t > RECENT_DEDUP_MS * 4) recentMessages.delete(k)
    }
  }

  const duration = opts.durationMs ?? (kind === 'error' ? 5500 : 3500)
  mountToastNode(kind, duration, [], (node, dismiss) => {
    node.textContent = message
    node.addEventListener('click', dismiss)
  })
}

export function showReadOnlyToast(): void {
  showToast('Read-only access — that action is unavailable for shared sessions.', 'warn')
}

export function showBusyToast(): void {
  showToast('Server busy, retrying…', 'info', { durationMs: 2500 })
}

export function showToastWithAction(message: string, opts: ToastActionOptions): void {
  if (typeof document === 'undefined') return
  const kind: ToastKind = opts.kind ?? 'info'
  const palette = PALETTE[kind]

  mountToastNode(
    kind,
    opts.durationMs ?? 5500,
    ['display:flex', 'align-items:center', 'gap:12px'],
    (node, dismiss) => {
      const label = document.createElement('span')
      label.textContent = message
      label.style.cssText = 'flex:1;min-width:0'
      node.appendChild(label)

      const button = document.createElement('button')
      button.type = 'button'
      button.textContent = opts.actionLabel
      button.style.cssText = `background:transparent;color:${palette.fg};border:1px solid ${palette.fg};border-radius:6px;padding:4px 10px;font:600 12px/1 inherit;cursor:pointer;flex-shrink:0;opacity:.9`
      button.addEventListener('click', (e) => {
        e.stopPropagation()
        if (dismiss()) opts.onAction()
      })
      node.appendChild(button)
    },
  )
}
