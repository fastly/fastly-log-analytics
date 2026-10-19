import { vi } from 'vitest'

function define(target: object, key: string, value: unknown) {
  Object.defineProperty(target, key, { value, configurable: true, writable: true })
}

/** jsdom lacks these; Radix/base-ui Select and AlertDialog call them when opening. */
export function stubPointerCaptureAndScroll() {
  define(window.HTMLElement.prototype, 'hasPointerCapture', vi.fn())
  define(window.HTMLElement.prototype, 'releasePointerCapture', vi.fn())
  define(window.HTMLElement.prototype, 'scrollIntoView', vi.fn())
}

/** jsdom has no Web Animations API; base-ui's ScrollArea probes it on a timer. */
export function stubGetAnimations() {
  define(window.Element.prototype, 'getAnimations', vi.fn(() => []))
}
