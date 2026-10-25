import { vi } from 'vitest'

type ResizeCallback = (entries: ResizeObserverEntry[], observer: ResizeObserver) => void

export class StubResizeObserver implements ResizeObserver {
  static instances: StubResizeObserver[] = []

  constructor(private readonly callback?: ResizeCallback) {
    StubResizeObserver.instances.push(this)
  }

  observe = vi.fn()
  unobserve = vi.fn()
  disconnect = vi.fn()

  fire(): void {
    this.callback?.([], this)
  }
}

export function stubResizeObserver(): {
  observe: ReturnType<typeof vi.fn>
  disconnect: ReturnType<typeof vi.fn>
  fire: () => void
} {
  StubResizeObserver.instances = []
  const observe = vi.fn()
  const disconnect = vi.fn()
  class TrackedResizeObserver extends StubResizeObserver {
    observe = observe
    disconnect = disconnect
  }
  vi.stubGlobal('ResizeObserver', TrackedResizeObserver)
  return {
    observe,
    disconnect,
    fire: () => {
      for (const ro of StubResizeObserver.instances) ro.fire()
    },
  }
}

type IntersectionCallback = (entries: IntersectionObserverEntry[], observer: IntersectionObserver) => void

export class StubIntersectionObserver implements IntersectionObserver {
  static instances: StubIntersectionObserver[] = []

  readonly root: Element | Document | null = null
  readonly rootMargin: string = '0px'
  readonly thresholds: ReadonlyArray<number> = [0]

  constructor(private readonly callback: IntersectionCallback) {
    StubIntersectionObserver.instances.push(this)
  }

  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
  takeRecords(): IntersectionObserverEntry[] {
    return []
  }

  fire(isIntersecting: boolean): void {
    const entry = { isIntersecting } satisfies Partial<IntersectionObserverEntry>
    this.callback([entry as IntersectionObserverEntry], this)
  }
}

export function stubIntersectionObserver(): { fire: (isIntersecting: boolean) => void } {
  StubIntersectionObserver.instances = []
  vi.stubGlobal('IntersectionObserver', StubIntersectionObserver)
  return {
    fire: (isIntersecting) => {
      for (const io of StubIntersectionObserver.instances) io.fire(isIntersecting)
    },
  }
}

export function stubSystemTimeZone(timeZone: string): void {
  const Real = Intl.DateTimeFormat
  // Intl.DateTimeFormat is callable with or without `new`; a Proxy keeps both forms working.
  const build = (args: ConstructorParameters<typeof Intl.DateTimeFormat>): Intl.DateTimeFormat => {
    const instance = new Real(...args)
    if (args.length === 0) {
      const resolvedOptions = instance.resolvedOptions.bind(instance)
      instance.resolvedOptions = () => ({ ...resolvedOptions(), timeZone })
    }
    return instance
  }
  const ZoneStub = new Proxy(Real, {
    apply: (_target, _thisArg, args: ConstructorParameters<typeof Intl.DateTimeFormat>) => build(args),
    construct: (_target, args: ConstructorParameters<typeof Intl.DateTimeFormat>) => build(args),
  })
  vi.stubGlobal('Intl', Object.assign(Object.create(Intl) as typeof Intl, { DateTimeFormat: ZoneStub }))
}

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

/** jsdom lacks window.matchMedia; Drawer/Dialog/Select primitives probe it. */
export function stubMatchMedia() {
  define(
    window,
    'matchMedia',
    vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  )
}

/** Stubs window.location.assign so hard redirects in tests are observable. */
export function stubWindowLocationAssign(assignSpy = vi.fn(), href = 'http://localhost/') {
  Object.defineProperty(window, 'location', {
    configurable: true,
    writable: true,
    value: {
      ...window.location,
      assign: assignSpy,
      origin: 'http://localhost',
      href,
    },
  })
  return assignSpy
}

/**
 * jsdom has no layout, so the real @tanstack/react-virtual virtualizer would
 * render zero rows for DataTable/DataTableReadonly. Use from a test file as
 *
 *   vi.mock('@tanstack/react-virtual', async () =>
 *     (await import('../helpers/browser-stubs')).reactVirtualModuleMock())
 *
 * (vi.mock is hoisted, so the factory has to be reached via a dynamic import.)
 */
export function reactVirtualModuleMock(rowHeight = 40) {
  return {
    useVirtualizer: (options: { count?: number }) => {
      const count = options.count ?? 0
      return {
        getVirtualItems: () =>
          Array.from({ length: count }).map((_, i) => ({
            index: i,
            key: i,
            start: i * rowHeight,
            end: (i + 1) * rowHeight,
            size: rowHeight,
          })),
        getTotalSize: () => count * rowHeight,
      }
    },
  }
}
