import { vi } from 'vitest'

export class StubResizeObserver implements ResizeObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

export function stubResizeObserver(): void {
  vi.stubGlobal('ResizeObserver', StubResizeObserver)
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
