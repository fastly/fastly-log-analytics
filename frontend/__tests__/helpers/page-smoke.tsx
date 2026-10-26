/**
 * Shared test scaffolding: page smoke mocks, typed fixtures, SSE streams,
 * QueryClient wrappers, Next.js SSR headers/HTTP server helpers, and DOM stubs.
 */

import { createServer, type IncomingHttpHeaders, type IncomingMessage, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, beforeEach, vi, type MockInstance } from 'vitest'
import React, { type ComponentProps } from 'react'
import { render, type RenderOptions, type RenderResult } from '@testing-library/react'
import { QueryClient, QueryClientProvider, type QueryClientConfig } from '@tanstack/react-query'
import 'nuqs'
import { NuqsTestingAdapter } from 'nuqs/adapters/testing'
import type { client } from '@/lib/api'

let navMod: typeof import('next/navigation') | null = null

export function ensureNuqsAdapter(): void {
  if (typeof window === 'undefined') return
  const ctx = (window as unknown as { __NuqsAdapterContext?: { _currentValue?: unknown; _currentValue2?: unknown } }).__NuqsAdapterContext
  if (!ctx) return
  const fallback = {
    useAdapter() {
      const firstRef = React.useRef(true)
      if (firstRef.current) {
        firstRef.current = false
        ;(globalThis as Record<symbol, { abort?: () => void }>)[Symbol.for('nuqs.2.10.2.throttle-queue')]?.abort?.()
      }
      const router = navMod?.useRouter()
      const pathname = navMod?.usePathname() || '/'
      const sp = navMod?.useSearchParams() ?? new URLSearchParams()
      return {
        searchParams: sp,
        getSearchParamsSnapshot: () => new URLSearchParams(sp.toString()),
        pathname,
        rateLimitFactor: 0,
        autoResetQueueOnUpdate: true,
        updateUrl: (search: URLSearchParams, options: { history?: 'push' | 'replace'; scroll?: boolean; shallow?: boolean }) => {
          const qs = search.toString()
          const url = qs ? `${pathname}?${qs}` : pathname
          if (!options.shallow) (options.history === 'push' ? router?.push : router?.replace)?.(url, { scroll: options.scroll ?? false })
        },
      }
    },
  }
  ctx._currentValue = fallback
  ctx._currentValue2 = fallback
}

ensureNuqsAdapter()
beforeEach(async () => {
  navMod = await import('next/navigation')
  ensureNuqsAdapter()
})
import type { PlotlyChart } from '@/components/PlotlyChart'
import type { MapOptions } from 'maplibre-gl'
import type { ProvisionConfig } from '@/components/ProvisionWizard/ProvisionWizard'

export type ReportLayoutChildArgs = {
  data?: unknown; isLoading?: boolean; isFetching?: boolean; config?: Record<string, unknown>
  setChartInterval?: (interval: string) => void; trend?: string; setTrend?: (trend: string) => void
  intervalButtons?: React.ReactNode; bucketSeconds?: number; startTime?: string | null
  endTime?: string | null; timezone?: string; activeServiceId?: string | null; filterPayload?: Record<string, unknown>
}

export function spyOnConsoleError(): MockInstance {
  return vi.spyOn(console, 'error').mockImplementation(() => {})
}

const zustandMock = <T,>(state: T) =>
  Object.assign(vi.fn((selector?: (s: T) => unknown) => (selector ? selector(state) : state)), { getState: () => state })

// ── Shared cross-page module mocks ───────────────────────────────────────────

export async function serviceStoreModuleMock(opts: { accessLevel?: string } = {}) {
  const actual = await vi.importActual<typeof import('@/stores/filterStore')>('@/stores/filterStore')
  const service: Record<string, unknown> = { id: 'test-svc', name: 'Test' }
  if (opts.accessLevel) service.accessLevel = opts.accessLevel
  const state = {
    activeServiceId: 'test-svc', isInitialized: true, services: [service],
    setServices: vi.fn(), setInitialized: vi.fn(), setActiveServiceId: vi.fn(),
  }
  const filterState: Record<string, unknown> = {
    startTime: '2026-01-01T00:00:00Z', endTime: '2026-01-01T01:00:00Z',
    filters: [], isAutoRange: false, compareMode: false, setRange: vi.fn(), clearFilters: vi.fn(),
  }
  return { ...actual, useServiceStore: zustandMock(state), useFilterStore: zustandMock(filterState) }
}

export const filterStoreModuleMock = serviceStoreModuleMock

export function navigationModuleMock(pathname: string) {
  return {
    usePathname: vi.fn(() => pathname),
    useSearchParams: vi.fn(() => new URLSearchParams()),
    useRouter: vi.fn(() => ({ replace: vi.fn(), push: vi.fn(), prefetch: vi.fn(), back: vi.fn() })),
  }
}

export function plotlyChartModuleMock(onRender?: (props: ComponentProps<typeof PlotlyChart>) => void) {
  return { PlotlyChart: (props: ComponentProps<typeof PlotlyChart>) => (onRender?.(props), <div data-testid="plotly-chart" />) }
}

export function filterValueCellModuleMock() {
  return {
    FilterValueCell: (props: { display?: React.ReactNode; filters?: Array<{ value?: React.ReactNode }> }) => (
      <span>{props.display ?? props.filters?.[0]?.value}</span>
    ),
  }
}

export async function dataTableModuleMock() {
  const actual = await vi.importActual<typeof import('@/components/DataTable')>('@/components/DataTable')
  return { ...actual, ...filterValueCellModuleMock(), DataTable: () => <div data-testid="data-table" />, ColumnVisibilityDropdown: () => null }
}

export type InteractiveTableRow = { name?: string; bot_name?: string; fingerprint?: string; ip?: string }
export type InteractiveDataTableMockProps = { data?: InteractiveTableRow[]; emptyMessage?: string }

export async function interactiveDataTableModuleMock(onDataTable?: (props: InteractiveDataTableMockProps) => void) {
  const actual = await vi.importActual<typeof import('@/components/DataTable')>('@/components/DataTable')
  return {
    ...actual,
    ...filterValueCellModuleMock(),
    DataTable: (props: InteractiveDataTableMockProps) => {
      onDataTable?.(props)
      return (
        <div data-testid={`data-table-${props.data?.length ?? 0}`}>
          {props.data?.length === 0 ? (
            <span data-testid="empty">{props.emptyMessage}</span>
          ) : (
            props.data?.map((row, idx) => {
              const label = row.name || row.bot_name || row.fingerprint || row.ip || String(idx)
              return <div key={label} data-testid="row">{label}</div>
            })
          )}
        </div>
      )
    },
    ColumnVisibilityDropdown: (props: { columns: Array<{ id: string; label: string }>; onChange?: (id: string, visible: boolean) => void }) => (
      <button type="button" data-testid="col-vis" onClick={() => props.onChange?.(props.columns[0]?.id, false)}>cols</button>
    ),
  }
}

export function activeLogFieldsSetModuleMock(activeFields: Set<string>) {
  return {
    useLogFieldsCatalog: () => ({ data: undefined, isLoading: false }),
    useFieldLabel: () => (id: string) => id,
    useFieldValues: () => ({ data: undefined, isLoading: false }),
    useActiveLogFields: () => ({ ready: true, isFieldActive: (id: string) => activeFields.has(id), isGroupActive: () => false }),
  }
}

export function dateFormatModuleMock(timezone = 'UTC') {
  return { useDateFormat: () => (d: unknown) => String(d ?? ''), useTimezone: () => timezone }
}

export async function reportLayoutModuleMock(childArgs: ReportLayoutChildArgs) {
  const actual = await vi.importActual<typeof import('@/components/ReportLayout')>('@/components/ReportLayout')
  return {
    ...actual,
    ReportLayout: ({
      children, title, headerActions,
    }: {
      children: (a: ReportLayoutChildArgs) => React.ReactNode; title: React.ReactNode; headerActions?: React.ReactNode
    }) => (
      <div>
        <h1>{title}</h1>
        <div data-testid="header-actions">{headerActions}</div>
        {children(childArgs)}
      </div>
    ),
  }
}

export function selectMockFactory() {
  const SelectCtx = React.createContext<((v: string) => void) | null>(null)
  return {
    Select: ({ value, onValueChange, children }: { value?: string; onValueChange?: (v: string) => void; children?: React.ReactNode }) => (
      <SelectCtx.Provider value={onValueChange ?? null}>
        <div data-testid="mock-select" data-select-value={value ?? ''}>{children}</div>
      </SelectCtx.Provider>
    ),
    SelectTrigger: ({ children }: { children?: React.ReactNode }) => <div data-testid="mock-select-trigger">{children}</div>,
    SelectValue: ({ children }: { children?: React.ReactNode }) => <span data-testid="mock-select-value">{children}</span>,
    SelectContent: ({ children }: { children?: React.ReactNode }) => <div data-testid="mock-select-content">{children}</div>,
    SelectSeparator: () => <hr data-testid="mock-select-separator" />,
    SelectItem: ({ value, children }: { value: string; children?: React.ReactNode }) => {
      const onValueChange = React.useContext(SelectCtx)
      return <button type="button" data-select-item-value={value} onClick={() => onValueChange?.(value)}>{children}</button>
    },
  }
}

export const API_BASE = 'http://127.0.0.1:8000'

export const FULL_CONFIG: ProvisionConfig = {
  endpoint_name: 'My EP', fos_region: 'us-east-1', fos_endpoint: '', fos_bucket_name: 'my-bucket',
  fos_prefix: 'logs/', fos_access_key: 'AK', fos_secret_key: 'SK', sample_rate: 1, edge_only: true,
  custom_condition: '', log_period: 60, rum_log_period: 300, cdn_service_name: 'My CDN',
  cdn_prefix: 'mycdn', cdn_shield: 'iad-va-us', cdn_url: '', cdn_secret: '', enable_cron_sync: true,
  delete_after: false, commit_interval_mins: 5, enable_cron_compact: true,
  log_fields: { groups: [], field_overrides: {} }, ngwaf_workspace_id: '', cmcd_enabled: false,
  cmcd_mode: 'query_string', cmcd_version: 1, logging_enabled: true, rum_enabled: false,
  log_retention_days: 30, rum_retention_days: 30, cache_retention_days: 14, rollup_retention_months: 12,
  rum_custom_condition: '', faro_version: null,
}

export function serviceStoreMockFactory() {
  return {
    useServiceStore: zustandMock({ activeServiceId: 'svc-test' as string | null }) as unknown,
    useDebugStore: zustandMock({ enabled: false, apiCallsEnabled: false }) as unknown,
    useSessionRoleStore: zustandMock({ isRemoteAnalyst: false, setIsRemoteAnalyst: vi.fn() }) as unknown,
  }
}

// ── Typed Partial / Fixture Helpers ──────────────────────────────────────────

type DeepPartial<T> = T extends readonly (infer U)[]
  ? DeepPartial<U>[]
  : T extends object ? { [K in keyof T]?: DeepPartial<T[K]> } : T

export const partial = <T,>(value: DeepPartial<T>): T => value as T

export function without<T extends object, K extends keyof T>(obj: T, key: K): Omit<T, K> {
  const copy: Partial<T> = { ...obj }
  delete copy[key]
  return copy as Omit<T, K>
}

export const invalidInput = (value: unknown): never => value as never

type GetResult = Awaited<ReturnType<typeof client.GET>>
type PostResult = Awaited<ReturnType<typeof client.POST>>

export const getOk = (data: NonNullable<GetResult['data']>): GetResult => partial<GetResult>({ data, error: undefined })
export const postOk = (data: NonNullable<PostResult['data']>): PostResult => partial<PostResult>({ data, error: undefined })

// ── SSE Mock Helpers ─────────────────────────────────────────────────────────

export function makeStreamResponse(messages: string[], status = 200): Response {
  const enc = new TextEncoder()
  let i = 0
  return new Response(
    new ReadableStream({
      pull(controller) {
        if (i >= messages.length) return controller.close()
        controller.enqueue(enc.encode(messages[i++]))
      },
    }),
    { status },
  )
}

export function makeNeverClosingStreamResponse(initialChunks: string[] = []): Response {
  const enc = new TextEncoder()
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of initialChunks) controller.enqueue(enc.encode(chunk))
      },
    }),
    { status: 200 },
  )
}

export type MockSSEStatus = 'idle' | 'streaming' | 'done' | 'error'

export function createUseSSEMock() {
  const start = vi.fn(), stop = vi.fn(), reset = vi.fn()
  const state = {
    lines: [] as Array<Record<string, unknown>>, status: 'idle' as MockSSEStatus,
    error: null as string | null, isDone: false, start, stop, reset,
  }
  return {
    state, start, stop, reset,
    resetState() {
      state.lines = []; state.status = 'idle'; state.error = null; state.isDone = false
      start.mockClear(); stop.mockClear(); reset.mockClear()
    },
  }
}

// ── QueryClient & Render Helpers ─────────────────────────────────────────────

export function createTestQueryClient(overrides?: QueryClientConfig['defaultOptions']): QueryClient {
  return new QueryClient({
    defaultOptions: { ...overrides, queries: { retry: false, ...overrides?.queries } },
  })
}

export function makeQueryWrapper(qc: QueryClient) {
  return function QueryWrapper({ children }: { children: React.ReactNode }) {
    return (
      <NuqsTestingAdapter searchParams={window.location.search}>
        <QueryClientProvider client={qc}>{children}</QueryClientProvider>
      </NuqsTestingAdapter>
    )
  }
}

interface RenderWithQueryOptions extends RenderOptions {
  queryClient?: QueryClient
  queryOptions?: QueryClientConfig['defaultOptions']
  withNuqs?: boolean
}

export function renderWithQuery(ui: React.ReactElement, options?: RenderWithQueryOptions): RenderResult & { queryClient: QueryClient } {
  const { queryClient: providedClient, queryOptions, withNuqs = false, wrapper: InnerWrapper, ...renderOptions } = options ?? {}
  const queryClient =
    providedClient ??
    createTestQueryClient({
      mutations: { retry: false, ...queryOptions?.mutations },
      ...queryOptions,
      queries: { retry: false, gcTime: 0, ...queryOptions?.queries },
    })
  const Wrapper = ({ children }: { children: React.ReactNode }) => {
    const content = InnerWrapper ? <InnerWrapper>{children}</InnerWrapper> : children
    return withNuqs ? (
      <NuqsTestingAdapter searchParams={window.location.search}>
        <QueryClientProvider client={queryClient}>{content}</QueryClientProvider>
      </NuqsTestingAdapter>
    ) : (
      <QueryClientProvider client={queryClient}>{content}</QueryClientProvider>
    )
  }
  return { queryClient, ...render(ui, { wrapper: Wrapper, ...renderOptions }) }
}

export const renderWithQueryClient = renderWithQuery

// ── SSR & Test HTTP Server Helpers ───────────────────────────────────────────

export const mockCookies = vi.fn((): { toString: () => string; get: (name: string) => { value: string } | undefined } => ({
  toString: () => '', get: () => undefined,
}))
export const mockHeaders = vi.fn(() => headerGetter({}))

vi.mock('next/headers', () => ({ cookies: () => mockCookies(), headers: () => mockHeaders() }))

export function headerGetter(entries: Record<string, string>) {
  const normalized: Record<string, string> = {}
  for (const [k, v] of Object.entries(entries)) normalized[k.toLowerCase()] = v
  return { get: (k: string) => normalized[k.toLowerCase()] ?? null }
}

export interface SetupNextHeadersOptions {
  cookies?: string
  headers?: Record<string, string>
}

export function setupNextHeadersMock(defaults: SetupNextHeadersOptions = {}) {
  beforeEach(() => {
    mockCookies.mockReturnValue({ toString: () => defaults.cookies ?? '', get: () => undefined })
    mockHeaders.mockReturnValue(headerGetter(defaults.headers ?? {}))
  })
  afterEach(() => {
    mockCookies.mockReset()
    mockHeaders.mockReset()
    delete process.env.API_PROXY_URL
    vi.unstubAllEnvs()
  })
  return {
    mockCookies, mockHeaders, headerGetter,
    setHeaders: (entries: Record<string, string>) => mockHeaders.mockReturnValue(headerGetter(entries)),
    setCookies: (cookieStr: string, getter?: (name: string) => { value: string } | undefined) =>
      mockCookies.mockReturnValue({ toString: () => cookieStr, get: getter ?? (() => undefined) }),
  }
}

export interface CapturedHttpRequest {
  headers: IncomingHttpHeaders; method: string; url: string; body: string
}

export interface TestHttpServerContext {
  port: number; url: string; requests: CapturedHttpRequest[]
  readonly lastRequest: CapturedHttpRequest | undefined; clearRequests: () => void
}

export type TestHttpRequestHandler = (req: IncomingMessage, res: ServerResponse, captured: CapturedHttpRequest) => void | Promise<void>

export async function withTestHttpServer<T>(handler: TestHttpRequestHandler, fn: (ctx: TestHttpServerContext) => Promise<T>): Promise<T> {
  const requests: CapturedHttpRequest[] = []
  const server = createServer((req, res) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk: Buffer) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)))
    req.on('end', () => {
      const captured: CapturedHttpRequest = {
        headers: req.headers, method: req.method ?? '', url: req.url ?? '', body: Buffer.concat(chunks).toString('utf8'),
      }
      requests.push(captured)
      void handler(req, res, captured)
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const port = (server.address() as AddressInfo).port
  const url = `http://127.0.0.1:${port}`
  process.env.API_PROXY_URL = url
  const ctx: TestHttpServerContext = {
    port, url, requests,
    get lastRequest() { return requests[requests.length - 1] },
    clearRequests: () => { requests.length = 0 },
  }
  try {
    return await fn(ctx)
  } finally {
    await new Promise<void>((resolve, reject) => server.close((err) => (err ? reject(err) : resolve())))
  }
}

// ── Browser & DOM Stubs ──────────────────────────────────────────────────────

type ResizeCallback = (entries: ResizeObserverEntry[], observer: ResizeObserver) => void

export class StubResizeObserver implements ResizeObserver {
  static instances: StubResizeObserver[] = []
  constructor(private readonly callback?: ResizeCallback) { StubResizeObserver.instances.push(this) }
  observe = vi.fn(); unobserve = vi.fn(); disconnect = vi.fn()
  fire(): void { this.callback?.([], this) }
}

export function stubResizeObserver() {
  StubResizeObserver.instances = []
  const observe = vi.fn(), disconnect = vi.fn()
  class TrackedResizeObserver extends StubResizeObserver { observe = observe; disconnect = disconnect }
  vi.stubGlobal('ResizeObserver', TrackedResizeObserver)
  return { observe, disconnect, fire: () => { for (const ro of StubResizeObserver.instances) ro.fire() } }
}

type IntersectionCallback = (entries: IntersectionObserverEntry[], observer: IntersectionObserver) => void

export class StubIntersectionObserver implements IntersectionObserver {
  static instances: StubIntersectionObserver[] = []
  readonly root: Element | Document | null = null
  readonly rootMargin: string = '0px'
  readonly thresholds: ReadonlyArray<number> = [0]
  constructor(private readonly callback: IntersectionCallback) { StubIntersectionObserver.instances.push(this) }
  observe(): void {}; unobserve(): void {}; disconnect(): void {}
  takeRecords(): IntersectionObserverEntry[] { return [] }
  fire(isIntersecting: boolean): void { this.callback([{ isIntersecting } as IntersectionObserverEntry], this) }
}

export function stubIntersectionObserver(): { fire: (isIntersecting: boolean) => void } {
  StubIntersectionObserver.instances = []
  vi.stubGlobal('IntersectionObserver', StubIntersectionObserver)
  return { fire: (isIntersecting) => { for (const io of StubIntersectionObserver.instances) io.fire(isIntersecting) } }
}

export function stubSystemTimeZone(timeZone: string): void {
  const Real = Intl.DateTimeFormat
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

const define = (target: object, key: string, value: unknown) =>
  Object.defineProperty(target, key, { value, configurable: true, writable: true })

export function stubPointerCaptureAndScroll() {
  define(window.HTMLElement.prototype, 'hasPointerCapture', vi.fn())
  define(window.HTMLElement.prototype, 'releasePointerCapture', vi.fn())
  define(window.HTMLElement.prototype, 'scrollIntoView', vi.fn())
}

export function stubGetAnimations() {
  define(window.Element.prototype, 'getAnimations', vi.fn(() => []))
}

export function stubMatchMedia() {
  define(
    window,
    'matchMedia',
    vi.fn().mockImplementation((media: string) => ({
      matches: false, media, onchange: null,
      addListener: vi.fn(), removeListener: vi.fn(),
      addEventListener: vi.fn(), removeEventListener: vi.fn(), dispatchEvent: vi.fn(),
    })),
  )
}

export function stubWindowLocationAssign(assignSpy = vi.fn(), href = 'http://localhost/') {
  define(window, 'location', { ...window.location, assign: assignSpy, origin: 'http://localhost', href })
  return assignSpy
}

export function reactVirtualModuleMock(rowHeight = 40) {
  return {
    useVirtualizer: (options: { count?: number }) => {
      const count = options.count ?? 0
      return {
        getVirtualItems: () =>
          Array.from({ length: count }, (_, i) => ({ index: i, key: i, start: i * rowHeight, end: (i + 1) * rowHeight, size: rowHeight })),
        getTotalSize: () => count * rowHeight,
      }
    },
  }
}

export type MockMapEventPayload = Record<string, unknown>
export type MockMapListener = (e: MockMapEventPayload) => void
export type MockMapSource = Record<string, unknown>
export type MockMapLayer = { id?: string } & Record<string, unknown>
export type MockMapSourceHandle = MockMapSource & { setData: (data: unknown) => void }

export interface MockMapInstance {
  container: string | HTMLElement | undefined; listeners: Record<string, MockMapListener[]>
  sources: Record<string, MockMapSource>; layers: MockMapLayer[]; options: Partial<MapOptions> | undefined
  on: (event: string, ...rest: unknown[]) => void; off: () => void
  addSource: (id: string, src: MockMapSource) => void; addLayer: (layer: MockMapLayer) => void
  removeLayer: () => void; removeSource: () => void
  getSource: (id: string) => MockMapSourceHandle | undefined; getLayer: (id: string) => MockMapLayer | undefined
  setFeatureState: () => void; setPaintProperty: (...args: unknown[]) => void; setLayoutProperty: () => void
  setFilter: () => void; setStyle: () => void; isStyleLoaded: () => boolean
  getStyle: () => { layers: MockMapLayer[] }; queryRenderedFeatures: () => unknown[]
  getCanvas: () => { style: Record<string, string> }; remove: () => void; resize: () => void
  removed: boolean; project: () => { x: number; y: number }; fitBounds: () => void; flyTo: () => void
  cameraForBounds: () => { center: [number, number]; zoom: number }; setMaxBounds: () => void
  addControl: () => void; emit: (event: string, payload?: MockMapEventPayload) => void
}

export const mapInstances: MockMapInstance[] = []

export function maplibreMockFactory(opts?: { strictSources?: boolean }) {
  const strictSources = opts?.strictSources ?? false
  class MockMap implements MockMapInstance {
    container: string | HTMLElement | undefined
    listeners: Record<string, MockMapListener[]> = {}
    sources: Record<string, MockMapSource> = {}
    layers: MockMapLayer[] = []
    options: Partial<MapOptions> | undefined
    removed = false
    constructor(mapOpts?: Partial<MapOptions>) {
      this.container = mapOpts?.container; this.options = mapOpts
      mapInstances.push(this)
      queueMicrotask(() => this.emit('load'))
    }
    on(event: string, ...rest: unknown[]) {
      ;(this.listeners[event] ??= []).push((rest.length === 2 ? rest[1] : rest[0]) as MockMapListener)
    }
    off() {}; removeLayer() {}; removeSource() {}; setFeatureState() {}; setPaintProperty() {}
    setLayoutProperty() {}; setFilter() {}; setStyle() {}; resize() {}; fitBounds() {}
    flyTo() {}; setMaxBounds() {}; addControl() {}
    addSource(id: string, src: MockMapSource) {
      const handle: MockMapSourceHandle = { ...src, setData: (data: unknown) => { handle.data = data } }
      this.sources[id] = handle
    }
    addLayer(layer: MockMapLayer) { this.layers.push(layer) }
    getSource(id: string): MockMapSourceHandle | undefined {
      const src = this.sources[id] as MockMapSourceHandle | undefined
      return strictSources ? src : (src ?? { setData: () => {} })
    }
    getLayer(id: string) { return this.layers.find((l) => l.id === id) }
    isStyleLoaded = () => true
    getStyle = () => ({ layers: this.layers })
    queryRenderedFeatures = (): unknown[] => []
    getCanvas = () => ({ style: {} as Record<string, string> })
    remove() { this.removed = true }
    project = () => ({ x: 0, y: 0 })
    cameraForBounds = (): { center: [number, number]; zoom: number } => ({ center: [0, 0], zoom: 2 })
    emit(event: string, payload: MockMapEventPayload = {}) {
      ;(this.listeners[event] || []).forEach((fn) => fn(payload))
    }
  }
  class MockLngLatBounds { extend() { return this } }
  const lib = { Map: MockMap, LngLatBounds: MockLngLatBounds, NavigationControl: class {}, GlobeControl: class {}, setWorkerUrl: () => {} }
  return { default: lib, ...lib }
}

export function installMaplibreSideEffects(): void {
  if (!('ResizeObserver' in globalThis)) globalThis.ResizeObserver = StubResizeObserver
}
