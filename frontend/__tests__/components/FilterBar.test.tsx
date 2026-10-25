/**
 * @vitest-environment jsdom
 *
 * Consolidated tests for FilterBar, ActiveFiltersBanner, AddFilterDialogContent, and FilterValueCell.
 */
import { stubResizeObserver, stubPointerCaptureAndScroll } from '@/__tests__/helpers/page-smoke'
import { render, screen, cleanup, act, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach, beforeAll, afterEach } from 'vitest'
import { axe } from 'vitest-axe'
import React from 'react'
import { FilterBar, ActiveFiltersBanner, AddFilterDialogContent } from '@/components/FilterBar/FilterBar'
import { FilterValueCell, buildDashboardFilterUrl } from '@/components/DataTable'
import { Dialog } from '@/components/ui/dialog'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/filterStore'
import { usePathname } from 'next/navigation'

vi.mock('next/navigation', async () => (await import('../helpers/page-smoke')).navigationModuleMock('/dashboard'))

let __capturedPredicate: ((q: { queryKey?: readonly unknown[] }) => boolean) | null = null
let __logExtentsData: unknown = undefined
let maskIps = false
let suggestionValues: { value: string | null; label?: string; count: number }[] = []

vi.mock('@tanstack/react-query', async () => {
  const actual = await vi.importActual<typeof import('@tanstack/react-query')>(
    '@tanstack/react-query',
  )
  return {
    ...actual,
    useQuery: () => ({ data: __logExtentsData, isLoading: false, error: null }),
    useMutation: () => ({ mutate: vi.fn(), isPending: false }),
    useQueryClient: () => ({ getQueryState: () => undefined, invalidateQueries: vi.fn() }),
    useIsFetching: (opts?: { predicate?: typeof __capturedPredicate }) => {
      if (opts?.predicate) __capturedPredicate = opts.predicate
      return 0
    },
  }
})

vi.mock('@/hooks/useBootstrap', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useBootstrap')>()),
  useIsAnalyst: () => false,
  useMaskIps: () => maskIps,
  useEnforceMaskedFilters: () => {},
  useLogFieldsCatalog: () => ({ data: undefined }),
  useFieldLabel: () => (f: string) => f,
  useFieldValues: () => ({ data: { values: suggestionValues }, isLoading: false }),
}))
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))
vi.mock('@/hooks/useDashboardCards', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useDashboardCards')>()),
  useDebounce: (v: string) => v,
}))

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

afterEach(() => cleanup())

beforeEach(() => {
  __logExtentsData = undefined
  maskIps = false
  suggestionValues = []
  vi.mocked(usePathname).mockReturnValue('/dashboard')
  act(() => {
    useFilterStore.getState().resetAll()
    useFilterStore.setState({
      filters: [],
      edgeOnly: false,
      compareMode: false,
      isAutoRange: true,
    })
    useServiceStore.setState({ activeServiceId: 'test-svc' })
  })
})

describe('FilterBar — filter chip rendering', () => {
  it('preserves date drafts until the committed range changes', () => {
    act(() => {
      useFilterStore.getState().setRange('2026-01-02T01:00:00Z', '2026-01-02T02:00:00Z')
    })
    render(<FilterBar />)
    const start = screen.getByLabelText<HTMLInputElement>('Range start')
    const initial = start.value
    fireEvent.change(start, { target: { value: '2026-01-03T03:00' } })
    act(() => {
      useFilterStore.getState().toggleEdgeOnly()
    })
    expect(start).toHaveValue('2026-01-03T03:00')
    act(() => {
      useFilterStore.getState().setRange('2026-01-04T01:00:00Z', '2026-01-04T02:00:00Z')
    })
    expect(start).not.toHaveValue('2026-01-03T03:00')
    expect(start.value).not.toBe(initial)
  })

  it('keeps switches synchronized with external store resets', async () => {
    const user = userEvent.setup()
    render(<FilterBar />)
    const edge = screen.getByRole('switch', { name: /edge only/i })
    const compare = screen.getByRole('switch', { name: /compare/i })
    await user.click(edge)
    await user.click(compare)
    expect(edge).toBeChecked()
    expect(compare).toBeChecked()
    act(() => {
      useFilterStore.getState().resetAll()
    })
    expect(edge).not.toBeChecked()
    expect(compare).not.toBeChecked()
  })

  it('renders no chips when filters is empty', () => {
    render(<FilterBar />)
    expect(screen.getByTestId('add-filter-stub')).toBeTruthy()
    expect(screen.queryByRole('button', { name: /toggle include\/exclude/i })).toBeNull()
  })

  it('renders one chip per filter with column:value', () => {
    act(() => {
      useFilterStore.setState({
        filters: [
          { id: 'f1', column: 'country', value: 'US', mode: 'include' },
          { id: 'f2', column: 'status', value: '500', mode: 'exclude' },
        ],
      })
    })
    render(<FilterBar />)

    expect(screen.getByText('country:')).toBeTruthy()
    expect(screen.getByText('status:')).toBeTruthy()
    expect(screen.getByText('US')).toBeTruthy()
    expect(screen.getByText('500')).toBeTruthy()
  })

  it('include filter shows "+" and exclude filter shows "-"', () => {
    act(() => {
      useFilterStore.setState({
        filters: [
          { id: 'inc', column: 'country', value: 'US', mode: 'include' },
          { id: 'exc', column: 'country', value: 'CN', mode: 'exclude' },
        ],
      })
    })
    render(<FilterBar />)
    const toggles = screen.getAllByRole('button', { name: /(including|excluding) country=.* activate to toggle/i })
    const glyphs = toggles.map((b) => b.textContent?.trim())
    expect(glyphs).toContain('+')
    expect(glyphs).toContain('-')
  })

  it('_ngwaf_bot_name filter renders "Fastly Verified Bot:" prefix and _bot_name renders "Bot:"', () => {
    act(() => {
      useFilterStore.setState({
        filters: [
          { id: 'b1', column: '_ngwaf_bot_name', value: 'GoogleBot', mode: 'include' },
          { id: 'b2', column: '_bot_name', value: 'googlebot', mode: 'include' },
        ],
      })
    })
    render(<FilterBar />)
    expect(screen.getByText('Fastly Verified Bot:')).toBeTruthy()
    expect(screen.getByText('GoogleBot')).toBeTruthy()
    expect(screen.queryByText('_ngwaf_bot_name:')).toBeNull()
    expect(screen.getByText('Bot:')).toBeTruthy()
    expect(screen.queryByText('_bot_name:')).toBeNull()
  })

  it('clicking the X on a chip removes that filter from the store', async () => {
    const user = userEvent.setup()
    act(() => {
      useFilterStore.setState({
        filters: [
          { id: 'f1', column: 'country', value: 'US', mode: 'include' },
          { id: 'f2', column: 'status', value: '500', mode: 'exclude' },
        ],
      })
    })
    render(<FilterBar />)

    expect(useFilterStore.getState().filters).toHaveLength(2)
    const removeUS = screen.getByRole('button', { name: /remove filter country: US/i })
    await user.click(removeUS)
    expect(useFilterStore.getState().filters).toHaveLength(1)
    expect(useFilterStore.getState().filters[0].id).toBe('f2')
  })

  it('clicking the +/- toggle flips the filter mode in the store', async () => {
    const user = userEvent.setup()
    act(() => {
      useFilterStore.setState({
        filters: [{ id: 'f1', column: 'country', value: 'US', mode: 'include' }],
      })
    })
    render(<FilterBar />)

    const toggle = screen.getByRole('button', { name: /(including|excluding) country=.* activate to toggle/i })
    await user.click(toggle)
    expect(useFilterStore.getState().filters[0].mode).toBe('exclude')

    const toggleAfter = screen.getByRole('button', { name: /(including|excluding) country=.* activate to toggle/i })
    await user.click(toggleAfter)
    expect(useFilterStore.getState().filters[0].mode).toBe('include')
  })

  it('has no axe-detectable a11y violations when chips are present', async () => {
    act(() => {
      useFilterStore.setState({
        filters: [
          { id: 'f1', column: 'country', value: 'US', mode: 'include' },
          { id: 'f2', column: 'status', value: '500', mode: 'exclude' },
        ],
      })
    })
    const { container } = render(<FilterBar />)
    const results = await axe(container, {
      rules: { 'color-contrast': { enabled: false } },
    })
    expect(results).toHaveNoViolations()
  })

  it('renders many chips without crashing (no key collisions)', () => {
    act(() => {
      useFilterStore.setState({
        filters: Array.from({ length: 20 }, (_, i) => ({
          id: `filter-${i}`,
          column: 'country',
          value: ['US', 'GB', 'DE'][i % 3],
          mode: 'include' as const,
        })),
      })
    })
    render(<FilterBar />)
    expect(screen.getAllByText('country:')).toHaveLength(20)
  })
})

describe('FilterBar — pill-flash predicate (time-bound allowlist)', () => {
  beforeEach(() => {
    __capturedPredicate = null
  })

  it('returns true only for queries the date range affects', () => {
    render(<FilterBar />)
    expect(__capturedPredicate).not.toBeNull()
    const p = __capturedPredicate!

    expect(p({ queryKey: ['dashboard', 'aggregates', 'svc'] })).toBe(true)
    expect(p({ queryKey: ['sessions', 'list', 'svc'] })).toBe(true)
    expect(p({ queryKey: ['usage', 'bandwidth', 'svc'] })).toBe(true)
    expect(p({ queryKey: ['insights', 'svc', 24, 24] })).toBe(true)
    expect(p({ queryKey: ['usage-log', 'head', 'svc'] })).toBe(true)
  })

  it('returns false for background polls and empty/malformed query keys', () => {
    render(<FilterBar />)
    const p = __capturedPredicate!

    expect(p({ queryKey: ['log-extents', 'svc'] })).toBe(false)
    expect(p({ queryKey: ['bootstrap'] })).toBe(false)
    expect(p({ queryKey: ['sync-status', 'svc'] })).toBe(false)
    expect(p({ queryKey: ['last-sync', 'svc'] })).toBe(false)
    expect(p({ queryKey: ['admin', 'health-snapshot'] })).toBe(false)
    expect(p({ queryKey: ['admin', 'overview', 'queries-summary'] })).toBe(false)
    expect(p({ queryKey: ['admin', 'iceberg', 'svc'] })).toBe(false)
    expect(p({ queryKey: ['admin', 'share', 'live'] })).toBe(false)
    expect(p({ queryKey: ['system-jobs'] })).toBe(false)
    expect(p({ queryKey: ['alerts', 'svc'] })).toBe(false)
    expect(p({ queryKey: undefined })).toBe(false)
    expect(p({ queryKey: [] })).toBe(false)
    expect(p({ queryKey: [123] })).toBe(false)
  })
})

describe('ActiveFiltersBanner', () => {
  it('renders nothing when no filters and edgeOnly is off', () => {
    const { container } = render(<ActiveFiltersBanner />)
    expect(container.firstChild).toBeNull()
  })

  it('renders a pill for each active filter and has region role', () => {
    useFilterStore.getState().addFilter('country', 'US', 'include')
    useFilterStore.getState().addFilter('status', '500', 'exclude')

    render(<ActiveFiltersBanner />)
    expect(screen.getByText('country:')).toBeInTheDocument()
    expect(screen.getByText('US')).toBeInTheDocument()
    expect(screen.getByText('status:')).toBeInTheDocument()
    expect(screen.getByText('500')).toBeInTheDocument()
    expect(
      screen.getByRole('region', { name: /active filters from other pages/i }),
    ).toBeInTheDocument()
  })

  it('renders the "Edge only" badge and "Clear all" wipes filters + edgeOnly', () => {
    useFilterStore.getState().addFilter('country', 'US', 'include')
    useFilterStore.getState().toggleEdgeOnly()

    render(<ActiveFiltersBanner />)
    expect(screen.getByText('Edge only')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /clear all/i }))

    expect(useFilterStore.getState().filters).toHaveLength(0)
    expect(useFilterStore.getState().edgeOnly).toBe(false)
  })
})

function renderDialogContent(onClose: () => void = () => {}) {
  return render(
    <Dialog open>
      <AddFilterDialogContent onClose={onClose} />
    </Dialog>,
  )
}

describe('AddFilterDialogContent', () => {
  it('keeps Apply Filter disabled until a value is typed', async () => {
    const user = userEvent.setup()
    renderDialogContent()
    const apply = screen.getByRole('button', { name: /Apply Filter/ }) as HTMLButtonElement
    expect(apply.disabled).toBe(true)
    await user.type(screen.getByPlaceholderText('Type a ip value...'), '1.2.3.4')
    await waitFor(() => expect(apply.disabled).toBe(false))
  })

  it('adds an include filter for the default field and closes', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    renderDialogContent(onClose)
    await user.type(screen.getByPlaceholderText('Type a ip value...'), '1.2.3.4')
    await user.click(screen.getByRole('button', { name: /Apply Filter/ }))
    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1))
    const [filter] = useFilterStore.getState().filters
    expect(filter).toMatchObject({ column: 'ip', value: '1.2.3.4', mode: 'include' })
  })

  it('honours the Exclude tab and submits on Enter', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    renderDialogContent(onClose)
    await user.click(screen.getByRole('tab', { name: /Exclude/ }))
    await user.type(screen.getByPlaceholderText('Type a ip value...'), 'bad{Enter}')
    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1))
    expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'ip', value: 'bad', mode: 'exclude' })
  })

  it('defaults a masking analyst to the url field', async () => {
    maskIps = true
    const user = userEvent.setup()
    renderDialogContent()
    await user.type(screen.getByPlaceholderText('Type a url value...'), '/x')
    await user.click(screen.getByRole('button', { name: /Apply Filter/ }))
    await waitFor(() => expect(useFilterStore.getState().filters.length).toBe(1))
    expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'url', value: '/x' })
  })

  it('fills the value from a suggestion without submitting', async () => {
    suggestionValues = [{ value: 'US', label: 'United States', count: 1200 }]
    const user = userEvent.setup()
    const onClose = vi.fn()
    renderDialogContent(onClose)
    const input = screen.getByPlaceholderText('Type a ip value...') as HTMLInputElement
    await user.type(input, 'U')
    await user.click(await screen.findByRole('button', { name: /United States/ }))
    expect(input.value).toBe('US')
    expect(onClose).not.toHaveBeenCalled()
    expect(useFilterStore.getState().filters.length).toBe(0)
  })
})

describe('buildDashboardFilterUrl', () => {
  it('builds a single-filter URL', () => {
    expect(buildDashboardFilterUrl([{ column: 'url', value: '/api/data' }]))
      .toBe('/dashboard?filter_url=%2Fapi%2Fdata')
  })

  it('builds a multi-filter URL with ampersands', () => {
    expect(buildDashboardFilterUrl([
      { column: 'city', value: 'London' },
      { column: 'region', value: 'England' },
      { column: 'country', value: 'GB' },
    ])).toBe('/dashboard?filter_city=London&filter_region=England&filter_country=GB')
  })

  it('escapes the underscore-prefixed bot id columns', () => {
    expect(buildDashboardFilterUrl([{ column: '_wellknown_bot_id', value: 'bot-1' }]))
      .toBe('/dashboard?filter__wellknown_bot_id=bot-1')
  })
})

describe('FilterValueCell', () => {
  beforeEach(() => {
    useFilterStore.getState().clearFilters()
    vi.mocked(usePathname).mockReturnValue('/origin')
    maskIps = false
  })

  afterEach(() => {
    useFilterStore.getState().clearFilters()
  })

  it('renders the display value', () => {
    render(<FilterValueCell filters={[{ column: 'url', value: '/api/data' }]} />)
    expect(screen.getByText('/api/data')).toBeInTheDocument()
  })

  it('prefers the explicit display prop over the filter value', () => {
    render(
      <FilterValueCell
        filters={[{ column: '_wellknown_bot_id', value: 'bot-1' }]}
        display="Googlebot"
      />,
    )
    expect(screen.getByText('Googlebot')).toBeInTheDocument()
    expect(screen.queryByText('bot-1')).not.toBeInTheDocument()
  })

  it('"Filter <page>" menu item calls addFilter for every entry', () => {
    const addFilterSpy = vi.spyOn(useFilterStore.getState(), 'addFilter')
    render(
      <FilterValueCell
        filters={[
          { column: 'city', value: 'London' },
          { column: 'country', value: 'GB' },
        ]}
        display="London"
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /Filter actions for London/i }))
    fireEvent.click(screen.getByText(/Filter origin page/i))
    expect(addFilterSpy).toHaveBeenCalledWith('city', 'London', 'include')
    expect(addFilterSpy).toHaveBeenCalledWith('country', 'GB', 'include')
  })

  it('"Copy value" writes the first filter value to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<FilterValueCell filters={[{ column: 'city', value: 'London' }]} display="London" />)
    fireEvent.click(screen.getByRole('button', { name: /Filter actions for London/i }))
    fireEvent.click(screen.getByText(/Copy value/i))
    await waitFor(() => expect(writeText).toHaveBeenCalledWith('London'))
  })

  it('hides "Open in dashboard" when already on /dashboard', () => {
    vi.mocked(usePathname).mockReturnValue('/dashboard')
    render(<FilterValueCell filters={[{ column: 'country', value: 'US' }]} />)
    fireEvent.click(screen.getByRole('button', { name: /Filter actions for US/i }))
    expect(screen.queryByText(/Open in dashboard/i)).not.toBeInTheDocument()
    expect(screen.getByText(/Filter dashboard page/i)).toBeInTheDocument()
  })

  it('opens dashboard URL in a new tab via "Open in dashboard"', () => {
    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null)
    render(<FilterValueCell filters={[{ column: 'pop', value: 'JFK' }]} />)
    fireEvent.click(screen.getByRole('button', { name: /Filter actions for JFK/i }))
    fireEvent.click(screen.getByText(/Open in dashboard/i))
    expect(openSpy).toHaveBeenCalledWith(
      '/dashboard?filter_pop=JFK',
      '_blank',
      'noopener,noreferrer',
    )
    openSpy.mockRestore()
  })

  it('renders empty cell (no trigger) when filters list is empty', () => {
    const { container } = render(<FilterValueCell filters={[]} display="—" />)
    expect(screen.getByText('—')).toBeInTheDocument()
    expect(container.querySelector('button')).toBeNull()
  })

  it('cmd/ctrl-click on the cell bypasses the menu and calls addFilter directly', () => {
    const addFilterSpy = vi.spyOn(useFilterStore.getState(), 'addFilter')
    render(<FilterValueCell filters={[{ column: 'pop', value: 'JFK' }]} />)
    const trigger = screen.getByRole('button', { name: /Filter actions for JFK/i })
    fireEvent.mouseDown(trigger, { metaKey: true })
    expect(addFilterSpy).toHaveBeenCalledWith('pop', 'JFK', 'include')
    expect(screen.queryByText(/Open in dashboard/i)).not.toBeInTheDocument()
  })

  it('plain click on the cell still opens the menu', () => {
    render(<FilterValueCell filters={[{ column: 'pop', value: 'JFK' }]} />)
    const trigger = screen.getByRole('button', { name: /Filter actions for JFK/i })
    fireEvent.click(trigger)
    expect(screen.getByText(/Filter origin page/i)).toBeInTheDocument()
  })

  it('renders an IP-family value as plain text (no filter menu) when masking', () => {
    maskIps = true
    const { container } = render(
      <FilterValueCell filters={[{ column: 'client_ip', value: '73.217.41.xxx' }]} />,
    )
    expect(screen.getByText('73.217.41.xxx')).toBeInTheDocument()
    expect(container.querySelector('button')).toBeNull()
  })

  it('still allows filtering by oip (origin IP) when masking', () => {
    maskIps = true
    render(<FilterValueCell filters={[{ column: 'oip', value: '151.101.1.51' }]} />)
    expect(
      screen.getByRole('button', { name: /Filter actions for 151.101.1.51/i }),
    ).toBeInTheDocument()
  })
})
