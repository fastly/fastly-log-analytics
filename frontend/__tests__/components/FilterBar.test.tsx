/**
 * @vitest-environment jsdom
 *
 * Tests for FilterBar — the FE's primary filter UI. Bug-prone because
 * it juggles 3 stores (filter, service, timezone), date-picker local
 * state, virtual filter columns (_bot_name, _ngwaf_bot_name,
 * waf_sig_ind), and Next/router navigation.
 *
 * Migration (TESTING_PLAN_3 item 10): fireEvent → userEvent, querySelector
 * → getByRole. Real user-event clicks model the focus + pointer chain
 * that fireEvent.click skips; getByRole assertions describe what an
 * assistive-tech user actually navigates, so a regression in the
 * aria-label or button-role surfaces as a test failure.
 *
 * Focus: filter-chip rendering and store interactions. The date-picker
 * + compare-mode flows are out of scope (covered by Playwright E2E).
 */
import { render, screen, cleanup, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, beforeEach, beforeAll, afterEach } from 'vitest'
import { axe } from 'vitest-axe'
import React from 'react'
import { FilterBar } from '@/components/FilterBar/FilterBar'
import { useFilterStore } from '@/stores/filterStore'
import { useServiceStore } from '@/stores/serviceStore'

vi.mock('next/navigation', () => ({
  usePathname: () => '/dashboard',
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
}))

vi.mock('@tanstack/react-query', async () => {
  const actual = await vi.importActual<typeof import('@tanstack/react-query')>(
    '@tanstack/react-query',
  )
  return {
    ...actual,
    useQuery: () => ({ data: undefined, isLoading: false, error: null }),
    // FilterBar uses queryClient.getQueryState(['bootstrap']) to gate
    // its log-extents query on bootstrap pending. The test doesn't
    // mount a QueryClientProvider; return a stub whose getQueryState
    // says "no bootstrap observed" so the FilterBar code path doesn't
    // crash and falls through to its existing enabled gate.
    useQueryClient: () => ({ getQueryState: () => undefined }),
  }
})

vi.mock('@/components/FilterBar/AddFilterDialog', () => ({
  AddFilterDialog: () => <button data-testid="add-filter-stub">Add Filter</button>,
}))
vi.mock('@/components/FilterBar/SaveViewDialog', () => ({
  SaveViewDialog: () => <div data-testid="save-view-stub" />,
}))
vi.mock('@/components/FilterBar/ViewSelector', () => ({
  ViewSelector: () => <div data-testid="view-selector-stub" />,
}))

beforeAll(() => {
  global.ResizeObserver = class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver
})

afterEach(() => cleanup())

beforeEach(() => {
  act(() => {
    useFilterStore.setState({
      filters: [],
      edgeOnly: false,
      compareMode: false,
      isAutoRange: true,
      hasSyncedExtents: false,
    })
    useServiceStore.setState({ activeServiceId: 'test-svc' })
  })
})

describe('FilterBar — filter chip rendering', () => {
  it('renders no chips when filters is empty', () => {
    render(<FilterBar />)
    expect(screen.getByTestId('add-filter-stub')).toBeTruthy()
    // No mode-toggle buttons present when no chips
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
    const toggles = screen.getAllByRole('button', { name: /toggle include\/exclude/i })
    const glyphs = toggles.map((b) => b.textContent?.trim())
    expect(glyphs).toContain('+')
    expect(glyphs).toContain('-')
  })

  it('_ngwaf_bot_name filter renders "Fastly Verified Bot:" prefix', () => {
    act(() => {
      useFilterStore.setState({
        filters: [
          { id: 'b1', column: '_ngwaf_bot_name', value: 'GoogleBot', mode: 'include' },
        ],
      })
    })
    render(<FilterBar />)
    expect(screen.getByText('Fastly Verified Bot:')).toBeTruthy()
    expect(screen.getByText('GoogleBot')).toBeTruthy()
    expect(screen.queryByText('_ngwaf_bot_name:')).toBeNull()
  })

  it('_bot_name filter renders "Bot:" prefix', () => {
    act(() => {
      useFilterStore.setState({
        filters: [{ id: 'b2', column: '_bot_name', value: 'googlebot', mode: 'include' }],
      })
    })
    render(<FilterBar />)
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

    // Aria-label scopes each remove button to its chip — no fragile
    // class-name selectors.
    const removeUS = screen.getByRole('button', {
      name: /remove filter country: US/i,
    })

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

    const toggle = screen.getByRole('button', { name: /toggle include\/exclude/i })

    await user.click(toggle)
    expect(useFilterStore.getState().filters[0].mode).toBe('exclude')

    await user.click(toggle)
    expect(useFilterStore.getState().filters[0].mode).toBe('include')
  })

  // TESTING_PLAN_3 item 19. Filter chips use aria-labels (verified by the
  // remove-button test above) and the +/- toggle has an aria-label too.
  // Pin those with axe so a future refactor that swaps to icon-only buttons
  // without aria text trips this assertion before merge.
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
