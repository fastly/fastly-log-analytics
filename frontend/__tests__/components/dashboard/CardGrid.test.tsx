/**
 * CardGrid renders categorized TopTenTable cards under collapsible
 * section headers. The component is presentational — visibility,
 * collapsed state, and click routing are passed in by the dashboard
 * page. These tests pin: (1) the skeleton renders when the catalog
 * hasn't returned yet (visibleCardList === []), (2) a populated
 * payload renders one section header per CARD_CATEGORIES with the
 * right card count, and (3) clicking a section header invokes
 * `toggleSectionCollapsed` with the section id.
 *
 * @vitest-environment jsdom
 */
import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import React from 'react'

// LazyMount uses IntersectionObserver — in jsdom it falls back to
// "render immediately" but the IntersectionObserver constructor only
// exists when the test polyfills it. Stub the wrapper to a passthrough
// so children render synchronously, regardless of environment.
vi.mock('@/components/LazyMount', () => ({
  LazyMount: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// TopTenTable pulls in useLogFieldsCatalog + tooltip primitives. We
// only care that the right `title` and `field` props arrive — render
// a leaf div so the assertions stay focused on CardGrid's branching.
vi.mock('@/components/Dashboard/TopTenTable', () => ({
  TopTenTable: ({ title, field }: { title: string; field?: string }) => (
    <div data-testid={`card-${field ?? title}`}>{title}</div>
  ),
}))

import { CardGrid } from '@/app/dashboard/_sections/CardGrid'
import { CARD_CATEGORIES } from '@/app/dashboard/_sections/categories'

const aggregates = {
  data: {
    status: { total: 100, top: [{ value: 200, label: '200', count: 90 }] },
    ip: { total: 100, top: [{ value: '1.1.1.1', label: '1.1.1.1', count: 10 }] },
  },
}

describe('CardGrid', () => {
  beforeEach(() => vi.clearAllMocks())

  it('renders the skeleton (one section per CARD_CATEGORIES) when visibleCardList is empty', () => {
    render(
      <CardGrid
        visibleCardList={[]}
        isReady={false}
        isLoadingAggs={true}
        isFetchingAggs={false}
        aggregates={undefined}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    // Every category label appears once in the skeleton.
    for (const cat of CARD_CATEGORIES) {
      expect(screen.getByText(cat.label)).toBeInTheDocument()
    }
    // Skeleton shows the "Initializing..." text because isReady=false.
    expect(screen.getAllByText('Initializing...').length).toBeGreaterThan(0)
    // No real cards mount — TopTenTable mock never fires.
    expect(screen.queryByTestId(/^card-/)).toBeNull()
  })

  it('renders categorized sections + the expected cards once aggregates arrive', () => {
    const visibleCardList = [
      { id: 'ip', label: 'Client IP' },
      { id: 'status', label: 'HTTP Status' },
    ]
    render(
      <CardGrid
        visibleCardList={visibleCardList}
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={aggregates}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    // Both cards land under the "Request" section.
    expect(screen.getByTestId('card-ip')).toBeInTheDocument()
    expect(screen.getByTestId('card-status')).toBeInTheDocument()
    // The "Request" section header is present; un-populated categories
    // (Cache, Geography, ...) are filtered out.
    expect(screen.getByText('Request')).toBeInTheDocument()
    expect(screen.queryByText('Cache')).toBeNull()
    expect(screen.queryByText('Geography')).toBeNull()
  })

  it('invokes toggleSectionCollapsed with the section id when the header is clicked', () => {
    const toggle = vi.fn()
    render(
      <CardGrid
        visibleCardList={[{ id: 'ip', label: 'Client IP' }]}
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={aggregates}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={toggle}
        onRowClick={vi.fn()}
      />,
    )
    const header = screen.getByRole('button', { name: /request/i })
    fireEvent.click(header)
    expect(toggle).toHaveBeenCalledWith('request')
  })

  it('hides the cards grid when the section is collapsed', () => {
    render(
      <CardGrid
        visibleCardList={[{ id: 'ip', label: 'Client IP' }]}
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={aggregates}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set(['request'])}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    const header = screen.getByRole('button', { name: /request/i })
    expect(header).toHaveAttribute('aria-expanded', 'false')
    // Collapsed section omits the card grid entirely.
    expect(screen.queryByTestId('card-ip')).toBeNull()
  })

  it('groups uncategorized cards under a Custom section', () => {
    render(
      <CardGrid
        visibleCardList={[{ id: 'custom_field_42', label: 'My Custom Field' }]}
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={{ data: { custom_field_42: { total: 1, top: [] } } }}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        collapsedSections={new Set()}
        toggleSectionCollapsed={vi.fn()}
        onRowClick={vi.fn()}
      />,
    )
    expect(screen.getByText('Custom')).toBeInTheDocument()
    expect(screen.getByTestId('card-custom_field_42')).toBeInTheDocument()
  })
})
