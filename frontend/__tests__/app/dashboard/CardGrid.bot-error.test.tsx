import { render, screen } from '@testing-library/react'
import { describe, test, expect, vi } from 'vitest'
import React from 'react'

// UX-7: the dashboard top-bots query destructured only `data`, so a 5xx left
// topBotsData undefined → the bot cards sat on a forever "Loading…" placeholder
// (isCardLoading stayed true). The fix renders a CardErrorState + Retry when
// topBotsError is set.

vi.mock('@/components/Dashboard/TopTenTable', () => ({ TopTenTable: () => <div data-testid="top-ten" /> }))
vi.mock('@/components/LazyMount', () => ({ LazyMount: ({ children }: { children: React.ReactNode }) => <>{children}</> }))

import { CardGrid } from '@/app/dashboard/_sections/CardGrid'

describe('CardGrid bot-card failure (UX-7)', () => {
  test('renders CardErrorState + Retry for a bot card when top-bots errors (not a forever spinner)', () => {
    const onRetryTopBots = vi.fn()
    render(
      <CardGrid
        visibleCardList={[{ id: '_bot_name', label: 'Top Bots', inActiveFormat: false }]}
        isReady={true}
        isLoadingAggs={false}
        isFetchingAggs={false}
        aggregates={{ data: {} }}
        compareAggregates={undefined}
        compareMode={false}
        topBotsData={undefined}
        topBotsError={new Error('top-bots boom')}
        onRetryTopBots={onRetryTopBots}
        collapsedSections={new Set()}
        toggleSectionCollapsed={() => {}}
        onRowClick={() => {}}
      />,
    )
    expect(screen.getByText('Failed to load bot data.')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument()
    expect(screen.queryByText('Loading...')).toBeNull()
  })
})
