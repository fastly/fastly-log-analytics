/** @vitest-environment jsdom */
import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { AnalyticsCard } from '@/components/AnalyticsCard'

describe('AnalyticsCard loading copy', () => {
  it('derives an accessible loading message from the card title', () => {
    render(
      <AnalyticsCard title="Active Viewers" isLoading>
        <div>loaded content</div>
      </AnalyticsCard>,
    )

    expect(screen.getByRole('status')).toHaveTextContent('Loading Active Viewers...')
  })

  it('preserves an explicit loading message', () => {
    render(
      <AnalyticsCard
        title="Active Viewers"
        isLoading
        loadingMessage="Reading streaming summaries..."
      >
        <div>loaded content</div>
      </AnalyticsCard>,
    )

    expect(screen.getByRole('status')).toHaveTextContent('Reading streaming summaries...')
  })
})
