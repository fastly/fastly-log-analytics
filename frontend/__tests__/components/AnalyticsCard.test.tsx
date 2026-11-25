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

  it('extracts text from an element-valued title', () => {
    render(
      <AnalyticsCard title={<span>Active Viewers</span>} isLoading>
        <div>loaded content</div>
      </AnalyticsCard>,
    )

    expect(screen.getByRole('status')).toHaveTextContent('Loading Active Viewers...')
  })

  it('marks hidden children inert when empty so focusable controls cannot receive focus', () => {
    const { rerender } = render(
      <AnalyticsCard title="ABR Bitrate Ladder" isEmpty>
        <button type="button">Sort column</button>
      </AnalyticsCard>,
    )

    const button = screen.getByRole('button', { name: 'Sort column', hidden: true })
    const wrapper = button.parentElement
    expect(wrapper).toHaveAttribute('aria-hidden', 'true')
    expect(wrapper).toHaveAttribute('inert')

    rerender(
      <AnalyticsCard title="ABR Bitrate Ladder" isEmpty={false}>
        <button type="button">Sort column</button>
      </AnalyticsCard>,
    )

    const activeWrapper = screen.getByRole('button', { name: 'Sort column' }).parentElement
    expect(activeWrapper).not.toHaveAttribute('aria-hidden')
    expect(activeWrapper).not.toHaveAttribute('inert')
  })
})
