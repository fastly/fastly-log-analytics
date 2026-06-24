/**
 * ActiveFiltersBanner — read-only chip strip that surfaces global filters
 * on pages where the FilterBar is hidden (/insights, /alerts, /admin, etc.).
 * Contract pinned here:
 *   - renders NOTHING when no filters and edgeOnly is off
 *   - renders pills + edge-only badge when present
 *   - "Clear all" wipes filters AND turns edgeOnly off
 */
import * as React from 'react'
import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ActiveFiltersBanner } from '@/components/FilterBar/ActiveFiltersBanner'
import { useFilterStore } from '@/stores/filterStore'

function resetStore() {
  // Reset is a top-level store action; tests must isolate themselves
  // because zustand state survives across renders within a test file.
  useFilterStore.getState().resetAll()
}

describe('ActiveFiltersBanner', () => {
  beforeEach(() => {
    resetStore()
  })

  it('renders nothing when no filters and edgeOnly is off', () => {
    const { container } = render(<ActiveFiltersBanner />)
    expect(container.firstChild).toBeNull()
  })

  it('renders a pill for each active filter', () => {
    useFilterStore.getState().addFilter('country', 'US', 'include')
    useFilterStore.getState().addFilter('status', '500', 'exclude')

    render(<ActiveFiltersBanner />)
    expect(screen.getByText('country:')).toBeInTheDocument()
    expect(screen.getByText('US')).toBeInTheDocument()
    expect(screen.getByText('status:')).toBeInTheDocument()
    expect(screen.getByText('500')).toBeInTheDocument()
  })

  it('renders the "Edge only" badge when edgeOnly is set', () => {
    useFilterStore.getState().toggleEdgeOnly()
    render(<ActiveFiltersBanner />)
    expect(screen.getByText('Edge only')).toBeInTheDocument()
  })

  it('"Clear all" wipes filters AND turns edgeOnly off', () => {
    useFilterStore.getState().addFilter('country', 'US', 'include')
    useFilterStore.getState().toggleEdgeOnly()

    render(<ActiveFiltersBanner />)
    fireEvent.click(screen.getByRole('button', { name: /clear all/i }))

    expect(useFilterStore.getState().filters).toHaveLength(0)
    expect(useFilterStore.getState().edgeOnly).toBe(false)
  })

  it('has region role and accessible label for screen readers', () => {
    useFilterStore.getState().addFilter('country', 'US', 'include')
    render(<ActiveFiltersBanner />)
    expect(
      screen.getByRole('region', { name: /active filters from other pages/i }),
    ).toBeInTheDocument()
  })
})
