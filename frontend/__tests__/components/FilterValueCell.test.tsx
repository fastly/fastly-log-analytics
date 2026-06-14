import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import React from 'react'
import { FilterValueCell, buildDashboardFilterUrl } from '@/components/FilterValueCell'
import { useFilterStore } from '@/stores/filterStore'

vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/origin'),
}))

import { usePathname } from 'next/navigation'

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
})
