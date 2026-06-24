/**
 * @vitest-environment jsdom
 *
 * ScoringControls is the row of widgets above the sessions table:
 *  - flagged-only switch
 *  - min-requests numeric input
 *  - min-4xx% numeric input
 *  - refresh button (calls refetch)
 *  - a "Clear filters" button that appears once any filter is active
 *
 * The component is a pure controlled-input shell — the parent owns the
 * state and refetch handle, so the tests just need to verify that user
 * input flows back to the supplied setters and that the refresh button
 * invokes the refetch callback. Placeholder hints come from the
 * `data.min_reqs_flag` / `data.min_4xx_pct_flag` fields on the sessions
 * response, so we also assert that those get rendered.
 */
import { render, screen, fireEvent, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, afterEach } from 'vitest'
import React from 'react'

import { ScoringControls } from '@/app/sessions/_sections/ScoringControls'

afterEach(() => cleanup())

function baseProps(overrides: Partial<React.ComponentProps<typeof ScoringControls>> = {}) {
  return {
    flaggedOnly: false,
    setFlaggedOnly: vi.fn(),
    minReqs: '' as number | '',
    setMinReqs: vi.fn(),
    min4xxPct: '' as number | '',
    setMin4xxPct: vi.fn(),
    data: { min_reqs_flag: 1000, min_4xx_pct_flag: 20 },
    isFetching: false,
    isLoadingInitial: false,
    refetch: vi.fn(),
    ...overrides,
  }
}

describe('ScoringControls', () => {
  it('renders the flagged-only switch label and the threshold inputs', () => {
    render(<ScoringControls {...baseProps()} />)
    expect(screen.getByText(/flagged only/i)).toBeInTheDocument()
    expect(screen.getByText(/min\. requests/i)).toBeInTheDocument()
    expect(screen.getByText(/min\. 4xx%/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /refresh/i })).toBeInTheDocument()
  })

  it('uses data.min_reqs_flag / data.min_4xx_pct_flag as placeholder hints', () => {
    render(
      <ScoringControls
        {...baseProps({ data: { min_reqs_flag: 5000, min_4xx_pct_flag: 35 } })}
      />,
    )
    expect(screen.getByPlaceholderText(/≥\s*5000/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/≥\s*35/)).toBeInTheDocument()
  })

  it('falls back to default placeholders when data is undefined', () => {
    render(<ScoringControls {...baseProps({ data: undefined })} />)
    expect(screen.getByPlaceholderText(/≥\s*1000/)).toBeInTheDocument()
    expect(screen.getByPlaceholderText(/≥\s*20/)).toBeInTheDocument()
  })

  it('fires setMinReqs with a number when the requests input changes', () => {
    const setMinReqs = vi.fn()
    render(<ScoringControls {...baseProps({ setMinReqs })} />)
    const reqInput = screen.getByPlaceholderText(/≥\s*1000/) as HTMLInputElement
    fireEvent.change(reqInput, { target: { value: '250' } })
    expect(setMinReqs).toHaveBeenCalledWith(250)
  })

  it('fires setMinReqs("") when the requests input is cleared', () => {
    const setMinReqs = vi.fn()
    render(<ScoringControls {...baseProps({ setMinReqs, minReqs: 250 })} />)
    const reqInput = screen.getByPlaceholderText(/≥\s*1000/) as HTMLInputElement
    fireEvent.change(reqInput, { target: { value: '' } })
    expect(setMinReqs).toHaveBeenCalledWith('')
  })

  it('fires setMin4xxPct with a number when the 4xx input changes', () => {
    const setMin4xxPct = vi.fn()
    render(<ScoringControls {...baseProps({ setMin4xxPct })} />)
    const pctInput = screen.getByPlaceholderText(/≥\s*20/) as HTMLInputElement
    fireEvent.change(pctInput, { target: { value: '40' } })
    expect(setMin4xxPct).toHaveBeenCalledWith(40)
  })

  it('fires setFlaggedOnly when the switch is toggled', async () => {
    const setFlaggedOnly = vi.fn()
    render(<ScoringControls {...baseProps({ setFlaggedOnly })} />)
    const sw = screen.getByRole('switch')
    // base-ui's Switch responds to pointer events, not raw .click(), so
    // go through userEvent to dispatch the full pointer chain. base-ui
    // passes a second arg (its event-details object) we don't care
    // about, so we just pin the first arg.
    const user = userEvent.setup()
    await user.click(sw)
    expect(setFlaggedOnly).toHaveBeenCalledTimes(1)
    expect(setFlaggedOnly.mock.calls[0][0]).toBe(true)
  })

  it('does not show "Clear filters" when no filter is active', () => {
    render(<ScoringControls {...baseProps()} />)
    expect(screen.queryByRole('button', { name: /clear filters/i })).toBeNull()
  })

  it('shows "Clear filters" once any threshold is set and resets all three on click', () => {
    const setFlaggedOnly = vi.fn()
    const setMinReqs = vi.fn()
    const setMin4xxPct = vi.fn()
    render(
      <ScoringControls
        {...baseProps({
          flaggedOnly: true,
          minReqs: 250,
          min4xxPct: 40,
          setFlaggedOnly,
          setMinReqs,
          setMin4xxPct,
        })}
      />,
    )
    const clearBtn = screen.getByRole('button', { name: /clear filters/i })
    fireEvent.click(clearBtn)
    expect(setFlaggedOnly).toHaveBeenCalledWith(false)
    expect(setMinReqs).toHaveBeenCalledWith('')
    expect(setMin4xxPct).toHaveBeenCalledWith('')
  })

  it('calls refetch when the Refresh button is clicked', () => {
    const refetch = vi.fn()
    render(<ScoringControls {...baseProps({ refetch })} />)
    fireEvent.click(screen.getByRole('button', { name: /refresh/i }))
    expect(refetch).toHaveBeenCalledTimes(1)
  })

  it('disables the Refresh button while isFetching=true', () => {
    render(<ScoringControls {...baseProps({ isFetching: true })} />)
    expect(screen.getByRole('button', { name: /refresh/i })).toBeDisabled()
  })

  it('dims the control bar while a background refetch is running', () => {
    // isFetching && !isLoadingInitial → opacity-40 pointer-events-none on
    // the wrapper. The initial load uses isLoadingInitial=true which
    // suppresses the dim (parent shows a skeleton instead).
    const { container } = render(
      <ScoringControls {...baseProps({ isFetching: true, isLoadingInitial: false })} />,
    )
    expect(container.querySelector('.opacity-40')).toBeInTheDocument()
  })
})
