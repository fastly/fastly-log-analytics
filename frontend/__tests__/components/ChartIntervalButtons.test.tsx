/**
 * @vitest-environment jsdom
 */
import { render, screen, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi, afterEach } from 'vitest'
import { ChartIntervalButtons } from '@/components/ChartIntervalButtons'
import { INTERVALS } from '@/lib/constants'

afterEach(() => cleanup())

const ALL_VALID = new Set(INTERVALS.map(i => i.value))

describe('ChartIntervalButtons', () => {
  it('renders one button per INTERVALS entry', () => {
    render(
      <ChartIntervalButtons
        effectiveInterval="1 minute"
        validIntervals={ALL_VALID}
        onIntervalChange={() => {}}
      />,
    )
    for (const i of INTERVALS) {
      expect(screen.getByRole('button', { name: `Chart bucket size: ${i.label}` })).toBeInTheDocument()
    }
  })

  it('marks the effective interval with aria-pressed=true', () => {
    render(
      <ChartIntervalButtons
        effectiveInterval="1 hour"
        validIntervals={ALL_VALID}
        onIntervalChange={() => {}}
      />,
    )
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1h' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByRole('button', { name: 'Chart bucket size: 1m' })).toHaveAttribute('aria-pressed', 'false')
  })

  it('fires onIntervalChange with the chosen value', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(
      <ChartIntervalButtons
        effectiveInterval="1 minute"
        validIntervals={ALL_VALID}
        onIntervalChange={onChange}
      />,
    )
    await user.click(screen.getByRole('button', { name: 'Chart bucket size: 1h' }))
    expect(onChange).toHaveBeenCalledWith('1 hour')
  })

  it('disables buttons not in validIntervals so clicks are ignored', async () => {
    const user = userEvent.setup()
    const onChange = vi.fn()
    render(
      <ChartIntervalButtons
        effectiveInterval="1 minute"
        validIntervals={new Set(['1 minute'])}
        onIntervalChange={onChange}
      />,
    )
    const hourBtn = screen.getByRole('button', { name: 'Chart bucket size: 1h' })
    expect(hourBtn).toBeDisabled()
    await user.click(hourBtn)
    expect(onChange).not.toHaveBeenCalled()
  })
})
