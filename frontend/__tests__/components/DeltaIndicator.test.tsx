/**
 * @vitest-environment jsdom
 */
import { render, screen, cleanup } from '@testing-library/react'
import { describe, it, expect, afterEach } from 'vitest'
import { DeltaIndicator } from '@/components/DeltaIndicator'

afterEach(() => cleanup())

describe('DeltaIndicator', () => {
  it('renders nothing when baseline is null', () => {
    const { container } = render(<DeltaIndicator current={100} baseline={null} />)
    expect(container.firstChild).toBeNull()
  })

  it('renders nothing when baseline is zero', () => {
    const { container } = render(<DeltaIndicator current={100} baseline={0} />)
    expect(container.firstChild).toBeNull()
  })

  it('renders a red TrendingUp + signed value when current > baseline', () => {
    // (200 - 100) / 100 = +100% → red trending-up branch
    const { container } = render(<DeltaIndicator current={200} baseline={100} />)
    const span = container.querySelector('span')
    expect(span).not.toBeNull()
    expect(span?.className).toMatch(/text-red-500/)
    expect(span?.textContent).toMatch(/\+100/)
    expect(span?.textContent).toMatch(/\+100%/)
  })

  it('renders a green TrendingDown + signed value when current < baseline', () => {
    // (50 - 100) / 100 = -50% → green trending-down branch
    const { container } = render(<DeltaIndicator current={50} baseline={100} />)
    const span = container.querySelector('span')
    expect(span).not.toBeNull()
    expect(span?.className).toMatch(/text-green-500/)
    expect(span?.textContent).toMatch(/-50/)
    expect(span?.textContent).toMatch(/-50%/)
  })

  it('renders a neutral Minus when |pct| < 1', () => {
    // (1000.5 - 1000) / 1000 = 0.05% → neutral Minus icon, no span
    const { container } = render(<DeltaIndicator current={1000.5} baseline={1000} />)
    expect(container.querySelector('span')).toBeNull()
    // lucide-react renders an svg for the Minus icon
    expect(container.querySelector('svg')).not.toBeNull()
  })

  it('formats absolute diff via toLocaleString (positive sign prefix)', () => {
    const { container } = render(<DeltaIndicator current={1234} baseline={1000} />)
    // +234 (with locale separators), and the +23% pct
    expect(container.textContent).toMatch(/\+234/)
  })
})
