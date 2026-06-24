/**
 * @vitest-environment jsdom
 */
import { render, cleanup } from '@testing-library/react'
import { describe, it, expect, afterEach } from 'vitest'
import { Sparkline } from '@/components/Sparkline'

afterEach(() => cleanup())

const pts = (vals: number[]) =>
  vals.map((value, i) => ({ ts: new Date(2026, 0, 1, 0, i).toISOString(), value }))

describe('Sparkline', () => {
  it('renders nothing for fewer than 2 points', () => {
    const { container: a } = render(<Sparkline points={[]} />)
    expect(a.firstChild).toBeNull()
    const { container: b } = render(<Sparkline points={pts([1])} />)
    expect(b.firstChild).toBeNull()
  })

  it('renders an svg with a polyline whose points attribute is set from data', () => {
    const { container } = render(<Sparkline points={pts([0, 5, 10])} />)
    const svg = container.querySelector('svg')
    expect(svg).not.toBeNull()
    const poly = svg!.querySelector('polyline')
    expect(poly).not.toBeNull()
    const pointsAttr = poly!.getAttribute('points') ?? ''
    // 3 coords, space-separated, each "x,y"
    expect(pointsAttr.trim().split(/\s+/)).toHaveLength(3)
    expect(pointsAttr).toMatch(/\d/)
  })

  it('polyline points change when data changes', () => {
    const { container: c1 } = render(<Sparkline points={pts([1, 2, 3])} />)
    const first = c1.querySelector('polyline')!.getAttribute('points')
    const { container: c2 } = render(<Sparkline points={pts([10, 20, 30, 40])} />)
    const second = c2.querySelector('polyline')!.getAttribute('points')
    expect(second).not.toEqual(first)
  })

  it('honours custom height in the svg attribute and viewBox', () => {
    const { container } = render(<Sparkline points={pts([1, 2])} height={64} />)
    const svg = container.querySelector('svg')!
    expect(svg.getAttribute('height')).toBe('64')
    expect(svg.getAttribute('viewBox')).toContain('64')
  })
})
