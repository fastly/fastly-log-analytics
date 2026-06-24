import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { SkeletonGrid } from '@/components/ui/skeleton-grid'

describe('SkeletonGrid', () => {
  it('renders the configured number of skeleton blocks', () => {
    const { container } = render(<SkeletonGrid count={4} />)
    expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(4)
  })

  it('renders a custom count when provided', () => {
    const { container } = render(<SkeletonGrid count={7} />)
    expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(7)
  })

  it('renders nothing when count is 0', () => {
    const { container } = render(<SkeletonGrid count={0} />)
    expect(container.querySelectorAll('[data-slot="skeleton"]')).toHaveLength(0)
  })

  it('applies the default height inline style', () => {
    const { container } = render(<SkeletonGrid count={1} />)
    const block = container.querySelector('[data-slot="skeleton"]') as HTMLElement
    expect(block.style.height).toBe('120px')
  })

  it('honors a custom height prop', () => {
    const { container } = render(<SkeletonGrid count={1} height="200px" />)
    const block = container.querySelector('[data-slot="skeleton"]') as HTMLElement
    expect(block.style.height).toBe('200px')
  })
})
