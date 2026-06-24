import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Star } from 'lucide-react'
import { SectionHeader } from '@/components/ui/section-header'

describe('SectionHeader', () => {
  it('renders the title text', () => {
    render(<SectionHeader title="Overview" />)
    expect(screen.getByRole('heading', { name: /overview/i })).toBeInTheDocument()
  })

  it('renders the icon when provided', () => {
    const { container } = render(<SectionHeader title="With Icon" icon={Star} />)
    expect(container.querySelector('svg')).toBeInTheDocument()
  })

  it('renders inline children alongside the title', () => {
    render(
      <SectionHeader title="Details">
        <span data-testid="action-slot"> · extra</span>
      </SectionHeader>
    )
    expect(screen.getByTestId('action-slot')).toBeInTheDocument()
  })

  it('applies the optional className to the root element', () => {
    const { container } = render(
      <SectionHeader title="Styled" className="custom-class" />
    )
    expect(container.firstChild).toHaveClass('custom-class')
  })
})
