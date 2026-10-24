import { describe, it, expect, test } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Activity, Star } from 'lucide-react'
import {
  ReviewCard,
  ReviewHeader,
  ReviewContent,
  ReviewItem,
} from '@/components/ui/review-card'
import { SectionHeader } from '@/components/ui/section-header'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { StatCard } from '@/components/ui/stat-card'
import { Switch } from '@/components/ui/switch'

describe('ReviewCard', () => {
  it('renders children inside the card container', () => {
    render(
      <ReviewCard>
        <span data-testid="child">hello</span>
      </ReviewCard>
    )
    expect(screen.getByTestId('child')).toBeInTheDocument()
  })

  it('renders header with optional icon and content text', () => {
    const { container } = render(
      <ReviewCard>
        <ReviewHeader icon={Star}>Summary</ReviewHeader>
        <ReviewContent>body text</ReviewContent>
      </ReviewCard>
    )
    expect(screen.getByText('Summary')).toBeInTheDocument()
    expect(screen.getByText('body text')).toBeInTheDocument()
    expect(container.querySelector('svg')).toBeInTheDocument()
  })

  it('renders a default ReviewItem with label and value stacked', () => {
    render(<ReviewItem label="Service" value="cdn-prod" />)
    expect(screen.getByText('Service')).toBeInTheDocument()
    expect(screen.getByText('cdn-prod')).toBeInTheDocument()
  })

  it('renders the between variant with label and value side by side', () => {
    const { container } = render(
      <ReviewItem label="Status" value="active" variant="between" />
    )
    expect(container.firstChild).toHaveClass('justify-between')
    expect(screen.getByText('active')).toBeInTheDocument()
  })
})

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

describe('StatCard', () => {
  it('renders title, value, and sub', () => {
    render(<StatCard title="Requests" value="1,234" sub="last 24h" />)
    expect(screen.getByText('Requests')).toBeInTheDocument()
    expect(screen.getByText('1,234')).toBeInTheDocument()
    expect(screen.getByText('last 24h')).toBeInTheDocument()
  })

  it('renders the icon when provided', () => {
    const { container } = render(
      <StatCard title="Hits" value={42} sub="ok" icon={Activity} />
    )
    expect(container.querySelector('svg')).toBeInTheDocument()
  })

  it('shows a skeleton instead of the value when loading', () => {
    const { container } = render(
      <StatCard title="Loading" value="hidden-value" sub="..." loading />
    )
    expect(screen.queryByText('hidden-value')).not.toBeInTheDocument()
    expect(container.querySelector('[data-slot="skeleton"]')).toBeInTheDocument()
  })

  it('renders the tooltip trigger when tooltip prop is provided', () => {
    const { container } = render(
      <StatCard title="Tipped" value="10" sub="info" tooltip="more info" />
    )
    expect(container.querySelector('svg.lucide-circle-help, svg')).toBeInTheDocument()
  })
})

describe('Switch', () => {
  test('takes its accessible name from an associated <label htmlFor>', () => {
    render(
      <>
        <label htmlFor="edge-only">Edge-only logging</label>
        <Switch id="edge-only" />
      </>,
    )
    expect(screen.getByRole('switch', { name: 'Edge-only logging' })).toBeInTheDocument()
  })

  test('an explicit aria-label wins', () => {
    render(<Switch aria-label="Background Sync" />)
    expect(screen.getByRole('switch', { name: 'Background Sync' })).toBeInTheDocument()
  })

  test('does not invent a generic name when the call site gives none', () => {
    render(<Switch />)
    expect(screen.queryByRole('switch', { name: 'Toggle' })).not.toBeInTheDocument()
  })
})
