import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Star } from 'lucide-react'
import {
  ReviewCard,
  ReviewHeader,
  ReviewContent,
  ReviewItem,
} from '@/components/ui/review-card'

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
