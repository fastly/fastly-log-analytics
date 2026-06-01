import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { HelpDialog } from '@/components/ui/help-dialog'

describe('HelpDialog', () => {
  it('renders title and children when open', () => {
    render(
      <HelpDialog open onOpenChange={vi.fn()} title="My Title">
        <p>My content</p>
      </HelpDialog>
    )
    expect(screen.getByText('My Title')).toBeInTheDocument()
    expect(screen.getByText('My content')).toBeInTheDocument()
  })

  it('does not render content when closed', () => {
    render(
      <HelpDialog open={false} onOpenChange={vi.fn()} title="Hidden">
        <p>Hidden body</p>
      </HelpDialog>
    )
    expect(screen.queryByText('Hidden body')).not.toBeInTheDocument()
  })

  it('renders the icon next to the title when provided', () => {
    render(
      <HelpDialog
        open
        onOpenChange={vi.fn()}
        title="With Icon"
        icon={<span data-testid="help-icon">★</span>}
      >
        body
      </HelpDialog>
    )
    expect(screen.getByTestId('help-icon')).toBeInTheDocument()
  })

  it('applies xl width class when size=xl', () => {
    const { unmount } = render(
      <HelpDialog open onOpenChange={vi.fn()} title="Large-only" size="xl">
        body
      </HelpDialog>
    )
    const dialogs = Array.from(document.querySelectorAll('[role="dialog"]'))
    const matched = dialogs.find(d => d.className.includes('max-w-xl'))
    expect(matched, 'expected a dialog with max-w-xl class').toBeTruthy()
    unmount()
  })

  it('defaults to lg width', () => {
    const { unmount } = render(
      <HelpDialog open onOpenChange={vi.fn()} title="Default-only">
        body
      </HelpDialog>
    )
    const dialogs = Array.from(document.querySelectorAll('[role="dialog"]'))
    const matched = dialogs.find(d => d.className.includes('max-w-lg'))
    expect(matched, 'expected a dialog with max-w-lg class').toBeTruthy()
    unmount()
  })
})
