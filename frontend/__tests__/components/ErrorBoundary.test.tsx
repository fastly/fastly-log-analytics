/**
 * @vitest-environment jsdom
 */
import type React from 'react'
import { render, screen, cleanup } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { ErrorBoundary } from '@/components/ErrorBoundary'

let errSpy: ReturnType<typeof vi.spyOn>
beforeEach(() => {
  // Suppress the noisy React 19 + jsdom uncaught render error log.
  errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
})
afterEach(() => {
  errSpy.mockRestore()
  cleanup()
})

function Boom(): React.ReactElement {
  throw new Error('kaboom')
}

describe('ErrorBoundary', () => {
  it('renders the default fallback when a child throws', () => {
    render(
      <ErrorBoundary>
        <Boom />
      </ErrorBoundary>,
    )
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByText(/Something went wrong/i)).toBeInTheDocument()
    expect(screen.getByText('kaboom')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /try again/i })).toBeInTheDocument()
  })

  it('renders children verbatim when no error occurs', () => {
    render(
      <ErrorBoundary>
        <div data-testid="ok">all good</div>
      </ErrorBoundary>,
    )
    expect(screen.getByTestId('ok')).toBeInTheDocument()
    expect(screen.queryByRole('alert')).toBeNull()
  })

  it('uses a custom fallback render prop when provided', () => {
    render(
      <ErrorBoundary fallback={(err) => <div data-testid="custom">caught: {err.message}</div>}>
        <Boom />
      </ErrorBoundary>,
    )
    expect(screen.getByTestId('custom')).toHaveTextContent('caught: kaboom')
  })
})
