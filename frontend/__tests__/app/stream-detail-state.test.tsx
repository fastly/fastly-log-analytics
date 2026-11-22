import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { expect, it, vi } from 'vitest'
import StreamDetailPage from '@/app/sessions/stream/page'

vi.mock('next/navigation', () => ({
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => '/sessions/stream',
  useRouter: () => ({ replace: vi.fn(), push: vi.fn() }),
}))
vi.mock('@/hooks/useBootstrap', async (original) => ({
  ...await original<typeof import('@/hooks/useBootstrap')>(),
  useEffectiveServiceId: () => 'fixture-service',
  useBootstrapResolved: () => true,
}))
vi.mock('@/app/streaming/_sections/StreamingClient', () => ({ OBJECT_TYPE_LABELS: {} }))

it('marks a bare stream detail route as parameter-required status, not a failed query', () => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(<QueryClientProvider client={queryClient}><StreamDetailPage /></QueryClientProvider>)
  expect(screen.getByTestId('stream-detail-parameter-required')).toHaveAttribute('role', 'status')
  expect(screen.getByRole('status')).toHaveTextContent('No session token')
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
})
