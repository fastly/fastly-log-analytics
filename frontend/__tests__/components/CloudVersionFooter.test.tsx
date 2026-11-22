/**
 * @vitest-environment jsdom
 */
import { act, cleanup, render, screen } from '@testing-library/react'
import { afterEach, expect, test, vi } from 'vitest'
import { CloudVersionFooter } from '@/components/CloudVersionFooter'
import { createTestQueryClient, makeQueryWrapper } from '@/__tests__/helpers/page-smoke'
import { queryKeys } from '@/lib/apiQueryKeys'
import type { BootstrapResponse } from '@/types/api'

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

test('reads the current build SHA from the bootstrap cache without fetching /version', async () => {
  const fetch = vi.fn()
  vi.stubGlobal('fetch', fetch)
  const queryClient = createTestQueryClient()
  queryClient.setQueryData<BootstrapResponse>(queryKeys.bootstrap(), {
    build_sha: 'abc123',
  } as BootstrapResponse)

  render(<CloudVersionFooter />, { wrapper: makeQueryWrapper(queryClient) })

  expect(screen.getByText(/commit:abc123/)).toBeInTheDocument()
  expect(fetch).not.toHaveBeenCalled()

  await act(async () => {
    queryClient.setQueryData<BootstrapResponse>(queryKeys.bootstrap(), {
      build_sha: 'def456',
    } as BootstrapResponse)
  })

  expect(screen.getByText(/commit:def456/)).toBeInTheDocument()
  expect(fetch).not.toHaveBeenCalled()
})
