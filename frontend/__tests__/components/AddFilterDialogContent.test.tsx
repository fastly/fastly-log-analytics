/**
 * AddFilterDialogContent submit contract (must hold across the
 * react-hook-form migration):
 *   - Apply Filter stays disabled until Match Value is non-empty.
 *   - Submitting adds {field, value, mode} to the filter store and closes.
 *   - Exclude mode is honoured; a masking analyst defaults to `url`, not `ip`.
 *   - Clicking a suggestion fills the value without submitting.
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import { stubPointerCaptureAndScroll } from '@/__tests__/helpers/jsdom-shims'
import * as React from 'react'
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

import { Dialog } from '@/components/ui/dialog'
import { useFilterStore } from '@/stores/filterStore'
import { AddFilterDialogContent } from '@/components/FilterBar/AddFilterDialogContent'

let maskIps = false
let suggestionValues: { value: string | null; label?: string; count: number }[] = []

vi.mock('@/hooks/useMaskIps', () => ({ useMaskIps: () => maskIps }))
vi.mock('@/hooks/useLogFieldsCatalog', () => ({ useLogFieldsCatalog: () => ({ data: undefined }) }))
vi.mock('@/hooks/useFieldValues', () => ({
  useFieldValues: () => ({ data: { values: suggestionValues }, isLoading: false }),
}))
// base-ui's ScrollArea calls Element.getAnimations, which jsdom lacks.
vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))
vi.mock('@/hooks/useDebounce', () => ({ useDebounce: (v: string) => v }))

beforeAll(() => {
  stubResizeObserver()
  stubPointerCaptureAndScroll()
})

beforeEach(() => {
  maskIps = false
  suggestionValues = []
  useFilterStore.setState({ filters: [] })
})

function renderContent(onClose: () => void = () => {}) {
  return render(
    <Dialog open>
      <AddFilterDialogContent onClose={onClose} />
    </Dialog>,
  )
}

describe('AddFilterDialogContent', () => {
  it('keeps Apply Filter disabled until a value is typed', async () => {
    const user = userEvent.setup()
    renderContent()
    const apply = screen.getByRole('button', { name: /Apply Filter/ }) as HTMLButtonElement
    expect(apply.disabled).toBe(true)
    await user.type(screen.getByPlaceholderText('Type a ip value...'), '1.2.3.4')
    await waitFor(() => expect(apply.disabled).toBe(false))
  })

  it('adds an include filter for the default field and closes', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    renderContent(onClose)
    await user.type(screen.getByPlaceholderText('Type a ip value...'), '1.2.3.4')
    await user.click(screen.getByRole('button', { name: /Apply Filter/ }))
    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1))
    const [filter] = useFilterStore.getState().filters
    expect(filter).toMatchObject({ column: 'ip', value: '1.2.3.4', mode: 'include' })
  })

  it('honours the Exclude tab', async () => {
    const user = userEvent.setup()
    renderContent()
    await user.click(screen.getByRole('tab', { name: /Exclude/ }))
    await user.type(screen.getByPlaceholderText('Type a ip value...'), 'bad')
    await user.click(screen.getByRole('button', { name: /Apply Filter/ }))
    await waitFor(() => expect(useFilterStore.getState().filters.length).toBe(1))
    expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'ip', value: 'bad', mode: 'exclude' })
  })

  it('submits on Enter', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    renderContent(onClose)
    await user.type(screen.getByPlaceholderText('Type a ip value...'), 'abc{Enter}')
    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1))
    expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'ip', value: 'abc' })
  })

  it('defaults a masking analyst to the url field', async () => {
    maskIps = true
    const user = userEvent.setup()
    renderContent()
    await user.type(screen.getByPlaceholderText('Type a url value...'), '/x')
    await user.click(screen.getByRole('button', { name: /Apply Filter/ }))
    await waitFor(() => expect(useFilterStore.getState().filters.length).toBe(1))
    expect(useFilterStore.getState().filters[0]).toMatchObject({ column: 'url', value: '/x' })
  })

  it('fills the value from a suggestion without submitting', async () => {
    suggestionValues = [{ value: 'US', label: 'United States', count: 1200 }]
    const user = userEvent.setup()
    const onClose = vi.fn()
    renderContent(onClose)
    const input = screen.getByPlaceholderText('Type a ip value...') as HTMLInputElement
    await user.type(input, 'U')
    await user.click(await screen.findByRole('button', { name: /United States/ }))
    expect(input.value).toBe('US')
    expect(onClose).not.toHaveBeenCalled()
    expect(useFilterStore.getState().filters.length).toBe(0)
  })
})
