/**
 * @vitest-environment jsdom
 */
import * as React from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  createTestQueryClient,
  makeQueryWrapper,
  stubMatchMedia,
  stubPointerCaptureAndScroll,
} from '@/__tests__/helpers/page-smoke'
import { ProvisionWizard } from '@/components/ProvisionWizard/ProvisionWizard'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'

stubMatchMedia()
stubPointerCaptureAndScroll()

describe('ProvisionWizard token input focus', () => {
  afterEach(() => {
    setCloudModeRuntime(false)
    window.localStorage.clear()
    vi.restoreAllMocks()
  })

  it('focuses the token input on open in Cloud mode and keeps focus when clicked', async () => {
    setCloudModeRuntime(true)
    const user = userEvent.setup()
    const qc = createTestQueryClient()
    const Wrapper = makeQueryWrapper(qc)

    function Harness() {
      const [open, setOpen] = React.useState(false)
      return (
        <Wrapper>
          <button type="button" onClick={() => setOpen(true)}>
            Add Service
          </button>
          <ProvisionWizard open={open} onOpenChange={setOpen} />
        </Wrapper>
      )
    }

    render(<Harness />)
    await user.click(screen.getByRole('button', { name: /add service/i }))
    const input = await screen.findByLabelText(/api token/i)
    await new Promise((r) => requestAnimationFrame(() => r(undefined)))
    expect(document.activeElement).toBe(input)

    await user.click(input)
    expect(document.activeElement).toBe(input)
    await new Promise((r) => setTimeout(r, 600))
    expect(document.activeElement).toBe(input)
  })

  it('focuses the token input after selecting a mode in non-Cloud mode and keeps focus when clicked', async () => {
    setCloudModeRuntime(false)
    const user = userEvent.setup()
    const qc = createTestQueryClient()
    const Wrapper = makeQueryWrapper(qc)

    function Harness() {
      const [open, setOpen] = React.useState(false)
      return (
        <Wrapper>
          <button type="button" onClick={() => setOpen(true)}>
            Add Service
          </button>
          <ProvisionWizard open={open} onOpenChange={setOpen} />
        </Wrapper>
      )
    }

    render(<Harness />)
    await user.click(screen.getByRole('button', { name: /add service/i }))
    const modeBtn = screen.getByRole('button', { name: /admin: provision/i })
    await user.click(modeBtn)
    const input = await screen.findByLabelText(/api token/i)
    await new Promise((r) => requestAnimationFrame(() => r(undefined)))
    expect(document.activeElement).toBe(input)

    await user.click(input)
    expect(document.activeElement).toBe(input)
  })
})
