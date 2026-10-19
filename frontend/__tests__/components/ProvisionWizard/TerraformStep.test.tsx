import { fireEvent, render, screen, act } from '@testing-library/react'
import { expect, test, vi } from 'vitest'
import React from 'react'
import { TerraformStep } from '@/components/ProvisionWizard/steps/TerraformStep'
import type { WizardState } from '@/components/ProvisionWizard/useWizardState'

const state = {
  terraformFiles: { 'main.tf': 'resource "x" "y" {}' },
  selectedTfFile: 'main.tf',
  setSelectedTfFile: vi.fn(),
  isFetchingTerraform: false,
  handleExportTerraform: vi.fn(),
} as unknown as WizardState

test('copy button writes the selected file to the clipboard', async () => {
  const writeText = vi.fn().mockResolvedValue(undefined)
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
  render(<TerraformStep s={state} />)
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' }))
  })
  expect(writeText).toHaveBeenCalledWith('resource "x" "y" {}')
})

test('a rejected clipboard write is swallowed instead of surfacing as an unhandled rejection', async () => {
  const writeText = vi.fn().mockRejectedValue(new Error('denied'))
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
  render(<TerraformStep s={state} />)
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' }))
  })
  await new Promise((r) => setTimeout(r, 0))
  expect(writeText).toHaveBeenCalledTimes(1)
})
