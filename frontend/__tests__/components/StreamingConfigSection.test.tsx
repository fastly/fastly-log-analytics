import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { StreamingConfigSection } from '@/components/StreamingConfigSection'
import { subscriberIdExprError } from '@/components/TokenConfigSection'
import { customFieldsApi } from '@/lib/api/custom-fields'

vi.mock('@/lib/api/custom-fields', () => ({
  customFieldsApi: { validateCustomVcl: vi.fn() },
}))

const lint = vi.mocked(customFieldsApi.validateCustomVcl)

beforeEach(() => {
  lint.mockReset()
  lint.mockResolvedValue({ valid: true, errors: [], warnings: [] } as never)
})

function Harness({ serviceId }: { serviceId?: string }) {
  const [cmcd, setCmcd] = React.useState(false)
  const [mode, setMode] = React.useState('query_string')
  const [version, setVersion] = React.useState(1)
  const [token, setToken] = React.useState(false)
  const [expr, setExpr] = React.useState('')
  return (
    <StreamingConfigSection
      cmcdEnabled={cmcd}
      onCmcdEnabledChange={setCmcd}
      cmcdMode={mode}
      onCmcdModeChange={setMode}
      cmcdVersion={version}
      onCmcdVersionChange={setVersion}
      tokenEnabled={token}
      onTokenEnabledChange={setToken}
      subscriberIdExpr={expr}
      onSubscriberIdExprChange={setExpr}
      serviceId={serviceId}
    />
  )
}

async function openAll() {
  await userEvent.click(screen.getByRole('button', { name: /streaming/i }))
  await userEvent.click(screen.getByRole('button', { name: /^token/i }))
}

test('groups CMCD Metrics and Token as independent sub-options', async () => {
  render(<QueryClientProvider client={createTestQueryClient()}><Harness /></QueryClientProvider>)
  expect(screen.queryByText('CMCD Metrics')).not.toBeInTheDocument()

  await openAll()
  expect(screen.getByText('CMCD Metrics')).toBeInTheDocument()
  expect(screen.getByText(/HMAC, JWT, or CAT format/)).toBeInTheDocument()
  expect(screen.getByText(/deploys a VCL snippet to your service to extract token information/)).toBeInTheDocument()
  expect(screen.getByText('Subscriber ID')).toBeInTheDocument()
  expect(screen.getByText('set req.http.x-subscriber-id =')).toBeInTheDocument()

  await userEvent.click(screen.getByRole('checkbox', { name: 'Enable Token' }))
  expect(screen.getByRole('checkbox', { name: 'Enable Token' })).toBeChecked()
  expect(screen.getByRole('checkbox', { name: 'Enable CMCD Metrics' })).not.toBeChecked()
  expect(screen.getByText('1 of 2 enabled')).toBeInTheDocument()
})

test('expression input is disabled until Token is enabled, then validated', async () => {
  render(<QueryClientProvider client={createTestQueryClient()}><Harness serviceId="svc-1" /></QueryClientProvider>)
  await openAll()

  const input = screen.getByRole('textbox', { name: 'Subscriber ID VCL expression' })
  expect(input).toBeDisabled()

  await userEvent.click(screen.getByRole('checkbox', { name: 'Enable Token' }))
  expect(screen.getByRole('alert')).toHaveTextContent('A VCL expression is required.')

  await userEvent.type(input, 'req.http.X-Sub')
  await waitFor(() =>
    expect(lint).toHaveBeenCalledWith('svc-1', { vcl_log_expression: 'req.http.X-Sub', collection_stage: 'edge' }),
  )
  expect(await screen.findByText('Expression looks valid.')).toBeInTheDocument()
})

test('subscriberIdExprError mirrors the backend injection guard', () => {
  expect(subscriberIdExprError('req.http.X-Sub')).toBeNull()
  expect(subscriberIdExprError('regsub(req.http.Authorization, "^Bearer (.{8}).*$", "\\1")')).toBeNull()
  expect(subscriberIdExprError('  ')).toMatch(/required/)
  expect(subscriberIdExprError('a; b')).toMatch(/semicolons/)
  expect(subscriberIdExprError('a # b')).toMatch(/comments/)
  expect(subscriberIdExprError('a\nb')).toMatch(/newlines/)
  expect(subscriberIdExprError('x'.repeat(513))).toMatch(/512/)
})
