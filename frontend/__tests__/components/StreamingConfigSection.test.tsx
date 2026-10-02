import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { StreamingConfigSection } from '@/components/StreamingConfigSection'
import { tokenConfigError, tokenExprError } from '@/components/TokenConfigSection'
import { customFieldsApi } from '@/lib/api/custom-fields'

vi.mock('@/lib/api/custom-fields', () => ({
  customFieldsApi: { validateCustomVcl: vi.fn() },
}))

const lint = vi.mocked(customFieldsApi.validateCustomVcl)

beforeEach(() => {
  lint.mockReset()
  lint.mockResolvedValue({ valid: true, errors: [], warnings: [] } as never)
})

function Harness({
  serviceId,
  initial = {},
}: {
  serviceId?: string
  initial?: { cmcd?: boolean; token?: boolean; sub?: string; cid?: string }
}) {
  const [cmcd, setCmcd] = React.useState(initial.cmcd ?? false)
  const [mode, setMode] = React.useState('query_string')
  const [version, setVersion] = React.useState(1)
  const [token, setToken] = React.useState(initial.token ?? false)
  const [expr, setExpr] = React.useState(initial.sub ?? '')
  const [cid, setCid] = React.useState(initial.cid ?? '')
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
      contentIdExpr={cid}
      onContentIdExprChange={setCid}
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
  expect(screen.getByText('set req.http.x-cs-sub-id =')).toBeInTheDocument()
  expect(screen.getByText('Content ID')).toBeInTheDocument()
  expect(screen.getByText('set req.http.x-cs-content-id =')).toBeInTheDocument()

  await userEvent.click(screen.getByRole('checkbox', { name: 'Enable Token' }))
  expect(screen.getByRole('checkbox', { name: 'Enable Token' })).toBeChecked()
  expect(screen.getByRole('checkbox', { name: 'Enable CMCD Metrics' })).not.toBeChecked()
  expect(screen.getByText('1 of 2 enabled')).toBeInTheDocument()
})

test('expression inputs are disabled until Token is enabled, then validated', async () => {
  render(<QueryClientProvider client={createTestQueryClient()}><Harness serviceId="svc-1" /></QueryClientProvider>)
  await openAll()

  const sub = screen.getByRole('textbox', { name: 'Subscriber ID VCL expression' })
  const cid = screen.getByRole('textbox', { name: 'Content ID VCL expression' })
  expect(sub).toBeDisabled()
  expect(cid).toBeDisabled()

  await userEvent.click(screen.getByRole('checkbox', { name: 'Enable Token' }))
  expect(screen.getByRole('alert')).toHaveTextContent('Configure at least one of Subscriber ID or Content ID.')

  // Content ID alone is enough; the requirement clears once one id is set.
  await userEvent.type(cid, 'req.http.X-Content')
  expect(screen.queryByText(/Configure at least one/)).not.toBeInTheDocument()
  await waitFor(() =>
    expect(lint).toHaveBeenCalledWith('svc-1', { vcl_log_expression: 'req.http.X-Content', collection_stage: 'edge' }),
  )
  expect(await screen.findByText('Expression looks valid.')).toBeInTheDocument()

  // Each row is validated on its own.
  await userEvent.type(sub, 'a; b')
  expect(screen.getByRole('alert')).toHaveTextContent('Must not contain semicolons (;).')
})

test('tokenExprError mirrors the backend injection guard; empty is allowed per id', () => {
  expect(tokenExprError('req.http.X-Sub')).toBeNull()
  expect(tokenExprError('regsub(req.http.Authorization, "^Bearer (.{8}).*$", "\\1")')).toBeNull()
  expect(tokenExprError('  ')).toBeNull()
  expect(tokenExprError('a; b')).toMatch(/semicolons/)
  expect(tokenExprError('a # b')).toMatch(/comments/)
  expect(tokenExprError('a\nb')).toMatch(/newlines/)
  expect(tokenExprError('x'.repeat(513))).toMatch(/512/)
})

test('tokenConfigError requires at least one id and labels the bad one', () => {
  expect(tokenConfigError('', ' ')).toMatch(/at least one/)
  expect(tokenConfigError('req.http.X-Sub', '')).toBeNull()
  expect(tokenConfigError('', 'req.http.X-Content')).toBeNull()
  expect(tokenConfigError('req.http.X-Sub', 'a // b')).toBe('Content ID: Must not contain VCL comments (//, /*, or #).')
  expect(tokenConfigError('a; b', 'req.http.X-Content')).toBe('Subscriber ID: Must not contain semicolons (;).')
})

function renderHarness(props: React.ComponentProps<typeof Harness> = {}) {
  return render(
    <QueryClientProvider client={createTestQueryClient()}>
      <Harness {...props} />
    </QueryClientProvider>,
  )
}

const streamingBox = () => screen.getByRole('checkbox', { name: 'Enable Streaming' })

test('the Streaming checkbox reflects both children: off, partial, on', () => {
  const { unmount } = renderHarness()
  expect(streamingBox()).not.toBeChecked()
  expect(streamingBox()).not.toBePartiallyChecked()
  unmount()

  const partial = renderHarness({ initial: { cmcd: true } })
  expect(streamingBox()).toBePartiallyChecked()
  expect(screen.getByText('1 of 2 enabled')).toBeInTheDocument()
  partial.unmount()

  renderHarness({ initial: { cmcd: true, token: true, sub: 'req.http.X-Sub' } })
  expect(streamingBox()).toBeChecked()
  expect(screen.getByText('2 of 2 enabled')).toBeInTheDocument()
})

test('checking Streaming enables both and opens only the child that needs input', async () => {
  renderHarness()
  expect(screen.queryByText('CMCD Metrics')).not.toBeInTheDocument()

  await userEvent.click(streamingBox())

  expect(streamingBox()).toBeChecked()
  expect(screen.getByRole('checkbox', { name: 'Enable CMCD Metrics' })).toBeChecked()
  expect(screen.getByRole('checkbox', { name: 'Enable Token' })).toBeChecked()
  // Token has no expression yet, so it opens ready for input...
  const sub = screen.getByRole('textbox', { name: 'Subscriber ID VCL expression' })
  expect(sub).toBeEnabled()
  expect(screen.getByRole('alert')).toHaveTextContent('Configure at least one of Subscriber ID or Content ID.')
  // ...while CMCD, which works with its defaults, stays collapsed.
  expect(screen.queryByText('CMCD Version')).not.toBeInTheDocument()
})

test('checking Streaming from partial does not reopen a Token that is already configured', async () => {
  renderHarness({ initial: { token: false, sub: 'req.http.X-Sub' } })

  await userEvent.click(streamingBox())

  expect(streamingBox()).toBeChecked()
  expect(screen.queryByText('CMCD Metrics')).not.toBeInTheDocument() // group stays collapsed
})

test('unchecking Streaming disables both children', async () => {
  renderHarness({ initial: { cmcd: true, token: true, sub: 'req.http.X-Sub' } })

  await userEvent.click(streamingBox())

  expect(streamingBox()).not.toBeChecked()
  await userEvent.click(screen.getByRole('button', { name: /streaming/i }))
  expect(screen.getByRole('checkbox', { name: 'Enable CMCD Metrics' })).not.toBeChecked()
  expect(screen.getByRole('checkbox', { name: 'Enable Token' })).not.toBeChecked()
})
