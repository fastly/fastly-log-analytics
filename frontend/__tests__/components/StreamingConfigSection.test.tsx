import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi, beforeEach } from 'vitest'
import React from 'react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createTestQueryClient } from '../helpers/query'
import { StreamingConfigSection } from '@/components/StreamingConfigSection'
import { contentSecurityConfigError, contentSecurityPayload, tokenExprError } from '@/components/TokenConfigSection'
import { customFieldsApi } from '@/lib/api/custom-fields'

vi.mock('@/lib/api/custom-fields', () => ({
  customFieldsApi: { validateCustomVcl: vi.fn() },
}))

const lint = vi.mocked(customFieldsApi.validateCustomVcl)

beforeEach(() => {
  lint.mockReset()
  lint.mockResolvedValue({ valid: true, errors: [], warnings: [] } as never)
})

type Initial = {
  cmcd?: boolean
  token?: boolean
  subOn?: boolean
  sub?: string
  cidOn?: boolean
  cid?: string
  enf?: boolean
}

function Harness({ serviceId, initial = {} }: { serviceId?: string; initial?: Initial }) {
  const [cmcd, setCmcd] = React.useState(initial.cmcd ?? false)
  const [mode, setMode] = React.useState('query_string')
  const [version, setVersion] = React.useState(1)
  const [token, setToken] = React.useState(initial.token ?? false)
  const [subOn, setSubOn] = React.useState(initial.subOn ?? false)
  const [expr, setExpr] = React.useState(initial.sub ?? '')
  const [cidOn, setCidOn] = React.useState(initial.cidOn ?? false)
  const [cid, setCid] = React.useState(initial.cid ?? '')
  const [enf, setEnf] = React.useState(initial.enf ?? false)
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
      subscriberIdEnabled={subOn}
      onSubscriberIdEnabledChange={setSubOn}
      subscriberIdExpr={expr}
      onSubscriberIdExprChange={setExpr}
      contentIdEnabled={cidOn}
      onContentIdEnabledChange={setCidOn}
      contentIdExpr={cid}
      onContentIdExprChange={setCid}
      enforcementEnabled={enf}
      onEnforcementEnabledChange={setEnf}
      serviceId={serviceId}
    />
  )
}

function renderHarness(props: React.ComponentProps<typeof Harness> = {}) {
  return render(
    <QueryClientProvider client={createTestQueryClient()}>
      <Harness {...props} />
    </QueryClientProvider>,
  )
}

async function openAll() {
  await userEvent.click(screen.getByRole('button', { name: /streaming/i }))
  await userEvent.click(screen.getByRole('button', { name: /^content security/i }))
}

const box = (name: string) => screen.getByRole('checkbox', { name })
const streamingBox = () => box('Enable Streaming')

test('groups CMCD Metrics and Content Security as independent sub-options', async () => {
  renderHarness()
  expect(screen.queryByText('CMCD Metrics')).not.toBeInTheDocument()

  await openAll()
  expect(screen.getByText('CMCD Metrics')).toBeInTheDocument()
  expect(screen.getByText(/HMAC, JWT, or CAT tokens/)).toBeInTheDocument()
  expect(screen.getByText(/deploys a VCL snippet to your service to extract it/)).toBeInTheDocument()
  expect(screen.getByText('set req.http.x-cs-sub-id =')).toBeInTheDocument()
  expect(screen.getByText('set req.http.x-cs-content-id =')).toBeInTheDocument()

  await userEvent.click(box('Enable Content Security'))
  expect(box('Enable Content Security')).toBeChecked()
  expect(box('Enable CMCD Metrics')).not.toBeChecked()
  expect(screen.getByText('1 of 2 enabled')).toBeInTheDocument()
})

test('checking Content Security checks all three options, which can then be unchecked one by one', async () => {
  renderHarness()
  await openAll()
  for (const name of ['Enable Subscriber ID', 'Enable Content ID', 'Log enforcement outcome']) {
    expect(box(name)).not.toBeChecked()
    expect(box(name)).toHaveAttribute('aria-disabled', 'true')
  }

  await userEvent.click(box('Enable Content Security'))
  for (const name of ['Enable Subscriber ID', 'Enable Content ID', 'Log enforcement outcome']) {
    expect(box(name)).toBeChecked()
    expect(box(name)).not.toHaveAttribute('aria-disabled', 'true')
  }

  await userEvent.click(box('Enable Subscriber ID'))
  expect(box('Enable Subscriber ID')).not.toBeChecked()
  expect(box('Enable Content ID')).toBeChecked()
  expect(box('Log enforcement outcome')).toBeChecked()
  expect(screen.getByRole('textbox', { name: 'Subscriber ID VCL expression' })).toBeDisabled()

  await userEvent.click(box('Enable Content Security'))
  expect(box('Enable Content ID')).not.toBeChecked()
  expect(box('Log enforcement outcome')).not.toBeChecked()
})

test('a checked ID needs an expression; enforcement needs none; each row is validated alone', async () => {
  renderHarness({ serviceId: 'svc-1' })
  await openAll()
  await userEvent.click(box('Enable Content Security'))

  expect(screen.getByRole('alert')).toHaveTextContent('Subscriber ID: enter a VCL expression or uncheck it.')

  // Unchecking an ID (leaving enforcement on) is a valid configuration.
  await userEvent.click(box('Enable Subscriber ID'))
  expect(screen.getByRole('alert')).toHaveTextContent('Content ID: enter a VCL expression or uncheck it.')
  await userEvent.click(box('Enable Content ID'))
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()

  await userEvent.click(box('Enable Content ID'))
  const cid = screen.getByRole('textbox', { name: 'Content ID VCL expression' })
  await userEvent.type(cid, 'req.http.X-Content')
  await waitFor(() =>
    expect(lint).toHaveBeenCalledWith('svc-1', { vcl_log_expression: 'req.http.X-Content', collection_stage: 'edge' }),
  )
  expect(await screen.findByText('Expression looks valid.')).toBeInTheDocument()

  await userEvent.type(cid, ';')
  // Reported on the row and again in the section summary.
  const alerts = screen.getAllByRole('alert').map((a) => (a.textContent ?? '').trim())
  expect(alerts).toContain('Must not contain semicolons (;).')
  expect(alerts).toContain('Content ID: Must not contain semicolons (;).')
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

const opts = (o: Partial<Parameters<typeof contentSecurityConfigError>[0]> = {}) => ({
  subscriberIdEnabled: false,
  subscriberIdExpr: '',
  contentIdEnabled: false,
  contentIdExpr: '',
  enforcementEnabled: false,
  ...o,
})

test('contentSecurityConfigError checks only the checked IDs and labels the bad one', () => {
  expect(contentSecurityConfigError(opts())).toBeNull()
  expect(contentSecurityConfigError(opts({ enforcementEnabled: true }))).toBeNull()
  expect(contentSecurityConfigError(opts({ subscriberIdEnabled: true }))).toMatch(/Subscriber ID: enter/)
  // An unchecked ID is ignored even if its leftover text is invalid.
  expect(contentSecurityConfigError(opts({ contentIdExpr: 'a; b' }))).toBeNull()
  expect(contentSecurityConfigError(opts({ contentIdEnabled: true, contentIdExpr: 'a // b' }))).toBe(
    'Content ID: Must not contain VCL comments (//, /*, or #).',
  )
})

test('contentSecurityPayload sends unchecked IDs as empty and is off when nothing is selected', () => {
  const all = opts({ subscriberIdEnabled: true, subscriberIdExpr: ' req.http.S ', contentIdExpr: 'req.http.C', enforcementEnabled: true })
  expect(contentSecurityPayload(true, all)).toEqual({ enabled: true, subscriber: 'req.http.S', content: '', enforcement: true })
  expect(contentSecurityPayload(false, all)).toEqual({ enabled: false, subscriber: '', content: '', enforcement: false })
  expect(contentSecurityPayload(true, opts())).toMatchObject({ enabled: false })
  expect(contentSecurityPayload(true, opts({ enforcementEnabled: true }))).toMatchObject({ enabled: true, subscriber: '' })
})

test('the Streaming checkbox reflects both children: off, partial, on', () => {
  const { unmount } = renderHarness()
  expect(streamingBox()).not.toBeChecked()
  expect(streamingBox()).not.toBePartiallyChecked()
  unmount()

  const partial = renderHarness({ initial: { cmcd: true } })
  expect(streamingBox()).toBePartiallyChecked()
  expect(screen.getByText('1 of 2 enabled')).toBeInTheDocument()
  partial.unmount()

  renderHarness({ initial: { cmcd: true, token: true, subOn: true, sub: 'req.http.X-Sub' } })
  expect(streamingBox()).toBeChecked()
  expect(screen.getByText('2 of 2 enabled')).toBeInTheDocument()
})

test('checking Streaming enables both, checks all of Content Security, and opens it for input', async () => {
  renderHarness()
  expect(screen.queryByText('CMCD Metrics')).not.toBeInTheDocument()

  await userEvent.click(streamingBox())

  expect(streamingBox()).toBeChecked()
  expect(box('Enable CMCD Metrics')).toBeChecked()
  expect(box('Enable Content Security')).toBeChecked()
  expect(box('Enable Subscriber ID')).toBeChecked()
  expect(box('Enable Content ID')).toBeChecked()
  expect(box('Log enforcement outcome')).toBeChecked()
  // The IDs have no expression yet, so Content Security opens ready for input...
  expect(screen.getByRole('textbox', { name: 'Subscriber ID VCL expression' })).toBeEnabled()
  expect(screen.getByRole('alert')).toHaveTextContent('enter a VCL expression')
  // ...while CMCD, which works with its defaults, stays collapsed.
  expect(screen.queryByText('CMCD Version')).not.toBeInTheDocument()
})

test('unchecking Streaming disables both children', async () => {
  renderHarness({ initial: { cmcd: true, token: true, subOn: true, sub: 'req.http.X-Sub', enf: true } })

  await userEvent.click(streamingBox())

  expect(streamingBox()).not.toBeChecked()
  await userEvent.click(screen.getByRole('button', { name: /streaming/i }))
  expect(box('Enable CMCD Metrics')).not.toBeChecked()
  expect(box('Enable Content Security')).not.toBeChecked()
})
