import { afterEach, expect, it, vi } from 'vitest'

afterEach(() => {
  vi.restoreAllMocks()
  vi.resetModules()
})

it('validates schemas without probing dynamic code generation during client startup', async () => {
  vi.resetModules()
  const dynamicCode = vi.spyOn(globalThis, 'Function').mockImplementation(() => {
    throw new EvalError('Dynamic code generation is blocked by CSP')
  })

  await import('@/instrumentation-client')
  const { z } = await import('zod')
  const schema = z.object({ service: z.string().min(1) })

  expect(schema.safeParse({ service: 'fixture' }).success).toBe(true)
  expect(schema.safeParse({ service: 42 }).success).toBe(false)
  expect(dynamicCode).not.toHaveBeenCalled()
})
