/**
 * Node-side MSW setup for vitest.
 *
 * The lifecycle (server.listen / resetHandlers / server.close) is wired
 * in [vitest.setup.ts](../../vitest.setup.ts). Tests import this module
 * only when they need to add or override handlers via ``server.use(...)``.
 */

import { setupServer } from 'msw/node'
import { handlers } from './handlers'

export const server = setupServer(...handlers)
