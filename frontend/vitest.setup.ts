import '@testing-library/jest-dom/vitest'
import { afterAll, afterEach, expect } from 'vitest'
import { act } from 'react'
import { cleanup } from '@testing-library/react'
import * as matchers from 'vitest-axe/matchers'
import { server } from './tests/msw/server'

// React 19 emits "The current testing environment is not configured to support
// act(...)" whenever a state update is flushed unless this flag is set on
// globalThis. React-Testing-Library still wraps user interactions in act, but
// the flag is what tells React that act-wrapping is in effect.
;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true

// Mock scrollIntoView
window.HTMLElement.prototype.scrollIntoView = function() {}

// jsdom doesn't implement <canvas>. Plotly probes getContext() during init
// and jsdom emits a noisy "Not implemented" warning for each call. Returning
// null is the correct signal (canvas-unavailable) and matches what Plotly
// already handles gracefully in non-browser environments.
window.HTMLCanvasElement.prototype.getContext = function () {
  return null
} as unknown as HTMLCanvasElement['getContext']

// Register vitest-axe's ``toHaveNoViolations`` matcher globally so any
// test can do ``expect(await axe(container)).toHaveNoViolations()``.
expect.extend(matchers)

// MSW lifecycle. ``onUnhandledRequest: 'bypass'`` means unmocked URLs
// fall through to the real network — under jsdom that's an immediate
// ECONNREFUSED for any request not handled by the default set, which is
// the loud-fail behavior we want. Switch to ``'error'`` to make every
// missing handler explicit if/when most tests have migrated.
//
// MUST run at module load (not in beforeAll), because libraries such as
// openapi-fetch capture ``globalThis.fetch`` at createClient time
// ([lib/api.ts](./lib/api.ts)). If we hadn't patched fetch yet, the
// captured reference would be the unpatched original and MSW would never
// see those requests.
server.listen({ onUnhandledRequest: 'bypass' })

// Reset any per-test handler overrides so tests stay independent.
afterEach(() => {
  server.resetHandlers()
})

afterAll(() => {
  server.close()
})

// Flush any pending React concurrent-mode scheduler work after each test so
// that React's setImmediate callbacks don't fire after jsdom tears down window.
afterEach(async () => {
  await act(async () => {})
})

// Unmount any trees still in the DOM and clear the container between tests.
// With ``globals: false`` in vitest.config, @testing-library/react does not
// auto-register its own afterEach(cleanup) hook — so without this, rendered
// components from earlier tests stick around and pollute screen queries.
afterEach(() => {
  cleanup()
})
