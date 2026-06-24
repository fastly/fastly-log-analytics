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

// jsdom polyfills: only patch globals when the runtime actually has
// them. Test files that opt into the ``node`` environment (e.g. the
// R-13 backend-contract test) load this same setup file but don't
// expose a ``window`` global, so the unconditional access throws on
// import. Skip the polyfills in that case — jsdom-specific tests are
// the only consumers.
if (typeof window !== 'undefined') {
  // Mock scrollIntoView
  window.HTMLElement.prototype.scrollIntoView = function() {}

  // jsdom doesn't implement <canvas>. Plotly probes getContext() during init
  // and jsdom emits a noisy "Not implemented" warning for each call. Returning
  // null is the correct signal (canvas-unavailable) and matches what Plotly
  // already handles gracefully in non-browser environments.
  window.HTMLCanvasElement.prototype.getContext = function () {
    return null
  } as unknown as HTMLCanvasElement['getContext']
}

// jsdom can't perform real navigation, so any code path that calls
// location.assign/replace/reload or assigns location.href (e.g. lib/api.ts's
// admin_token_invalid redirect, app/error.tsx's reload) makes jsdom emit a
// "Not implemented: navigation to another Document" error via its
// virtualConsole — noise in tests that only assert the surrounding behavior.
// jsdom's Location methods are non-configurable, so we replace window.location
// with a stub whose reads DELEGATE (via getters) to the real Location — so
// history.pushState-driven URL tests keep seeing live pathname/search/href and
// `{ ...window.location }` spreads still capture real values — while the
// navigation methods + href assignment become no-ops. Tests that assert
// navigation replace window.location wholesale (Object.defineProperty) and win.
if (typeof window !== 'undefined') {
  const realLocation = window.location
  const navNoop = () => {}
  const locationStub: Record<PropertyKey, unknown> = {
    assign: navNoop,
    replace: navNoop,
    reload: navNoop,
    toString: () => realLocation.href,
  }
  for (const key of [
    'href', 'protocol', 'host', 'hostname', 'port', 'pathname', 'search', 'hash', 'origin',
  ] as const) {
    Object.defineProperty(locationStub, key, {
      enumerable: true,
      configurable: true,
      get: () => realLocation[key],
      // href assignment navigates; swallow it. Other components stay live.
      set: key === 'href' ? () => {} : (v: string) => { (realLocation as unknown as Record<string, string>)[key] = v },
    })
  }
  Object.defineProperty(window, 'location', { configurable: true, writable: true, value: locationStub })
}

// Register vitest-axe's ``toHaveNoViolations`` matcher globally so any
// test can do ``expect(await axe(container)).toHaveNoViolations()``.
expect.extend(matchers)

// MSW lifecycle. ``onUnhandledRequest`` is a callback (R-2) so that:
//  - unmatched calls to the backend base URL fail loudly (the old
//    ``'bypass'`` default silently let tests pass against responses
//    that never resolved); and
//  - the SSR / E2E tests that spin up their OWN node:http server on a
//    random loopback port stay reachable. MSW's node integration
//    intercepts node:http too, so we explicitly skip enforcement for
//    those.
//
// MUST run at module load (not in beforeAll), because libraries such as
// openapi-fetch capture ``globalThis.fetch`` at createClient time
// ([lib/api.ts](./lib/api.ts)). If we hadn't patched fetch yet, the
// captured reference would be the unpatched original and MSW would never
// see those requests.
server.listen({
  onUnhandledRequest(request, print) {
    const url = new URL(request.url)
    // Backend base URL = 127.0.0.1:8000 (see ``handlers.ts``). Any
    // other localhost port belongs to a test-owned http server (the
    // SSR helper tests stand one up on a random port); let those
    // through untouched.
    const isBackendBase = url.hostname === '127.0.0.1' && url.port === '8000'
    if (!isBackendBase) return
    print.error()
  },
})

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
