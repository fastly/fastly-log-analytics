/**
 * MSW request handlers for vitest.
 *
 * Why MSW (vs per-test ``vi.mock('@/lib/api')``): MSW intercepts at the
 * fetch boundary, so the openapi-fetch client + its middleware (the
 * ``x-service-id`` header injection, the ``onResponse`` error-throwing
 * shim) all run for real. ``vi.mock`` short-circuits at the module
 * boundary and silently bypasses the middleware — which is exactly the
 * code a few of our bugs have lived in.
 *
 * The base URL must match what [lib/api.ts](../../lib/api.ts) computes
 * under jsdom: in jsdom, ``typeof window !== 'undefined'`` is true and
 * the helper returns ``${window.location.protocol}//127.0.0.1:8000``.
 * jsdom's default protocol is ``http:``.
 */

import { http, HttpResponse } from 'msw'

const API_BASE = 'http://127.0.0.1:8000'

/**
 * Default handlers used by ``server.listen()``. Override per-test with
 * ``server.use(http.get(...))`` rather than redefining the default set.
 */
export const handlers = [
  http.get(`${API_BASE}/api/bootstrap`, () =>
    HttpResponse.json({
      services: [
        { service_id: 'svc-default', name: 'Default Service', access_level: 'read_write' },
      ],
      active_service_id: 'svc-default',
    }),
  ),

  http.get(`${API_BASE}/api/services`, () =>
    HttpResponse.json({
      services: [
        { service_id: 'svc-default', name: 'Default Service', access_level: 'read_write' },
      ],
    }),
  ),
]
