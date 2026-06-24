# ADR-05 — Frontend Rendering Boundary

**Status:** Accepted (Phase 0)
**Decided by:** v2.0 cleanup planning
**Supersedes:** the implicit "CSR everywhere" pattern + the warm-up workarounds it forced

## Context

The current frontend is essentially CSR-everywhere with Next.js routing. Symptoms of not having an RSC/CSR decision in writing:

- **Hidden Plotly pre-warm** (commit 2d3a663) — a hidden 1-pixel Plotly chart on the dashboard route to force chunk download before the real chart needs it
- **Hidden MapLibre pre-warm** (commit 0762acf) — same pattern for the network map
- **`PlotlyChart` with `visible=false`** + **`LazyMount`** + **per-page `dynamic` imports** — three different lazy-loading mechanisms doing similar work
- **`styledata` event swap** (commit aa1a096) — the only pattern that actually works for MapLibre style changes, but only used in one place
- **`useUrlFilterSync` / `useUrlServiceSync` custom hooks** — manually syncing Zustand store to/from URL query params with useEffects (hydration desync source)
- **Route prefetch chips** in `next/link` — ad-hoc, not policy-driven

Each is a local fix. None followed from a stated rule.

## Decision

Every route has an explicit rendering classification, documented in `frontend/app/_routing.md` (added in Phase 9a). The classifications:

- **RSC** — Server-rendered, no client JS for the initial paint. Used for routes that are read-mostly and don't need interactivity in the critical path. Data fetched server-side, streamed to the client.
- **CSR** — Client-rendered. Used for routes that are inherently interactive (live filtering, charts, maps).
- **Hybrid** — RSC shell + CSR islands. Used for routes where the layout / navigation is static but the data viz is interactive.

### Per-route rules (Phase 9a populates with the actual table)

The actual route classifications get filled in during Phase 9a after auditing each route. The decision factors:

| Factor | Pulls toward RSC | Pulls toward CSR |
|---|---|---|
| Initial paint contains heavy chart | — | yes |
| Route is reachable only after auth | — | yes (already client-bound) |
| Data is static for the session | yes | — |
| Filter state changes URL | yes (URL → server) | (hooks like nuqs let CSR do it too) |
| First paint timing matters | yes | — |

### Code-split policy

- **One `dynamic()` import per heavy chunk per route.** No multi-route shared dynamic imports.
- **`modulepreload` is the policy hint for chunks that we know will be needed within ~1s of route entry.** Replaces the hidden-pre-warm pattern.
- **`LazyMount` and `visible=false` collapse into one shared utility** documented in the routing table.

### Prefetch policy

- **`prefetch={true}` on `<Link>` only when the linked route has been benchmarked as fast-to-RSC-render.** Otherwise leave default (Next.js heuristic).
- **No manual prefetch in `useEffect`.**

### URL state policy

- **`nuqs` is the *target* single source of truth for URL-driven state** (filters, active service, time window, custom metrics). Adoption is incremental: `serviceStore` is migrated via `useUrlServiceSync` (the first nuqs adoption); `filterStore` still uses the legacy `useUrlFilterSync` (plain `URLSearchParams` + `history.replaceState`) and its nuqs migration is deferred. See `frontend/app/_routing.md` for the live state.
- **Zustand stores own UI-only state** that doesn't survive a refresh. Anything that needs to survive a refresh lives in URL state.

## Consequences

- The hidden-Plotly and hidden-MapLibre pre-warm patterns get deleted in Phase 9a, replaced with `modulepreload` declared per the routing rule.
- The three lazy-loading mechanisms collapse to one shared utility.
- The `styledata` event swap pattern becomes the default for MapLibre style changes.
- `useUrlFilterSync` / `useUrlServiceSync` become thin wrappers over `nuqs` or disappear.
- Phase 9b's frontend file splits work alongside the routing table — splits respect RSC/CSR boundaries (RSC modules don't import client-only React hooks).

## Out of scope

- Web Vitals / Lighthouse CI gates (separate concern; can layer on later)
- Per-locale routing
- Server Actions for mutations (frontend talks to the FastAPI backend; Server Actions aren't a fit here)
- Edge runtime for Next.js routes (the Next.js app runs in the same container as Caddy in front; no edge runtime needed)
