/**
 * Knip config for fastly-log-analytics frontend.
 *
 * Knip catches dead exports and unused dependencies, but in a Next.js +
 * shadcn/ui codebase a lot of legitimate code looks like "unused":
 *
 *   - shadcn UI components are a vendored design system. Every variant
 *     export (CardFooter, DropdownMenuPortal, …) is intentional public
 *     API even if no current page consumes it. Treating these as
 *     ignored keeps them out of the unused-export list.
 *   - Dynamic imports of stringly-typed package names (e.g. the
 *     ``import('plotly.js-cartesian-dist-min' as any)`` in
 *     PlotlyChart.tsx) are invisible to static analysis. Knip can't
 *     trace the resolution.
 *   - ``openapi-typescript`` is invoked via ``npm run gen:types``, a
 *     package.json script knip doesn't introspect.
 *
 * Run via ``make knip`` (or ``cd frontend && npx knip``). The gate
 * runs but stays advisory — the baseline of intentionally-exported
 * design-system types in ``types/api.ts`` still needs per-export
 * curation before gating CI on it.
 */
import type { KnipConfig } from 'knip'

const config: KnipConfig = {
  entry: [
    'app/**/{page,layout,loading,error,not-found,route,template,default,global-error}.{ts,tsx}',
    'next.config.{js,mjs,ts}',
    'tests/**/*.{ts,tsx}',
    '__tests__/**/*.{test,spec}.{ts,tsx}',
    'e2e/**/*.{ts,tsx}',
    'scripts/**/*.{js,mjs,ts}',
  ],

  project: ['**/*.{ts,tsx}!'],

  ignore: [
    // Shadcn UI is a vendored design system; every component file
    // exports the full variant API even when no current page uses
    // every export. Treat the whole dir as ignored.
    'components/ui/**',
    // Generated on every commit via the regen-openapi pre-commit
    // hook. Operation IDs and schema definitions aren't always
    // consumed by hand-written TS.
    'types/api.generated.ts',
  ],

  ignoreDependencies: [
    // Loaded by react-plotly.js via the cartesian-only factory at
    // ``components/PlotlyChart/PlotlyChart.tsx`` through a stringly-
    // typed dynamic import (``import('plotly.js-cartesian-dist-min'
    // as any)``). Knip can't follow the string through ``as any``.
    'plotly.js-cartesian-dist-min',
    // Invoked by ``npm run gen:types``; knip doesn't introspect npm
    // scripts.
    'openapi-typescript',
    // Tailwind / shadcn ecosystem deps loaded via PostCSS / shadcn
    // CLI / Tailwind's content-scan pipeline. Knip's import graph
    // doesn't traverse CSS imports or shadcn's vendoring pipeline.
    'tailwindcss',
    'tw-animate-css',
    'shadcn',
    // CodeMirror commands sub-package — pulled in transitively by
    // @codemirror/view (which is a direct dep) and used in
    // components/CodeEditor for keymap composition. Knip resolves
    // this as "unlisted" because the import looks transitive.
    '@codemirror/commands',
  ],

  ignoreBinaries: [
    // System binary used by Python test launchers (uv runs the FastAPI
    // contract backend for the Playwright suite). Not a JS dep.
    'uv',
  ],
}

export default config
