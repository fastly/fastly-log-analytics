/**
 * O6 / bootstrap-manifest — Server-side reader for the modulepreload
 * chunk manifest.
 *
 * Imports ``lib/_preload-chunks.json`` STATICALLY so the values are
 * inlined into the bundle at compile time. Critically:
 *
 *   - SSG-time renders of the root layout see the chunks list at
 *     ``next build`` time, which means the static HTML can bake in
 *     correct ``<link rel="modulepreload">`` tags for plotly and any
 *     other heavy chunk. The browser starts the chunk fetch during
 *     initial HTML parse — by the time the main bundle resolves the
 *     dynamic import, the chunk is already cached.
 *
 *   - The reader stays SYNCHRONOUS (the layout doesn't need ``async``
 *     and stays statically-renderable — no per-navigation SSR
 *     roundtrip).
 *
 * The committed JSON file is updated by ``scripts/build-preload-manifest.mjs``
 * after every ``next build``. Workflow:
 *
 *   1. Run ``npm run build`` locally after a plotly upgrade.
 *   2. The scanner rewrites ``lib/_preload-chunks.json`` with the
 *      current build's chunk hashes.
 *   3. ``git add frontend/lib/_preload-chunks.json && git commit``.
 *   4. Next deploy bakes the fresh values into SSG.
 *
 * If the JSON has ``preload: []`` (initial state, or scanner found no
 * plotly chunks), no preload links are emitted. Modulepreload is an
 * optimisation, not a correctness gate.
 */
import 'server-only'

import manifest from './_preload-chunks.json'

type ManifestEntry = { file: string; bytes: number }
type Manifest = {
  generatedAt?: string
  markers?: string[]
  minBytes?: number
  preload?: ManifestEntry[]
}

const PRELOAD_CHUNKS: string[] = (() => {
  const m = manifest as Manifest
  const preload = Array.isArray(m.preload) ? m.preload : []
  return preload
    .filter((e): e is ManifestEntry => !!e && typeof e.file === 'string')
    .map((e) => `/_next/static/chunks/${e.file}`)
})()

export function getPreloadChunks(): string[] {
  return PRELOAD_CHUNKS
}
