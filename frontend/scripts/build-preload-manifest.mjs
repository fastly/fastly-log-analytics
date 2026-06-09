#!/usr/bin/env node
/**
 * O6 — Post-build chunk scanner for <link rel="modulepreload">.
 *
 * Bootstrap-manifest variant (2026-06-06): writes BOTH the legacy
 * runtime location (``.next/static/preload-manifest.json``) AND the
 * **committed** location (``lib/_preload-chunks.json``).
 *
 * Why two locations:
 *   - The committed file is what ``lib/preload-manifest.ts`` imports
 *     STATICALLY via a JSON import — Webpack/Turbopack inlines its
 *     content into the bundle at compile time. SSG-time renders of
 *     the layout therefore see the values from the LAST time the file
 *     was committed (= the previous build's chunk hashes). Plotly's
 *     chunk name is content-hashed and stable across builds as long
 *     as plotly itself is unchanged → preload hrefs stay correct.
 *   - The runtime ``.next/static`` location is kept for backward
 *     compat with any runtime reader that still uses the dynamic
 *     read path (none today, but harmless to keep emitting it).
 *
 * Workflow:
 *   1. ``next build`` runs, layout SSGs with whatever's in
 *      lib/_preload-chunks.json at git HEAD.
 *   2. This scanner runs, writes the JSON file with the CURRENT
 *      build's chunk hashes.
 *   3. Developer commits the updated file (``git add
 *      frontend/lib/_preload-chunks.json``). Next deploy benefits.
 *   4. Docker builds skip step 3 — they update the file inside the
 *      image but the change isn't persisted to git, so the next
 *      docker build still uses the committed value. That's fine
 *      because plotly's content-hashed name is stable (only changes
 *      when plotly itself is upgraded, which is rare).
 *
 * After a plotly upgrade: run ``npm run build`` locally, commit the
 * regenerated ``lib/_preload-chunks.json``, redeploy.
 *
 * If the scan finds nothing (e.g. plotly was removed, or the bundler
 * inlined it into a chunk without the literal marker) the script
 * writes an empty list and prints a warning. It MUST NOT fail the
 * build — modulepreload is an optimisation, not a correctness gate.
 */

import { promises as fs } from 'node:fs'
import path from 'node:path'
import process from 'node:process'

const CHUNKS_DIR = path.resolve(process.cwd(), '.next', 'static', 'chunks')
const RUNTIME_MANIFEST_PATH = path.resolve(process.cwd(), '.next', 'static', 'preload-manifest.json')
// COMMITTED location — imported statically by lib/preload-manifest.ts so the
// values get inlined into the bundle at compile time (visible to SSG).
const COMMITTED_MANIFEST_PATH = path.resolve(process.cwd(), 'lib', '_preload-chunks.json')

// Markers that appear ONLY when plotly's actual library code is
// bundled into a chunk (not just a reference / dynamic-import shim).
// Both are internal plotly identifiers:
//   - plotly-logomark: SVG class for the modebar watermark, only in
//     the rendering layer.
//   - plotly_afterplot: event-system hook name, only in core code.
// A chunk needs at least ONE marker hit to qualify — gives us a bit
// of resilience to plotly tree-shaking some markers in a future version.
// Detected empirically by grepping production chunks: 1 chunk of ~60
// (1.4 MB) contained both on the 2026-06-05 build; other chunks with
// "plotly" substrings were the much smaller dynamic-import shims.
const PLOTLY_MARKERS = ['plotly-logomark', 'plotly_afterplot']

// Size floor: chunks below this aren't worth the preload overhead
// (a modulepreload request that resolves smaller than the TCP RTT
// would have saved is net neutral). 100 KB is a conservative cut.
const MIN_BYTES = 100 * 1024

async function main() {
  let entries
  try {
    entries = await fs.readdir(CHUNKS_DIR, { withFileTypes: true })
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.warn(
        `[preload-manifest] ${CHUNKS_DIR} not found — skipping (likely a dev build).`,
      )
      return
    }
    throw err
  }

  const jsFiles = entries.filter((d) => d.isFile() && d.name.endsWith('.js'))
  const matches = []

  for (const dirent of jsFiles) {
    const full = path.join(CHUNKS_DIR, dirent.name)
    const stat = await fs.stat(full)
    if (stat.size < MIN_BYTES) continue
    // Read as utf-8; the markers are short ASCII so this is robust to
    // any non-ASCII content elsewhere in the chunk (it just won't match).
    const buf = await fs.readFile(full, 'utf8')
    if (PLOTLY_MARKERS.some((m) => buf.includes(m))) {
      matches.push({ file: dirent.name, bytes: stat.size })
    }
  }

  // Sort by size descending so the layout preloads the biggest chunk
  // first — that's the one the browser will spend the most time
  // fetching once the main bundle resolves the dynamic import.
  matches.sort((a, b) => b.bytes - a.bytes)

  const manifest = {
    generatedAt: new Date().toISOString(),
    markers: PLOTLY_MARKERS,
    minBytes: MIN_BYTES,
    // Path is RELATIVE to /_next/static/chunks/ as served by Next.
    // The runtime reader prepends "/_next/static/chunks/" before
    // emitting the <link href>.
    preload: matches.map((m) => ({ file: m.file, bytes: m.bytes })),
  }

  const serialized = JSON.stringify(manifest, null, 2) + '\n'
  // Runtime location (unchanged for backwards compat).
  await fs.writeFile(RUNTIME_MANIFEST_PATH, serialized, 'utf8')
  // Committed location — the source of truth for the next build's SSG.
  await fs.writeFile(COMMITTED_MANIFEST_PATH, serialized, 'utf8')

  if (matches.length === 0) {
    console.warn(
      `[preload-manifest] no chunks matched markers ${PLOTLY_MARKERS.join('/')} — written empty manifest. ` +
      `If plotly is still in the bundle, the markers may have moved.`,
    )
  } else {
    const totalKb = (matches.reduce((s, m) => s + m.bytes, 0) / 1024).toFixed(0)
    console.log(
      `[preload-manifest] ${matches.length} chunk(s) marked for modulepreload, ${totalKb} KB total: ` +
      matches.map((m) => `${m.file} (${(m.bytes / 1024).toFixed(0)}KB)`).join(', '),
    )
  }
}

main().catch((err) => {
  // Optimisation, not a build gate — log and exit 0.
  console.warn('[preload-manifest] scan failed:', err)
  process.exit(0)
})
