/**
 * O6 — Tests for the post-build preload-manifest scanner.
 *
 * The script lives at scripts/build-preload-manifest.mjs and is run
 * by ``npm run build`` after ``next build``. It walks
 * .next/static/chunks/*.js for the plotly-package markers and emits
 * .next/static/preload-manifest.json.
 *
 * These tests spawn the script as a child process against a fixture
 * directory we build per-test under os.tmpdir(). Spawning preserves
 * the real CLI behavior — cwd resolution, exit codes, log messages.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { execFileSync } from 'node:child_process'
import { promises as fs } from 'node:fs'
import path from 'node:path'
import os from 'node:os'

const SCRIPT = path.resolve(__dirname, '..', 'scripts', 'build-preload-manifest.mjs')

let tmpRoot: string

beforeEach(async () => {
  tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'preload-manifest-test-'))
  await fs.mkdir(path.join(tmpRoot, '.next', 'static', 'chunks'), { recursive: true })
})

afterEach(async () => {
  await fs.rm(tmpRoot, { recursive: true, force: true })
})

async function writeChunk(name: string, content: string) {
  await fs.writeFile(path.join(tmpRoot, '.next', 'static', 'chunks', name), content, 'utf8')
}

function runScript(): { stdout: string; stderr: string } {
  // Spawn from the tmp dir so the script's path.resolve(process.cwd(), ...)
  // anchors to our fixture instead of the real frontend dir.
  const out = execFileSync('node', [SCRIPT], { cwd: tmpRoot, encoding: 'utf8', stdio: 'pipe' })
  return { stdout: out, stderr: '' }
}

async function readManifest(): Promise<any> {
  const raw = await fs.readFile(
    path.join(tmpRoot, '.next', 'static', 'preload-manifest.json'),
    'utf8',
  )
  return JSON.parse(raw)
}

describe('build-preload-manifest', () => {
  it('matches a chunk that contains a plotly marker AND exceeds the size floor', async () => {
    // Padding ensures the file is > MIN_BYTES (100 KB) without being absurd.
    const padding = 'x'.repeat(200_000)
    await writeChunk('big-with-plotly.js', `// plotly-logomark\n${padding}`)
    runScript()
    const m = await readManifest()
    expect(m.preload).toHaveLength(1)
    expect(m.preload[0].file).toBe('big-with-plotly.js')
    expect(m.preload[0].bytes).toBeGreaterThan(100_000)
  })

  it('excludes a chunk that has the marker but is below the size floor', async () => {
    // 5 KB — well below the 100 KB floor. Even though the marker is
    // present, modulepreloading a chunk this small is net neutral.
    await writeChunk('tiny-with-plotly.js', '// plotly-logomark\nconsole.log(1)')
    runScript()
    const m = await readManifest()
    expect(m.preload).toHaveLength(0)
  })

  it('excludes a chunk that lacks the marker', async () => {
    const padding = 'y'.repeat(200_000)
    await writeChunk('big-no-marker.js', `// just bundle code\n${padding}`)
    runScript()
    const m = await readManifest()
    expect(m.preload).toHaveLength(0)
  })

  it('matches either marker (logomark OR afterplot) — resilient to plotly tree-shaking one', async () => {
    const padding = 'z'.repeat(200_000)
    await writeChunk('big-afterplot.js', `// plotly_afterplot hook\n${padding}`)
    runScript()
    const m = await readManifest()
    expect(m.preload).toHaveLength(1)
    expect(m.preload[0].file).toBe('big-afterplot.js')
  })

  it('sorts matches by size descending so the biggest chunk preloads first', async () => {
    const small = 'a'.repeat(150_000) // ~150 KB
    const big = 'b'.repeat(600_000)   // ~600 KB
    await writeChunk('small.js', `// plotly-logomark\n${small}`)
    await writeChunk('big.js', `// plotly-logomark\n${big}`)
    runScript()
    const m = await readManifest()
    expect(m.preload).toHaveLength(2)
    expect(m.preload[0].file).toBe('big.js')
    expect(m.preload[1].file).toBe('small.js')
    expect(m.preload[0].bytes).toBeGreaterThan(m.preload[1].bytes)
  })

  it('writes a valid empty manifest when no chunks match — never fails the build', async () => {
    // Empty chunks dir; the script must still write a manifest with
    // preload=[] so the runtime reader can parse it.
    runScript()
    const m = await readManifest()
    expect(m.preload).toEqual([])
    expect(m.generatedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/)
    expect(m.markers).toContain('plotly-logomark')
  })

  it('skips silently when the chunks dir does not exist (dev build, etc.)', async () => {
    await fs.rm(path.join(tmpRoot, '.next'), { recursive: true })
    // Must NOT throw and must NOT create a manifest file.
    expect(() => runScript()).not.toThrow()
    await expect(
      fs.access(path.join(tmpRoot, '.next', 'static', 'preload-manifest.json')),
    ).rejects.toThrow()
  })
})
