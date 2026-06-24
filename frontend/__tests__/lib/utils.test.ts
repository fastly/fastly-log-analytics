import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { downloadAsCsv } from '@/lib/utils';
import { formatBytes } from '@/lib/format'

describe('formatBytes', () => {
  it('returns "0 B" for zero', () => {
    expect(formatBytes(0)).toBe('0 B')
  })

  it('formats bytes', () => {
    expect(formatBytes(1)).toBe('1 B')
    expect(formatBytes(500)).toBe('500 B')
  })

  it('formats kilobytes', () => {
    expect(formatBytes(1024)).toBe('1 KB')
    expect(formatBytes(1536)).toBe('1.5 KB')
  })

  it('formats megabytes', () => {
    expect(formatBytes(1024 * 1024)).toBe('1 MB')
    expect(formatBytes(1024 * 1024 * 2.5)).toBe('2.5 MB')
  })

  it('formats gigabytes', () => {
    expect(formatBytes(1024 ** 3)).toBe('1 GB')
  })

  it('formats terabytes', () => {
    expect(formatBytes(1024 ** 4)).toBe('1 TB')
  })
})

describe('downloadAsCsv', () => {
  let capturedCsv = ''

  beforeEach(() => {
    capturedCsv = ''
    // jsdom Blob doesn't implement .text() — capture content at construction time
    vi.stubGlobal('Blob', class MockBlob {
      constructor(parts: BlobPart[]) { capturedCsv = (parts as string[]).join('') }
    })
    vi.stubGlobal('URL', { createObjectURL: vi.fn(() => 'blob:mock'), revokeObjectURL: vi.fn() })
    vi.spyOn(HTMLElement.prototype, 'click').mockImplementation(() => {})
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  function buildCsv(rows: Record<string, unknown>[], cols: string[]): string {
    downloadAsCsv(rows, cols, 'test.csv')
    return capturedCsv
  }

  it('first row is the header matching column order', () => {
    const csv = buildCsv([], ['name', 'count'])
    expect(csv.split('\n')[0]).toBe('name,count')
  })

  it('data rows follow column order', () => {
    const csv = buildCsv([{ name: 'alice', count: 5 }, { name: 'bob', count: 3 }], ['name', 'count'])
    const lines = csv.split('\n')
    expect(lines[1]).toBe('alice,5')
    expect(lines[2]).toBe('bob,3')
  })

  it('quotes values containing commas', () => {
    const csv = buildCsv([{ url: '/a,b', n: 1 }], ['url', 'n'])
    expect(csv).toContain('"/a,b"')
  })

  it('escapes double quotes by doubling them', () => {
    const csv = buildCsv([{ label: 'say "hello"', n: 1 }], ['label', 'n'])
    expect(csv).toContain('"say ""hello"""')
  })

  it('quotes values containing newlines', () => {
    const csv = buildCsv([{ text: 'line1\nline2', n: 1 }], ['text', 'n'])
    expect(csv).toContain('"line1\nline2"')
  })

  it('renders null as empty string', () => {
    const csv = buildCsv([{ url: null, count: 5 }], ['url', 'count'])
    expect(csv.split('\n')[1]).toBe(',5')
  })

  it('renders a missing column key as empty string', () => {
    const csv = buildCsv([{ count: 5 }], ['url', 'count'])
    expect(csv.split('\n')[1]).toBe(',5')
  })

  it('sets the download attribute to the given filename', () => {
    const appendSpy = vi.spyOn(document.body, 'appendChild')
    downloadAsCsv([], ['col'], 'my-export.csv')
    const anchor = appendSpy.mock.calls[0][0] as HTMLAnchorElement
    expect(anchor.download).toBe('my-export.csv')
  })

  it('produces correct row count including header', () => {
    const rows = [{ a: 1 }, { a: 2 }, { a: 3 }]
    const csv = buildCsv(rows, ['a'])
    expect(csv.split('\n')).toHaveLength(4) // 1 header + 3 data rows
  })
})
