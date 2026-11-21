import { describe, expect, it, vi } from 'vitest'
import { downloadAsCsv, sanitizeCsvCell } from './exportCsv'
import { cloudAdminWire } from './cloud-engine/admin-wire'
import { queryOpsLogs } from './cloud-engine/telemetry/client'

vi.mock('./cloud-engine/telemetry/admin', () => ({
  adminRequest: vi.fn(),
  deleteLogHistory: vi.fn(),
  purgeQuarantine: vi.fn(),
}))
vi.mock('./cloud-engine/telemetry/client', () => ({ queryOpsLogs: vi.fn() }))

describe('T5-FE-07: CSV formula-injection neutralization in exportCsv and cloud-engine admin-wire', () => {
  it('neutralizes formula prefixes including leading whitespace/control/zero-width chars while preserving signed numbers', () => {
    for (const dangerous of [
      '=HYPERLINK("https://evil.example","click")',
      '+1+1',
      "-cmd|' /C calc'!A0",
      '@SUM(A1:A2)',
      '|calc',
      '%calc',
      '\t=1+1',
      '\r=1+1',
      '\n=1+1',
      ' =1+1',
      '\u200b=1+1',
      '\u0085=1+1',
      '\u2060=1+1',
      '\x01=1+1',
    ]) {
      const cell = sanitizeCsvCell(dangerous)
      const unquoted = cell.startsWith('"') && cell.endsWith('"') ? cell.slice(1, -1).replaceAll('""', '"') : cell
      expect(unquoted.startsWith("'"), `expected leading ' for ${JSON.stringify(dangerous)}, got ${JSON.stringify(cell)}`).toBe(true)
    }

    expect(sanitizeCsvCell(-12.5)).toBe('-12.5')
    expect(sanitizeCsvCell('-12.5')).toBe('-12.5')
    expect(sanitizeCsvCell('+3.14')).toBe('+3.14')
    expect(sanitizeCsvCell(0)).toBe('0')
    expect(sanitizeCsvCell(42)).toBe('42')
    expect(sanitizeCsvCell(true)).toBe('true')
  })

  it('sanitizes both header columns and row cells in downloadAsCsv and cloudAdminWire usage export', async () => {
    let capturedBlob: Blob | null = null
    const origCreateObjectURL = window.URL.createObjectURL
    const origRevokeObjectURL = window.URL.revokeObjectURL
    window.URL.createObjectURL = vi.fn((b: Blob | MediaSource) => {
      capturedBlob = b as Blob
      return 'blob:test'
    })
    window.URL.revokeObjectURL = vi.fn()
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})

    try {
      downloadAsCsv(
        [{ '=evil_col': ' =CMD|calc', delta: -12.5 }],
        ['=evil_col', 'delta'],
        'out.csv',
      )
      expect(capturedBlob).not.toBeNull()
      const csvText = await capturedBlob!.text()
      expect(csvText).toBe("'=evil_col,delta\n' =CMD|calc,-12.5")
    } finally {
      clickSpy.mockRestore()
      window.URL.createObjectURL = origCreateObjectURL
      window.URL.revokeObjectURL = origRevokeObjectURL
    }

    vi.mocked(queryOpsLogs).mockResolvedValueOnce({
      kind: 'fos_usage',
      from: '2026-01-01T00:00:00Z',
      to: '2026-01-02T00:00:00Z',
      rows: [
        {
          timestamp: Date.parse('2026-01-01T01:00:00Z'),
          op_class: 'A',
          op: '|calc',
          dataset: '\u0085=1+1',
          bytes_in: 10,
          bytes_out: 20,
          caller: '\u2060@SUM(1)',
          est_cost_usd: -12.5,
          op_count: 1,
        },
      ],
      oldestLiveMs: null,
      status: null,
      liveTruncated: false,
      stats: { listed: 1, read: 1, cached: 0, cdnHits: 0, skippedCovered: 0, ignoredCompact: 0, evicted: 0, liveRows: 0 },
    } as never)

    const resp = await cloudAdminWire(
      new Request(
        'http://localhost/api/admin/usage-log/export?start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z',
        { headers: { 'x-service-id': 'svc-1' } },
      ),
    )
    expect(resp?.status).toBe(200)
    const usageCsv = await resp!.text()
    expect(usageCsv).toContain("'|calc")
    expect(usageCsv).toContain("'\u0085=1+1")
    expect(usageCsv).toContain("'\u2060@SUM(1)")
    expect(usageCsv).toContain(',-12.5,')
  })
})
