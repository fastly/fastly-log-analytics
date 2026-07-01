// Adaptive default window/baseline selection for the Insights page.
//
// The backend skips an insight (renders an info placeholder "Requires Nh of
// historical data") whenever the chosen baseline period is longer than the
// data that actually exists. So a brand-new service with the static 7-day
// baseline default sees almost every card degrade to "not enough data". This
// module picks the best window+baseline pair for how much history a service
// actually has, using ONLY values that the existing dropdowns + backend accept
// (window_size_hrs ∈ {0.25,1,4,24}; baseline_hours ∈ {1,24,168,720}). The
// pure functions here are the single source of truth shared by the page, the
// useInsightsDefaults hook, and the unit tests.

export const WINDOW_OPTIONS = [
  { label: 'Last 15 Minutes', value: '0.25' },
  { label: 'Last 1 Hour', value: '1' },
  { label: 'Last 4 Hours', value: '4' },
  { label: 'Last 24 Hours', value: '24' },
]

export const BASELINE_OPTIONS = [
  { label: 'Previous 1 Hour', value: '1' },
  { label: 'Last 24 Hours', value: '24' },
  { label: 'Last 7 Days', value: '168' },
  { label: 'Last 30 Days', value: '720' },
]

// The historical default (also the backend default): recent 1h vs previous 7d.
// Used whenever we have no extents to adapt to, so behavior is unchanged for
// services with no data and on the server/first render.
export const STATIC_DEFAULT = { window: '1', baseline: '168' } as const

// Hours of history available = now − earliest_log_at. The baseline period the
// Insights query compares against reaches back (now − baseline − window), so
// "how far back data goes from now" is exactly the quantity a baseline must
// fit inside. Returns null when there's no/unparseable extent (→ STATIC_DEFAULT).
export function historyHoursFromExtents(
  earliest: string | null | undefined,
  now: number = Date.now(),
): number | null {
  if (!earliest) return null
  // Mirror FilterBar / useDataWindowOverlap: a date-only extent ("2026-06-15")
  // is widened to UTC start-of-day before parsing so a non-UTC machine doesn't
  // mis-bucket the span.
  const iso = earliest.length === 10 ? earliest + 'T00:00:00.000Z' : earliest
  const ms = Date.parse(iso)
  if (!Number.isFinite(ms)) return null
  return Math.max(0, (now - ms) / 3_600_000)
}

// Map available history (hours) to the best { window, baseline } the existing
// dropdown options allow. Half-open intervals: an exact boundary (e.g. 24h)
// selects the higher bucket. Returns STATIC_DEFAULT for null/NaN so no-data
// services are unchanged.
export function pickInsightsDefault(
  historyHours: number | null,
): { window: string; baseline: string } {
  if (historyHours == null || !Number.isFinite(historyHours)) {
    return { window: STATIC_DEFAULT.window, baseline: STATIC_DEFAULT.baseline }
  }
  const h = historyHours
  if (h < 1) return { window: '0.25', baseline: '1' } // <1h: smallest pair; cards light up once 1h elapses
  if (h < 4) return { window: '1', baseline: '1' } //  ~1–4h: this hour vs the previous hour
  if (h < 24) return { window: '4', baseline: '1' } //  ~4–24h: last 4h vs previous hour
  if (h < 48) return { window: '4', baseline: '24' } // ~1–2d: last 4h vs previous day
  if (h < 168) return { window: '24', baseline: '24' } // ~2–7d: day over day
  if (h < 720) return { window: '1', baseline: '168' } // ≥7d: today's static default (1h vs 7d)
  return { window: '1', baseline: '720' } //                   ≥30d: 1h vs 30d
}
