import { usePopGeoStore } from '@/stores/popGeoStore'

export type PopGeo = { city?: string; region?: string; country?: string }

// Shield-derived 2-letter country codes that read better in their customary
// short form. Anything else falls back to the uppercased code (DE, FR, JP…).
const COUNTRY_ABBR: Record<string, string> = { us: 'USA', uk: 'UK', gb: 'UK' }

/**
 * Format a PoP's location as "Denver, CO - USA" (state omitted when unknown,
 * e.g. "London - UK"). Returns "" when no usable location is known so callers
 * can fall back to the bare PoP code. This is the single source of truth for
 * how a PoP's geography reads anywhere in the app.
 */
export function formatPopGeo(geo?: PopGeo | null): string {
  if (!geo) return ''
  const city = (geo.city ?? '').replace(' (Metro)', '').trim()
  if (!city) return ''
  const region = (geo.region ?? '').toUpperCase()
  const cc = (geo.country ?? '').toLowerCase()
  const country = COUNTRY_ABBR[cc] ?? cc.toUpperCase()
  // Many non-US shield codes repeat the city in the middle segment
  // (e.g. "hyd-hyderabad-in"), which would render "Hyderabad, HYDERABAD".
  // Drop the region when it's just the city again.
  const norm = (s: string) => s.replace(/[^a-z]/gi, '').toUpperCase()
  const showRegion = region && norm(region) !== norm(city)
  const locality = showRegion ? `${city}, ${region}` : city
  return country ? `${locality} - ${country}` : locality
}

/**
 * Read the PoP geo map ({CODE: {city,region,country}}) populated from bootstrap
 * (see useBootstrap). Reads from a plain store so <PopLabel> works without a
 * QueryClientProvider in context.
 */
function usePopGeoMap(): Record<string, PopGeo> {
  return usePopGeoStore((s) => s.map)
}

/** Resolve a single PoP code to its formatted geo string ("" if unknown). */
export function usePopGeo(code?: string | null): string {
  const map = usePopGeoMap()
  if (!code || !map) return ''
  return formatPopGeo(map[String(code).toUpperCase()])
}
