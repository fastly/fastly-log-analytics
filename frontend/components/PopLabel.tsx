import type { components } from '@/types/api.generated'
import { usePopGeoStore } from '@/stores/popGeoStore'

export type PopGeo = components['schemas']['BootstrapPopGeo']

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

/**
 * Renders a Fastly PoP as "DEN (Denver, CO - USA)" — the code at the caller's
 * text size, the city/state/country smaller + muted in parentheses. Falls back
 * to the bare code when the PoP isn't in the bootstrap pop_geo map (or for
 * non-code values like "Direct to Origin", which are passed through verbatim).
 *
 * This is THE shared PoP renderer — use it everywhere a PoP (edge or shield)
 * is shown so the format stays consistent.
 */
export function PopLabel({ code, className }: { code?: string | null; className?: string }) {
  const raw = (code ?? '').toString()
  const geo = usePopGeo(raw)
  if (!raw) return null
  // Uppercase bare 3-4 letter codes (jfk -> JFK); leave special labels like
  // "Direct to Origin" untouched.
  const display = /^[a-z]{2,4}$/i.test(raw) ? raw.toUpperCase() : raw
  return (
    <span className={className}>
      {display}
      {geo && <span className="ml-1 text-[10px] font-normal text-muted-foreground">({geo})</span>}
    </span>
  )
}
