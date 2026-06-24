import { usePopGeo } from '@/lib/pop'

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
