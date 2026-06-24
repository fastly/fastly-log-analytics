'use client'

export function UpdatingBadge() {
  // text-foreground (not text-primary): primary-coloured text on the
  // primary/10 tint is sub-AA contrast (same hue). Keep the primary accent
  // via the pulsing dot + tint; the label reads in foreground so it meets
  // WCAG 2.1 AA in both themes.
  return (
    <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-primary/10 text-foreground text-[10px] font-bold uppercase tracking-wider animate-pulse">
      <span className="w-1.5 h-1.5 rounded-full bg-primary" />
      Updating
    </div>
  )
}
