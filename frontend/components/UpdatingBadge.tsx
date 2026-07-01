'use client'

export function UpdatingBadge() {
  // text-foreground (not text-primary): primary-coloured text on the
  // primary/10 tint is sub-AA contrast (same hue). Keep the primary accent
  // via the pulsing dot + tint; the label reads in foreground so it meets
  // WCAG 2.1 AA in both themes.
  //
  // animate-pulse lives on the DOT, not the pill: pulsing the whole element
  // drops the text to ~0.5 opacity mid-animation, which blends the
  // foreground into the tint (~3.4:1, sub-AA) and trips axe color-contrast.
  // Confining the pulse to the (decorative, non-text) dot keeps the live
  // affordance while the label stays at full opacity / AA contrast.
  return (
    <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-primary/10 text-foreground text-[10px] font-bold uppercase tracking-wider">
      <span className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse" />
      Updating
    </div>
  )
}
