/**
 * Shared MapLibre country/world-layer color tokens.
 *
 * Four map components (ChoroplethMap, NetworkMap/MapLayer, ShieldingMap,
 * Insights/ImpossibleDistanceModal) each inlined the same
 * ``theme === 'dark' ? '#27272a' : '#e4e4e7'`` ternary pattern for the
 * country-fill + country-outline layers. Centralizing here so a future
 * theme-token swap (e.g. picking up a Tailwind v5 zinc-shade adjustment)
 * lands in one place.
 *
 * Values match Tailwind's zinc-800 / zinc-300 (fill) and zinc-700 /
 * zinc-400 (outline). Hex literals (not Tailwind class names) because
 * MapLibre's paint props take raw color strings.
 */

export const COUNTRY_FILL_DARK = '#27272a'
export const COUNTRY_FILL_LIGHT = '#e4e4e7'
export const COUNTRY_OUTLINE_DARK = '#3f3f46'
export const COUNTRY_OUTLINE_LIGHT = '#d4d4d8'

export function countryFill(isDark: boolean): string {
  return isDark ? COUNTRY_FILL_DARK : COUNTRY_FILL_LIGHT
}

export function countryOutline(isDark: boolean): string {
  return isDark ? COUNTRY_OUTLINE_DARK : COUNTRY_OUTLINE_LIGHT
}
