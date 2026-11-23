/**
 * A month of storage at the selected range's average size: billed GB-hours over the range divided
 * by its hours is the average stored GB, which a month holds as that many GB-months.
 */
export function measuredStorageGbMonths(
  gbHours: number | null | undefined,
  periodHours: number | null | undefined,
): number | null {
  if (gbHours == null || periodHours == null || !(periodHours > 0)) return null
  return gbHours / periodHours
}
