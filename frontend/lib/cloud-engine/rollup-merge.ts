import { Histogram } from "./dashboard/histogram";
import { Hll } from "./dashboard/hll";

export function mergeNullableHistogram(
  into: Histogram | null,
  from: Histogram | null,
): Histogram | null {
  if (!from) return into;
  if (!into) {
    const copy = new Histogram();
    copy.merge(from);
    return copy;
  }
  into.merge(from);
  return into;
}

export function mergeNullableHll(into: Hll | null, from: Hll | null): Hll | null {
  if (!from) return into;
  if (!into) return from.clone();
  into.merge(from);
  return into;
}

export function mergeHistogramBytes(target: Histogram, value: unknown): void {
  if (!(value instanceof Uint8Array)) return;
  const parsed = Histogram.fromBytes(value);
  if (parsed) target.merge(parsed);
}

export function mergeHllBytes(target: Hll, value: unknown): void {
  if (!(value instanceof Uint8Array)) return;
  const parsed = Hll.fromBytes(value);
  if (parsed) target.merge(parsed);
}

export function mergeNumberMap<K>(
  into: Map<K, number>,
  source: ReadonlyMap<K, number>,
): void {
  for (const [key, value] of source) {
    into.set(key, (into.get(key) ?? 0) + value);
  }
}
