import { MINUTE_MS, type MinuteBucket } from "./engine";
import { fetchManifest, type FetchJson, type Manifest } from "./manifest";

export const PRESETS = [
  { id: "1h", label: "1h", hours: 1 },
  { id: "3h", label: "3h", hours: 3 },
  { id: "6h", label: "6h", hours: 6 },
  { id: "12h", label: "12h", hours: 12 },
  { id: "24h", label: "24h", hours: 24 },
  { id: "3d", label: "3d", hours: 72 },
  { id: "7d", label: "7d", hours: 168 },
  { id: "30d", label: "30d", hours: 720 },
] as const;

/**
 * The presets `discoverPresetsWithManifest` probes. Multi-day ranges are
 * selectable but deliberately excluded: probing them would make a multi-day
 * manifest fetch the first request of every cold dashboard load, against the
 * bounded service-wide listing. They are served from summary bundles and are
 * never gated as unavailable.
 */
export const DISCOVERY_PRESETS = PRESETS.slice(0, 5) as readonly {
  id: PresetId;
  label: string;
  hours: number;
}[];

export type PresetId = (typeof PRESETS)[number]["id"];
export type Range = { from: string; to: string };
export type Interval = "minute" | "hour";

const HOUR_MS = 3_600_000;

export function presetRange(
  hours: number,
  anchorMs: number,
  options: { exactEnd?: boolean } = {},
): Range {
  const to = options.exactEnd
    ? anchorMs
    : Math.floor(anchorMs / MINUTE_MS) * MINUTE_MS;
  const from = to - hours * HOUR_MS;
  return { from: new Date(from).toISOString(), to: new Date(to).toISOString() };
}

/** Only the probed presets appear; an absent id means "not gated", not "unavailable". */
export type PresetAvailability = Partial<Record<PresetId, boolean>>;
export type PresetDiscovery = {
  availability: PresetAvailability;
  manifest: { range: Range; manifest: Manifest } | null;
};

/**
 * Lists the largest preset first; a complete listing makes every smaller preset
 * complete too, so one request chain normally decides all five and yields the
 * covering manifest for reuse by the dashboard worker.
 */
export async function discoverPresetsWithManifest(
  fetchJson: FetchJson,
  anchorMs: number,
  signal?: AbortSignal,
  options: { exactEnd?: boolean } = {},
): Promise<PresetDiscovery> {
  const available = Object.fromEntries(
    DISCOVERY_PRESETS.map((preset) => [preset.id, false]),
  ) as PresetAvailability;
  let discovered: { range: Range; manifest: Manifest } | null = null;
  for (let i = DISCOVERY_PRESETS.length - 1; i >= 0; i--) {
    const range = presetRange(DISCOVERY_PRESETS[i].hours, anchorMs, options);
    const manifest = await fetchManifest(
      fetchJson,
      range.from,
      range.to,
      signal,
      undefined,
      undefined,
      true,
    );
    if (manifest.complete) {
      for (let j = 0; j <= i; j++) available[DISCOVERY_PRESETS[j].id] = true;
      discovered = { range, manifest };
      break;
    }
  }
  return { availability: available, manifest: discovered };
}

export async function discoverPresets(
  fetchJson: FetchJson,
  anchorMs: number,
  signal?: AbortSignal,
): Promise<PresetAvailability> {
  const { availability } = await discoverPresetsWithManifest(
    fetchJson,
    anchorMs,
    signal,
  );
  return availability;
}

export function defaultInterval(range: Range): Interval {
  return Date.parse(range.to) - Date.parse(range.from) > 6 * HOUR_MS
    ? "hour"
    : "minute";
}

export type ChartPoint = {
  t: number;
  requests: number;
  bytes: number;
  s4xx: number;
  s5xx: number;
  /** Percent; null when no row in the bucket carried a cache state. */
  chr: number | null;
};

/** Every interval in the range gets a point so quiet intervals render as zero bars. */
export function buildSeries(
  minutes: MinuteBucket[],
  range: Range,
  interval: Interval,
): ChartPoint[] {
  const step = interval === "hour" ? HOUR_MS : MINUTE_MS;
  const start = Math.floor(Date.parse(range.from) / step) * step;
  const end = Date.parse(range.to);
  const merged = new Map<number, MinuteBucket>();
  for (const minute of minutes) {
    const t = Math.floor(minute.t / step) * step;
    const bucket = merged.get(t) ?? {
      t,
      requests: 0,
      bytes: 0,
      s4xx: 0,
      s5xx: 0,
      hits: 0,
      cacheRows: 0,
    };
    bucket.requests += minute.requests;
    bucket.bytes += minute.bytes;
    bucket.s4xx += minute.s4xx;
    bucket.s5xx += minute.s5xx;
    bucket.hits += minute.hits;
    bucket.cacheRows += minute.cacheRows;
    merged.set(t, bucket);
  }
  const points: ChartPoint[] = [];
  for (let t = start; t < end; t += step) {
    const bucket = merged.get(t);
    points.push({
      t,
      requests: bucket?.requests ?? 0,
      bytes: bucket?.bytes ?? 0,
      s4xx: bucket?.s4xx ?? 0,
      s5xx: bucket?.s5xx ?? 0,
      chr: bucket?.cacheRows ? (bucket.hits * 100) / bucket.cacheRows : null,
    });
  }
  return points;
}
