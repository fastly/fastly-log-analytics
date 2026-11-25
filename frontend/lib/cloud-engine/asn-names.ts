import { resolveCountryName } from "@/lib/format";

const ASN_NAMES_URL = "/geo/asn-names.tsv";
const EMPTY_ASN_MAP: ReadonlyMap<number, string> = new Map();

let cachedAsnMap: Map<number, string> | null = null;
let inflightLoad: Promise<ReadonlyMap<number, string>> | null = null;

function parseAsnTsv(text: string): Map<number, string> {
  const map = new Map<number, string>();
  let start = 0;
  const len = text.length;
  while (start < len) {
    let end = text.indexOf("\n", start);
    if (end === -1) end = len;
    const tab = text.indexOf("\t", start);
    if (tab > start && tab < end) {
      const asn = Number(text.slice(start, tab).trim());
      if (Number.isInteger(asn) && asn > 0) {
        const name = normalizeAsnOrgName(text.slice(tab + 1, end));
        if (name) map.set(asn, name);
      }
    }
    start = end + 1;
  }
  return map;
}

function canFetchAsnAsset(): boolean {
  if (typeof fetch !== "function") return false;
  if (typeof window !== "undefined") return true;
  // Module Web Workers (dashboard/engine.worker.ts) have `self.location` without `window`.
  const loc = (globalThis as { location?: { protocol?: string } }).location;
  return loc?.protocol === "http:" || loc?.protocol === "https:";
}

/**
 * Lazily loads the VCL-cached `/geo/asn-names.tsv` table once per JS realm
 * (main thread or engine Web Worker). Fails open to an empty map if unavailable.
 */
export function loadAsnNames(): Promise<ReadonlyMap<number, string>> {
  if (cachedAsnMap) return Promise.resolve(cachedAsnMap);
  if (!canFetchAsnAsset()) return Promise.resolve(EMPTY_ASN_MAP);
  if (inflightLoad) return inflightLoad;

  inflightLoad = (async () => {
    try {
      const res = await fetch(ASN_NAMES_URL, { credentials: "same-origin" });
      if (!res.ok) return EMPTY_ASN_MAP;
      const text = await res.text();
      const parsed = parseAsnTsv(text);
      if (parsed.size > 0) {
        cachedAsnMap = parsed;
        return cachedAsnMap;
      }
      return EMPTY_ASN_MAP;
    } catch {
      return EMPTY_ASN_MAP;
    } finally {
      inflightLoad = null;
    }
  })();

  return inflightLoad;
}

export function normalizeAsnNumber(asn: string | number | null | undefined): number | null {
  if (asn == null || typeof asn === "boolean") return null;
  const clean = String(asn).replace(/^AS/i, "").trim();
  if (!/^\d+$/.test(clean)) return null;
  const num = Number(clean);
  return Number.isSafeInteger(num) && num > 0 ? num : null;
}

export function getCachedAsnName(asn: string | number | null | undefined): string | null {
  if (!cachedAsnMap) return null;
  const num = normalizeAsnNumber(asn);
  if (num === null) return null;
  return cachedAsnMap.get(num) ?? null;
}

function isBareAsnString(value: string): boolean {
  return /^AS\d+$/i.test(value.trim());
}

/**
 * Normalizes RIR handles out of CAIDA/Cymru organization names (e.g. `"NFORCE NForce..."`,
 * `"TDDE-ASN1 Telefonica..."`, `"AS12876 Scaleway..."`, `"OVH OVH SAS, FR"`, `"HANDLE - Org"`)
 * while preserving legitimate uppercase organization names such as `"TDS TELECOM, US"`.
 */
export function normalizeAsnOrgName(name: string | null | undefined): string {
  let clean = (name ?? "").trim();
  if (!clean) return "";
  const dashIdx = clean.indexOf(" - ");
  if (dashIdx > 0 && /^[A-Z0-9][A-Z0-9_-]*$/.test(clean.slice(0, dashIdx).trim())) {
    clean = clean.slice(dashIdx + 3).trim();
  }
  const spaceIdx = clean.indexOf(" ");
  if (spaceIdx > 0) {
    const token = clean.slice(0, spaceIdx);
    const rest = clean.slice(spaceIdx + 1).trim();
    const restSansCountry = rest.replace(/,\s*[A-Z]{2}$/, "").trim();
    if (
      rest &&
      restSansCountry &&
      /^[A-Z0-9][A-Z0-9_-]*$/.test(token) &&
      !isBareAsnString(rest)
    ) {
      const isHandleToken =
        /^AS\d+$/.test(token) ||
        /[-_\d]/.test(token) ||
        rest.toLowerCase().startsWith(token.toLowerCase());
      if (isHandleToken) {
        return rest;
      }
    }
  }
  return clean;
}

/**
 * Formats an ASN label to match High-Scale's `format_asn_label(asn, name)`:
 * `"<Org Name> (<asn>)"` when a name is known, otherwise `"AS<asn>"`.
 */
export function formatAsnDisplayLabel(
  asn: string | number | null | undefined,
  loggedName?: string | null,
): string {
  const num = normalizeAsnNumber(asn);
  if (num === null) {
    const raw = String(asn ?? "").trim();
    return raw || "Unknown ASN";
  }
  const trimmedLogged = typeof loggedName === "string" ? loggedName.trim() : "";
  const rawResolved =
    trimmedLogged && !isBareAsnString(trimmedLogged)
      ? trimmedLogged
      : (cachedAsnMap?.get(num) ?? "");
  const resolved = normalizeAsnOrgName(rawResolved);
  if (!resolved || isBareAsnString(resolved)) {
    return `AS${num}`;
  }
  const suffix = `(${num})`;
  return resolved.endsWith(suffix) ? resolved : `${resolved} ${suffix}`;
}

function titleCaseCity(city: string): string {
  return city
    .trim()
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

/**
 * Formats a city/region/country label to match High-Scale's `format_city_label(city, country, region)`:
 * e.g. `"Council Bluffs, IA, United States"`, `"Denver, United States"`, or `"Brazil"`.
 */
export function formatCityCountryLabel(
  city: string | null | undefined,
  country: string | null | undefined,
  region?: string | null,
): string {
  const parts: string[] = [];
  const cleanCity = (city ?? "").trim();
  const cleanRegion = (region ?? "").trim();
  const cleanCountry = (country ?? "").trim();

  if (cleanCity && cleanCity.toLowerCase() !== "unknown") {
    parts.push(cleanCity === cleanCity.toLowerCase() ? titleCaseCity(cleanCity) : cleanCity);
  }
  if (cleanRegion && cleanRegion.toLowerCase() !== "unknown") {
    parts.push(cleanRegion.toUpperCase());
  }
  if (cleanCountry && cleanCountry.toLowerCase() !== "unknown") {
    const upper = cleanCountry.toUpperCase();
    parts.push(upper.length === 2 ? resolveCountryName(upper) : cleanCountry);
  }
  return parts.length > 0 ? parts.join(", ") : "Unknown";
}

export function setAsnNamesForTest(entries: Iterable<readonly [number, string]> | null): void {
  inflightLoad = null;
  cachedAsnMap = entries ? new Map(entries) : null;
}

export function resetAsnNamesForTest(): void {
  inflightLoad = null;
  cachedAsnMap = null;
}
