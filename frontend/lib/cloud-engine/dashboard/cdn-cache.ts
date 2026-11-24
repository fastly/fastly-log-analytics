/**
 * Every Fastly cache hit state starts with `HIT` (`HIT`, `HIT-STALE`, `HIT-CLUSTER`,
 * `HIT-SYNTH`, etc.). Counting only exact `"HIT"` undercounts stale-while-revalidate
 * and cluster hits on `/fos/*` and `/api/file` responses.
 */
export function isCdnHit(cdn: string | null | undefined): boolean {
  if (!cdn) return false;
  return cdn
    .split(",")
    .some((part) => part.trim().toUpperCase().startsWith("HIT"));
}

/**
 * Checks whether a string or `Headers` / header map represents a CDN cache hit,
 * inspecting `"x-flp-cdn-cache"`, `"x-cache"`, and `"x-cache-hits"`.
 */
export function isCdnCacheHit(
  input:
    | string
    | null
    | undefined
    | Headers
    | { get?: (name: string) => string | null | undefined }
    | Record<string, string | null | undefined>,
): boolean {
  if (!input) return false;
  if (typeof input === "string") return isCdnHit(input);
  if (typeof (input as Headers).get === "function") {
    const h = input as { get: (name: string) => string | null | undefined };
    const cdn = h.get("x-flp-cdn-cache") ?? h.get("x-cache");
    if (isCdnHit(cdn)) return true;
    const hits = h.get("x-cache-hits");
    return Boolean(
      hits &&
        hits
          .split(",")
          .some((part) => Number(part.trim()) > 0),
    );
  }
  const rec = input as Record<string, string | null | undefined>;
  const cdn =
    rec["x-flp-cdn-cache"] ??
    rec["X-FLP-CDN-Cache"] ??
    rec["x-cache"] ??
    rec["X-Cache"];
  if (isCdnHit(cdn)) return true;
  const hits = rec["x-cache-hits"] ?? rec["X-Cache-Hits"];
  return Boolean(
    hits &&
      hits
        .split(",")
        .some((part) => Number(part.trim()) > 0),
  );
}
