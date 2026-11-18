const CACHE_PREFIX = "flp-duckdb-module-";

/**
 * The DuckDB module as a URL its worker can instantiate from: a `blob:` URL of
 * the copy kept in Cache Storage, downloaded once per DuckDB version. The
 * module is an 8 MB gzip response, and a private window's in-memory HTTP cache
 * keeps no entry over an eighth of its 50 MB, so each new tab would otherwise
 * download it again; Cache Storage has no per-entry cap. Without Cache Storage,
 * or when it fails, the module URL itself. `release` frees the blob once the
 * module is instantiated.
 */
export async function cachedDuckDBModule(url: string, version: string): Promise<{ url: string; release(): void }> {
  const direct = { url, release() {} };
  if (typeof caches === "undefined" || typeof URL.createObjectURL !== "function") return direct;
  const name = `${CACHE_PREFIX}${version}`;
  let cache: Cache;
  let hit: Response | undefined;
  try {
    cache = await caches.open(name);
    hit = await cache.match(url);
  } catch {
    // Storage is unavailable (blocked or private-browsing failure): the worker downloads it as before.
    return direct;
  }
  let wasm: Blob;
  if (hit) {
    wasm = await hit.blob();
  } else {
    const response = await fetch(url, { credentials: "same-origin" });
    if (!response.ok) throw new Error(`DuckDB module download failed (${response.status})`);
    wasm = await response.blob();
    try {
      await cache.put(url, new Response(wasm, { headers: { "content-type": "application/wasm" } }));
      for (const key of await caches.keys()) {
        if (key.startsWith(CACHE_PREFIX) && key !== name) await caches.delete(key);
      }
    } catch {
      // Over quota: this tab still instantiates from the download it has.
    }
  }
  // Streaming instantiation requires the wasm content type.
  const objectUrl = URL.createObjectURL(new Blob([wasm], { type: "application/wasm" }));
  return { url: objectUrl, release: () => URL.revokeObjectURL(objectUrl) };
}
