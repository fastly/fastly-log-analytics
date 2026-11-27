export const DUCKDB_WORKER_PATCH_MARKER = "/* flp-wasm-stream-fallback */";

const STREAMING_WITH_PROGRESS =
  "WebAssembly.instantiateStreaming(a,e).then(l=>{t(l.instance,l.module)})";
const STREAMING_WITHOUT_PROGRESS =
  "WebAssembly.instantiateStreaming(fetch(n),e).then(a=>{t(a.instance,a.module)})";

const FALLBACK_CHAIN =
  `${DUCKDB_WORKER_PATCH_MARKER}.catch(()=>fetch(this.mainModuleURL,{credentials:"same-origin",cache:"no-store"})` +
  '.then(w=>{if(!w.ok)throw new Error("DuckDB Wasm HTTP "+w.status);return w.arrayBuffer()})' +
  ".then(w=>WebAssembly.instantiate(w,e))" +
  ".then(w=>{t(w.instance,w.module)}))" +
  ".catch(w=>{setTimeout(()=>{throw w},0)})";

/**
 * Adds a cache-bypass non-streaming Wasm fallback and error surfacing around
 * DuckDB-Wasm's bare `WebAssembly.instantiateStreaming(...).then(...)` calls so
 * Chromium disk-cache write failures recover instead of hanging initialization.
 */
export function patchDuckDBBrowserWorker(source) {
  if (source.includes(DUCKDB_WORKER_PATCH_MARKER)) {
    return source;
  }
  if (
    !source.includes(STREAMING_WITH_PROGRESS) ||
    !source.includes(STREAMING_WITHOUT_PROGRESS)
  ) {
    throw new Error(
      "Expected WebAssembly.instantiateStreaming call sites were not found in duckdb-browser-eh.worker.js",
    );
  }
  return source
    .replace(STREAMING_WITH_PROGRESS, `${STREAMING_WITH_PROGRESS}${FALLBACK_CHAIN}`)
    .replace(
      STREAMING_WITHOUT_PROGRESS,
      `${STREAMING_WITHOUT_PROGRESS}${FALLBACK_CHAIN}`,
    );
}
