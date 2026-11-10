import { isCdnHit } from "../dashboard/cdn-cache";
import {
  deleteCaches,
  emitDebugSubCalls,
  fileDisplayPath,
  fileRequestPath,
  keyFromFileUrl,
  openCache,
  type BrowserSourceOptions,
} from "../dashboard/sources";
import type { OpsSource } from "./engine";

export const OPS_CACHE_PREFIX = "flp-ops-parquet-v1:";

export const opsCacheName = (user: string, role: string) =>
  `${OPS_CACHE_PREFIX}${user}:${role}`;

export type OpsIdentity = { user: string; role: string };

let activeUserId: string | null = null;

/** The signed-in user whose session cookies the browser sends with ops reads. */
export const activeOpsUser = (): string | null => activeUserId;

/** Deletes ops caches: all of them, or one user's (optionally keeping one partition). */
export async function clearOpsCaches(
  options: { user?: string; keep?: string } = {},
): Promise<void> {
  if (typeof caches === "undefined") return;
  const prefix = options.user
    ? `${OPS_CACHE_PREFIX}${options.user}:`
    : OPS_CACHE_PREFIX;
  if (!options.keep) {
    await deleteCaches((name) => name.startsWith(prefix));
    return;
  }
  try {
    for (const name of await caches.keys()) {
      if (name.startsWith(prefix) && name !== options.keep) {
        await caches.delete(name);
      }
    }
  } catch {
    // Cache cleanup is best-effort.
  }
}

export async function syncOpsCacheForSession(session: {
  userId?: string | null;
  needsLogin?: boolean;
}): Promise<void> {
  const nextUser = session.needsLogin ? null : (session.userId ?? null);
  if (activeUserId !== null && activeUserId !== nextUser) {
    await clearOpsCaches();
  } else if (session.needsLogin) {
    await clearOpsCaches();
  }
  activeUserId = nextUser;
}

export function browserOpsSource(
  baseUrl: string,
  identityOrPageLoad?: OpsIdentity | (() => string | null),
  options?: BrowserSourceOptions,
): OpsSource {
  const identity: OpsIdentity =
    typeof identityOrPageLoad === "object" && identityOrPageLoad !== null
      ? identityOrPageLoad
      : { user: activeUserId ?? "default", role: "admin" };
  const resolvedOptions: BrowserSourceOptions | undefined =
    typeof identityOrPageLoad === "function"
      ? {
          ...options,
          getPageLoadId: () => identityOrPageLoad() ?? "",
        }
      : options;
  const name = opsCacheName(identity.user, identity.role);
  const open = (): Promise<Cache | null> => openCache(name);
  const fileUrl = (key: string) => `${baseUrl}${fileRequestPath(key)}`;
  const denied = async (response: Response) => {
    if (response.status === 401 || response.status === 403) {
      await clearOpsCaches({ user: identity.user });
    }
  };
  const buildRequestHeaders = () => {
    const headers: Record<string, string> = {};
    const pageLoadId = resolvedOptions?.getPageLoadId?.();
    if (pageLoadId) headers["X-Page-Load-ID"] = pageLoadId;
    if (resolvedOptions?.debugResponses?.()) headers["X-Debug-Responses"] = "1";
    return Object.keys(headers).length > 0 ? headers : undefined;
  };
  return {
    async fetchJson(url, signal) {
      const headers = buildRequestHeaders();
      const pageLoadId = resolvedOptions?.getPageLoadId?.();
      const start =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      const response = await fetch(baseUrl + url, {
        credentials: "same-origin",
        ...(headers ? { headers } : {}),
        signal,
      });
      const end =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      const endpoint = `GET ${url.split("?")[0]}`;
      const vcpuHeader = response?.headers?.get?.("x-flp-vcpu-ms");
      const vcpuMs =
        vcpuHeader != null && vcpuHeader !== "" ? Number(vcpuHeader) : null;
      resolvedOptions?.onApiCall?.({
        source: "Compute API",
        method: "GET",
        path: url,
        status: response?.status ?? null,
        duration_ms: Math.max(0, Math.round(end - start)),
        vcpu_ms: Number.isFinite(vcpuMs) ? vcpuMs : null,
        endpoint,
        page_load_id: pageLoadId,
      });
      emitDebugSubCalls(
        response,
        endpoint,
        pageLoadId,
        resolvedOptions?.onApiCall,
      );
      if (!response.ok) {
        await denied(response);
        const body = await response.json().catch(() => null);
        const detail = typeof body?.detail === "string" ? body.detail : null;
        throw new Error(
          detail
            ? `${detail} (${response.status})`
            : `Request failed (${response.status})`,
        );
      }
      return response.json();
    },
    async fetchFile(key, signal) {
      const path = fileDisplayPath(key);
      const pageLoadId = resolvedOptions?.getPageLoadId?.();
      const url = fileUrl(key);
      const cacheStart =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      const cache = await open();
      const hit = await cache?.match(url);
      if (hit) {
        const bytes = await hit.arrayBuffer();
        const cacheEnd =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        resolvedOptions?.onApiCall?.({
          source: "Browser Cache",
          method: "GET",
          path,
          status: 200,
          duration_ms: Math.max(0, Math.round(cacheEnd - cacheStart)),
          bytes: bytes.byteLength,
          cache: "browser",
          endpoint: "GET /api/file",
          page_load_id: pageLoadId,
        });
        return { bytes, cached: true };
      }
      const headers = buildRequestHeaders();
      const start =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      const response = await fetch(url, {
        credentials: "same-origin",
        cache: "no-store",
        ...(headers ? { headers } : {}),
        signal,
      });
      if (!response.ok) {
        const end =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        resolvedOptions?.onApiCall?.({
          source: "Compute API",
          method: "GET",
          path,
          status: response.status,
          duration_ms: Math.max(0, Math.round(end - start)),
          endpoint: "GET /api/file",
          page_load_id: pageLoadId,
        });
        await denied(response);
        throw new Error(`File ${key} failed (${response.status})`);
      }
      if (cache) await cache.put(url, response.clone()).catch(() => {});
      const bytes = await response.arrayBuffer();
      const end =
        typeof performance !== "undefined" ? performance.now() : Date.now();
      const cdn = response.headers.get("x-flp-cdn-cache");
      const cdnHit = isCdnHit(cdn);
      const vcpuHeader = response?.headers?.get?.("x-flp-vcpu-ms");
      const vcpuMs =
        vcpuHeader != null && vcpuHeader !== "" ? Number(vcpuHeader) : null;
      const directCdnMiss = cdn !== null && vcpuMs === null;
      resolvedOptions?.onApiCall?.({
        source: cdnHit
          ? "CDN Cache"
          : directCdnMiss
            ? "FOS S3"
            : "Compute API",
        method: "GET",
        path,
        status: response.status,
        duration_ms: Math.max(0, Math.round(end - start)),
        vcpu_ms: Number.isFinite(vcpuMs) ? vcpuMs : null,
        bytes: bytes.byteLength,
        cache: cdnHit ? "cdn" : "miss",
        endpoint: "GET /api/file",
        page_load_id: pageLoadId,
      });
      emitDebugSubCalls(
        response,
        "GET /api/file",
        pageLoadId,
        resolvedOptions?.onApiCall,
      );
      return {
        bytes,
        cached: false,
        cdn,
      };
    },
    async cachedKeys() {
      const cache = await open();
      if (!cache) return [];
      return (await cache.keys()).map((request) =>
        keyFromFileUrl(request.url)!,
      );
    },
    async evict(key) {
      await (await open())?.delete(fileUrl(key));
    },
  };
}
