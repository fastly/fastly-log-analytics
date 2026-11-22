import { isCdnHit } from "../dashboard/cdn-cache";
import {
  dashboardCacheIdentity,
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
let activeUserRole: string | null = null;
let opsCacheEpoch = 0;
let volatileOpsScopeCounter = 0;
let volatileOpsScope = `volatile-ops-${++volatileOpsScopeCounter}`;
const activeOpsControllers = new Set<AbortController>();
const activeOpsWrites = new Set<Promise<unknown>>();

function opsAbortError(): DOMException {
  return new DOMException("The operation was aborted.", "AbortError");
}

/** The signed-in user whose session cookies the browser sends with ops reads. */
export const activeOpsUser = (): string | null => activeUserId;

export const currentOpsCacheEpoch = (): number => opsCacheEpoch;

export const currentVolatileOpsScope = (): string => volatileOpsScope;

export function assertOpsEpoch(
  entryOpsEpoch: number,
  signal?: AbortSignal,
): void {
  signal?.throwIfAborted();
  if (entryOpsEpoch !== opsCacheEpoch) {
    throw opsAbortError();
  }
}

export function currentOpsIdentity(
  explicit?: { user?: string; role?: string } | null,
): OpsIdentity {
  const dashId = dashboardCacheIdentity();
  let dashUser: string | null = null;
  let dashRole: string | null = null;
  if (dashId && dashId !== "anonymous") {
    const sep = dashId.lastIndexOf(":");
    if (sep > 0) {
      dashUser = dashId.slice(0, sep);
      dashRole = dashId.slice(sep + 1) || null;
    } else {
      dashUser = dashId;
    }
  }
  const rawExplicitUser =
    explicit?.user && explicit.user.trim().length > 0
      ? explicit.user.trim()
      : null;
  let explicitUser =
    rawExplicitUser && rawExplicitUser !== "anonymous"
      ? rawExplicitUser
      : null;
  let explicitRole =
    explicit?.role && explicit.role.trim().length > 0
      ? explicit.role.trim()
      : null;
  if (explicitUser) {
    const sep = explicitUser.lastIndexOf(":");
    if (sep > 0) {
      const parsedUser = explicitUser.slice(0, sep);
      const parsedRole = explicitUser.slice(sep + 1) || null;
      if (parsedUser === "anonymous") {
        explicitUser = null;
      } else {
        explicitUser = parsedUser;
        explicitRole ??= parsedRole;
      }
    }
  }
  const user = explicitUser ?? activeUserId ?? dashUser ?? volatileOpsScope;
  const role =
    explicitRole ??
    (activeUserId !== null &&
    (explicitUser === null || explicitUser === activeUserId)
      ? activeUserRole
      : null) ??
    (explicitUser !== null
      ? explicitUser === dashUser
        ? dashRole
        : null
      : activeUserId === null || activeUserId === dashUser
        ? dashRole
        : null) ??
    "admin";
  return { user, role };
}

/** Deletes ops caches: all of them, or one user's (optionally keeping one partition). */
export async function clearOpsCaches(
  options: { user?: string; keep?: string } = {},
): Promise<void> {
  if (!options.user) {
    activeUserId = null;
    activeUserRole = null;
  }
  opsCacheEpoch += 1;
  volatileOpsScope = `volatile-ops-${++volatileOpsScopeCounter}`;
  for (const controller of activeOpsControllers) {
    controller.abort(opsAbortError());
  }
  activeOpsControllers.clear();
  const pendingWrites = [...activeOpsWrites];
  if (pendingWrites.length > 0) {
    await Promise.allSettled(pendingWrites);
  }
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

function dashboardRoleForUser(userId: string): string | null {
  const dashId = dashboardCacheIdentity();
  if (!dashId || dashId === "anonymous") return null;
  const sep = dashId.lastIndexOf(":");
  if (sep <= 0) return null;
  return dashId.slice(0, sep) === userId ? dashId.slice(sep + 1) || null : null;
}

export async function syncOpsCacheForSession(session: {
  userId?: string | null;
  role?: string | null;
  maskIps?: boolean;
  needsLogin?: boolean;
}): Promise<void> {
  const nextUser = session.needsLogin ? null : (session.userId ?? null);
  const rawRole = session.needsLogin
    ? null
    : session.role && session.role.trim().length > 0
      ? session.role.trim()
      : null;
  const baseRole =
    nextUser === null
      ? null
      : ((rawRole === "analyst_masked" ? "analyst-no-pii" : rawRole) ??
        dashboardRoleForUser(nextUser));
  const nextRole =
    nextUser === null
      ? null
      : session.maskIps
        ? !baseRole || baseRole === "analyst" || baseRole === "analyst-no-pii"
          ? "analyst-no-pii"
          : baseRole.endsWith("-no-pii")
            ? baseRole
            : `${baseRole}-no-pii`
        : baseRole;
  const effectivePrevRole =
    activeUserId !== null
      ? (activeUserRole ?? dashboardRoleForUser(activeUserId) ?? "admin")
      : null;
  const effectiveNextRole = nextUser !== null ? (nextRole ?? "admin") : null;
  const userChanged = activeUserId !== null && activeUserId !== nextUser;
  const roleChanged =
    activeUserId !== null &&
    nextUser !== null &&
    effectivePrevRole !== effectiveNextRole;
  const initialMismatch =
    activeUserId === null &&
    nextUser !== null &&
    dashboardRoleForUser(nextUser) !== effectiveNextRole;
  if (userChanged || roleChanged || initialMismatch || session.needsLogin) {
    await clearOpsCaches();
  }
  activeUserId = nextUser;
  activeUserRole = effectiveNextRole;
}

export function browserOpsSource(
  baseUrl: string,
  identityOrPageLoad?: OpsIdentity | (() => string | null),
  options?: BrowserSourceOptions,
): OpsSource {
  const explicitIdentity =
    typeof identityOrPageLoad === "object" && identityOrPageLoad !== null
      ? identityOrPageLoad
      : null;
  const identity = currentOpsIdentity(explicitIdentity);
  const resolvedOptions: BrowserSourceOptions | undefined =
    typeof identityOrPageLoad === "function"
      ? {
          ...options,
          getPageLoadId: () => identityOrPageLoad() ?? "",
        }
      : options;
  const name = opsCacheName(identity.user, identity.role);
  const open = (callEpoch: number, aborted = false): Promise<Cache | null> =>
    callEpoch === opsCacheEpoch && !aborted
      ? openCache(name)
      : Promise.resolve(null);
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
  const withOpsController = async <T>(
    callerSignal: AbortSignal | undefined,
    work: (callEpoch: number, controller: AbortController) => Promise<T>,
  ): Promise<T> => {
    callerSignal?.throwIfAborted();
    const callEpoch = opsCacheEpoch;
    const controller = new AbortController();
    const onCallerAbort = () =>
      controller.abort(callerSignal?.reason ?? opsAbortError());
    if (callerSignal) {
      callerSignal.addEventListener("abort", onCallerAbort, { once: true });
    }
    activeOpsControllers.add(controller);
    try {
      return await work(callEpoch, controller);
    } finally {
      callerSignal?.removeEventListener("abort", onCallerAbort);
      activeOpsControllers.delete(controller);
    }
  };
  const assertOpsActive = (
    callEpoch: number,
    controller: AbortController,
  ): void => {
    if (controller.signal.aborted) {
      throw controller.signal.reason ?? opsAbortError();
    }
    if (callEpoch !== opsCacheEpoch) {
      throw opsAbortError();
    }
  };
  return {
    async fetchJson(url, signal) {
      return withOpsController(signal, async (callEpoch, controller) => {
        const headers = buildRequestHeaders();
        const pageLoadId = resolvedOptions?.getPageLoadId?.();
        const start =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        const response = await fetch(baseUrl + url, {
          credentials: "same-origin",
          ...(headers ? { headers } : {}),
          signal: controller.signal,
        });
        assertOpsActive(callEpoch, controller);
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
        const parsed = await response.json();
        assertOpsActive(callEpoch, controller);
        return parsed;
      });
    },
    async fetchFile(key, signal) {
      return withOpsController(signal, async (callEpoch, controller) => {
        const path = fileDisplayPath(key);
        const pageLoadId = resolvedOptions?.getPageLoadId?.();
        const url = fileUrl(key);
        const cacheStart =
          typeof performance !== "undefined" ? performance.now() : Date.now();
        const cache = await open(callEpoch, controller.signal.aborted);
        assertOpsActive(callEpoch, controller);
        const hit =
          callEpoch === opsCacheEpoch && !controller.signal.aborted
            ? await cache?.match(url)
            : undefined;
        assertOpsActive(callEpoch, controller);
        if (hit) {
          const bytes = await hit.arrayBuffer();
          assertOpsActive(callEpoch, controller);
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
        let onAbortRace!: () => void;
        const abortRace = new Promise<never>((_, reject) => {
          onAbortRace = () =>
            reject(controller.signal.reason ?? opsAbortError());
          if (controller.signal.aborted) {
            onAbortRace();
            return;
          }
          controller.signal.addEventListener("abort", onAbortRace, {
            once: true,
          });
        });
        let response: Response;
        try {
          response = await Promise.race([
            fetch(url, {
              credentials: "same-origin",
              cache: "no-store",
              ...(headers ? { headers } : {}),
              signal: controller.signal,
            }),
            abortRace,
          ]);
        } finally {
          controller.signal.removeEventListener("abort", onAbortRace);
        }
        assertOpsActive(callEpoch, controller);
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
        if (cache && callEpoch === opsCacheEpoch && !controller.signal.aborted) {
          const writePromise = cache.put(url, response.clone()).catch(() => {});
          activeOpsWrites.add(writePromise);
          writePromise.finally(() => activeOpsWrites.delete(writePromise));
          await writePromise;
        }
        assertOpsActive(callEpoch, controller);
        const bytes = await response.arrayBuffer();
        assertOpsActive(callEpoch, controller);
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
      });
    },
    async cachedKeys() {
      const callEpoch = opsCacheEpoch;
      const cache = await open(callEpoch);
      if (!cache || callEpoch !== opsCacheEpoch) return [];
      return (await cache.keys()).map((request) =>
        keyFromFileUrl(request.url)!,
      );
    },
    async evict(key) {
      const callEpoch = opsCacheEpoch;
      await (await open(callEpoch))?.delete(fileUrl(key));
    },
  };
}
