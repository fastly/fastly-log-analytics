import type { IncomingMessage } from "node:http";
import { DEBUG_RESPONSES_COOKIE } from "@/lib/utils";

// ── Server-Side Upstream Proxy & SSR Transport ────────────────────────────────

const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailers",
  "transfer-encoding",
  "upgrade",
]);
const IDEMPOTENT_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);
const SSR_TIMEOUT_MS = 5000;

function getNodeRequestFn(protocol: string) {
  const mod = process.getBuiltinModule(protocol === "https:" ? "node:https" : "node:http") as
    | typeof import("node:http")
    | undefined;
  if (!mod?.request) throw new Error("Node HTTP module unavailable in browser runtime");
  return mod.request;
}

function apiProxyTarget(): string {
  return (process.env.API_PROXY_URL || process.env.NEXT_PUBLIC_API_PROXY_URL || "http://caddy:80").replace(/\/+$/, "");
}

function isBenignDisconnectError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as NodeJS.ErrnoException).code;
  const message = (err as Error).message || "";
  return (
    code === "ECONNRESET" ||
    code === "EPIPE" ||
    code === "ERR_STREAM_PREMATURE_CLOSE" ||
    code === "ABORT_ERR" ||
    message.includes("aborted") ||
    message.includes("socket hang up")
  );
}

function buildUpstreamHeaders(request: Request, targetUrl: URL, bodyBuffer?: Buffer): Record<string, string> {
  const out: Record<string, string> = {};
  request.headers.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (!HOP_BY_HOP_HEADERS.has(lower) && lower !== "host" && lower !== "content-length") out[key] = value;
  });
  out["host"] = targetUrl.host;
  out["connection"] = "close";
  if (bodyBuffer && bodyBuffer.byteLength > 0) out["content-length"] = String(bodyBuffer.byteLength);
  return out;
}

function sendUpstreamOnce(
  targetUrl: URL,
  method: string,
  headers: Record<string, string>,
  bodyBuffer: Buffer | undefined,
  signal: AbortSignal | undefined,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      resolve(new Response(null, { status: 499 }));
      return;
    }
    const lib = getNodeRequestFn(targetUrl.protocol);
    let settledHeaders = false;
    const upstreamReq = lib(
      {
        hostname: targetUrl.hostname,
        port: targetUrl.port || (targetUrl.protocol === "https:" ? 443 : 80),
        path: `${targetUrl.pathname}${targetUrl.search}`,
        method,
        headers,
        agent: false,
      },
      (upstreamRes: IncomingMessage) => {
        settledHeaders = true;
        const status = upstreamRes.statusCode || 502;
        const resHeaders = new Headers();
        for (const [rawKey, rawVal] of Object.entries(upstreamRes.headers)) {
          if (rawVal === undefined) continue;
          const lower = rawKey.toLowerCase();
          if (HOP_BY_HOP_HEADERS.has(lower)) continue;
          if (lower === "set-cookie") {
            for (const c of Array.isArray(rawVal) ? rawVal : [String(rawVal)]) resHeaders.append("set-cookie", c);
            continue;
          }
          resHeaders.set(rawKey, Array.isArray(rawVal) ? rawVal.join(", ") : String(rawVal));
        }
        if (method === "HEAD" || status === 204 || status === 205 || status === 304) {
          upstreamRes.resume();
          resolve(new Response(null, { status, statusText: upstreamRes.statusMessage, headers: resHeaders }));
          return;
        }
        let streamClosed = false;
        const closeCleanly = (controller: ReadableStreamDefaultController<Uint8Array>) => {
          if (streamClosed) return;
          streamClosed = true;
          try {
            controller.close();
          } catch {
            /* already closed */
          }
        };
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            upstreamRes.on("data", (chunk: Buffer) => {
              if (streamClosed) return;
              try {
                controller.enqueue(new Uint8Array(chunk));
              } catch {
                streamClosed = true;
                upstreamReq.destroy();
              }
            });
            upstreamRes.on("end", () => closeCleanly(controller));
            upstreamRes.on("close", () => closeCleanly(controller));
            upstreamRes.on("error", (err) => {
              if (isBenignDisconnectError(err) || signal?.aborted) {
                closeCleanly(controller);
                return;
              }
              if (!streamClosed) {
                streamClosed = true;
                try {
                  controller.error(err);
                } catch {
                  /* ignore */
                }
              }
            });
          },
          cancel() {
            streamClosed = true;
            upstreamRes.destroy();
            upstreamReq.destroy();
          },
        });
        resolve(new Response(stream, { status, statusText: upstreamRes.statusMessage, headers: resHeaders }));
      },
    );
    const onAbort = () => {
      upstreamReq.destroy();
      if (!settledHeaders) {
        settledHeaders = true;
        resolve(new Response(null, { status: 499 }));
      }
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    upstreamReq.on("close", () => signal?.removeEventListener("abort", onAbort));
    upstreamReq.on("error", (err) => {
      if (settledHeaders) return;
      settledHeaders = true;
      if (signal?.aborted) {
        resolve(new Response(null, { status: 499 }));
        return;
      }
      reject(err);
    });
    if (bodyBuffer && bodyBuffer.byteLength > 0) upstreamReq.write(bodyBuffer);
    upstreamReq.end();
  });
}

export async function proxyUpstreamRequest(request: Request): Promise<Response> {
  const reqUrl = new URL(request.url);
  const targetUrl = new URL(`${apiProxyTarget()}${reqUrl.pathname}${reqUrl.search}`);
  const method = (request.method || "GET").toUpperCase();
  let bodyBuffer: Buffer | undefined;
  if (method !== "GET" && method !== "HEAD") {
    const ab = await request.arrayBuffer();
    if (ab.byteLength > 0) bodyBuffer = Buffer.from(ab);
  }
  const headers = buildUpstreamHeaders(request, targetUrl, bodyBuffer);
  try {
    return await sendUpstreamOnce(targetUrl, method, headers, bodyBuffer, request.signal);
  } catch (firstErr) {
    const code = (firstErr as NodeJS.ErrnoException | undefined)?.code;
    if (IDEMPOTENT_METHODS.has(method) && !request.signal?.aborted && (code === "ECONNRESET" || code === "EPIPE")) {
      try {
        return await sendUpstreamOnce(targetUrl, method, headers, bodyBuffer, request.signal);
      } catch (retryErr) {
        if (isBenignDisconnectError(retryErr) && request.signal?.aborted) return new Response(null, { status: 499 });
        return Response.json({ detail: "Upstream service unavailable" }, { status: 502 });
      }
    }
    if (isBenignDisconnectError(firstErr) && request.signal?.aborted) return new Response(null, { status: 499 });
    return Response.json({ detail: "Upstream service unavailable" }, { status: 502 });
  }
}

function isLoopbackHost(host: string): boolean {
  const bare = host.startsWith("[") ? host.slice(1, host.indexOf("]")) : host.split(":", 1)[0];
  const lc = bare.toLowerCase();
  return lc === "localhost" || lc === "127.0.0.1" || lc === "::1";
}

function timingSafeStringEqual(a: string, b: string): boolean {
  const enc = new TextEncoder();
  const ba = enc.encode(a);
  const bb = enc.encode(b);
  if (ba.byteLength !== bb.byteLength) return false;
  let diff = 0;
  for (let i = 0; i < ba.byteLength; i++) diff |= ba[i] ^ bb[i];
  return diff === 0;
}

export function classifySsrRequest(headerBag: Pick<Headers, "get">): {
  admin: boolean;
  gatewayAuthenticated: boolean;
  refusal: string | null;
} {
  const proxiedByCaddy = headerBag.get("x-proxied-by-caddy");
  const inboundHost = headerBag.get("host");
  const gatewayToken = headerBag.get("x-admin-gateway-token");
  const gatewaySecret = process.env.ADMIN_GATEWAY_SECRET || "";
  const gatewayAuthenticated =
    !proxiedByCaddy && gatewaySecret.length >= 32 && timingSafeStringEqual(gatewayToken || "", gatewaySecret);
  let refusal: string | null = null;
  if (gatewayToken !== null && !gatewayAuthenticated) {
    refusal = "invalid admin gateway credential";
  } else if (gatewayAuthenticated && !inboundHost) {
    refusal = "admin gateway request without Host";
  } else if (
    gatewayAuthenticated &&
    (headerBag.get("sec-fetch-site") === "cross-site" ||
      (headerBag.get("origin") !== null &&
        headerBag.get("origin")?.toLowerCase() !== `https://${inboundHost?.toLowerCase()}`))
  ) {
    refusal = "cross-origin admin gateway request";
  } else if (process.env.ADMIN_GATEWAY_REQUIRED === "1" && !gatewayAuthenticated && !proxiedByCaddy) {
    refusal = "authenticated admin gateway required";
  } else if (!gatewayAuthenticated && !proxiedByCaddy && (!inboundHost || !isLoopbackHost(inboundHost))) {
    refusal = `no X-Proxied-By-Caddy marker and non-loopback/absent Host=${inboundHost ?? "<none>"}`;
  }
  return { admin: !refusal && !proxiedByCaddy, gatewayAuthenticated, refusal };
}

export interface SSRUpstreamResponse {
  statusCode: number;
  body: string;
}

export function parseSsrJson<T = unknown>(resp: SSRUpstreamResponse | null, logPrefix: string): T | null {
  if (!resp) return null;
  if (resp.statusCode < 200 || resp.statusCode >= 300) {
    console.warn(`[${logPrefix}] upstream returned ${resp.statusCode}; falling back to client fetch`);
    return null;
  }
  try {
    return JSON.parse(resp.body) as T;
  } catch {
    console.warn(`[${logPrefix}] malformed upstream body; falling back to client fetch`);
    return null;
  }
}

export interface SSRGetOpts {
  path: string;
  logPrefix: string;
  extraHeaders?: Record<string, string>;
  method?: "GET" | "POST";
  body?: unknown;
  timeoutMs?: number;
}

type SsrHeadersProvider = {
  cookies: () =>
    | Promise<{ toString(): string; get(name: string): { value: string } | undefined }>
    | { toString(): string; get(name: string): { value: string } | undefined };
  headers: () => Promise<Pick<Headers, "get">> | Pick<Headers, "get">;
};

let _ssrHeadersProvider: SsrHeadersProvider | null = null;
export function registerSsrHeaders(provider: SsrHeadersProvider): void {
  _ssrHeadersProvider = provider;
}

async function resolveSsrHeaders(): Promise<SsrHeadersProvider> {
  if (_ssrHeadersProvider) return _ssrHeadersProvider;
  const pkg = ["next", "headers"].join("/");
  return (await import(/* webpackIgnore: true */ pkg)) as SsrHeadersProvider;
}

export async function ssrUpstreamGet({
  path,
  logPrefix,
  extraHeaders,
  method = "GET",
  body,
  timeoutMs = SSR_TIMEOUT_MS,
}: SSRGetOpts): Promise<SSRUpstreamResponse | null> {
  const base = process.env.API_PROXY_URL;
  if (!base) return null;
  try {
    const nh = await resolveSsrHeaders();
    const [cookieJar, headerBag] = await Promise.all([nh.cookies(), nh.headers()]);
    const cookieHeader = cookieJar.toString();
    const proxiedByCaddy = headerBag.get("x-proxied-by-caddy");
    const inboundHost = headerBag.get("host");
    const gatewaySecret = process.env.ADMIN_GATEWAY_SECRET || "";
    const { gatewayAuthenticated, refusal } = classifySsrRequest(headerBag);
    if (refusal) {
      console.warn(`[${logPrefix}] refusal: ${refusal} — falling back to client fetch`);
      return null;
    }
    const debugResponsesRequested = cookieJar.get(DEBUG_RESPONSES_COOKIE)?.value === "1";
    const upstreamHeaders: Record<string, string> = { Accept: "application/json" };
    if (cookieHeader) upstreamHeaders.Cookie = cookieHeader;
    const ua = headerBag.get("user-agent");
    if (ua) upstreamHeaders["user-agent"] = ua;
    const platform = headerBag.get("sec-ch-ua-platform");
    if (platform) upstreamHeaders["sec-ch-ua-platform"] = platform;
    if (extraHeaders) {
      const trustHeaders = new Set(["host", "x-remote-analyst", "x-admin-gateway-token"]);
      for (const [key, value] of Object.entries(extraHeaders)) {
        if (!trustHeaders.has(key.toLowerCase())) upstreamHeaders[key] = value;
      }
    }
    if (gatewayAuthenticated) {
      upstreamHeaders["X-Admin-Gateway-Token"] = gatewaySecret;
      if (inboundHost) upstreamHeaders.Host = inboundHost;
    }
    if (proxiedByCaddy) {
      upstreamHeaders["X-Remote-Analyst"] = "1";
      if (inboundHost) upstreamHeaders.Host = inboundHost;
    }
    if (debugResponsesRequested && !proxiedByCaddy) upstreamHeaders["x-debug-responses"] = "1";
    let serialisedBody: string | undefined;
    if (method === "POST" && body !== undefined) {
      serialisedBody = JSON.stringify(body);
      upstreamHeaders["Content-Type"] = "application/json";
      upstreamHeaders["Content-Length"] = String(Buffer.byteLength(serialisedBody));
    }
    upstreamHeaders["Connection"] = "close";
    try {
      return await rawRequest(`${base}${path}`, upstreamHeaders, timeoutMs, method, serialisedBody);
    } catch (firstErr) {
      const code = (firstErr as NodeJS.ErrnoException | undefined)?.code;
      if (code === "ECONNRESET" || code === "EPIPE") {
        return await rawRequest(`${base}${path}`, upstreamHeaders, timeoutMs, method, serialisedBody);
      }
      throw firstErr;
    }
  } catch (err) {
    console.warn(`[${logPrefix}] fetch failed; falling back to client fetch:`, err);
    return null;
  }
}

function rawRequest(
  urlStr: string,
  reqHeaders: Record<string, string>,
  timeoutMs: number,
  method: "GET" | "POST" = "GET",
  body?: string,
): Promise<SSRUpstreamResponse> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const lib = getNodeRequestFn(url.protocol);
    const req = lib(
      {
        hostname: url.hostname,
        port: url.port || (url.protocol === "https:" ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method,
        headers: reqHeaders,
        timeout: timeoutMs,
        agent: false,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => resolve({ statusCode: res.statusCode ?? 0, body: Buffer.concat(chunks).toString("utf8") }));
      },
    );
    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error(`SSR upstream timeout after ${timeoutMs}ms`)));
    if (method === "POST" && body !== undefined) req.write(body);
    req.end();
  });
}
