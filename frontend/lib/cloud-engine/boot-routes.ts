/** Routes whose first answer always needs a main-thread raw scan, not only summaries. */
export const CLOUD_RAW_SCAN_PREFIXES = [
  "/insights", "/sessions", "/rum", "/query", "/admin/session-scoring",
] as const;

export const matchesCloudRoute = (pathname: string, prefixes: readonly string[]) =>
  prefixes.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
