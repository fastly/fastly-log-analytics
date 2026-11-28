/** Routes whose first answer always needs a main-thread raw scan, not only summaries. */
export const CLOUD_RAW_SCAN_PREFIXES = [
  "/sessions", "/rum", "/query", "/admin/session-scoring",
] as const;

/** Routes that answer initial or baseline reads via the dashboard rollup worker. */
export const CLOUD_ROLLUP_WORKER_PREFIXES = [
  "/dashboard", "/insights", "/network", "/origin", "/performance",
  "/security", "/assets-shield", "/fastly-value", "/rum", "/cmcd",
] as const;

export const matchesCloudRoute = (pathname: string, prefixes: readonly string[]) =>
  prefixes.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`));
