// Shared classifier & Playwright verification helpers for scripts/verify_dashboard.js.
function isTransientConsoleBlip(text) {
  return /ERR_CONNECTION_REFUSED|ERR_ABORTED|ERR_NETWORK_CHANGED|net::ERR_CONNECTION|net::ERR_FAILED|ChunkLoadError|Failed to load chunk/i.test(text);
}

async function logPoolAtStart(context, baseUrl, name) {
  try {
    const res = await context.request.get(`${new URL(baseUrl).origin}/api/admin/health`, { timeout: 15000 });
    if (!res.ok()) return;
    const pools = (await res.json()).pool_wait || [];
    const busy = pools.reduce((n, p) => n + Math.max(0, (p.in_use || 0) - (p.idle || 0)), 0);
    console.log(`[POOL-START ${name}] busy=${busy}`);
  } catch (err) {
    console.log(`[POOL-START ${name}] unavailable: ${err.message}`);
  }
}

async function logPoolStats(context, baseUrl, name) {
  try {
    const res = await context.request.get(`${new URL(baseUrl).origin}/api/admin/health`, { timeout: 20000 });
    if (!res.ok()) throw new Error(`status ${res.status()}`);
    const pools = ((await res.json()).pool_wait || []).map(p => ({
      max: p.max_size, open: p.in_use, idle: p.idle, busy: Math.max(0, (p.in_use || 0) - (p.idle || 0)),
      created: p.created_total, reused: p.reused_total, discarded: p.discarded_total,
      saturated: p.saturated_rejects_total, wait: p.wait, rebindWait: p.rebind_wait, heavy: p.heavy,
    }));
    console.log(`[POOL ${name}] ${JSON.stringify(pools)}`);
  } catch (err) {
    console.log(`[POOL ${name}] unavailable: ${err.message}`);
  }
}

async function logPagePerf(page, name, budgets, responseMeasurements = [], measurementGaps = []) {
  try {
    const perf = await page.evaluate(() => new Promise(resolve => {
      const nav = performance.getEntriesByType('navigation')[0];
      const fcp = performance.getEntriesByName('first-contentful-paint')[0];
      let lcp = null, cls = 0;
      try {
        new PerformanceObserver(list => {
          const entries = list.getEntries();
          if (entries.length) lcp = entries[entries.length - 1].startTime;
        }).observe({ type: 'largest-contentful-paint', buffered: true });
        new PerformanceObserver(list => {
          for (const e of list.getEntries()) if (!e.hadRecentInput) cls += e.value;
        }).observe({ type: 'layout-shift', buffered: true });
      } catch { /* unsupported entry type */ }
      setTimeout(() => {
        const api = {};
        for (const r of performance.getEntriesByType('resource')) {
          if (!r.name.includes('/api/') || r.name.includes('/api/web-vitals') || r.name.includes('/api/ux-events')) continue;
          const key = new URL(r.name).pathname;
          const row = api[key] || (api[key] = { n: 0, maxMs: 0, kb: 0 });
          row.n += 1;
          if (Math.round(r.duration) >= row.maxMs) {
            row.stallMs = Math.round(r.requestStart - r.startTime);
            row.waitMs = Math.round(r.responseStart - r.requestStart);
            row.dlMs = Math.round(r.responseEnd - r.responseStart);
          }
          row.maxMs = Math.max(row.maxMs, Math.round(r.duration));
          row.kb = Math.max(row.kb, Math.round((r.transferSize || r.encodedBodySize || 0) / 102.4) / 10);
        }
        resolve({
          ttfbMs: nav ? Math.round(nav.responseStart) : null,
          dclMs: nav ? Math.round(nav.domContentLoadedEventEnd) : null,
          loadMs: nav ? Math.round(nav.loadEventEnd) : null,
          fcpMs: fcp ? Math.round(fcp.startTime) : null,
          lcpMs: lcp === null ? null : Math.round(lcp),
          cls: Math.round(cls * 1000) / 1000,
          api,
          duplicateApi: Object.entries(api).filter(([, v]) => v.n > 1).map(([k, v]) => `${k} x${v.n}`),
        });
      }, 500);
    }));
    perf.responseMeasurements = responseMeasurements;
    perf.budgets = budgets;
    perf.qualification = 'incomplete';
    perf.unknowns = [...new Set([
      ...measurementGaps,
      ...responseMeasurements.flatMap(row => row.gaps),
      ...(responseMeasurements.length ? [] : ['aggregate_responses_unobserved']),
      'database_cache_state_unknown', 'history_unverified', 'reader_coverage_unverified',
      'query_io_attribution_unverified', 'page_ready_unmeasured', 'interaction_completion_unmeasured',
    ])];
    console.log(`[PERF ${name}] ${JSON.stringify(perf)}`);
  } catch (err) {
    console.log(`[PERF ${name}] unavailable: ${err.message}`);
  }
}

module.exports = { isTransientConsoleBlip, logPoolAtStart, logPoolStats, logPagePerf };
