import { assetsFromRollup, type AssetsRollupBody } from "./assets-rollup";
import type { RollupResult } from "./dashboard/rollup-query";
import type { InsightsRequest } from "./insights";
import { insightsBaselineFromRollup, insightsBaselineRollup } from "./insights-rollup";
import type { NetworkQueryParams } from "./network";
import { networkFromRollup } from "./network-rollup";
import { originFromRollup, type OriginRollupBody } from "./origin-rollup";
import { performanceFromRollup, type PerformanceRollupBody } from "./performance-rollup";
import { securityFromRollup, type SecurityRollupBody } from "./security-rollup";
import { valueFromRollup, type ValueRollupBody } from "./value-rollup";

export type RollupAnswerSource = Parameters<typeof networkFromRollup>[2];

type Answer = (
  path: string,
  params: Record<string, unknown>,
  source: RollupAnswerSource,
  signal?: AbortSignal,
) => Promise<unknown>;

const network: Answer = (path, params, source, signal) =>
  networkFromRollup(path, params as NetworkQueryParams, source, signal);
const performance: Answer = (path, params, source, signal) =>
  performanceFromRollup(path, params as PerformanceRollupBody, source, signal);

const security: Answer = (path, params, source, signal) =>
  securityFromRollup(path, params as SecurityRollupBody, source, signal);

const ANSWERS: Record<string, Answer> = {
  "/api/network-health": network,
  "/api/network-quality": network,
  "/api/network/pop-health": network,
  "/api/network/bundle": network,
  "/api/origin/aggregates": (path, params, source, signal) =>
    originFromRollup(path, params as OriginRollupBody, source, signal),
  "/api/performance": performance,
  "/api/performance/aggregates": performance,
  "/api/security/aggregates": security,
  "/api/security/bundle": security,
  "/api/assets/aggregates": (path, params, source, signal) =>
    assetsFromRollup(path, params as AssetsRollupBody, source, signal),
  "/api/value/summary": (path, params, source, signal) =>
    valueFromRollup(path, params as ValueRollupBody, source, signal),
  // The adapter passes only unfiltered baselines; the window is read raw on the page.
  "/api/insights": async (_path, body, source, signal): Promise<RollupResult | null> => {
    const result = await insightsBaselineFromRollup(body as InsightsRequest, [], source, signal);
    return result && insightsBaselineRollup(result);
  },
};

/**
 * Answers one Cloud read from v3 summaries, or null when the raw scan must: the
 * response body for /network, /origin, /performance, /security, /assets and
 * /fastly-value, or the /insights baseline cut by `insightsBaselineRollup`.
 * Every answer is plain data that structured cloning keeps unchanged, so the
 * dashboard engine worker runs this off the page's main thread and the page
 * falls back to it in-thread.
 */
export function answerFromRollup(
  path: string,
  params: Record<string, unknown>,
  source: RollupAnswerSource,
  signal?: AbortSignal,
): Promise<unknown> {
  const answer = ANSWERS[path];
  return answer ? answer(path, params, source, signal) : Promise.resolve(null);
}
