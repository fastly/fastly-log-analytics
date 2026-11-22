import type { components } from "@/types/api";
import { loadWellKnownBots, matchWellKnownBots, titleCase, type WellKnownBot } from "./classify";

export type WellknownBotRow = components["schemas"]["WellknownBotRow"];
export type BotEfficiencyRow = components["schemas"]["BotEfficiencyRow"];

const WELLKNOWN_BOTS_LIMIT = 50;
const BOT_EFFICIENCY_LIMIT = 25;

const GENERIC_BOT_RE = /([a-z0-9_.-]*(?:bot|crawler|spider|slurp|archiver|scraper|externalagent|extended)[a-z0-9_.-]*)/i;
const BOT_TOKEN_SUFFIX_RE =
  /(-searchbot|-assistbot|-externalagent|-externalfetcher|-extended|-user|searchbot|assistbot|externalagent|externalfetcher|extended|crawler|spider|slurp|archiver|scraper|preview|other|bot).*$/i;
const VOWELS_RE = /[aeiou]/g;

function stripChars(s: string, chars: string): string {
  let start = 0;
  let end = s.length;
  while (start < end && chars.includes(s[start])) start++;
  while (end > start && chars.includes(s[end - 1])) end--;
  return s.slice(start, end);
}

/** Python round(): the exact binary value rounded to `digits` places, exact halves to even. */
export function pyRound(value: number, digits: number): number {
  const abs = Math.abs(value);
  const [whole, frac = ""] = abs.toFixed(100).split(".");
  const tie = frac[digits] === "5" && /^0*$/.test(frac.slice(digits + 1));
  let rounded = Number(abs.toFixed(digits));
  if (tie) {
    const last = digits === 0 ? whole[whole.length - 1] : frac[digits - 1];
    if (Number(last) % 2 === 0) rounded = Number(digits === 0 ? whole : `${whole}.${frac.slice(0, digits)}`);
  }
  return value < 0 ? -rounded : rounded;
}

/**
 * Cloud port of backend summarize_wellknown_bot_rows over (ua, ip, count) rows.
 * The browser has no rDNS cache, so every IP is classified the way High-Scale
 * classifies one whose lookup has not landed: pending for bots with verification
 * domains, unverified for the rest. Verified and impersonator stay 0.
 */
export async function summarizeWellKnownBotRows(
  rows: Iterable<readonly [ua: string, ip: string, count: number]>,
): Promise<WellknownBotRow[]> {
  const all = await loadWellKnownBots();
  const bots = new Map<string, WellknownBotRow>();
  for (const [ua, ip, count] of rows) {
    if (!ua || !ip) continue;
    for (const bot of matchWellKnownBots(all, ua)) {
      let row = bots.get(bot.id);
      if (!row) {
        row = {
          id: bot.id,
          name: bot.name,
          category: bot.category,
          request_count: 0,
          verified_count: 0,
          impersonator_count: 0,
          unverified_count: 0,
          pending_count: 0,
          verification_coverage: 0,
        };
        bots.set(bot.id, row);
      }
      row.request_count += count;
      if (bot.verificationDomains.length > 0) row.pending_count += count;
      else row.unverified_count += count;
    }
  }
  const sorted = [...bots.values()].sort((a, b) => b.request_count - a.request_count).slice(0, WELLKNOWN_BOTS_LIMIT);
  for (const row of sorted) {
    const covered = row.verified_count + row.impersonator_count;
    row.verification_coverage = row.request_count ? pyRound(covered / row.request_count, 3) : 0;
  }
  return sorted;
}

function stemsMatch(botStem: string, refStem: string): boolean {
  if (!botStem || !refStem) return false;
  if (botStem === refStem) return true;
  const botConsonants = botStem.replace(VOWELS_RE, "");
  if (botConsonants.length >= 3 && botConsonants === refStem.replace(VOWELS_RE, "")) return true;
  return botStem.length >= 3 && refStem.length >= 3 && (refStem.includes(botStem) || botStem.includes(refStem));
}

type DisplayIndex = { exact: Map<string, string>; substrings: [pattern: string, name: string][] };

function buildDisplayIndex(all: readonly WellKnownBot[]): DisplayIndex {
  const exact = new Map<string, string>();
  const substrings: [string, string][] = [];
  for (const bot of all) {
    if (!bot.id) continue;
    for (const key of [bot.id, ...bot.aliases]) {
      const low = key.toLowerCase();
      if (!exact.has(low)) exact.set(low, bot.name);
    }
    for (const pattern of bot.patternTexts) substrings.push([pattern.toLowerCase(), bot.name]);
  }
  return { exact, substrings };
}

/** Cloud port of backend resolve_bot_display_name for a bot token the catalog did not match. */
function resolveBotDisplayName(all: readonly WellKnownBot[], index: () => DisplayIndex, token: string): string {
  const raw = token.trim();
  if (!raw) return "Other Bot";
  const probes = [
    raw,
    `Mozilla/5.0 (compatible; ${raw}/2.0)`,
    `Mozilla/5.0 (compatible; ${raw}/1.0)`,
    `Mozilla/5.0 (compatible; ${raw}/0.1)`,
    `${raw}/1.0`,
  ];
  for (const probe of probes) {
    const matches = matchWellKnownBots(all, probe);
    if (matches.length) return matches[0].name;
  }
  const rawLow = stripChars(raw.toLowerCase(), "/._- ");
  if (rawLow) {
    const { exact, substrings } = index();
    const name = exact.get(rawLow) ?? substrings.find(([pattern]) => pattern.includes(rawLow))?.[1];
    if (name) return name;
  }
  const cleaned = stripChars(raw, "/._- ");
  return cleaned ? titleCase(cleaned.replace(/_/g, " ")) : "Other Bot";
}

/**
 * Cloud port of backend summarize_bot_efficiency_rows over (kind, key, count, bytes)
 * rows: one `site` total, `ua` rows per user agent and `ref` rows of unique
 * referring IPs per referral stem.
 */
export async function summarizeBotEfficiencyRows(
  rows: Iterable<readonly [kind: string, key: string, count: number, respBytes: number]>,
): Promise<BotEfficiencyRow[]> {
  const all = await loadWellKnownBots();
  const vendorStems = new Map<string, Set<string>>();
  for (const bot of all) {
    const vendor = bot.id.split("-")[0].toLowerCase();
    if (!vendor) continue;
    let stems = vendorStems.get(vendor);
    if (!stems) vendorStems.set(vendor, (stems = new Set()));
    for (const stem of bot.referralStems) stems.add(stem);
  }
  let displayIndex: DisplayIndex | undefined;
  const index = () => (displayIndex ??= buildDisplayIndex(all));

  let siteBytes = 0;
  const refCounts = new Map<string, number>();
  const uaRows: [string, number, number][] = [];
  for (const [kind, key, count, bytes] of rows) {
    if (kind === "site") siteBytes += bytes;
    else if (kind === "ref" && key) refCounts.set(key.toLowerCase(), (refCounts.get(key.toLowerCase()) ?? 0) + count);
    else if (kind === "ua" && key) uaRows.push([key, count, bytes]);
  }

  type Bucket = { bot_name: string; category: string; request_count: number; resp_bytes: number; stems: Set<string> };
  const buckets = new Map<string, Bucket>();
  for (const [ua, count, bytes] of uaRows) {
    const matched = matchWellKnownBots(all, ua)[0];
    let botId: string;
    let botName: string;
    let category: string;
    let stems: Iterable<string>;
    if (matched) {
      botId = matched.id;
      botName = matched.name;
      category = titleCase(matched.category);
      const vendor = matched.id.split("-")[0].toLowerCase();
      stems = [...matched.referralStems, ...(vendorStems.get(vendor) ?? [])];
    } else {
      const m = GENERIC_BOT_RE.exec(ua.toLowerCase());
      if (!m) continue;
      botId = m[1];
      botName = resolveBotDisplayName(all, index, m[1]);
      category = "General Bot";
      const stem = stripChars(m[1].replace(BOT_TOKEN_SUFFIX_RE, ""), "-_.");
      stems = stem.length >= 3 ? [stem] : [];
    }
    let bucket = buckets.get(botId);
    if (!bucket) {
      bucket = { bot_name: botName, category, request_count: 0, resp_bytes: 0, stems: new Set() };
      buckets.set(botId, bucket);
    }
    bucket.request_count += count;
    bucket.resp_bytes += bytes;
    for (const stem of stems) bucket.stems.add(stem);
  }

  const denomBytes = Math.max(siteBytes, 1);
  const results: BotEfficiencyRow[] = [];
  for (const bucket of buckets.values()) {
    let referrals = 0;
    for (const [refStem, refCount] of refCounts) {
      if ([...bucket.stems].some((stem) => stemsMatch(stem, refStem))) referrals += refCount;
    }
    const refDenom = Math.max(referrals, 1);
    results.push({
      bot_name: bucket.bot_name,
      category: bucket.category,
      request_count: bucket.request_count,
      resp_bytes: bucket.resp_bytes,
      bandwidth_pct: pyRound((bucket.resp_bytes * 100) / denomBytes, 2),
      referrals,
      requests_per_referral: pyRound(bucket.request_count / refDenom, 1),
      bytes_per_referral: pyRound(bucket.resp_bytes / refDenom, 1),
    });
  }
  return results
    .sort((a, b) => b.resp_bytes - a.resp_bytes || b.request_count - a.request_count)
    .slice(0, BOT_EFFICIENCY_LIMIT);
}
