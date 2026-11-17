export type BotRow = { id: string; name: string; category: string; request_count: number };

type Bot = { id: string; name: string; category: string; patterns: RegExp[] };

/** High-Scale classifies the top 2,000 user agents by request count. */
export const BOT_UA_CLASSIFY_LIMIT = 2000;
const UA_CACHE_LIMIT = 10_000;

let bots: Promise<Bot[]> | undefined;
const matchCache = new Map<string, readonly Bot[]>();

// Python str.title(): upper-case the first letter of every letter run.
function titleCase(id: string): string {
  return id
    .replace(/-/g, " ")
    .replace(/[A-Za-z]+/g, (w) => w[0].toUpperCase() + w.slice(1).toLowerCase());
}

function compile(pattern: string): RegExp[] {
  try {
    return [new RegExp(pattern, "i")];
  } catch {
    return [];
  }
}

// The seed is ~40 KB, so it loads only when a page first classifies.
function loadBots(): Promise<Bot[]> {
  bots ??= import("./well-known-bots.generated").then(({ WELL_KNOWN_BOTS }) =>
    WELL_KNOWN_BOTS.map(([id, category, patterns]) => ({
      id,
      name: titleCase(id),
      category,
      patterns: patterns.flatMap(compile),
    })),
  );
  return bots;
}

function match(all: readonly Bot[], ua: string): readonly Bot[] {
  if (!ua) return [];
  const cached = matchCache.get(ua);
  if (cached) return cached;
  const matched = all.filter((bot) => bot.patterns.some((p) => p.test(ua)));
  if (matchCache.size >= UA_CACHE_LIMIT) matchCache.clear();
  matchCache.set(ua, matched);
  return matched;
}

/**
 * Cloud port of backend classify_ua_counts: every well-known bot whose
 * patterns match a user agent gains that user agent's count.
 */
export async function classifyUserAgentCounts(
  rows: Iterable<readonly [ua: string, count: number]>,
  n: number,
): Promise<BotRow[]> {
  const all = await loadBots();
  const counts = new Map<string, BotRow>();
  for (const [ua, count] of rows) {
    for (const bot of match(all, ua)) {
      let row = counts.get(bot.id);
      if (!row) {
        row = { id: bot.id, name: bot.name, category: bot.category, request_count: 0 };
        counts.set(bot.id, row);
      }
      row.request_count += count;
    }
  }
  return [...counts.values()].sort((a, b) => b.request_count - a.request_count).slice(0, n);
}
