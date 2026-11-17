// @vitest-environment node
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { classifyUserAgentCounts } from "./classify";
import { WELL_KNOWN_BOTS } from "./well-known-bots.generated";

const GOOGLEBOT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
const BINGBOT = "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)";
const CHROME =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36";
const ADIDXBOT = "Mozilla/5.0 (compatible; adidxbot/2.0; +http://www.bing.com/bingbot.htm)";

const ROWS: [string, number][] = [
  [GOOGLEBOT, 50],
  [BINGBOT, 40],
  ["curl/8.4.0", 30],
  [CHROME, 1000],
  [ADIDXBOT, 7],
  ["facebookexternalhit/1.1", 5],
  ["", 99],
];

// Expected values come from backend classify_ua_counts over the bundled seed.
describe("classifyUserAgentCounts", () => {
  it("matches the High-Scale classifier on real user agents", async () => {
    expect(await classifyUserAgentCounts(ROWS, 10)).toEqual([
      { id: "google-crawler", name: "Google Crawler", category: "google", request_count: 50 },
      { id: "bing-crawler", name: "Bing Crawler", category: "microsoft", request_count: 47 },
      { id: "curl", name: "Curl", category: "tool", request_count: 30 },
      { id: "bing-ads", name: "Bing Ads", category: "microsoft", request_count: 7 },
      { id: "facebook-share-crawler", name: "Facebook Share Crawler", category: "ai", request_count: 5 },
      { id: "imessage-preview", name: "Imessage Preview", category: "apple", request_count: 5 },
    ]);
  });

  it("keeps the top n by request count", async () => {
    expect((await classifyUserAgentCounts(ROWS, 2)).map((b) => [b.id, b.request_count])).toEqual([
      ["google-crawler", 50],
      ["bing-crawler", 47],
    ]);
  });

  it("names bots the way Python title-cases ids, including after digits", async () => {
    expect(
      await classifyUserAgentCounts(
        [
          ["Mozilla/5.0 (compatible; 007ac9 Crawler; http://crawler.007ac9.net/)", 4],
          ["crawler4j (http://code.google.com/p/crawler4j/)", 2],
          ["CCBot/2.0 (http://commoncrawl.org/faq/)", 3],
        ],
        10,
      ),
    ).toEqual([
      { id: "sistrix-007ac9-crawler", name: "Sistrix 007Ac9 Crawler", category: "unknown", request_count: 4 },
      { id: "commoncrawl-crawler", name: "Commoncrawl Crawler", category: "academic", request_count: 3 },
      { id: "ccbot-crawler", name: "Ccbot Crawler", category: "archive", request_count: 3 },
      { id: "java-crawler4j", name: "Java Crawler4J", category: "programmatic", request_count: 2 },
    ]);
  });

  it("returns nothing when no user agent is logged", async () => {
    expect(await classifyUserAgentCounts([], 10)).toEqual([]);
    expect(await classifyUserAgentCounts([["", 5]], 10)).toEqual([]);
  });
});

describe("well-known bots browser copy", () => {
  it("matches backend/utils/well_known_bots_seed.json (rerun scripts/generate-well-known-bots.mjs)", () => {
    const seed = JSON.parse(
      readFileSync(path.join(__dirname, "../../../../backend/utils/well_known_bots_seed.json"), "utf8"),
    ) as { id?: string; categories?: string[]; pattern?: { accepted?: string[] } }[];
    const derived = seed.map((e) => [e.id ?? "unknown", e.categories?.[0] ?? "unknown", e.pattern?.accepted ?? []]);
    expect(WELL_KNOWN_BOTS).toEqual(derived);
  });

  it("compiles every pattern as a JavaScript regular expression", () => {
    const bad = WELL_KNOWN_BOTS.flatMap(([id, , patterns]) =>
      patterns.flatMap((p) => {
        try {
          new RegExp(p, "i");
          return [];
        } catch {
          return [`${id}: ${p}`];
        }
      }),
    );
    expect(bad).toEqual([]);
  });
});
