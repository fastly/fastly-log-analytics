import { readFileSync } from "node:fs";
import path from "node:path";
import type { ManifestFile } from "./manifest";

// Written by `cargo test --test summary -- --ignored write_network_fixtures`:
// raw windows of 2027-01-15 08:00-10:00Z carrying the /network columns, their
// summaries and the bundles the server builds from them.
const DIR = path.join(__dirname, "..", "__fixtures__", "network-summaries");

export type SummaryFixture = {
  key: string;
  file: string;
  size: number;
  kind: "raw" | "window" | "hour" | "day";
  start: number;
  end: number;
  rows: number;
  sources?: number;
};

export const SUMMARY_FIXTURES: SummaryFixture[] = JSON.parse(
  readFileSync(path.join(DIR, "index.json"), "utf8"),
);

export const SUMMARY_FIXTURE_BUFFERS: Record<string, ArrayBuffer> =
  Object.fromEntries(
    SUMMARY_FIXTURES.map((entry) => {
      const bytes = readFileSync(path.join(DIR, entry.file));
      return [
        entry.key,
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
      ];
    }),
  );

/** 2027-01-15 08:00Z in epoch seconds; the fixtures span two hours from here. */
export const FIXTURE_START = 1_800_000_000;

const iso = (s: number) => new Date(s * 1000).toISOString();

export function fixtureFile(e: SummaryFixture): ManifestFile {
  return {
    key: e.key,
    start: iso(e.start),
    end: iso(e.end),
    size: e.size,
    rows: e.rows,
    ...(e.kind === "raw" ? {} : { kind: e.kind, sources: e.sources }),
  };
}

export function fixtureFiles(
  kind: SummaryFixture["kind"],
  pred: (e: SummaryFixture) => boolean = () => true,
): ManifestFile[] {
  return SUMMARY_FIXTURES.filter((e) => e.kind === kind && pred(e)).map(
    fixtureFile,
  );
}

export const fixturePage = (files: ManifestFile[]) => ({
  files,
  next_cursor: null,
  complete: true,
});
