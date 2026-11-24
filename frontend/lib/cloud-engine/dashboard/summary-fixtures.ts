import { readFileSync } from "node:fs";
import path from "node:path";
import type { ManifestFile } from "./manifest";

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

/**
 * Reads `__fixtures__/{dir}`, written by `cargo test --test summary -- --ignored
 * write_network_fixtures` (or `write_origin_fixtures`): raw windows of
 * 2027-01-15 08:00-10:00Z, their summaries and the bundles the server builds from them.
 */
export function loadSummaryFixtures(dir: string) {
  const root = path.join(__dirname, "..", "__fixtures__", dir);
  const fixtures: SummaryFixture[] = JSON.parse(
    readFileSync(path.join(root, "index.json"), "utf8"),
  );
  const buffers: Record<string, ArrayBuffer> = Object.fromEntries(
    fixtures.map((entry) => {
      const bytes = readFileSync(path.join(root, entry.file));
      return [
        entry.key,
        bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
      ];
    }),
  );
  const files = (
    kind: SummaryFixture["kind"],
    pred: (e: SummaryFixture) => boolean = () => true,
  ): ManifestFile[] =>
    fixtures.filter((e) => e.kind === kind && pred(e)).map(fixtureFile);
  return { fixtures, buffers, files };
}

const network = loadSummaryFixtures("network-summaries");

export const SUMMARY_FIXTURES: SummaryFixture[] = network.fixtures;

export const SUMMARY_FIXTURE_BUFFERS: Record<string, ArrayBuffer> =
  network.buffers;

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

export const fixtureFiles = network.files;

export const fixturePage = (files: ManifestFile[]) => ({
  files,
  next_cursor: null,
  complete: true,
});
