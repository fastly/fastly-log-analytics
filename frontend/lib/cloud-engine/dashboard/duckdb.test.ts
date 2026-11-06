// @vitest-environment node
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { duckdbAssetUrls } from "./duckdb";

describe("DuckDB browser assets", () => {
  it("load from an immutable versioned path", () => {
    expect(duckdbAssetUrls("https://flp.example", "1.33.1-dev57.0")).toEqual({
      worker:
        "https://flp.example/duckdb/1.33.1-dev57.0/duckdb-browser-eh.worker.js",
      wasm: "https://flp.example/duckdb/1.33.1-dev57.0/duckdb-eh.wasm",
      extensions: "https://flp.example/duckdb/1.33.1-dev57.0/extensions",
    });
  });

  it("pin the exact installed duckdb-wasm version", () => {
    const read = (path: string) =>
      JSON.parse(readFileSync(join(__dirname, "../../..", path), "utf8"));
    const installed = read(
      "node_modules/@duckdb/duckdb-wasm/package.json",
    ).version;
    expect(read("package.json").dependencies["@duckdb/duckdb-wasm"]).toBe(
      installed,
    );
  });

  it("vendor the parquet extension built for the bundled DuckDB engine", () => {
    const vendorDir = join(__dirname, "../../../vendor/duckdb-extensions");
    const manifest = JSON.parse(
      readFileSync(join(vendorDir, "manifest.json"), "utf8"),
    ) as { duckdb_version: string; files: Record<string, string> };
    expect(Object.keys(manifest.files)).toEqual([
      `${manifest.duckdb_version}/wasm_eh/parquet.duckdb_extension.wasm`,
    ]);
    for (const [file, sha256] of Object.entries(manifest.files)) {
      const digest = createHash("sha256")
        .update(readFileSync(join(vendorDir, file)))
        .digest("hex");
      expect(digest, file).toBe(sha256);
    }
    const duckdb = createRequire(import.meta.url)(
      "@duckdb/duckdb-wasm/dist/duckdb-node-blocking",
    ) as {
      createDuckDB: (
        bundles: unknown,
        logger: unknown,
        runtime: unknown,
      ) => Promise<{
        instantiate: () => Promise<void>;
        connect: () => {
          query: (sql: string) => { toArray: () => { v: string }[] };
        };
      }>;
      VoidLogger: new () => unknown;
      NODE_RUNTIME: unknown;
    };
    const dist = join(
      __dirname,
      "../../../node_modules/@duckdb/duckdb-wasm/dist",
    );
    return duckdb
      .createDuckDB(
        {
          mvp: { mainModule: join(dist, "duckdb-mvp.wasm"), mainWorker: "" },
          eh: { mainModule: join(dist, "duckdb-eh.wasm"), mainWorker: "" },
        },
        new duckdb.VoidLogger(),
        duckdb.NODE_RUNTIME,
      )
      .then(async (db) => {
        await db.instantiate();
        const [row] = db.connect().query("SELECT version() AS v").toArray();
        expect(row.v).toBe(manifest.duckdb_version);
      });
  });
});
