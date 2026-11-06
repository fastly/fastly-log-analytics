// @vitest-environment node
import { readFileSync, existsSync, readdirSync } from "node:fs";
import path from "node:path";
import vm from "node:vm";
import ts from "typescript";
import { afterEach, describe, expect, it, vi } from "vitest";

const root = path.resolve(import.meta.dirname, "../..");

function runtimeModules(
  entry: string,
  modules = new Set<string>(),
  external = new Set<string>(),
): Set<string> {
  const file = path.resolve(root, entry);
  if (modules.has(file)) return modules;
  modules.add(file);
  const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, true);
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) && !ts.isExportDeclaration(statement)) continue;
    if (!statement.moduleSpecifier || !ts.isStringLiteral(statement.moduleSpecifier)) continue;
    if (ts.isExportDeclaration(statement) && statement.isTypeOnly) continue;
    if (ts.isImportDeclaration(statement)) {
      const clause = statement.importClause;
      if (clause?.isTypeOnly) continue;
      if (
        clause && !clause.name && clause.namedBindings &&
        ts.isNamedImports(clause.namedBindings) &&
        clause.namedBindings.elements.every((element) => element.isTypeOnly)
      ) continue;
    }
    if (
      ts.isExportDeclaration(statement) && statement.exportClause &&
      ts.isNamedExports(statement.exportClause) &&
      statement.exportClause.elements.every((element) => element.isTypeOnly)
    ) continue;
    const specifier = statement.moduleSpecifier.text;
    if (!specifier.startsWith(".") && !specifier.startsWith("@/")) {
      external.add(specifier);
      continue;
    }
    const base = specifier.startsWith("@/")
      ? path.resolve(root, specifier.slice(2))
      : path.resolve(path.dirname(file), specifier);
    if (/\.(css|svg|png)$/.test(base)) continue;
    const dependency = [
      base + ".ts", base + ".tsx", path.join(base, "index.ts"), path.join(base, "index.tsx"),
      ...(path.extname(base) ? [base] : []),
    ].find(existsSync);
    if (!dependency) throw new Error(`Unresolved worker dependency: ${specifier} in ${file}`);
    runtimeModules(path.relative(root, dependency), modules, external);
  }
  return modules;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Cloud worker import safety", () => {
  it("keeps every non-Cloud route, AppLayout and bootstrap runtime graph free of analytical adapters and Wasm", () => {
    const modules = new Set<string>();
    const external = new Set<string>();
    const routes = readdirSync(path.join(root, "app"), { recursive: true, encoding: "utf8" })
      .filter((file) => /\.(ts|tsx)$/.test(file) && !file.includes(".test."));
    for (const entry of ["components/AppLayout.tsx", "hooks/useBootstrap.ts", ...routes.map((file) => `app/${file}`)]) {
      runtimeModules(entry, modules, external);
    }
    expect([...modules].map((file) => path.relative(root, file)).filter(
      (file) => file === "lib/cloud-engine/adapter.ts" ||
        /^lib\/cloud-engine\/(dashboard|telemetry)\/(client|engine|parquet)\.ts$/.test(file),
    )).toEqual([]);
    expect([...external].filter((specifier) =>
      ["hyparquet-compressors", "hysnappy", "@duckdb/duckdb-wasm"].includes(specifier),
    )).toEqual([]);
  });

  it("keeps browser stores and UI utilities outside both complete runtime import graphs", () => {
    const modules = runtimeModules("lib/cloud-engine/dashboard/worker.ts");
    runtimeModules("lib/cloud-engine/telemetry/worker.ts", modules);
    const browserModules = [...modules].map((file) => path.relative(root, file)).filter(
      (file) => file.startsWith("stores/") || file.startsWith("components/") ||
        ["lib/utils.ts", "lib/urlFilter.ts", "lib/exportCsv.ts"].includes(file),
    );
    expect(browserModules).toEqual([]);
  });

  it("evaluates both actual worker graphs without window or document", async () => {
    expect(globalThis.window).toBeUndefined();
    expect(globalThis.document).toBeUndefined();
    const scope = {
      location: { origin: "https://cloud.example.test" },
      onmessage: null as unknown,
      postMessage: vi.fn(),
    };
    vi.stubGlobal("self", scope);
    await import("./dashboard/worker");
    expect(scope.onmessage).toBeTypeOf("function");
    scope.onmessage = null;
    await import("./telemetry/worker");
    expect(scope.onmessage).toBeTypeOf("function");
  });

  it("imports activity utilities when a browser bundler folds typeof window in a worker", () => {
    const source = readFileSync(path.join(root, "lib/utils.ts"), "utf8");
    const compiled = ts.transpileModule(source.replaceAll("typeof window", '"object"'), {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    const exports: { isUserActive?: () => boolean; msSinceLastInteraction?: () => number } = {};
    vm.runInNewContext(compiled, { exports, require: () => ({}) });
    expect(exports.isUserActive?.()).toBe(true);
    expect(exports.msSinceLastInteraction?.()).toBe(0);
  });
});
