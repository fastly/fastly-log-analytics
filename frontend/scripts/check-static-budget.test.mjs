import assert from "node:assert/strict";
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { brotliCompressSync, constants as zlibConstants } from "node:zlib";

import {
  assertBudgets,
  BUDGETS,
  measureRoute,
} from "./check-static-budget.mjs";

const scriptsDir = dirname(fileURLToPath(import.meta.url));

function fixture() {
  const root = mkdtempSync(join(scriptsDir, ".check-static-budget-"));
  const staticDir = join(root, "_next", "static");
  mkdirSync(staticDir, { recursive: true });
  return root;
}

function compressed(content) {
  return brotliCompressSync(content, {
    params: {
      [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
    },
  });
}

test("measureRoute deduplicates dashboard assets and reports Brotli sizes", () => {
  const root = fixture();
  try {
    const shared = Buffer.from('console.log("shared");');
    const dashboard = Buffer.from('console.log("dashboard");');
    const css = Buffer.from("body { color: navy; }");
    writeFileSync(join(root, "_next/static/shared.js"), shared);
    writeFileSync(join(root, "_next/static/dashboard.js"), dashboard);
    writeFileSync(join(root, "_next/static/dashboard.css"), css);
    writeFileSync(
      join(root, "dashboard.html"),
      [
        '<script src="/_next/static/shared.js?v=1"></script>',
        '<script src="/_next/static/dashboard.js"></script>',
        '<link rel="stylesheet" href="/_next/static/dashboard.css?v=1">',
        '<link rel="stylesheet" href="/_next/static/dashboard.css#again">',
      ].join("\n"),
    );

    const result = measureRoute(root);

    assert.equal(result.route, "/dashboard");
    assert.equal(result.javascript_files, 2);
    assert.equal(result.css_files, 1);
    assert.equal(
      result.javascript_brotli_bytes,
      compressed(Buffer.concat([shared, dashboard])).byteLength,
    );
    assert.equal(result.css_brotli_bytes, compressed(css).byteLength);
    assert.equal(
      result.largest_js_brotli_bytes,
      Math.max(compressed(shared).byteLength, compressed(dashboard).byteLength),
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("measureRoute rejects asset paths that escape the export root", () => {
  const parent = mkdtempSync(join(scriptsDir, ".check-static-budget-"));
  const root = join(parent, "out");
  try {
    mkdirSync(root);
    writeFileSync(join(parent, "outside.js"), "outside");
    symlinkSync(join(parent, "outside.js"), join(root, "escaped.js"));
    writeFileSync(
      join(root, "dashboard.html"),
      '<script src="/escaped.js"></script>',
    );

    assert.throws(
      () => measureRoute(root),
      /escapes the static export root.*escaped\.js/,
    );
  } finally {
    rmSync(parent, { recursive: true, force: true });
  }
});

test("assertBudgets reports every exceeded threshold and byte delta", () => {
  const measurement = {
    javascript_brotli_bytes: BUDGETS.dashboardJs + 7,
    css_brotli_bytes: BUDGETS.dashboardCss + 11,
    largest_js_brotli_bytes: BUDGETS.largestJs + 13,
  };

  assert.throws(
    () => assertBudgets(measurement),
    (error) =>
      /dashboard JavaScript.*7 bytes over/.test(error.message) &&
      /dashboard CSS.*11 bytes over/.test(error.message) &&
      /largest JavaScript chunk.*13 bytes over/.test(error.message),
  );
});
