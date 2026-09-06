import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";

const eslintConfig = defineConfig([
  ...nextVitals,
  ...nextTs,
  {
    rules: {
      "react/no-array-index-key": "error",
      "jsx-a11y/click-events-have-key-events": "error",
      "jsx-a11y/no-static-element-interactions": "error",
    },
  },
  // Override default ignores of eslint-config-next.
  globalIgnores([
    // Default ignores of eslint-config-next:
    ".next/**",
    "out/**",
    "build/**",
    "public/**",
    "next-env.d.ts",
    // Build / generated / report output a bare `eslint .` would otherwise
    // lint as if it were source — minified chunks under `.next-e2e/`
    // (the Playwright dist tree) and instrumented files under `coverage/`
    // produced ~21k bogus problems incl. rules-of-hooks false positives in
    // vendor code. Keep these out so the lint gate sees only real source.
    ".next-e2e/**",
    "coverage/**",
    "playwright-report/**",
    "test-results/**",
  ]),
]);

export default eslintConfig;
