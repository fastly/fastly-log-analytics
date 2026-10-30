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
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],
    },
  },
  // F5/F6 DRY enforcement (app-wide): UI must format numbers/dates through the
  // shared helpers in lib/format.ts (formatCount) and lib/date.ts
  // (formatLocalDateTime), never ad-hoc `.toLocaleString()` /
  // `.toLocaleDateString()` / `.toLocaleTimeString()` / `new Intl.*`. This keeps
  // locale/precision consistent and prevents the inline-formatting drift the
  // 2026 DRY audit flagged. The shared helpers themselves live in lib/ and are
  // intentionally outside this scope.
  {
    files: ["app/**", "components/**", "hooks/**"],
    rules: {
      "no-restricted-syntax": [
        "error",
        {
          selector:
            "CallExpression[callee.property.name=/^(toLocaleString|toLocaleDateString|toLocaleTimeString)$/]",
          message:
            "Don't call .toLocaleString()/.toLocaleDateString()/.toLocaleTimeString() in analytics UI — use formatCount() from @/lib/format (numbers) or formatLocalDateTime() from @/lib/date (dates).",
        },
        {
          selector: "NewExpression[callee.object.name='Intl']",
          message:
            "Don't construct Intl.* formatters in analytics UI — add or reuse a helper in @/lib/format or @/lib/date.",
        },
      ],
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
