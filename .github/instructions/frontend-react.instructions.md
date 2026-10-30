---
applyTo: "frontend/**/*.{ts,tsx}"
---

# Frontend (React / Next.js App Router)

- Stack: **React + Next.js App Router**, shadcn/Radix, TanStack (Query/Table/Virtual), Zustand for client state, nuqs for URL state, Tailwind. Do not introduce Material UI, Chakra, Redux, or CRA.
- API types come from the generated OpenAPI types (`types/api.generated.ts`); call the backend through `lib/api.ts`, never a hand-written fetch with hand-written response types.
- Tests: **Vitest** + Testing Library (unit/component, run with `npm run test`), **Playwright** (e2e). Not Jest/Cypress.
- Package manager is **npm**. Verify dependency bumps with `npm ci` (the Docker deploy command), not just `npm install` — `npm ci` hard-fails on peer conflicts that `install` only warns about.
- **ESLint ceiling is 0** (`scripts/check_eslint_count.sh`, wired into `make ci`): production code may have no errors and tests no errors or warnings, so any new violation fails CI.
- Format numbers and dates through `lib/format` / `lib/date`; `toLocaleString`, `toLocaleDateString`, `toLocaleTimeString` and `new Intl.*` are banned outside `lib/`.
- **Strict react-hooks rules are ERRORS**: no `setState` synchronously in a `useEffect` body (`react-hooks/set-state-in-effect`), no reading `ref.current` during render (`react-hooks/refs`). For an SSR-safe on-mount read of browser/external state, use **`useSyncExternalStore(subscribe, getClientSnapshot, getServerSnapshot)`** — not a mount-flag effect.
- Friendly-copy edits must never smart-quote JS string delimiters inside inline `<script>` — it's a silent SyntaxError that kills the whole page script.
