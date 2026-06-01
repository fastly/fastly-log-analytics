/// <reference types="vitest-axe/matchers" />

// vitest-axe registers ``toHaveNoViolations`` on vitest's expect via the
// matchers module. Importing the types side-loads the matcher declarations
// so ``expect(await axe(container)).toHaveNoViolations()`` typechecks.

import 'vitest'
import type { AxeMatchers } from 'vitest-axe/matchers'

declare module 'vitest' {
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface Assertion extends AxeMatchers {}
  // eslint-disable-next-line @typescript-eslint/no-empty-object-type
  interface AsymmetricMatchersContaining extends AxeMatchers {}
}
