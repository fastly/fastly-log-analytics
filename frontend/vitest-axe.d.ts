// vitest-axe registers ``toHaveNoViolations`` on vitest's expect via the
// matchers module. Importing the types side-loads the matcher declarations
// so ``expect(await axe(container)).toHaveNoViolations()`` typechecks.

import 'vitest'
import type { AxeMatchers } from 'vitest-axe/matchers'

declare module 'vitest' {
  interface Assertion {
    toHaveNoViolations: AxeMatchers['toHaveNoViolations']
  }
  interface AsymmetricMatchersContaining {
    toHaveNoViolations: AxeMatchers['toHaveNoViolations']
  }
}
