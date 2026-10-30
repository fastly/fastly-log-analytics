#!/usr/bin/env bash
# check_eslint_count.sh: ESLint error-count CEILING gate for frontend production and test dirs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/../frontend"
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

CEILING=9
SCOPE=(app components hooks lib stores)
TEST_CEILING=0
TEST_SCOPE=(__tests__ e2e vitest.setup.ts)

check_eslint_scope production "$CEILING" 0 "${SCOPE[@]}"
check_eslint_scope tests "$TEST_CEILING" 1 "${TEST_SCOPE[@]}"
echo "OK"
