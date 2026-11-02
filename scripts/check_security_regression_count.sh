#!/usr/bin/env bash
# check_security_regression_count.sh: assert @pytest.mark.security_regression count >= FLOOR.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

FLOOR=206
run_count_floor "$FLOOR" "security_regression"
