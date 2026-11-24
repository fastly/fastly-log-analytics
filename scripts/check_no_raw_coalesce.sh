#!/usr/bin/env bash
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check_no_inline_metric_coalesce.sh" "$@"
