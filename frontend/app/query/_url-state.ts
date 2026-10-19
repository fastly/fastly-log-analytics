import { parseAsString, parseAsStringLiteral } from 'nuqs'

/**
 * /query URL contract (shared links and the dashboard "See Raw Logs" CTA):
 *   ?mode=raw            raw-SQL editor (also implied by ?q=)
 *   ?q=<sql>             seeds the raw editor
 *   ?start_time&end_time absolute range, applied once to the filter store
 *   ?filters=<json>      filter payload, applied once to the filter store
 * Unrecognised `mode` values mean structured mode, as before.
 */
export const queryPageUrlParsers = {
  q: parseAsString,
  mode: parseAsStringLiteral(['raw'] as const),
  start_time: parseAsString,
  end_time: parseAsString,
  filters: parseAsString,
}
