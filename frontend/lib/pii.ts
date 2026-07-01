/**
 * Client-IP-family filter columns.
 *
 * When PII masking is enabled for an analyst (invite `pii_policy.mask_ips`),
 * these columns are non-filterable: display masking is response-side only, so
 * letting an analyst filter by an exact IP would be a presence oracle (and a
 * dead-end on the masked value anyway). The frontend hides the drill-down
 * affordances for these; the real guarantee is the server-side lock in
 * `backend/utils/remote_access.py` (`_PII_FORBIDDEN_FILTER_COLS`), which 403s
 * any such filter. Keep this list in sync with that set.
 *
 * `oip` (origin / CDN shield IP) is intentionally NOT here — it's operator
 * infra, not end-user PII, and the Origin IP-Health card depends on filtering
 * by it.
 */
export const IP_FAMILY_FIELDS = ['ip', 'client_ip', 'ip_address', 'remote_addr'] as const

/** True when `column` is a client-IP-family field that masking analysts may not filter on. */
export function isIpFamilyField(column: string): boolean {
  return (IP_FAMILY_FIELDS as readonly string[]).includes(column)
}
