const REDACTED = "[redacted]";

function parseIpv4Octets(ip: string): [number, number, number, number] | null {
  const parts = ip.split(".");
  if (parts.length !== 4) return null;
  const nums: number[] = [];
  for (const part of parts) {
    if (!/^(?:0|[1-9]\d{0,2})$/.test(part)) return null;
    const val = Number(part);
    if (!Number.isInteger(val) || val < 0 || val > 255) return null;
    nums.push(val);
  }
  return [nums[0], nums[1], nums[2], nums[3]];
}

function parseIpv6Hextets(ip: string): number[] | null {
  if (!ip || ip.includes("%")) return null;
  const doubleColonParts = ip.split("::");
  if (doubleColonParts.length > 2) return null;

  const parseSegment = (seg: string): number[] | null => {
    if (seg === "") return [];
    const tokens = seg.split(":");
    const out: number[] = [];
    for (let i = 0; i < tokens.length; i++) {
      const tok = tokens[i];
      if (tok.includes(".")) {
        if (i !== tokens.length - 1 || doubleColonParts.length === 2 && seg === doubleColonParts[0]) {
          return null;
        }
        const v4 = parseIpv4Octets(tok);
        if (!v4) return null;
        out.push((v4[0] << 8) | v4[1], (v4[2] << 8) | v4[3]);
      } else {
        if (!/^[0-9a-fA-F]{1,4}$/.test(tok)) return null;
        out.push(Number.parseInt(tok, 16));
      }
    }
    return out;
  };

  if (doubleColonParts.length === 1) {
    const groups = parseSegment(doubleColonParts[0]);
    return groups && groups.length === 8 ? groups : null;
  }

  const left = parseSegment(doubleColonParts[0]);
  const right = parseSegment(doubleColonParts[1]);
  if (!left || !right) return null;
  const missing = 8 - (left.length + right.length);
  if (missing < 1) return null;
  return [...left, ...new Array<number>(missing).fill(0), ...right];
}

function formatIpv6Canonical(hextets: readonly number[]): string {
  let bestStart = -1;
  let bestLen = 0;
  let curStart = -1;
  let curLen = 0;

  for (let i = 0; i < 8; i++) {
    if (hextets[i] === 0) {
      if (curStart === -1) curStart = i;
      curLen += 1;
      if (curLen > bestLen) {
        bestStart = curStart;
        bestLen = curLen;
      }
    } else {
      curStart = -1;
      curLen = 0;
    }
  }

  if (bestLen < 2) {
    return hextets.map((h) => h.toString(16)).join(":");
  }
  const head = hextets.slice(0, bestStart).map((h) => h.toString(16)).join(":");
  const tail = hextets.slice(bestStart + bestLen).map((h) => h.toString(16)).join(":");
  return `${head}::${tail}`;
}

/**
 * Mask a client IP address matching `backend/core/share_db/__init__.py:mask_ip`:
 * - IPv4 -> `a.b.c.xxx` (and already-masked `a.b.c.xxx` preserved)
 * - IPv6 -> first 3 hextets followed by `::` (canonical RFC 5952 compression)
 * - empty or invalid -> `"[redacted]"`
 */
export function maskIp(ip: string | null | undefined): string {
  if (typeof ip !== "string") return REDACTED;
  const trimmed = ip.trim();
  if (!trimmed) return REDACTED;

  if (trimmed.endsWith(".xxx")) {
    const prefix = `${trimmed.slice(0, -4)}.0`;
    const parsed = parseIpv4Octets(prefix);
    if (parsed) {
      return `${parsed[0]}.${parsed[1]}.${parsed[2]}.xxx`;
    }
  }

  const ipv4 = parseIpv4Octets(trimmed);
  if (ipv4) {
    return `${ipv4[0]}.${ipv4[1]}.${ipv4[2]}.xxx`;
  }

  const ipv6 = parseIpv6Hextets(trimmed);
  if (ipv6) {
    return formatIpv6Canonical([ipv6[0], ipv6[1], ipv6[2], 0, 0, 0, 0, 0]);
  }

  return REDACTED;
}
