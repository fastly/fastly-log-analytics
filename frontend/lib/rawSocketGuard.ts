export const RAW_SOCKET_UNTRUSTED_HEADER = 'x-fla-raw-untrusted'

const SECRET_SYM = Symbol.for('fla.rawSocketProxySecret')
const INSTALLED_SYM = Symbol.for('fla.rawSocketProxyGuardInstalled')

const STRICT_LOOPBACK_IPV4_RE = /^127(?:\.(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$/
const EXPANDED_IPV6_LOOPBACK_RE = /^(?:0{1,4}:){7}0{0,3}1$/
const MAPPED_IPV6_PREFIX_RE = /^(?:::ffff:|(?:0{1,4}:){5}ffff:)/
const MAPPED_HEX_LOOPBACK_RE = /^7f[0-9a-f]{2}:[0-9a-f]{1,4}$/

type GlobalGuardState = typeof globalThis & {
  [SECRET_SYM]?: string
  [INSTALLED_SYM]?: boolean
}

let cachedLoopbackBlockList: import('node:net').BlockList | null | undefined

function getLoopbackBlockList(): import('node:net').BlockList | null {
  if (cachedLoopbackBlockList !== undefined) return cachedLoopbackBlockList
  const netMod =
    typeof process !== 'undefined' && typeof process.getBuiltinModule === 'function'
      ? (process.getBuiltinModule('node:net') as typeof import('node:net') | undefined)
      : undefined
  if (!netMod?.BlockList) {
    cachedLoopbackBlockList = null
    return null
  }
  const bl = new netMod.BlockList()
  bl.addSubnet('127.0.0.0', 8, 'ipv4')
  bl.addSubnet('::1', 128, 'ipv6')
  bl.addSubnet('::ffff:127.0.0.0', 104, 'ipv6')
  cachedLoopbackBlockList = bl
  return bl
}

export function isLoopbackIpLiteral(ip: string): boolean {
  const candidate = ip.trim()
  if (!candidate) return false
  const netMod =
    typeof process !== 'undefined' && typeof process.getBuiltinModule === 'function'
      ? (process.getBuiltinModule('node:net') as typeof import('node:net') | undefined)
      : undefined
  const bl = getLoopbackBlockList()
  if (netMod?.isIP && bl) {
    const ver = netMod.isIP(candidate)
    if (ver === 4) return bl.check(candidate, 'ipv4')
    if (ver === 6) return bl.check(candidate, 'ipv6')
    return false
  }
  const lc = candidate.toLowerCase()
  if (STRICT_LOOPBACK_IPV4_RE.test(lc)) return true
  if (lc === '::1' || EXPANDED_IPV6_LOOPBACK_RE.test(lc)) return true
  if (MAPPED_IPV6_PREFIX_RE.test(lc)) {
    const tail = lc.replace(MAPPED_IPV6_PREFIX_RE, '')
    return STRICT_LOOPBACK_IPV4_RE.test(tail) || MAPPED_HEX_LOOPBACK_RE.test(tail)
  }
  return false
}

function getRawSocketProxySecret(): string {
  const g = globalThis as GlobalGuardState
  if (typeof g[SECRET_SYM] === 'string' && g[SECRET_SYM].length >= 32) {
    return g[SECRET_SYM]
  }
  let secret = ''
  if (typeof process !== 'undefined' && typeof process.getBuiltinModule === 'function') {
    const cryptoMod = process.getBuiltinModule('node:crypto') as typeof import('node:crypto') | undefined
    if (cryptoMod?.randomBytes) {
      secret = cryptoMod.randomBytes(16).toString('hex')
    }
  }
  if (!secret && typeof globalThis.crypto?.getRandomValues === 'function') {
    const buf = new Uint8Array(16)
    globalThis.crypto.getRandomValues(buf)
    secret = Array.from(buf, b => b.toString(16).padStart(2, '0')).join('')
  }
  g[SECRET_SYM] = secret
  return secret
}

function timingSafeStringEqual(a: string, b: string): boolean {
  const enc = new TextEncoder()
  const ba = enc.encode(a)
  const bb = enc.encode(b)
  if (ba.byteLength !== bb.byteLength) return false
  let diff = 0
  for (let i = 0; i < ba.byteLength; i++) diff |= ba[i] ^ bb[i]
  return diff === 0
}

function isHeadersInstance(
  bag: Headers | Record<string, string | string[] | undefined>,
): bag is Headers {
  return typeof (bag as Headers).get === 'function' && typeof (bag as Headers).delete === 'function'
}

function headerHasNonEmptyValue(val: string | string[] | null | undefined): boolean {
  if (Array.isArray(val)) return val.some(item => item.trim() !== '')
  return typeof val === 'string' && val.trim() !== ''
}

export function applyRawSocketProxyGuard(
  headers: Headers | Record<string, string | string[] | undefined>,
  socketRemoteAddress?: string | null,
): void {
  const secret = getRawSocketProxySecret()
  if (!secret) return

  const rawPeer = (socketRemoteAddress ?? '').trim()
  const peerIsExternal = rawPeer !== '' && !isLoopbackIpLiteral(rawPeer)

  if (isHeadersInstance(headers)) {
    headers.delete(RAW_SOCKET_UNTRUSTED_HEADER)
    const hasGatewayHeader =
      headerHasNonEmptyValue(headers.get('x-admin-gateway-token')) ||
      headerHasNonEmptyValue(headers.get('x-fla-admin-gateway'))
    const wireXff = headers.get('x-forwarded-for')
    const hasWireXff = headerHasNonEmptyValue(wireXff)
    if (!hasGatewayHeader && (hasWireXff || peerIsExternal)) {
      headers.set(RAW_SOCKET_UNTRUSTED_HEADER, secret)
      if (peerIsExternal && hasWireXff) {
        headers.set('x-forwarded-for', `${wireXff!.trim()}, ${rawPeer}`)
      }
    }
    return
  }

  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === RAW_SOCKET_UNTRUSTED_HEADER) {
      delete headers[key]
    }
  }
  const hasGatewayHeader =
    headerHasNonEmptyValue(headers['x-admin-gateway-token']) ||
    headerHasNonEmptyValue(headers['x-fla-admin-gateway'])
  const wireXff = headers['x-forwarded-for']
  const hasWireXff = headerHasNonEmptyValue(wireXff)
  if (!hasGatewayHeader && (hasWireXff || peerIsExternal)) {
    headers[RAW_SOCKET_UNTRUSTED_HEADER] = secret
    if (peerIsExternal && hasWireXff) {
      const prev = Array.isArray(wireXff) ? wireXff.join(', ') : String(wireXff).trim()
      headers['x-forwarded-for'] = `${prev}, ${rawPeer}`
    }
  }
}

export function isRawSocketUntrustedRequest(headers: Pick<Headers, 'get'>): boolean {
  const raw = headers.get(RAW_SOCKET_UNTRUSTED_HEADER)
  if (!raw || raw.trim() === '') return false
  const secret = getRawSocketProxySecret()
  if (secret.length < 32) return false
  return timingSafeStringEqual(raw.trim(), secret)
}

export function installRawSocketProxyGuard(): boolean {
  if (
    typeof process === 'undefined' ||
    process.env?.STATIC_EXPORT === '1' ||
    typeof process.getBuiltinModule !== 'function'
  ) {
    return false
  }
  const g = globalThis as GlobalGuardState
  if (g[INSTALLED_SYM] === true) return true

  const httpMod = process.getBuiltinModule('node:http') as typeof import('node:http') | undefined
  const proto = httpMod?.Server?.prototype
  if (!proto || typeof proto.emit !== 'function') return false

  const origEmit = proto.emit
  proto.emit = function wrappedFlaServerEmit(
    this: import('node:http').Server,
    event: string | symbol,
    ...args: unknown[]
  ): boolean {
    if (event === 'request') {
      const req = args[0] as import('node:http').IncomingMessage | undefined
      if (req && typeof req === 'object' && req.headers && typeof req.headers === 'object') {
        applyRawSocketProxyGuard(req.headers, req.socket?.remoteAddress ?? null)
      }
    }
    return Reflect.apply(origEmit, this, [event, ...args])
  } as typeof proto.emit

  g[INSTALLED_SYM] = true
  return true
}
