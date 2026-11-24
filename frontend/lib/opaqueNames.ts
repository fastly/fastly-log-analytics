const OPAQUE_PREFIX = 'fla-'
const ENTROPY_BYTES = 12 // 96 bits -> 24 lowercase hex chars -> 28 chars total (<= 63)

function randomOpaqueHex(): string {
  const bytes = new Uint8Array(ENTROPY_BYTES)
  globalThis.crypto.getRandomValues(bytes)
  let hex = ''
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i]!.toString(16).padStart(2, '0')
  }
  return hex
}

export function generateOpaqueBucketName(): string {
  return `${OPAQUE_PREFIX}${randomOpaqueHex()}`
}

export function generateOpaqueSubdomainPrefix(): string {
  return `${OPAQUE_PREFIX}${randomOpaqueHex()}`
}
