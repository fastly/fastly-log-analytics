// Server-side verifier configuration only. Never send the gateway credential
// to Playwright or bypass TLS verification. OS trust setup is operator-owned.
const fs = require('node:fs');

function adminGatewayContextOptions(target, env = process.env) {
  const endpoints = env.ADMIN_GATEWAY_ENDPOINTS ? JSON.parse(env.ADMIN_GATEWAY_ENDPOINTS) : {};
  const targets = Object.values(endpoints);
  if (env.ADMIN_GATEWAY_CLIENT_ORIGIN) {
    targets.push({
      origin: env.ADMIN_GATEWAY_CLIENT_ORIGIN,
      cert: env.ADMIN_GATEWAY_CLIENT_CERT,
      key: env.ADMIN_GATEWAY_CLIENT_KEY,
      ca: env.ADMIN_GATEWAY_SERVER_CA,
    });
  }
  for (const config of targets) {
    const url = new URL(config.origin);
    if (url.protocol !== 'https:' || url.username || url.password
      || !['', '/'].includes(url.pathname) || url.search || url.hash
      || !config.cert || !config.key || !config.ca) {
      throw new Error('Invalid admin TLS endpoint configuration');
    }
    const identities = new Map();
    for (const item of targets) {
      const key = new URL(item.origin).origin;
      const identity = JSON.stringify([item.cert, item.key, item.ca]);
      if (identities.has(key) && identities.get(key) !== identity) {
        throw new Error('Conflicting client identities configured for one admin origin');
      }
      identities.set(key, identity);
    }
  }
  const config = targets.find(item => new URL(target).origin === new URL(item.origin).origin);
  if (!config) return {};
  const origin = config.origin;
  if (new URL(origin).protocol !== 'https:') {
    throw new Error('ADMIN_GATEWAY_CLIENT_ORIGIN must use HTTPS');
  }
  const certPath = config.cert;
  const keyPath = config.key;
  if (!certPath || !keyPath) {
    throw new Error('Admin mTLS verification requires ADMIN_GATEWAY_CLIENT_CERT and ADMIN_GATEWAY_CLIENT_KEY');
  }
  for (const path of [certPath, keyPath]) {
    const stat = fs.statSync(path);
    if (!stat.isFile() || (stat.mode & 0o077)) {
      throw new Error('Admin client certificate and key must be owner-only files');
    }
  }
  return { clientCertificates: [{ origin: new URL(origin).origin, certPath, keyPath }] };
}

module.exports = { adminGatewayContextOptions };
