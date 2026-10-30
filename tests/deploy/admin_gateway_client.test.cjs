const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');
const { adminGatewayContextOptions } = require('../../scripts/lib/admin_gateway_client.cjs');

test('only the exact admin TLS origin receives the client identity', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'gateway-client-'));
  try {
    const cert = path.join(directory, 'client.crt');
    const key = path.join(directory, 'client.key');
    fs.writeFileSync(cert, 'test certificate', { mode: 0o600 });
    fs.writeFileSync(key, 'test private key', { mode: 0o600 });
    const env = {
      ADMIN_GATEWAY_CLIENT_ORIGIN: 'https://admin.example.com:9443',
      ADMIN_GATEWAY_CLIENT_CERT: cert,
      ADMIN_GATEWAY_CLIENT_KEY: key,
      ADMIN_GATEWAY_SERVER_CA: cert,
    };
    assert.deepEqual(adminGatewayContextOptions('https://analyst.example.com/dashboard', env), {});
    const options = adminGatewayContextOptions('https://admin.example.com:9443/dashboard', env);
    assert.equal(options.clientCertificates[0].origin, env.ADMIN_GATEWAY_CLIENT_ORIGIN);
    assert.equal(options.clientCertificates[0].keyPath, key);
    assert.equal(options.ignoreHTTPSErrors, undefined);
    fs.chmodSync(key, 0o644);
    assert.throws(() => adminGatewayContextOptions(env.ADMIN_GATEWAY_CLIENT_ORIGIN, env), /owner-only/);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test('HTTP origins and missing identities fail before opening a browser', () => {
  assert.throws(() => adminGatewayContextOptions('http://admin.example.com', {
    ADMIN_GATEWAY_CLIENT_ORIGIN: 'http://admin.example.com',
  }), /Invalid/);
  assert.throws(() => adminGatewayContextOptions('https://admin.example.com', {
    ADMIN_GATEWAY_CLIENT_ORIGIN: 'https://admin.example.com',
  }), /Invalid/);
});

test('multiple deployment endpoints use only their own exact-origin identity', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'gateway-identities-'));
  try {
    const targets = {};
    for (const [environment, host] of [
      ['remote-standard', 'vm-admin.example.com'],
      ['remote-high-scale', 'cluster-admin.example.com'],
    ]) {
      const cert = path.join(directory, `${environment}.crt`);
      const key = path.join(directory, `${environment}.key`);
      fs.writeFileSync(cert, 'test certificate', { mode: 0o600 });
      fs.writeFileSync(key, 'test key', { mode: 0o600 });
      targets[environment] = { origin: `https://${host}:8443`, cert, key, ca: cert };
    }
    const env = { ADMIN_GATEWAY_ENDPOINTS: JSON.stringify(targets) };
    for (const config of Object.values(targets)) {
      const result = adminGatewayContextOptions(`${config.origin}/dashboard`, env);
      assert.equal(result.clientCertificates[0].keyPath, config.key);
      assert.equal(result.ignoreHTTPSErrors, undefined);
    }
    assert.deepEqual(adminGatewayContextOptions('https://analyst.example.com', env), {});
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
