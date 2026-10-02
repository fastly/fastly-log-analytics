const { test } = require('node:test');
const assert = require('node:assert');
const { isTransientConsoleBlip } = require('./console_blip');

test('classifies the K8s port-forward / abort transient blips as transient', () => {
  assert.equal(isTransientConsoleBlip('Failed to load resource: net::ERR_CONNECTION_REFUSED'), true);
  assert.equal(isTransientConsoleBlip('net::ERR_ABORTED'), true);
  assert.equal(isTransientConsoleBlip('net::ERR_NETWORK_CHANGED'), true);
});

test('classifies net::ERR_FAILED as a transient self-healing blip', () => {
  // A heavy 30d page load whose in-flight request is aborted/reset during the
  // parallel-verify load spike surfaces in Chromium as the generic
  // net::ERR_FAILED rather than ERR_ABORTED/ERR_CONNECTION_REFUSED. It is the
  // same self-healing class and must be absorbed under the same bounded budget.
  assert.equal(isTransientConsoleBlip('[RUM] Failed to load resource: net::ERR_FAILED'), true);
});

test('does NOT classify a real application error as transient', () => {
  assert.equal(isTransientConsoleBlip('Uncaught TypeError: cannot read properties of undefined'), false);
  assert.equal(isTransientConsoleBlip('500 Internal Server Error: duckdb binder error'), false);
});
