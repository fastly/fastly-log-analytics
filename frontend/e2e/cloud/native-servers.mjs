import { spawn, spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(__dirname, "../..");
const repoRoot = resolve(__dirname, "../../..");
const computeManifest = resolve(repoRoot, "compute/Cargo.toml");
const targetDir = process.env.CARGO_TARGET_DIR
  ? resolve(process.env.CARGO_TARGET_DIR)
  : resolve(repoRoot, "compute/target");

const frontKey = process.env.FLP_TEST_FRONT_KEY || "test-front-key";
const setupCode = process.env.FLP_TEST_SETUP_CODE || "fixture-owner-code";
process.env.FLP_TEST_FRONT_KEY = frontKey;
process.env.FLP_TEST_SETUP_CODE = setupCode;

const tlsPort = Number(process.env.FLP_E2E_NATIVE_TLS_PORT || "8443");
const missingTlsPort = tlsPort + 1; // 8444 by default
const backendPort = Number(process.env.FLP_E2E_NATIVE_BACKEND_PORT || "8787");

import { existsSync } from "node:fs";

const cachedWebOut = resolve(repoRoot, "target/release-cache/web-out");
const webOut = existsSync(resolve(cachedWebOut, "dashboard.html"))
  ? cachedWebOut
  : resolve(frontendRoot, "out");
const assetCacheDir = resolve(repoRoot, "target/release-cache/precompressed");

const buildSteps = [];
if (
  process.env.FLP_SKIP_NEXT_BUILD !== "1" &&
  !existsSync(resolve(webOut, "dashboard.html"))
) {
  buildSteps.push([
    "npm",
    ["run", "build"],
    {
      cwd: frontendRoot,
      env: {
        ...process.env,
        STATIC_EXPORT: "1",
        NEXT_PUBLIC_DEPLOYMENT_MODE: "cloud",
      },
    },
  ]);
}
buildSteps.push([
  "cargo",
  [
    "build",
    "--quiet",
    "--manifest-path",
    computeManifest,
    "--bin",
    "shell-test-server",
    "--target-dir",
    targetDir,
  ],
  {
    cwd: repoRoot,
    env: {
      ...process.env,
      FLP_WEB_OUT: webOut,
      FLP_ASSET_CACHE_DIR: assetCacheDir,
    },
  },
]);

for (const [command, args, opts] of buildSteps) {
  const result = spawnSync(command, args, { stdio: "inherit", ...opts });
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(`${command} failed (${result.status})`);
}

const children = new Set();
let stopping = false;
function stop(code) {
  if (stopping) return;
  stopping = true;
  for (const child of children) child.kill("SIGTERM");
  process.exit(code);
}
process.on("SIGTERM", () => stop(0));
process.on("SIGINT", () => stop(0));
function start(command, args, env, ipc = false) {
  const child = spawn(command, args, {
    cwd: frontendRoot,
    stdio: ipc ? ["inherit", "inherit", "inherit", "ipc"] : "inherit",
    env: { ...process.env, ...env },
  });
  children.add(child);
  child.on("error", (error) => {
    console.error(error.message);
    stop(1);
  });
  child.on("exit", (code) => {
    children.delete(child);
    if (!stopping) stop(code || 1);
  });
  return child;
}
async function ready(port) {
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/healthz`, {
        headers: { "x-flp-front-key": frontKey },
        signal: AbortSignal.timeout(1_000),
      });
      if (response.ok) return;
    } catch (error) {
      if (attempt === 99) throw error;
    }
    await delay(50);
  }
  throw new Error(`Native test runtime on ${port} is not ready`);
}

try {
  const binary = resolve(targetDir, "debug/shell-test-server");
  start(binary, [], {
    FLP_TEST_ORIGIN: `https://localhost:${tlsPort}`,
    FLP_TEST_LISTEN: `127.0.0.1:${backendPort}`,
    FLP_TEST_MISSING: "",
  });
  start(binary, [], {
    FLP_TEST_ORIGIN: `https://localhost:${missingTlsPort}`,
    FLP_TEST_LISTEN: `127.0.0.1:${backendPort + 1}`,
    FLP_TEST_MISSING: "fastly_token",
  });
  await Promise.all([ready(backendPort), ready(backendPort + 1)]);
  await new Promise((resolveReady) => {
    const missingProxy = start(
      process.execPath,
      ["e2e/cloud/native-proxy.mjs"],
      {
        FLP_E2E_NATIVE_BACKEND: `http://127.0.0.1:${backendPort + 1}`,
        FLP_E2E_NATIVE_TLS_PORT: String(missingTlsPort),
      },
      true,
    );
    missingProxy.once("message", resolveReady);
  });
  start(process.execPath, ["e2e/cloud/native-proxy.mjs"], {
    FLP_E2E_NATIVE_BACKEND: `http://127.0.0.1:${backendPort}`,
    FLP_E2E_NATIVE_TLS_PORT: String(tlsPort),
  });
} catch (error) {
  console.error(error.message);
  stop(1);
}
