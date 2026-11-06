import { createServer } from "node:https";
import { request } from "node:http";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";

const frontKey = process.env.FLP_TEST_FRONT_KEY;
if (!frontKey)
  throw new Error("FLP_TEST_FRONT_KEY is required for the native test proxy");
const backend = new URL(
  process.env.FLP_E2E_NATIVE_BACKEND || "http://127.0.0.1:8787",
);
if (
  backend.protocol !== "http:" ||
  !["localhost", "127.0.0.1"].includes(backend.hostname)
)
  throw new Error("The native test proxy must target local HTTP");
const stem = resolve(`.e2e-${randomUUID()}`);
const keyPath = `${stem}-key.pem`;
const certPath = `${stem}-cert.pem`;
let credentials;
try {
  const certificate = spawnSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-days",
      "1",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost",
      "-keyout",
      keyPath,
      "-out",
      certPath,
    ],
    { encoding: "utf8" },
  );
  if (certificate.error) throw certificate.error;
  if (certificate.status !== 0)
    throw new Error(
      `Test certificate generation failed: ${certificate.stderr}`,
    );
  credentials = { key: readFileSync(keyPath), cert: readFileSync(certPath) };
} finally {
  for (const file of [keyPath, certPath])
    if (existsSync(file)) unlinkSync(file);
}

createServer(credentials, (incoming, outgoing) => {
  const omitFrontKey = incoming.headers["x-flp-test-omit-front-key"] === "1";
  const frontRateLimit = incoming.headers["x-flp-test-front-rl"] === "1" ? "1" : "0";
  const headers = {
    ...incoming.headers,
    "x-flp-front-rl": frontRateLimit,
    "fastly-client-ip": incoming.headers["fastly-client-ip"] || "127.0.0.1",
    "x-forwarded-proto": "https",
  };
  if (!omitFrontKey) {
    headers["x-flp-front-key"] = frontKey;
  }
  const upstream = request(
    backend,
    {
      path: incoming.url,
      method: incoming.method,
      headers,
    },
    (response) => {
      outgoing.writeHead(response.statusCode || 502, response.headers);
      response.pipe(outgoing);
    },
  );
  outgoing.on("close", () => {
    if (!outgoing.writableFinished) upstream.destroy();
  });
  upstream.on("error", (error) => {
    if (outgoing.headersSent) {
      outgoing.destroy(error);
      return;
    }
    outgoing.writeHead(502, { "content-type": "application/json" });
    outgoing.end(
      JSON.stringify({ detail: "Native test runtime is unavailable" }),
    );
  });
  incoming.pipe(upstream);
}).listen(
  Number(process.env.FLP_E2E_NATIVE_TLS_PORT || "8443"),
  "127.0.0.1",
  () => {
    process.send?.({ ready: true });
  },
);
