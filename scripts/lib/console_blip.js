// Shared classifier for self-healing transient console/network errors seen by
// the Playwright deploy verifier (scripts/verify_dashboard.js). Extracted into
// its own CommonJS module so the pure classification can be unit-tested without
// launching a browser.
//
// These errors occur during the harness's own verify-phase load spike (4
// parallel browsers + continuous seeders + crons on a shared 6-CPU Colima, plus
// K8s port-forward churn) and recover within seconds with NO code defect. The
// verifier absorbs a BOUNDED number of them (TRANSIENT_ERROR_BUDGET); the
// positive per-section checks remain the real pass/fail arbiter.
function isTransientConsoleBlip(text) {
  return /ERR_CONNECTION_REFUSED|ERR_ABORTED|ERR_NETWORK_CHANGED|net::ERR_CONNECTION|net::ERR_FAILED/i.test(text);
}

module.exports = { isTransientConsoleBlip };
