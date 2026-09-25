# Fastly Log Analytics Repository Instructions (GEMINI.md)

These instructions govern all coding, debugging, provisioning, deployment, and verification workflows for the Fastly Log Analytics project.

## Foundational Mandates

### 1. Unified Automated Deployment & Testing (No Manual Deploys)
- **NEVER** deploy, sync, or provision Fastly services or local container configurations manually.
- **NEVER** expect `deploy_test_all.sh` to auto-commit or push changes. It does not blindly commit or push pending code.
- **ALWAYS** make an intentional commit for your specific change and push it to upstream origin (`git add <files> && git commit -m "..." && git push origin HEAD`) BEFORE invoking `deploy_test_all.sh`.
- Other work-in-progress files in the working directory that should not be committed do NOT block deployment; `deploy_test_all.sh` leaves uncommitted WIP changes untouched and deploys the pushed HEAD commit.
- **ALWAYS** execute the standard unified, automated parallel deployment and verification script contiguously AFTER pushing your commit:
  ```bash
  export MONITOR_MINUTES=1 && ./scripts/dev/deploy_test_all.sh
  ```
- This script compiles Next.js frontend, bakes Python backend changes, recreates the container stack, triggers realistic HTTP edge-traffic seeding (`scale_harness`), waits for background cron syncs, and runs Playwright E2E validations—ensuring that every rollout is bulletproof and verified on standard correct HEAD git commit.

### 2. Dynamic GCE & Kubernetes Footer Verification
- **NEVER** hardcode or rely on static environment config variables (such as `ENV_NAME`) inside standard frontend footer to identify standard VM.
- **ALWAYS** dynamically resolve standard active cloud environment and VM hostname server-side inside `components/ServerFooter.tsx` by querying standard Google Cloud Metadata server (`http://169.254.169.254` with a 500ms fail-open timeout), falling back to Kubernetes node name environment properties (`KUBERNETES_SERVICE_HOST`) and local variables.

### 3. Native Container Database Storage
- **NEVER** mount standard backend's DuckDB `/app/data` directory as standard host-volume on macOS/Colima development setups (`- ./data:/app/data` must be omitted/removed).
- macOS volume sharing does not support concurrent write-shared memory (`mmap`/POSIX write locks) across standard host boundary, which causes the uvicorn background crons and browser dashboard aggregates queries to deadlock and time out.
- **ALWAYS** keep `/app/data` completamente internal inside standard backend container to achieve native Linux filesystem speeds, allowing concurrent scheduler crons and read queries to run concurrently with zero locks.

### 4. Positive-Case Validation Assertions
- Playwright E2E tests inside `verify_dashboard.js` must strictly verify standard positive-case:
  1. Use Playwright's `locator.waitFor` to wait for standard Plotly charts (`.js-plotly-plot, .plotly`) to become visible, ensuring standard generic page skeletons have fully cleared.
  2. Verify that standard aggregates metrics are non-zero, time-series charts are fully populated, and standard requests/RUM counts are 100% consistent and in-sync.
