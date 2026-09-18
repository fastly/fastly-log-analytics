# Current Project State and Current Goals

## Current Project State

v3.0.0 is designed to add a new architecture for this project which we call "high-scale". The current architecture we call "standard". The "standard" architecture is meant for sites or situations when you only want to log like 10k RPS (we need to confirm what we think is the max for "standard"). The "high-scale" architecture is designed for sites that get much more traffic like 2M RPS on average with bursts to up to 5M RPS.

The "standard" architecture is meant to be run on a laptop or a small to medium sized VM. The "high-scale" architecture is meant to run on a Kubernetes cluster (or equivalent) with auto-scaling for one or more of the pods.

We have written and deployed a significant amount of code for the "high-scale" architecture, but there is still work to do to make sure it is 100% complete.

We deployed the "standard" architecture to the GCE machine (see the .claude/skills/deploy-to-gce-and-verify/SKILL.md skill) which you can connect to here:

gcloud compute ssh fastly-log-analysis --zone=us-central1-a --project=se-development-9566

That site has active logs coming in from service cVnu9mYB3Cvmob3lsqjQU3 and has many days of historical logs.

We deployed the "high-scale" architecture to Fastly Elevation dev-usc1 cluster (see the local-docs/elevation-deployment.md file for more info). That architecture has the site ZEZ4mcAjoSFDTg7tpkDKV2 deployed to it, but that site does not get active traffic. We have some scripts to push logs to that service and you can send test traffic to that service at will as well. We have sent some logs in the past but also have done some cleanup of the logs along the way.

The "standard" deployment can also be run on my laptop.

## Current Goals

We need to complete all of the code for the "high-scale" architecture. We also need to confirm that the "standard" architecture still works flawlessly alongside it. This means that all pages on the site work the same with each architeture, the only difference being how they load the data for each page.

Here is the current list of top-level pages:

Dashboard
Control Room
Service Summary
Performance
Origin
Security
Insights
Network
Streaming
RUM
Sessions
Usage & Cost
Query
Alerts
Data Management
Admin

Some of those pages have sub-pages and/or tabs on them and we need to account for all of those pages and tabs as well (as well as their sub-pages and tabs). Some of the pages have modals and smoe of those modals have more links (like the modal for a streaming session).

We also need to make sure everything works for both the admin role connected over SSH and an analyst viewing over the remote dashboard.

Many pages include the ability to filter that page by one or more fields, or to link to the dashboard pre-filtered. The dashboard itself also allows for filtering by one or more fields. We need to confirm that all filtering works on all pages (accounting for all sections on all of those pages).

In additional to confirming that all functionality works, we also need to confirm that we're loading all data and pages in an optimal way. Log all queries that were involved in the page loads along with all API calls. Examine all of the queries and API calls to confirm they are appropriate and optimized for performance and cost. Also look for any queries or API we did not include in our logging properly and confirm they are included. We also need to confirm the user experience is optimal. Therefore, you should look at how quickly the page becomes interactive, how quickly all data loads in and how fast the page becomes fully interactive. Use real browser interactions and things like HAR files to analyze what is happening, and do it over repeated iterations to look at averages as well as things like p95 performance.

To repeat, all funtionality and pages and interactions need to be tested for both roles and both architecutres and while under expected load.

## What you are allowed to do

You have full control over the v3.0.0-beta1 branch as well as the GCE machine, my local laptop and Elevation dev-usc1 cluster for compiling and testing everything. You are free to build and deploy to those systems as needed, and if you need image tags from me for Elevation deploys please ask.

The cVnu9mYB3Cvmob3lsqjQU3 site deployed on GCE has active traffic and existing logs, do not delete any logs but you can send additional test traffic or synthetic logs as needed. The ZEZ4mcAjoSFDTg7tpkDKV2 site deployed to Elevation has no active traffic but is a real Fastly service. You are welcome to send as many logs as you want or send real synthetic traffic and delete log data at will if needed. You are also authorized to deploy VCL updates during testing to either service or its ancillary services using the tokens from the existing services or better yet using the mechanisms already built into the code and UI.

If you do send real traffic, do not go over 25k RPS. You can push sythetic logs to simulate traffic higher than that.

## Local Access & Port Forwarding

When testing locally, it is critical to confirm that port forwarding is active and functioning correctly after every deployment so you can view and test the changes alongside the automated tools.

**Note:** GCE and Elevation architectures run the frontend in production mode (via `next build` and Next.js Turbopack where applicable) to validate true production performance. The Local instance runs in dev mode (`next dev`) for easier debugging.

**For "Standard" Architecture (GCE):**
The GCE deployment uses a direct SSH tunnel to forward the internal loops to your local machine.
1. Run this command on your laptop to start the tunnel:
   `gcloud compute ssh fastly-log-analysis --project=se-development-9566 --zone=us-central1-a -- -N -L 3001:127.0.0.1:3000 -L 8001:127.0.0.1:8000`
2. Access the Admin UI locally at: `http://localhost:3001/admin`
3. Access the Analyst view via the public Fastly URL at `/share-login`.

**For "High-Scale" Architecture (Elevation cluster):**
The Elevation cluster uses standard Kubernetes port-forwarding to the `se-demo` namespace.
1. Forward the frontend service to your local machine:
   `kubectl port-forward svc/frontend-svc -n se-demo 3002:3000`
2. Forward the backend service:
   `kubectl port-forward svc/backend-svc -n se-demo 8002:8000`
3. Check the UI locally at `http://localhost:3002/admin`

**For Local Development (Native):**
If you are running the stack natively on your laptop (e.g., using `uv run` and `npm run dev`):
1. The frontend typically runs on `http://localhost:3000`
2. The backend typically runs on `http://localhost:8000`
3. Check the UI locally at `http://localhost:3000/admin`

*Reminder: Always confirm your port forwards are running and haven't dropped after triggering redeployments or container restarts.*

## Missing Architecture Gaps & Auto-Discovery

As we work through the finalization of "high-scale" (v3.0.0-beta1), our approach is to tackle issues one at a time, auto-discovering edge cases and bugs during testing, and continuously updating this document.

A few immediate architecture decisions/gaps to address:
- [ ] **Disable Analyst Path A (Standalone Mode) for High-Scale**: Since the DuckLake cutover means the catalog is no longer FOS-resident, Analyst Path A (where analysts use FOS credentials to download files locally/standalone) is incompatible with high-scale. We will disable this flow entirely for high-scale services rather than building a workaround.
- [ ] **Broad System Robustness**: Investigate all aspects of ingestion and the serving tier to ensure absolute robustness. This includes auto-discovering and fixing any concurrency or pipeline recovery issues as they arise under load.

## Expanded Testing Plan & Harness

To properly validate both "standard" and "high-scale" architectures, we need a robust testing harness:

1.  **Automated End-to-End (E2E) & Performance Harness**:
    - Build a best-in-class Playwright testing harness following industry best practices.
    - Automatically load each of the 16 top-level pages (and their sub-tabs/modals) for all roles.
    - Capture network HAR files, API call durations, and page interactive timings (LCP, INP, fully loaded) during runs to calculate p95 metrics over repeated iterations.
    - Implement a synthetic log generator script capable of pushing sustained 2M RPS (with 5M bursts) to push limits.

2.  **Comprehensive Ingestion & System Robustness**:
    - Investigate all aspects of the ingest pipelines (both standard and high-scale) under heavy load.
    - Ensure all fault-tolerance mechanisms, state recoveries, and data retention policies are fully robust and operate flawlessly without impacting the serving tier.

3.  **Role & Topology Validation**:
    - **Roles**: Test as Admin (read_write) vs Analyst Path B (live shared instance). (Analyst Path A will be disabled for high-scale).
    - **Provisioning**: Test the full Provision Wizard flow to ensure new high-scale services can be instantiated from scratch. *Note: We are authorized to repeatedly tear down the existing test service `ZEZ4mcAjoSFDTg7tpkDKV2` and its ancillary services on the Elevation cluster to start fresh and validate the full provisioning lifecycle.*
    - **VCL Deployments**: Confirm that deploying VCL updates from the UI correctly propagates to the Fastly edge.

## To-Dos

We will tackle these one at a time, auto-discovering issues and updating this list dynamically.

- [x] Stabilize "standard" architecture (GCE) — fixed DuckDB connection pool saturation and stuck cron ingestion.
- [x] Tear down `ZEZ4mcAjoSFDTg7tpkDKV2` (and ancillary services) to test the full provisioning flow for "high-scale" architecture (Elevation cluster). Redeployed from scratch with all log fields including `cmcd` enabled.
- [ ] Disable Analyst Path A for high-scale architectures.
- [ ] Develop a best-in-class Playwright E2E performance testing harness.
- [ ] Develop synthetic log generator for 5M RPS load testing.
- [ ] Execute baseline performance tests on the "standard" architecture (GCE).
- [ ] Execute baseline performance tests on the "high-scale" architecture.
- [ ] Validate all 16 pages, sub-pages, filters, and modals under both architectures.
- [ ] Investigate and validate all aspects of ingestion, cron jobs, and general system robustness under expected load.

## First Steps

Keep this document updated as we go and track it in git. We'll delete it and squash the branch at the end when we're done and record everything we accomplish in the docs and ADRs.
