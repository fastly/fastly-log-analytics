# How it works (`/how-it-works`)

A public explainer of the install: how request logs travel from Fastly to the pages, where data lives, and what drives cost. The content depends on the deployment.

Source: [`frontend/app/how-it-works/page.tsx`](../../frontend/app/how-it-works/page.tsx) renders [`components/how-it-works/cloud/`](../../frontend/components/how-it-works/cloud/) when `IS_CLOUD_MODE` is set at build time, otherwise [`components/how-it-works/high-scale/`](../../frontend/components/how-it-works/high-scale/).

- **Cloud (Fastly Compute):** the architecture diagram, the cron fan-out, the KV and object-storage layers, browser-side analytics, setup and costs. Restored from the original Cloud app.
- **High-Scale (Kubernetes):** the ingest, archive, ClickHouse publication and serving path, autoscaling and retention.

Both are static server components with no data fetching and no API calls.

## Access

Public. `AppLayout` renders the page without the sidebar for anonymous visitors instead of redirecting to `/share-login`, and never sends a signed-in install with no services to the `/admin` setup wizard from it. Signed-in admins and analysts see it in the app layout, linked from the System section of the sidebar. In Cloud the Compute shell serves `/how-it-works`, `.html`, `.txt` and its `__next.*.txt` payloads without a session (`compute/cloud/src/shell.rs`).

## Tests

- Layout access and nav: `/how-it-works access` in [`frontend/__tests__/components/AppLayout.test.tsx`](../../frontend/__tests__/components/AppLayout.test.tsx)
- Content and links: [`frontend/__tests__/app/how-it-works.test.tsx`](../../frontend/__tests__/app/how-it-works.test.tsx)
- Cloud live check: the browser verification step owned by `fdt`
