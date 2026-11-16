import Link from "next/link";
import {
  ArrowRight,
  Clock3,
  Database,
  Globe,
  Layers,
  LockKeyhole,
  Workflow,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArchitectureDiagram } from "./ArchitectureDiagram";
import { CronFanout } from "./CronFanout";

const maintenance = [
  {
    name: "Retention sweep",
    description:
      "Delete data older than the retention settings. A sweep starts every six hours and continues across turns instead of scanning everything at once.",
  },
  {
    name: "Operational log compaction",
    description:
      "Combine small operational-log Parquet files into larger ones when compaction is enabled.",
  },
  {
    name: "History deletion",
    description:
      "Advance deletion jobs an admin has requested. This stage never decides to delete history on its own.",
  },
  {
    name: "Storage reconciliation",
    description:
      "When requested, inventory stored objects in bounded steps to refresh the storage-usage ledger.",
  },
  {
    name: "Summary bundles",
    description:
      "Merge window summaries into closed-hour and closed-day bundles, so longer dashboard ranges need fewer downloads. The open hour stays per window.",
  },
  {
    name: "Delete Data",
    description:
      "Advance a data reset an admin has started and confirmed for the logged service.",
  },
];

const pipeline = [
  {
    title: "Capture request logs",
    icon: Globe,
    description:
      "Your Fastly service streams JSON logs, zstd-compressed by default, to Fastly Object Storage, once per log period of at least 60 seconds. One install logs one service in your own Fastly account. Enable RUM to bring browser events into the pipeline too.",
  },
  {
    title: "Convert to Parquet",
    icon: Workflow,
    description:
      "The Compute dispatcher coordinates ingestion. Peer workers turn rows into immutable, time-windowed Parquet files, each with a summary file beside it. Optional compaction merges small request-log files, and maintenance bundles summaries by hour and day.",
  },
  {
    title: "Explore in your browser",
    icon: Database,
    description:
      "The front VCL service checks a signed, expiring access cookie and serves authorized Parquet straight from Object Storage through the CDN. DuckDB-WASM runs the analytics in Web Workers; summaries and locally cached files make repeat exploration cheaper.",
  },
];

export function CloudHowItWorks() {
  return (
    <div className="mx-auto max-w-6xl space-y-10">
      <header className="max-w-3xl space-y-4">
        <h1 className="text-sm font-semibold text-primary">How it works</h1>
        <p className="text-sm text-muted-foreground">
          This page explains the Cloud (Fastly Compute) deployment of Fastly
          Log Analytics.
        </p>
        <p className="text-3xl font-bold leading-tight tracking-tight sm:text-5xl">
          An analytics product.
          <br />
          Built from edge primitives.
        </p>
        <p className="text-base leading-relaxed text-muted-foreground">
          A CDN becomes a clock. Request collapsing keeps that clock from
          becoming a stampede. Compute turns logs into Parquet, and your browser
          turns Parquet into answers. No always-on application server, external
          scheduler or separate database cluster.
        </p>
        <p className="text-sm text-muted-foreground">
          It is a working example of what you can build by combining VCL,
          Compute and Fastly&apos;s storage services.
        </p>
      </header>

      <ArchitectureDiagram />

      <section aria-labelledby="clock-heading" className="space-y-5">
        <div className="max-w-3xl">
          <h2
            id="clock-heading"
            className="text-xl font-semibold tracking-tight"
          >
            Many probes. One shared fetch.
          </h2>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Health checks arrive from across Fastly&apos;s network every second.
            They should wake the app, not all run the same job. Two layers of
            caching and a Compute-side lease turn that noisy heartbeat into
            coordinated work.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-3">
          <Card>
            <CardHeader>
              <Layers className="size-5 text-primary" aria-hidden="true" />
              <CardTitle className="text-base">
                Collapse before Compute
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
              <p>
                The front VCL service normalizes clock requests to{" "}
                <code>/_tick</code> or <code>/_flush</code>, dropping the query
                string and cookies. Probes reuse the same cache entry instead of
                creating a new one every second.
              </p>
              <p>
                Edge hits stop locally. Misses converge at the IAD shield, where
                concurrent requests for the same cache entry share a single
                in-flight backend fetch. One request reaches Compute; the
                waiters share its response.
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <Clock3 className="size-5 text-primary" aria-hidden="true" />
              <CardTitle className="text-base">
                Let the app set the pace
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
              <p>
                Compute acknowledges the tick before it starts the work, with an{" "}
                <code>x-flp-tick-ttl</code> sleep interval of up to a minute.
                VCL caches the answer until the next wake-up is needed,
                subtracting the response&apos;s shield age so it does not add
                another full sleep interval.
              </p>
              <p>
                While work is due or running, the interval stays at one second.
                When the entry expires, the next matching miss wakes Compute
                again. The health checks keep arriving; most get a cached answer
                rather than starting an application execution.
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <LockKeyhole className="size-5 text-primary" aria-hidden="true" />
              <CardTitle className="text-base">
                Coordinate, then verify
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
              <p>
                On the log shield, the dispatcher serializes updates to a Core
                Cache schedule record and claims a short lease. That hot record
                tells it whether work is due, so an idle tick reads and writes
                no KV at all.
              </p>
              <p>
                Collapsing is not a globally exactly-once guarantee. Cache
                eviction or lease expiry can cause another dispatch. Each job
                also checks its own durable lease, generation or checkpoint
                before advancing state, so a duplicate finds nothing to do.
              </p>
            </CardContent>
          </Card>
        </div>
      </section>

      <section
        aria-labelledby="dispatcher-heading"
        className="grid gap-6 rounded-2xl border bg-muted/20 p-5 sm:p-6 lg:grid-cols-[1fr_1.2fr]"
      >
        <div className="space-y-3">
          <span className="font-mono text-xs text-primary">THE DISPATCHER</span>
          <h2
            id="dispatcher-heading"
            className="text-xl font-semibold tracking-tight"
          >
            Cron behavior, without a cron server.
          </h2>
          <p className="text-sm leading-relaxed text-muted-foreground">
            Compute is request-driven. There is no daemon waiting around between
            ticks, and no shared process memory to rely on. Each wake-up checks
            the schedule, runs a bounded slice of due work and saves progress
            for the next request.
          </p>
          <p className="text-sm leading-relaxed text-muted-foreground">
            Work is bounded by backend calls, cache operations, bytes and wall
            time. Dispatch goes to a peer Compute worker service, not back to
            the application&apos;s own domain. Separate executions give the
            workers their own execution budgets.
          </p>
        </div>
        <dl className="divide-y text-sm">
          <div className="pb-4">
            <dt className="font-semibold">
              Ingestion, on each stream&apos;s schedule
            </dt>
            <dd className="mt-1 leading-relaxed text-muted-foreground">
              Request and optional RUM logs each become due once per log
              period, and the minimum log period is 60 seconds. Shorter periods
              would add files and compaction work without making the dashboard
              fresher. When a pass leaves work pending, ingestion runs again on
              the next tick until it catches up.
            </dd>
          </div>
          <div className="py-4">
            <dt className="font-semibold">
              Raw deletion, after committed ingestion
            </dt>
            <dd className="mt-1 leading-relaxed text-muted-foreground">
              When the logging settings delete raw files, only objects a
              committed plan converted are removed. Files that failed to convert
              are quarantined instead, and only an admin purges them.
            </dd>
          </div>
          <div className="py-4">
            <dt className="font-semibold">
              Compaction, beside ingestion
            </dt>
            <dd className="mt-1 leading-relaxed text-muted-foreground">
              When request-log compaction is on, small window files are merged
              toward a target size, every 30 seconds once ingestion has caught
              up. Column chunks are copied without decoding, and the sources are
              deleted only after a grace period for readers.
            </dd>
          </div>
          <div className="py-4">
            <dt className="font-semibold">Maintenance, one stage at a time</dt>
            <dd className="mt-1 leading-relaxed text-muted-foreground">
              A maintenance turn is eligible at most every two minutes. It walks
              the rotation below from where the last turn stopped, skips stages
              with no work and runs the first one that has some. That is one
              turn every two minutes, not every maintenance job running every
              two minutes.
            </dd>
          </div>
          <div className="pt-4">
            <dt className="font-semibold">
              Operational-log flush, its own wake-up
            </dt>
            <dd className="mt-1 leading-relaxed text-muted-foreground">
              The separate <code>/_flush</code> path archives telemetry in its
              own execution, with its own schedule and lease, so it never
              competes with ingestion for one execution&apos;s budget. When idle
              it wakes at most once a minute; unfinished work continues on the
              next probe.
            </dd>
          </div>
        </dl>
        <div className="space-y-6 lg:col-span-2">
          <CronFanout />
          <div>
            <h3 className="font-semibold">The maintenance rotation</h3>
            <dl className="mt-3 grid gap-x-6 gap-y-4 sm:grid-cols-2 lg:grid-cols-3">
              {maintenance.map(({ name, description }) => (
                <div key={name}>
                  <dt className="text-sm font-medium">{name}</dt>
                  <dd className="mt-1 text-sm leading-relaxed text-muted-foreground">
                    {description}
                  </dd>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </section>

      <section aria-labelledby="capacity-heading" className="space-y-4">
        <div className="max-w-3xl">
          <h2
            id="capacity-heading"
            className="text-xl font-semibold tracking-tight"
          >
            Bounded work, not unlimited scale.
          </h2>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Every execution works inside fixed limits and stops early enough to
            save its progress.
          </p>
        </div>
        <dl className="grid gap-4 sm:grid-cols-3">
          <div className="rounded-xl border bg-card p-5">
            <dt className="text-xs font-medium text-muted-foreground">
              Backend calls per execution
            </dt>
            <dd className="mt-2 text-3xl font-semibold tracking-tight">32</dd>
            <dd className="mt-1 text-xs text-muted-foreground">
              Object Storage, KV and worker requests all count.
            </dd>
          </div>
          <div className="rounded-xl border bg-card p-5">
            <dt className="text-xs font-medium text-muted-foreground">
              Workers per ingestion pass
            </dt>
            <dd className="mt-2 text-3xl font-semibold tracking-tight">
              Up to 8
            </dd>
            <dd className="mt-1 text-xs text-muted-foreground">
              Each shard holds up to 22 raw files and 8 MiB compressed.
            </dd>
          </div>
          <div className="rounded-xl border bg-card p-5">
            <dt className="text-xs font-medium text-muted-foreground">
              New work stops after
            </dt>
            <dd className="mt-2 text-3xl font-semibold tracking-tight">40 s</dd>
            <dd className="mt-1 text-xs text-muted-foreground">
              The rest of the execution is kept for saving progress.
            </dd>
          </div>
        </dl>
        <p className="text-sm leading-relaxed text-muted-foreground">
          These are design limits, not a throughput guarantee. Log width,
          compression and time-window spread affect how much work each batch
          needs. When work does not fit, the pipeline checkpoints and continues;
          if arrivals outpace processing, backlog and freshness lag grow. Use
          ingestion rows, bytes, run duration and pending work in the
          installation&apos;s logs to judge whether it is keeping up.
        </p>
      </section>

      <section aria-labelledby="storage-heading" className="space-y-5">
        <div className="max-w-3xl">
          <h2
            id="storage-heading"
            className="text-xl font-semibold tracking-tight"
          >
            Cache is fast. KV remembers.
          </h2>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Not all state needs the same durability. The project keeps hot,
            replaceable data in cache, correctness-critical state in KV, and
            analytical history in Object Storage.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">
                Core Cache: the hot layer
              </CardTitle>
              <Badge variant="secondary">Fast, bounded, evictable</Badge>
            </CardHeader>
            <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
              <p>
                The Cache API holds the dispatcher&apos;s schedule mirror and
                recent API, storage-usage and platform-error telemetry in
                sharded minute buckets at the log shield. Rows buffer during a
                request and append in batches instead of making a cache call per
                event.
              </p>
              <p>
                This is not the CDN response cache, and it is not durable
                storage. Buckets expire after a day and can be evicted sooner;
                eviction before archival loses that cache-tier telemetry. An
                evicted schedule is rebuilt from KV.
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle className="text-base">KV: the durable layer</CardTitle>
              <Badge variant="secondary">
                Checkpoints, outboxes, account state
              </Badge>
            </CardHeader>
            <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
              <p>
                KV stores users and roles, ingestion plans and watermarks, job
                progress and the durable cron schedule. Cron-run and
                ingestion-history rows go into batched KV outboxes; audit events
                use their own durable outboxes rather than relying on an
                evictable cache entry.
              </p>
              <p>
                Generation-matched writes protect state transitions from
                competing writers. Batching avoids turning every operational
                event into an individual KV write, while checkpoints let later
                executions pick up the work.
              </p>
            </CardContent>
          </Card>
        </div>
        <div className="rounded-xl border border-dashed p-5 text-sm leading-relaxed">
          <h3 className="font-semibold">
            The app logs its own work, then queries it.
          </h3>
          <p className="mt-2 text-muted-foreground">
            The flush reads cache buckets and durable outboxes, groups events by
            kind and writes operational Parquet files to Object Storage. Cron
            runs, ingestion history, API calls, storage operations, audit events
            and platform errors become explorable history for admins, not just
            lines disappearing into a terminal.
          </p>
        </div>
      </section>

      <section aria-labelledby="pipeline-heading" className="space-y-4">
        <div>
          <h2
            id="pipeline-heading"
            className="text-xl font-semibold tracking-tight"
          >
            From request to insight
          </h2>
          <p className="mt-2 text-sm text-muted-foreground">
            The result is a file-based analytics pipeline, not a database server
            hidden behind a dashboard.
          </p>
        </div>
        <div className="grid gap-4 lg:grid-cols-3">
          {pipeline.map(({ title, icon: Icon, description }, index) => (
            <Card key={title}>
              <CardHeader>
                <div className="flex items-center gap-3 text-muted-foreground">
                  <Icon className="size-5" aria-hidden="true" />
                  <span className="text-xs font-mono">0{index + 1}</span>
                  {index < pipeline.length - 1 && (
                    <ArrowRight className="size-4 ml-auto" aria-hidden="true" />
                  )}
                </div>
                <CardTitle className="text-base">{title}</CardTitle>
              </CardHeader>
              <CardContent className="text-sm leading-relaxed text-muted-foreground">
                {description}
              </CardContent>
            </Card>
          ))}
        </div>
      </section>

      <section aria-labelledby="browser-heading" className="space-y-4">
        <div className="max-w-3xl">
          <h2
            id="browser-heading"
            className="text-xl font-semibold tracking-tight"
          >
            The browser is the query engine.
          </h2>
          <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
            Dashboards run where the question is asked, inside a fixed memory
            budget, and only download what they need.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Checked at the edge</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
              <p>
                Signing in sets a signed, expiring cookie tied to the session
                and role. The front VCL service verifies it and fetches
                authorized files from Object Storage with signed requests,
                caching them at the edge.
              </p>
              <p>
                Analysts without PII access cannot download raw files, and
                operational logs are admin-only.
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Read less, keep more</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
              <p>
                DuckDB-WASM runs in Web Workers with a 256 MB memory limit, so
                a large query cannot starve the tab. Parquet footers let the
                engine request only the byte ranges of the columns it needs.
              </p>
              <p>
                Hour and day bundles answer longer ranges with fewer downloads.
                Files and filtered results are cached locally, scoped to the
                signed-in user and cleared when that changes.
              </p>
            </CardContent>
          </Card>
        </div>
      </section>

      <Card>
        <CardHeader>
          <CardTitle>Setup and account access</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4 text-sm">
          <p>
            The front VCL service routes app requests to Fastly Compute. The
            first deploy prints a one-time admin setup code, which the first
            admin uses to register a passkey. Admins then invite users, give
            them a role (admin, analyst, or analyst without PII access), check
            the installation and configure logging for one service.
          </p>
          <p className="text-muted-foreground">
            Invitations expire after seven days and sessions after twelve hours.
            Credentials stay in a Fastly Secret Store. Each Compute request is
            independent; durable configuration and account state live in
            Fastly&apos;s stores, not a long-running application process.
          </p>
          <Link
            href="/admin"
            className="inline-flex items-center gap-2 underline underline-offset-4"
          >
            Open admin
            <ArrowRight className="size-4" aria-hidden="true" />
          </Link>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Understand the costs</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3 text-sm text-muted-foreground">
          <p>
            Costs come from Object Storage operations and capacity, Compute
            requests and vCPU on both the app and worker services, KV
            operations, Secret Store and Config Store, and CDN delivery. There
            is no ClickHouse cluster or external database to price.
          </p>
          <p>
            Admins see measured counts on the Usage page: Object Storage Class
            A and Class B operations, CDN hits, Compute requests and vCPU time,
            and KV operations. Storage is estimated from a ledger. Writing,
            listing and deleting objects count as Class A; reads count as Class
            B, and a CDN hit costs no Object Storage operation. Compacted-away
            files still bill Object Storage&apos;s 30-day minimum. Usage is not
            an invoice.
          </p>
          <p>
            The Cost Estimator extrapolates a month from the logged
            service&apos;s traffic and logging settings, such as requests a day,
            sample rate, log period and retention, priced at the
            installation&apos;s configured rates. Admins can fill it from
            measured values.
          </p>
          <Link
            href="/usage"
            className="inline-flex items-center gap-2 text-foreground underline underline-offset-4"
          >
            Open the cost estimator
            <ArrowRight className="size-4" aria-hidden="true" />
          </Link>
        </CardContent>
      </Card>

      <section className="space-y-4 border-t pt-8 pb-4">
        <h2 className="text-xl font-semibold tracking-tight">
          Build the product, not the server fleet.
        </h2>
        <p className="max-w-3xl text-sm leading-relaxed text-muted-foreground">
          The useful trick is the composition: let VCL absorb duplicate traffic,
          let Compute do bounded work, keep durable progress in stores and move
          interactive analysis close to the person asking the questions. These
          are building blocks for your own product, not just a logging tool.
        </p>
        <div className="flex flex-wrap gap-x-6 gap-y-3 text-sm">
          <a
            href="https://www.fastly.com/documentation/guides/compute/"
            className="inline-flex items-center gap-2 font-medium underline underline-offset-4"
          >
            Start building with Compute
            <ArrowRight className="size-4" aria-hidden="true" />
          </a>
          <a
            href="https://docs.fastly.com/products/compute-resource-limits"
            className="text-muted-foreground underline underline-offset-4"
          >
            Design within the execution limits
          </a>
        </div>
        <p className="text-xs text-muted-foreground">
          This page explains the implementation, not a capacity benchmark or a
          guarantee of ingestion freshness. Check the installation&apos;s logs,
          health and measured usage for its current behavior.
        </p>
      </section>
    </div>
  );
}
