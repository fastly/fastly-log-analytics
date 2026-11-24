import Link from "next/link";
import {
  Archive,
  ArrowRight,
  Clock,
  Database,
  Gauge,
  HardDrive,
  Layers,
  ListChecks,
  RotateCcw,
  Scale,
  Search,
  Server,
  ShieldCheck,
  Sparkles,
  Timer,
  Workflow,
  type LucideIcon,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { HighScaleArchitectureDiagram } from "./ArchitectureDiagram";

const linkClass = "font-medium text-foreground underline underline-offset-4";

const pipeline: { name: string; icon: LucideIcon; body: string }[] = [
  {
    name: "Capture",
    icon: HardDrive,
    body: "Your Fastly service logs every request as JSON and writes a compressed file (zstd by default) to Fastly Object Storage once per log period, 10 seconds by default. RUM beacons follow the same path on a 5-minute period.",
  },
  {
    name: "Ingest",
    icon: Workflow,
    body: "Workers list new files, record them in a PostgreSQL ledger, and claim a page of up to 100 under a lease. A crashed worker’s lease expires and another worker picks the files up.",
  },
  {
    name: "Archive",
    icon: Archive,
    body: "Each batch is decoded in memory and written as one merged Parquet file. The worker verifies its checksum, then commits a manifest that records which raw files it holds.",
  },
  {
    name: "Load & publish",
    icon: Database,
    body: "Rows are bulk-inserted into ClickHouse as pending. Once the row count matches, the batch becomes visible, and only then is the work acknowledged in PostgreSQL.",
  },
  {
    name: "Query",
    icon: Search,
    body: "The FastAPI backend reads only visible batches from ClickHouse and returns ready-to-chart responses to the pages in your browser.",
  },
];

const highScaleTraits: { title: string; icon: LucideIcon; body: string }[] = [
  {
    title: "Batched pages",
    icon: Layers,
    body: "Small log files are merged into one Parquet archive per batch (10 files or 6 MiB by default on the standard worker pod), so object writes, checksum checks and manifests are paid once per batch instead of once per file.",
  },
  {
    title: "Leased, fenced claims",
    icon: ListChecks,
    body: "Claims carry a lease (5 minutes by default) and an owner epoch, so two workers never publish the same file. A weighted selector keeps one busy service from starving the rest.",
  },
  {
    title: "Scaling on backlog",
    icon: Gauge,
    body: "With KEDA enabled, worker replicas scale from 1 to 10 based on how many discovered files are waiting: one replica per 100, the size of a page.",
  },
  {
    title: "Archive-first recovery",
    icon: RotateCcw,
    body: "The Parquet archive is the source of truth. A failed ClickHouse insert is retried from the archive, and ClickHouse can be rebuilt from archive manifests without the raw logs.",
  },
  {
    title: "Rollups at insert time",
    icon: Sparkles,
    body: "Materialized views keep minute and hourly aggregates as rows arrive, so long dashboard windows and Insights baselines read pre-summed rows instead of every request.",
  },
  {
    title: "Room to grow",
    icon: Scale,
    body: "Workers are stateless and scale horizontally. ClickHouse uses replicated tables and can run with more replicas and a Keeper quorum when one node is not enough.",
  },
];

const publicationSteps: { term: string; detail: string }[] = [
  {
    term: "Gate",
    detail:
      "A batch inserted into ClickHouse starts as pending. Pages filter on published batches only, so a half-loaded batch is never counted.",
  },
  {
    term: "Retry",
    detail:
      "If publication fails after the archive is committed, the worker retries from the checksum-verified archive under the original batch ids, backing off up to an hour per manifest. The raw file no longer needs to exist.",
  },
  {
    term: "Reap",
    detail:
      "A publication reaper retires pending manifests whose rows were already republished under a replacement batch. It only changes PostgreSQL bookkeeping.",
  },
  {
    term: "Replay",
    detail:
      "Recovery replays archive manifests into ClickHouse, the most recent 24 hours first and then the rest of the 30-day window.",
  },
  {
    term: "Delete raw",
    detail:
      "A raw log file is deleted only after its manifest is committed, the work is acknowledged and a 15-minute grace period has passed.",
  },
];

const dataStores: {
  name: string;
  icon: LucideIcon;
  holds: string;
  keeps: string;
}[] = [
  {
    name: "Raw log files",
    icon: HardDrive,
    holds: "Compressed JSON from Fastly, in Fastly Object Storage.",
    keeps:
      "Deleted once their batch is archived and acknowledged and a 15-minute grace period has passed.",
  },
  {
    name: "Parquet archive",
    icon: Archive,
    holds:
      "Merged, checksum-verified batches with manifests, in Fastly Object Storage.",
    keeps:
      "The service’s data retention (30 days by default; RUM can have its own) plus a one-day margin.",
  },
  {
    name: "ClickHouse",
    icon: Database,
    holds: "Request and RUM facts, minute aggregates and hourly rollups.",
    keeps:
      "30 days for facts and minute aggregates; 90 days for hourly request aggregates.",
  },
  {
    name: "PostgreSQL",
    icon: ListChecks,
    holds:
      "The file ledger, leases and manifests, plus app state such as alerts, saved views and share invites.",
    keeps: "Bookkeeping for the pipeline; no request rows.",
  },
];

const queryPaths: { title: string; icon: LucideIcon; body: string }[] = [
  {
    title: "Dashboards",
    icon: Layers,
    body: "Windows of a day or more read whole hours from the hourly rollup and only the partial edges from minute aggregates. Multi-panel pages fetch one composite endpoint instead of one call per chart.",
  },
  {
    title: "Insights",
    icon: Sparkles,
    body: "Detections compare against a 7-day baseline. Plain count detections use an hourly rollup filled at insert time, but only for hours whose counts match the raw facts, so results equal a raw-only query. The page shows the last result at once while a background evaluation refreshes it.",
  },
  {
    title: "Raw rows and SQL",
    icon: Search,
    body: "Row browsers return at most 500 rows per page with signed keyset cursors. The SQL console runs read-only against per-service views that already apply the publication gate, with memory and result-size caps.",
  },
];

export function HighScaleHowItWorks() {
  return (
    <div className="mx-auto max-w-6xl space-y-12 px-4 py-8 sm:px-6 lg:py-12">
      <header className="max-w-3xl space-y-4">
        <h1 className="text-sm font-semibold text-primary">How it works</h1>
        <p className="text-sm text-muted-foreground">
          This page explains the High-Scale (Kubernetes) deployment of Fastly
          Log Analytics.
        </p>
        <p className="text-3xl font-bold tracking-tight sm:text-5xl">
          Archive every log, then publish it.
        </p>
        <p className="text-base leading-relaxed text-muted-foreground">
          Your Fastly service streams request logs to Fastly Object Storage.
          Workers in your Kubernetes cluster batch those files into a Parquet
          archive, load them into ClickHouse, and publish each batch only after
          it checks out. The pages you use read from ClickHouse through a
          FastAPI backend.
        </p>
        <p className="text-base leading-relaxed text-muted-foreground">
          Because the archive is written first, a failed load can be retried and
          ClickHouse can be rebuilt from it without the raw logs.
        </p>
      </header>

      <HighScaleArchitectureDiagram />

      <section aria-labelledby="hs-pipeline" className="space-y-4">
        <div className="space-y-1">
          <h2 id="hs-pipeline" className="text-xl font-semibold tracking-tight">
            The pipeline
          </h2>
          <p className="text-sm text-muted-foreground">
            Five steps take a request from the edge to a chart.
          </p>
        </div>
        <ol className="grid gap-3 lg:grid-cols-[1fr_auto_1fr_auto_1fr_auto_1fr_auto_1fr]">
          {pipeline.map(({ name, icon: Icon, body }, index) => (
            <li key={name} className="contents">
              {index > 0 ? (
                <ArrowRight
                  className="hidden size-4 self-center text-muted-foreground lg:block"
                  aria-hidden="true"
                />
              ) : null}
              <Card className="h-full">
                <CardHeader>
                  <span className="font-mono text-xs text-muted-foreground">
                    0{index + 1}
                  </span>
                  <CardTitle className="flex items-center gap-2">
                    <Icon className="size-4 text-primary" aria-hidden="true" />
                    {name}
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm leading-relaxed text-muted-foreground">
                  {body}
                </CardContent>
              </Card>
            </li>
          ))}
        </ol>
      </section>

      <section aria-labelledby="hs-scale" className="space-y-4">
        <div className="space-y-1">
          <h2 id="hs-scale" className="text-xl font-semibold tracking-tight">
            What makes it high-scale
          </h2>
          <p className="text-sm text-muted-foreground">
            The work is split so that each piece can grow on its own.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-3">
          {highScaleTraits.map(({ title, icon: Icon, body }) => (
            <Card key={title}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Icon className="size-4 text-primary" aria-hidden="true" />
                  {title}
                </CardTitle>
              </CardHeader>
              <CardContent className="text-sm leading-relaxed text-muted-foreground">
                {body}
              </CardContent>
            </Card>
          ))}
        </div>
      </section>

      <section
        aria-labelledby="hs-publication"
        className="rounded-2xl border bg-muted/20 p-5 sm:p-6"
      >
        <p className="font-mono text-xs font-medium text-primary">
          THE PUBLICATION GATE
        </p>
        <h2
          id="hs-publication"
          className="mt-2 text-xl font-semibold tracking-tight"
        >
          Archive first. Publish second.
        </h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Every batch goes through the same order: archive write, checksum
          check, manifest commit, ClickHouse insert, count check, visible,
          acknowledged.
        </p>
        <dl className="mt-5 grid gap-4 sm:grid-cols-2 lg:grid-cols-5">
          {publicationSteps.map(({ term, detail }) => (
            <div key={term} className="space-y-1">
              <dt className="text-sm font-semibold">{term}</dt>
              <dd className="text-sm leading-relaxed text-muted-foreground">
                {detail}
              </dd>
            </div>
          ))}
        </dl>
      </section>

      <section aria-labelledby="hs-data" className="space-y-4">
        <div className="space-y-1">
          <h2 id="hs-data" className="text-xl font-semibold tracking-tight">
            Where data lives and for how long
          </h2>
          <p className="text-sm text-muted-foreground">
            Defaults shown. Retention is set per service, and an archive
            retention of 0 keeps the archive forever.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {dataStores.map(({ name, icon: Icon, holds, keeps }) => (
            <Card key={name}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Icon className="size-4 text-primary" aria-hidden="true" />
                  {name}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
                <p>{holds}</p>
                <p className="flex items-start gap-2">
                  <Clock
                    className="mt-0.5 size-4 shrink-0"
                    aria-hidden="true"
                  />
                  <span>{keeps}</span>
                </p>
              </CardContent>
            </Card>
          ))}
        </div>
      </section>

      <section aria-labelledby="hs-query" className="space-y-4">
        <div className="space-y-1">
          <h2 id="hs-query" className="text-xl font-semibold tracking-tight">
            How pages query it
          </h2>
          <p className="text-sm text-muted-foreground">
            Every page goes through the backend API; the browser never talks to
            ClickHouse.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-3">
          {queryPaths.map(({ title, icon: Icon, body }) => (
            <Card key={title}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Icon className="size-4 text-primary" aria-hidden="true" />
                  {title}
                </CardTitle>
              </CardHeader>
              <CardContent className="text-sm leading-relaxed text-muted-foreground">
                {body}
              </CardContent>
            </Card>
          ))}
        </div>
      </section>

      <section aria-labelledby="hs-freshness" className="space-y-4">
        <div className="space-y-1">
          <h2
            id="hs-freshness"
            className="text-xl font-semibold tracking-tight"
          >
            Freshness
          </h2>
          <p className="text-sm text-muted-foreground">
            How long a request takes to show up depends mostly on the log
            period.
          </p>
        </div>
        <Card>
          <CardContent className="grid gap-4 pt-6 text-sm leading-relaxed text-muted-foreground md:grid-cols-3">
            <p className="flex items-start gap-2">
              <Timer
                className="mt-0.5 size-4 shrink-0 text-primary"
                aria-hidden="true"
              />
              <span>
                <strong className="text-foreground">Log period.</strong> Fastly
                writes a file every 10 seconds by default for request logs and
                every 5 minutes for RUM.
              </span>
            </p>
            <p className="flex items-start gap-2">
              <Workflow
                className="mt-0.5 size-4 shrink-0 text-primary"
                aria-hidden="true"
              />
              <span>
                <strong className="text-foreground">Ingest.</strong> Each worker
                loop checks for new files about every 2 seconds and goes
                straight to the next page when the last one was full.
              </span>
            </p>
            <p className="flex items-start gap-2">
              <Server
                className="mt-0.5 size-4 shrink-0 text-primary"
                aria-hidden="true"
              />
              <span>
                <strong className="text-foreground">Visible.</strong> The header
                shows the newest request time. The backend checks for new data
                about every 15 seconds and pushes changes to open pages, and a
                health check flags a service whose last ingest is older than 30
                minutes by default.
              </span>
            </p>
          </CardContent>
        </Card>
      </section>

      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <ShieldCheck className="size-4 text-primary" aria-hidden="true" />
              Setup and access
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
            <div className="flex flex-wrap gap-2">
              <Badge variant="secondary">Helm chart</Badge>
              <Badge variant="secondary">PostgreSQL 16</Badge>
              <Badge variant="secondary">ClickHouse</Badge>
            </div>
            <p>
              An admin connects a Fastly service from{" "}
              <Link href="/admin" className={linkClass}>
                Admin
              </Link>
              . The setup wizard creates an opaque-named Object Storage bucket,
              a bucket-scoped access key, an opaque-named delivery service and
              the logging endpoint, and rolls back if a step fails.
            </p>
            <p>
              Admins have full access. Analysts join through a share invite from{" "}
              <Link href="/admin/share" className={linkClass}>
                Admin › Share
              </Link>
              : read-only, limited to the invited services and time range, with
              client IPs masked when the invite says so. The backend enforces
              these limits, not the page.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Gauge className="size-4 text-primary" aria-hidden="true" />
              Costs at a glance
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm leading-relaxed text-muted-foreground">
            <div className="flex flex-wrap gap-2">
              <Badge variant="secondary">Object Storage</Badge>
              <Badge variant="secondary">Class A / B operations</Badge>
              <Badge variant="secondary">CDN bandwidth</Badge>
            </div>
            <p>
              The{" "}
              <Link href="/usage" className={linkClass}>
                Usage
              </Link>{" "}
              page measures Object Storage operations, CDN bandwidth, and log
              and RUM activity, computes service-local billed GB-hours directly
              from PostgreSQL&apos;s source-object and archive-manifest ledgers
              (including live objects, deleted objects and Object Storage&apos;s
              30-day minimum), and estimates cost from rates you can edit.
            </p>
            <p>
              It does not price your Kubernetes cluster: ClickHouse, PostgreSQL
              and the app pods are yours to size and pay for.
            </p>
          </CardContent>
        </Card>
      </div>

      <section className="space-y-3 border-t pt-8 text-sm leading-relaxed text-muted-foreground">
        <p>
          Learn more about{" "}
          <a
            href="https://www.fastly.com/documentation/guides/integrations/logging-endpoints/object-and-cloud-storage/log-streaming-fastly-object-storage/"
            className={linkClass}
            rel="noreferrer"
            target="_blank"
          >
            log streaming to Fastly Object Storage
          </a>
          ,{" "}
          <a
            href="https://www.fastly.com/documentation/guides/integrations/streaming-logs/about-fastlys-realtime-log-streaming-features/"
            className={linkClass}
            rel="noreferrer"
            target="_blank"
          >
            real-time log streaming
          </a>{" "}
          and{" "}
          <a
            href="https://docs.fastly.com/products/object-storage"
            className={linkClass}
            rel="noreferrer"
            target="_blank"
          >
            Fastly Object Storage
          </a>
          .
        </p>
        <p className="text-xs">
          Defaults describe the shipped Helm chart and backend settings. Your
          install may override them, and nothing here is a capacity benchmark.
        </p>
      </section>
    </div>
  );
}
