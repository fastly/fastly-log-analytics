import {
  Archive,
  ArrowDown,
  ArrowRight,
  Database,
  Gauge,
  Globe,
  HardDrive,
  KeyRound,
  ListChecks,
  Monitor,
  Network,
  Server,
  Users,
  Workflow,
  type LucideIcon,
} from "lucide-react";

function DiagramNode({
  icon: Icon,
  title,
  detail,
  highlighted = false,
}: {
  icon: LucideIcon;
  title: string;
  detail: string;
  highlighted?: boolean;
}) {
  return (
    <div
      className={`relative h-full rounded-xl border p-4 ${
        highlighted ? "border-primary/40 bg-primary/5" : "border-border bg-card"
      }`}
    >
      <Icon className="mb-3 size-5 text-primary" aria-hidden="true" />
      <p className="text-sm font-semibold">{title}</p>
      <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
        {detail}
      </p>
    </div>
  );
}

function Connector({ label }: { label: string }) {
  return (
    <div className="flex items-center justify-center gap-2 py-2 text-muted-foreground lg:flex-col lg:px-1">
      <ArrowRight className="hidden size-4 lg:block" aria-hidden="true" />
      <ArrowDown className="size-4 lg:hidden" aria-hidden="true" />
      <span className="text-center font-mono text-[10px]">{label}</span>
    </div>
  );
}

const supportingPieces: { icon: LucideIcon; name: string; detail: string }[] = [
  {
    icon: ListChecks,
    name: "PostgreSQL 16",
    detail:
      "Control plane: the file ledger, leases, archive manifests and app metadata.",
  },
  {
    icon: Gauge,
    name: "KEDA (optional)",
    detail:
      "Scales worker replicas on the count of discovered, unclaimed files.",
  },
  {
    icon: Network,
    name: "ClickHouse Keeper (optional)",
    detail:
      "Coordinates ClickHouse replicas when you run more than one. Three members when enabled.",
  },
  {
    icon: KeyRound,
    name: "Admin gateway (optional)",
    detail:
      "Requires an admin client certificate (mTLS) before admin traffic reaches the app.",
  },
];

export function HighScaleArchitectureDiagram() {
  return (
    <figure
      aria-label="High-Scale deployment architecture"
      className="overflow-hidden rounded-2xl border bg-muted/20"
    >
      <div className="border-b px-5 py-4 sm:px-6">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="font-semibold">From edge log to answer.</h2>
          <span className="font-mono text-xs text-muted-foreground">
            Fastly + Object Storage + Kubernetes
          </span>
        </div>
      </div>

      <div className="space-y-6 p-5 sm:p-6">
        <div>
          <p className="mb-3 font-mono text-xs font-medium text-primary">
            CAPTURE / Edge traffic becomes log files
          </p>
          <div className="grid lg:grid-cols-[1fr_48px_1fr_48px_1fr_48px_1fr]">
            <DiagramNode
              icon={Users}
              title="Visitors"
              detail="Requests reach your Fastly service. With RUM on, the Faro SDK in your pages also posts beacons to /rum-beacon."
            />
            <Connector label="requests" />
            <DiagramNode
              icon={Globe}
              title="Your Fastly service"
              detail="A logging endpoint writes compressed JSON request logs each log period. VCL answers beacons with a 204 at the edge and logs them separately."
            />
            <Connector label="log period" />
            <DiagramNode
              icon={HardDrive}
              title="Fastly Object Storage"
              detail="Raw files land under raw/request/ and raw/rum/ in the install's bucket."
            />
            <Connector label="list + claim" />
            <DiagramNode
              icon={Workflow}
              title="Ingest workers"
              detail="Stateless pods lease batches of new files from the PostgreSQL ledger and decode them in memory."
              highlighted
            />
          </div>
        </div>

        <div className="border-t pt-6">
          <p className="mb-3 font-mono text-xs font-medium text-primary">
            LOAD + SERVE / Archive first, then publish
          </p>
          <div className="grid lg:grid-cols-[1fr_48px_1fr_48px_1fr_48px_1fr]">
            <DiagramNode
              icon={Archive}
              title="Parquet archive"
              detail="One merged, checksum-verified Parquet file per batch in Fastly Object Storage, recorded in a manifest."
            />
            <Connector label="then insert" />
            <DiagramNode
              icon={Database}
              title="ClickHouse"
              detail="Rows land as pending. After a count check the batch flips to visible."
              highlighted
            />
            <Connector label="visible only" />
            <DiagramNode
              icon={Server}
              title="Backend API"
              detail="FastAPI reads published facts, aggregates and rollups, then shapes each page's response."
            />
            <Connector label="/api" />
            <DiagramNode
              icon={Monitor}
              title="Your browser"
              detail="Next.js pages: dashboard, Insights, RUM, sessions, SQL console and more."
            />
          </div>
        </div>

        <div className="grid gap-3 border-t pt-5 text-xs sm:grid-cols-2 lg:grid-cols-4">
          {supportingPieces.map(({ icon: Icon, name, detail }) => (
            <p key={name} className="flex items-start gap-2">
              <Icon
                className="mt-0.5 size-4 shrink-0 text-primary"
                aria-hidden="true"
              />
              <span>
                <strong>{name}</strong>
                <span className="mt-1 block text-muted-foreground">
                  {detail}
                </span>
              </span>
            </p>
          ))}
        </div>
      </div>
      <figcaption className="border-t bg-card px-5 py-4 text-xs leading-relaxed text-muted-foreground sm:px-6">
        Arrows show logical flow, not network hops. A worker writes the archive
        before it inserts into ClickHouse, and pages only read batches that
        passed the publication gate.
      </figcaption>
    </figure>
  );
}
