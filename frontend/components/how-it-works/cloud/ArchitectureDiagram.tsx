import {
  Blocks,
  Braces,
  Database,
  Globe,
  Layers,
  Monitor,
  Radio,
  Workflow,
} from "lucide-react";

import { Connector, DiagramNode } from "../DiagramParts";

const PROBES = ["a", "b", "c", "d", "e"] as const;

export function ArchitectureDiagram() {
  return (
    <figure
      aria-label="Fastly Log Analytics Cloud architecture"
      className="overflow-hidden rounded-2xl border bg-muted/20"
    >
      <div className="border-b px-5 py-4 sm:px-6">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="font-semibold">One platform. Two connected flows.</h2>
          <span className="font-mono text-xs text-muted-foreground">
            VCL + Compute + Fastly stores
          </span>
        </div>
      </div>

      <div className="space-y-6 p-5 sm:p-6">
        <div>
          <p className="mb-3 font-mono text-xs font-medium text-primary">
            CONTROL / Wake up only when there is work
          </p>
          <div className="grid lg:grid-cols-[1fr_48px_1fr_48px_1fr_48px_1fr]">
            <DiagramNode
              icon={Radio}
              title="Health-check probes"
              detail="Fastly's health checks supply the heartbeat — set to half the shortest log period, up to 30 seconds (30 s at the 60 s minimum). No external scheduler."
            />
            <Connector label="many" />
            <div className="relative rounded-xl border border-primary/40 bg-primary/5 p-4">
              <div aria-hidden="true" className="mb-3">
                <div className="flex justify-between gap-1">
                  {PROBES.map((probe) => (
                    <span
                      key={probe}
                      className="h-1.5 w-4 rounded-full bg-primary/50"
                    />
                  ))}
                </div>
                <svg
                  viewBox="0 0 160 28"
                  fill="none"
                  className="h-7 w-full text-primary/50"
                >
                  <path
                    d="M8 0L80 25M44 0L80 25M80 0V25M116 0L80 25M152 0L80 25"
                    stroke="currentColor"
                  />
                </svg>
                <div className="mx-auto h-1.5 w-4 rounded-full bg-primary" />
              </div>
              <p className="text-sm font-semibold">VCL edge + IAD shield</p>
              <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                Cache hits stop here. Matching misses share one backend fetch.
              </p>
            </div>
            <Connector label="one fetch" />
            <DiagramNode
              icon={Workflow}
              title="Compute dispatcher"
              detail="Claims a schedule lease, selects the jobs that are due and coordinates ingestion."
              highlighted
            />
            <Connector label="dispatch" />
            <DiagramNode
              icon={Blocks}
              title="Peer Compute workers"
              detail="A separate worker service runs bounded conversion, compaction and maintenance, then reports back."
            />
          </div>
        </div>

        <div className="border-t pt-6">
          <p className="mb-3 font-mono text-xs font-medium text-primary">
            DATA / Turn edge traffic into answers
          </p>
          <div className="grid lg:grid-cols-[1fr_48px_1fr_48px_1fr_48px_1fr]">
            <DiagramNode
              icon={Globe}
              title="Your Fastly service"
              detail="Streams JSON request logs, zstd-compressed by default. RUM is an optional second stream."
            />
            <Connector label="stream" />
            <DiagramNode
              icon={Database}
              title="Fastly Object Storage"
              detail="Raw log files wait here for the next ingestion pass."
            />
            <Connector label="workers" />
            <DiagramNode
              icon={Layers}
              title="Parquet in Object Storage"
              detail="Workers write time-windowed Parquet files with a summary beside each one. Maintenance merges summaries into hour and day bundles."
            />
            <Connector label="via CDN" />
            <DiagramNode
              icon={Monitor}
              title="Your browser"
              detail="Edge-checked file access, local caching and DuckDB-WASM analytics in Web Workers."
            />
          </div>
        </div>

        <div className="grid gap-3 border-t pt-5 text-xs sm:grid-cols-3">
          <p className="flex items-start gap-2">
            <Layers
              className="mt-0.5 size-4 shrink-0 text-primary"
              aria-hidden="true"
            />
            <span>
              <strong>Core Cache</strong>
              <span className="mt-1 block text-muted-foreground">
                Hot schedules and recent telemetry.
              </span>
            </span>
          </p>
          <p className="flex items-start gap-2">
            <Database
              className="mt-0.5 size-4 shrink-0 text-primary"
              aria-hidden="true"
            />
            <span>
              <strong>KV Store</strong>
              <span className="mt-1 block text-muted-foreground">
                Durable state, checkpoints and log outboxes.
              </span>
            </span>
          </p>
          <p className="flex items-start gap-2">
            <Braces
              className="mt-0.5 size-4 shrink-0 text-primary"
              aria-hidden="true"
            />
            <span>
              <strong>Secret Store</strong>
              <span className="mt-1 block text-muted-foreground">
                Runtime credentials, outside the app bundle.
              </span>
            </span>
          </p>
        </div>
      </div>
      <figcaption className="border-t bg-card px-5 py-4 text-xs leading-relaxed text-muted-foreground sm:px-6">
        The clock coordinates the pipeline; logs travel through Object Storage.
        Arrows show logical flow, not one long-running Compute process. Request
        collapsing applies to matching cacheable requests, not every API call.
      </figcaption>
    </figure>
  );
}
