import { ArrowDown, GitBranch, Save } from "lucide-react";

const SHARDS = [1, 2, 3, 4, 5, 6, 7, 8] as const;

export function CronFanout() {
  return (
    <figure
      aria-label="Ingestion worker fan-out"
      className="space-y-4 rounded-xl border bg-card p-4 sm:p-5"
    >
      <div className="flex items-start gap-3">
        <GitBranch
          className="mt-0.5 size-5 shrink-0 text-primary"
          aria-hidden="true"
        />
        <div>
          <h3 className="font-semibold">One coordinator, parallel workers.</h3>
          <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
            The coordinator saves a plan, packs raw files into shards of up to
            22 objects and 8 MiB compressed, and sends up to eight asynchronous
            ingestion requests to the peer Compute service. These are separate
            executions, not eight always-on servers.
          </p>
        </div>
      </div>
      <div className="flex justify-center text-primary" aria-hidden="true">
        <ArrowDown className="size-5" />
      </div>
      <ol className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-8">
        {SHARDS.map((shard) => (
          <li
            key={shard}
            className="rounded-lg border border-primary/30 bg-primary/5 px-2 py-3 text-center font-mono text-xs font-medium"
          >
            Shard {shard}
          </li>
        ))}
      </ol>
      <p className="text-center text-xs text-muted-foreground">
        Each worker reads its files, converts rows, and writes Parquet, window
        summaries and an intent manifest. A worker that runs short of memory or
        budget asks for its shard to be split.
      </p>
      <div className="rounded-lg border border-dashed p-3 text-sm">
        <span className="font-semibold">
          + Compaction and maintenance, when due
        </span>
        <p className="mt-1 leading-relaxed text-muted-foreground">
          Request-log compaction and one maintenance stage ride along in the
          same parallel dispatch batch, each in its own worker execution with
          its own budget.
        </p>
      </div>
      <div className="flex justify-center text-primary" aria-hidden="true">
        <ArrowDown className="size-5" />
      </div>
      <div className="flex items-start gap-3 rounded-lg bg-muted/40 p-3">
        <Save
          className="mt-0.5 size-4 shrink-0 text-primary"
          aria-hidden="true"
        />
        <p className="text-sm leading-relaxed">
          <strong>Collect results. Commit once. Resume.</strong>{" "}
          <span className="text-muted-foreground">
            The coordinator verifies every shard&apos;s result, then commits the
            whole plan with a single watermark write. Output names are fixed by
            the plan, so a retry rewrites the same files instead of duplicating
            rows. Unfinished shards resume on a later pass; split shards come
            back smaller.
          </span>
        </p>
      </div>
      <figcaption className="text-xs leading-relaxed text-muted-foreground">
        Eight is both the default and the maximum ingestion width, not a
        promise that every pass launches eight workers. Remaining backend-call
        budget and deadlines can reduce the batch. An optional relay setting
        lets one worker call carry several shards. Request and RUM ingestion
        share the dispatcher&apos;s budget and take turns going first.
      </figcaption>
    </figure>
  );
}
