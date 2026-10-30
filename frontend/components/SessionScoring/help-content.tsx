'use client'

import React from 'react'
import {
  Activity,
  AlertTriangle,
  Bookmark,
  CheckCircle,
  Gauge,
  History,
  Info,
  LineChart,
  ListChecks,
  ScrollText,
  Shield,
  ShieldAlert,
  SlidersHorizontal,
  Users,
  XCircle,
} from 'lucide-react'

export function HelpList({
  intro,
  items,
}: {
  intro: React.ReactNode
  items: [React.ComponentType<{ className?: string }>, string, React.ReactNode][]
}) {
  return (
    <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
      <p>{intro}</p>
      <ul className="space-y-3 list-none pl-0">
        {items.map(([Icon, cls, body], idx) => (
          <li key={`${cls}-${idx.toString()}`} className="flex gap-3">
            <Icon className={`h-5 w-5 shrink-0 ${cls} mt-0.5`} />
            <span>{body}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

export const StatusPanelHelp = () => (
  <HelpList
    intro={
      <>
        The Session Scorer uses a highly-optimized, multi-layer classification engine running at the{' '}
        <strong>Fastly Compute</strong> edge. Every client request is evaluated in real-time to assess risk.
      </>
    }
    items={[
      [
        Shield,
        'text-primary',
        <>
          <strong>Layer 1 (L1 - Deterministic Checks):</strong> Fast, rules-based verification checking session
          duration limits, query structures, and cryptographic signature validity of the session cookie.
        </>,
      ],
      [
        Activity,
        'text-emerald-500',
        <>
          <strong>Layer 2 (L2 - Transition Analysis):</strong> Evaluates navigational behavior. It scores the
          unlikeliness of specific route-to-route transitions using a site-specific PageRank-trained matrix.
        </>,
      ],
      [
        Info,
        'text-blue-500',
        <>
          <strong>Infrastructure:</strong> Toggling the scorer dynamically deploys Wasm binaries, registers 6 custom
          logging custom_fields, and updates Edge VCL snippets without downtime.
        </>,
      ],
    ]}
  />
)

export const ScoringHealthHelp = () => (
  <HelpList
    intro="Operational metrics monitoring the throughput, effectiveness, and reliability of the edge scoring system over the selected time window."
    items={[
      [
        Activity,
        'text-primary',
        <>
          <strong>Fire Rate:</strong> The percentage of total CDN request volume routed through and successfully
          classified by the scorer.
        </>,
      ],
      [
        Gauge,
        'text-blue-500',
        <>
          <strong>Average &amp; Percentiles:</strong> Average, median (p50), tail latency (p95), and maximum scores.
          Higher values denote greater anomalous/threat activity.
        </>,
      ],
      [
        XCircle,
        'text-destructive',
        <>
          <strong>Scorer Errors:</strong> Failures like token authentication issues or timeouts. High numbers indicate
          configuration mismatch or network degradation (the system safely fails-open).
        </>,
      ],
      [
        AlertTriangle,
        'text-amber-500',
        <>
          <strong>Matrix Staleness:</strong> Triggered when a high percentage of live traffic scores ≥ 50, indicating
          the matrix is treating legitimate, updated routes as anomalous. Click <strong>Retrain matrix</strong> to
          resolve.
        </>,
      ],
    ]}
  />
)

export const ThresholdSliderHelp = () => (
  <HelpList
    intro="Simulate and enforce the sensitivity threshold for edge blocking."
    items={[
      [
        CheckCircle,
        'text-emerald-500',
        <>
          <strong>Commit Threshold:</strong> Saves a local reference cutoff for your team to visualize. This does not
          block live traffic, but serves as a tuning baseline.
        </>,
      ],
      [
        ShieldAlert,
        'text-destructive',
        <>
          <strong>Enforce (LIVE BLOCKING):</strong> Deploys the threshold directly to Fastly ConfigStore. Real-time
          requests scoring ≥ the threshold will be immediately blocked at the edge with the configured HTTP status code
          (default 429; override via the &quot;Enforce response code&quot; selector — 403 / 451 / 503 also supported).
        </>,
      ],
      [
        SlidersHorizontal,
        'text-primary',
        <>
          <strong>Precision &amp; Recall:</strong> Statistical performance measures. Precision shows what portion of
          flagged sessions are actually labeled bad. Recall shows what portion of all bad sessions were successfully
          caught.
        </>,
      ],
    ]}
  />
)

export const RocPrCurvesHelp = () => (
  <HelpList
    intro="Sanity checks displaying the classification power of the current scoring model against your analyst-assigned labels."
    items={[
      [
        LineChart,
        'text-primary',
        <>
          <strong>ROC Curve (AUC):</strong> Measures how well the matrix separates good and bad sessions. Curves
          hugging the top-left indicate great separation; the diagonal represents random guessing.
        </>,
      ],
      [
        LineChart,
        'text-emerald-500',
        <>
          <strong>Precision-Recall Curve (AP):</strong> Evaluates quality when dealing with highly imbalanced classes
          (typical of bot traffic). High AP (area under PR curve) means the model maintains high accuracy even when
          catching most threats.
        </>,
      ],
    ]}
  />
)

export const PerReasonAucHelp = () => (
  <HelpList
    intro="Predictive performance of individual L1/L2 rule atoms (e.g., cookie compliance issues, impossible speed, anomalous navigation paths)."
    items={[
      [
        ListChecks,
        'text-primary',
        <>
          <strong>Relevance:</strong> Helps operators see which rules are highly predictive (AUC &gt; 0.8) and which
          are mostly noisy (AUC ~ 0.5) for the current site&apos;s legitimate traffic patterns.
        </>,
      ],
    ]}
  />
)

export const TopFlaggedHelp = () => (
  <HelpList
    intro="Real-time feed of the highest-scoring client sessions currently visiting your service."
    items={[
      [
        Users,
        'text-primary',
        <>
          <strong>Session ID (SID):</strong> Click any SID to open the deep request timeline and observe absolute
          timing offsets, sequence headers, and referrers.
        </>,
      ],
      [
        Bookmark,
        'text-emerald-500',
        <>
          <strong>Labeling:</strong> Use the Flag column to label a session as Good, Bad, or Neutral. This updates the
          dataset used to compute AUC metrics and retrain the behavioral model.
        </>,
      ],
    ]}
  />
)

export const ScoreDistHelp = () => (
  <HelpList
    intro="Hourly aggregation of requests categorized into four distinct risk buckets."
    items={[
      [
        Activity,
        'text-primary',
        <>
          <strong>Risk Buckets:</strong> 0-25 (Safe), 25-50 (Low Risk), 50-75 (Suspicious), and 75-100 (Anomalous).
        </>,
      ],
      [
        AlertTriangle,
        'text-amber-500',
        <>
          <strong>Analysis:</strong> Sudden spikes in orange (50-75) or red (75-100) buckets represent volumetric
          scanning, scraper surges, or credential-stuffing campaigns.
        </>,
      ],
    ]}
  />
)

export const ScorerLatencyHelp = () => (
  <HelpList
    intro={
      <>
        Hourly latency of the Compute scorer sub-fetch. Read it alongside the Scorer&nbsp;errors chart — fail-opens are
        a latency symptom.
      </>
    }
    items={[
      [
        LineChart,
        'text-primary',
        <>
          <strong>Round-trip (solid):</strong> edge-observed p50/p95/p99 latency (<code>edge_score_rtt_us</code>) —
          network + Compute cold-start + Wasm exec. This is the number compared against the backend timeout (~100&nbsp;ms).
        </>,
      ],
      [
        Activity,
        'text-emerald-500',
        <>
          <strong>Exec (dashed):</strong> the scorer&apos;s own Wasm time (<code>edge_score_exec_us</code>) is ~µs; if
          the round-trip lines sit far above the exec lines, the cost is network/cold-start, not compute.
        </>,
      ],
    ]}
  />
)

export const ScorerErrorsHelp = () => (
  <HelpList
    intro={
      <>
        Hourly count of scorer fail-opens. Read it alongside the Scorer&nbsp;latency chart — these rise as latency does.
      </>
    }
    items={[
      [
        AlertTriangle,
        'text-destructive',
        <>
          <strong>Fail-opens:</strong> rows that timed out or failed auth and were let through unscored (the system
          fails open rather than blocking on a scorer error).
        </>,
      ],
      [
        LineChart,
        'text-primary',
        <>
          <strong>Correlate with latency:</strong> when scorer p95 climbs toward the ~100&nbsp;ms timeout budget, these
          bars rise with it — that&apos;s the signal to raise the timeout or investigate cold starts.
        </>,
      ],
    ]}
  />
)

export const ScorerFailOpenHelp = () => (
  <HelpList
    intro={
      <>
        When the scorer can&apos;t produce a verdict it fails <strong>open</strong> — the request flows through unscored
        rather than being blocked. This card groups those fail-opens by the exact reason so you can tell the failure
        modes apart (the Scorer&nbsp;errors chart shows <em>when</em>; this shows <em>what kind</em>).
      </>
    }
    items={[
      [
        AlertTriangle,
        'text-destructive',
        <>
          <strong>compute-unavailable-503:</strong> the scorer sub-fetch timed out or the service was unreachable —
          usually cold-start latency past the timeout budget. Raise the timeout or investigate cold starts.
        </>,
      ],
      [
        XCircle,
        'text-destructive',
        <>
          <strong>compute-unavailable-500 / -401:</strong> a 500 is a Wasm trap inside the scorer; a 401 means the
          VCL-stamped <code>X-Edge-Scorer-Auth</code> secret drifted from the scoring_keys store — redeploy to re-sync
          it.
        </>,
      ],
      [
        ShieldAlert,
        'text-amber-500',
        <>
          <strong>internal-error-keys:</strong> the scorer started but couldn&apos;t load the AES keys / KV store — a
          provisioning issue on the scoring service, not a transient timeout.
        </>,
      ],
    ]}
  />
)

export const ComplianceHelp = () => (
  <HelpList
    intro="Cookie state integrity evaluated on the initial Edge VCL pass before routing requests."
    items={[
      [
        AlertTriangle,
        'text-amber-500',
        <>
          <strong>missing:</strong> The request requested a dynamic path but did not include the session cookie. This is
          a primary indicator of automated web scrapers.
        </>,
      ],
      [
        ShieldAlert,
        'text-destructive',
        <>
          <strong>tampered:</strong> The session cookie signature failed cryptographic validation, indicating active
          replay or session hijacking attempts.
        </>,
      ],
      [
        Info,
        'text-blue-500',
        <>
          <strong>expired:</strong> The session exceeded its hard-cap lifetime and is slated to receive a clean renewal
          cookie.
        </>,
      ],
    ]}
  />
)

export const LabelsHelp = () => (
  <HelpList
    intro="Management interface for analyst-defined ground-truth session ratings."
    items={[
      [
        Bookmark,
        'text-emerald-500',
        <>
          <strong>Purpose:</strong> Labels are compiled into evaluation datasets for calculating ROC curves. They are
          not active blocking rules — edge blocking is driven exclusively by the score threshold.
        </>,
      ],
      [
        Info,
        'text-blue-500',
        <>
          <strong>SID Lifespans:</strong> SIDs are encrypted state tokens that naturally rotate to secure client-side
          transactions. If a visitor clears cookies or is idle too long, they will get a fresh SID.
        </>,
      ],
    ]}
  />
)

export const MatrixHistoryHelp = () => (
  <HelpList
    intro="Deploy and manage transition matrices for behavioral modeling."
    items={[
      [
        History,
        'text-primary',
        <>
          <strong>History:</strong> Lists all transition matrices generated by retraining. You can roll back to any
          prior matrix version with one click.
        </>,
      ],
      [
        Shield,
        'text-blue-500',
        <>
          <strong>Deployment:</strong> Restoring a matrix immediately switches L2 edge evaluation to that behavioral
          dataset.
        </>,
      ],
    ]}
  />
)

export const AuditLogHelp = () => (
  <HelpList
    intro="Immutable ledger tracking administrative session scoring operations on this service."
    items={[
      [
        ScrollText,
        'text-primary',
        <>
          <strong>Tracking:</strong> Records operator-attributed events such as threshold overrides, matrix deployments,
          key rotations, and toggles.
        </>,
      ],
    ]}
  />
)
