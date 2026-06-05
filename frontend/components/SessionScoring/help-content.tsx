import React from "react"
import {
  Activity,
  Gauge,
  Users,
  XCircle,
  AlertTriangle,
  SlidersHorizontal,
  LineChart,
  ListChecks,
  ScrollText,
  Shield,
  ShieldAlert,
  History,
  CheckCircle,
  Info,
  Bookmark
} from "lucide-react"

export const StatusPanelHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      The Session Scorer uses a highly-optimized, multi-layer classification engine running at the <strong>Fastly Compute</strong> edge. Every client request is evaluated in real-time to assess risk.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Shield className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>Layer 1 (L1 - Deterministic Checks):</strong> Fast, rules-based verification checking session duration limits, query structures, and cryptographic signature validity of the session cookie.
        </span>
      </li>
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-emerald-500 mt-0.5" />
        <span>
          <strong>Layer 2 (L2 - Transition Analysis):</strong> Evaluates navigational behavior. It scores the unlikeliness of specific route-to-route transitions using a site-specific PageRank-trained matrix.
        </span>
      </li>
      <li className="flex gap-3">
        <Info className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span>
          <strong>Infrastructure:</strong> Toggling the scorer dynamically deploys Wasm binaries, registers 6 custom logging custom_fields, and updates Edge VCL snippets without downtime.
        </span>
      </li>
    </ul>
  </div>
)

export const ScoringHealthHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Operational metrics monitoring the throughput, effectiveness, and reliability of the edge scoring system over the selected time window.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>Fire Rate:</strong> The percentage of total CDN request volume routed through and successfully classified by the scorer.
        </span>
      </li>
      <li className="flex gap-3">
        <Gauge className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span>
          <strong>Average & Percentiles:</strong> Average, median (p50), tail latency (p95), and maximum scores. Higher values denote greater anomalous/threat activity.
        </span>
      </li>
      <li className="flex gap-3">
        <XCircle className="h-5 w-5 shrink-0 text-destructive mt-0.5" />
        <span>
          <strong>Scorer Errors:</strong> Failures like token authentication issues or timeouts. High numbers indicate configuration mismatch or network degradation (the system safely fails-open).
        </span>
      </li>
      <li className="flex gap-3">
        <AlertTriangle className="h-5 w-5 shrink-0 text-amber-500 mt-0.5" />
        <span>
          <strong>Matrix Staleness:</strong> Triggered when a high percentage of live traffic scores ≥ 50, indicating the matrix is treating legitimate, updated routes as anomalous. Click <strong>Retrain matrix</strong> to resolve.
        </span>
      </li>
    </ul>
  </div>
)

export const ThresholdSliderHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Simulate and enforce the sensitivity threshold for edge blocking.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <CheckCircle className="h-5 w-5 shrink-0 text-emerald-500 mt-0.5" />
        <span>
          <strong>Commit Threshold:</strong> Saves a local reference cutoff for your team to visualize. This does not block live traffic, but serves as a tuning baseline.
        </span>
      </li>
      <li className="flex gap-3">
        <ShieldAlert className="h-5 w-5 shrink-0 text-destructive mt-0.5" />
        <span>
          <strong>Enforce (LIVE BLOCKING):</strong> Deploys the threshold directly to Fastly ConfigStore. Real-time requests scoring ≥ the threshold will be immediately blocked at the edge with the configured HTTP status code (default 429; override via the &quot;Enforce response code&quot; selector — 403 / 451 / 503 also supported).
        </span>
      </li>
      <li className="flex gap-3">
        <SlidersHorizontal className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>Precision & Recall:</strong> Statistical performance measures. Precision shows what portion of flagged sessions are actually labeled bad. Recall shows what portion of all bad sessions were successfully caught.
        </span>
      </li>
    </ul>
  </div>
)

export const RocPrCurvesHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Sanity checks displaying the classification power of the current scoring model against your analyst-assigned labels.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <LineChart className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>ROC Curve (AUC):</strong> Measures how well the matrix separates good and bad sessions. Curves hugging the top-left indicate great separation; the diagonal represents random guessing.
        </span>
      </li>
      <li className="flex gap-3">
        <LineChart className="h-5 w-5 shrink-0 text-emerald-500 mt-0.5" />
        <span>
          <strong>Precision-Recall Curve (AP):</strong> Evaluates quality when dealing with highly imbalanced classes (typical of bot traffic). High AP (area under PR curve) means the model maintains high accuracy even when catching most threats.
        </span>
      </li>
    </ul>
  </div>
)

export const PerReasonAucHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Predictive performance of individual L1/L2 rule atoms (e.g., cookie compliance issues, impossible speed, anomalous navigation paths).
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <ListChecks className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>Relevance:</strong> Helps operators see which rules are highly predictive (AUC &gt; 0.8) and which are mostly noisy (AUC ~ 0.5) for the current site&apos;s legitimate traffic patterns.
        </span>
      </li>
    </ul>
  </div>
)

export const TopFlaggedHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Real-time feed of the highest-scoring client sessions currently visiting your service.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Users className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>Session ID (SID):</strong> Click any SID to open the deep request timeline and observe absolute timing offsets, sequence headers, and referrers.
        </span>
      </li>
      <li className="flex gap-3">
        <Bookmark className="h-5 w-5 shrink-0 text-emerald-500 mt-0.5" />
        <span>
          <strong>Labeling:</strong> Use the Flag column to label a session as Good, Bad, or Neutral. This updates the dataset used to compute AUC metrics and retrain the behavioral model.
        </span>
      </li>
    </ul>
  </div>
)

export const ScoreDistHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Hourly aggregation of requests categorized into four distinct risk buckets.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Activity className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>Risk Buckets:</strong> 0-25 (Safe), 25-50 (Low Risk), 50-75 (Suspicious), and 75-100 (Anomalous).
        </span>
      </li>
      <li className="flex gap-3">
        <AlertTriangle className="h-5 w-5 shrink-0 text-amber-500 mt-0.5" />
        <span>
          <strong>Analysis:</strong> Sudden spikes in orange (50-75) or red (75-100) buckets represent volumetric scanning, scraper surges, or credential-stuffing campaigns.
        </span>
      </li>
    </ul>
  </div>
)

export const ComplianceHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Cookie state integrity evaluated on the initial Edge VCL pass before routing requests.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <AlertTriangle className="h-5 w-5 shrink-0 text-amber-500 mt-0.5" />
        <span>
          <strong>missing:</strong> The request requested a dynamic path but did not include the session cookie. This is a primary indicator of automated web scrapers.
        </span>
      </li>
      <li className="flex gap-3">
        <ShieldAlert className="h-5 w-5 shrink-0 text-destructive mt-0.5" />
        <span>
          <strong>tampered:</strong> The session cookie signature failed cryptographic validation, indicating active replay or session hijacking attempts.
        </span>
      </li>
      <li className="flex gap-3">
        <Info className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span>
          <strong>expired:</strong> The session exceeded its hard-cap lifetime and is slated to receive a clean renewal cookie.
        </span>
      </li>
    </ul>
  </div>
)

export const LabelsHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Management interface for analyst-defined ground-truth session ratings.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <Bookmark className="h-5 w-5 shrink-0 text-emerald-500 mt-0.5" />
        <span>
          <strong>Purpose:</strong> Labels are compiled into evaluation datasets for calculating ROC curves. They are not active blocking rules — edge blocking is driven exclusively by the score threshold.
        </span>
      </li>
      <li className="flex gap-3">
        <Info className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span>
          <strong>SID Lifespans:</strong> SIDs are encrypted state tokens that naturally rotate to secure client-side transactions. If a visitor clears cookies or is idle too long, they will get a fresh SID.
        </span>
      </li>
    </ul>
  </div>
)

export const MatrixHistoryHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Deploy and manage transition matrices for behavioral modeling.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <History className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>History:</strong> Lists all transition matrices generated by retraining. You can roll back to any prior matrix version with one click.
        </span>
      </li>
      <li className="flex gap-3">
        <Shield className="h-5 w-5 shrink-0 text-blue-500 mt-0.5" />
        <span>
          <strong>Deployment:</strong> Restoring a matrix immediately switches L2 edge evaluation to that behavioral dataset.
        </span>
      </li>
    </ul>
  </div>
)

export const AuditLogHelp = () => (
  <div className="space-y-4 text-sm text-muted-foreground leading-relaxed">
    <p>
      Immutable ledger tracking administrative session scoring operations on this service.
    </p>
    <ul className="space-y-3 list-none pl-0">
      <li className="flex gap-3">
        <ScrollText className="h-5 w-5 shrink-0 text-primary mt-0.5" />
        <span>
          <strong>Tracking:</strong> Records operator-attributed events such as threshold overrides, matrix deployments, key rotations, and toggles.
        </span>
      </li>
    </ul>
  </div>
)
