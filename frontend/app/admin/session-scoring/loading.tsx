import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

// Pass the route's static title/description so the skeleton paints the real
// PageHeader at navigation-start, matching what the page renders a beat later
// (no header pop-in on the skeleton→real swap). Keep these in sync with the
// PageHeader in ./page.tsx.
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Session Scoring"
      description="Real-time edge scoring of every request via the scorer Compute service. Toggle scoring on/off, watch the score distribution, and label sessions to evaluate matrix quality (ROC-AUC)."
    />
  )
}
