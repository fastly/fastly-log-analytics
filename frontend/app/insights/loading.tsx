import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

// Insights uses the same ReportLayout chrome as the other report pages
// (dashboard / performance / security / origin / usage / sessions), so
// it should show the same skeleton shape on click — keeps the click-
// to-paint experience consistent across the sidebar. Previously this
// imported GenericPageSkeleton (3 small grey boxes) which looked like
// a different app loading vs the proper card grid + chart placeholder.
// title/description mirror the ReportLayout props in page.tsx so the
// instant route skeleton shows the real header before the bundle loads.
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Anomaly Detection"
      description="Automated insights comparing recent traffic to historical baselines."
    />
  )
}
