import { DashboardSkeleton } from '@/components/skeletons/PageSkeleton'

// Insights uses the same ReportLayout chrome as the other report pages
// (dashboard / performance / security / origin / usage / sessions), so
// it should show the same skeleton shape on click — keeps the click-
// to-paint experience consistent across the sidebar. Previously this
// imported GenericPageSkeleton (3 small grey boxes) which looked like
// a different app loading vs the proper card grid + chart placeholder.
export default function Loading() {
  return <DashboardSkeleton />
}
