import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Anomaly Detection"
      description="Automated insights comparing recent traffic to historical baselines."
    />
  )
}
