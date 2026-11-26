import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Anomaly Detection"
      description="Automated anomaly detection comparing recent edge traffic windows against historical baselines across error spikes, latency regressions, cache collapse, bot surges, and origin health."
    />
  )
}
