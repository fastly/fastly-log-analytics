import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Security"
      description="Monitor TLS health, identify bot fingerprints, and detect request anomalies."
    />
  )
}
