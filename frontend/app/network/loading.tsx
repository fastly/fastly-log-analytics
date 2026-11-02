import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Network & ASN Health"
      description="Analysis of TCP performance, packet loss, and jitter by ASN and geography."
    />
  )
}
