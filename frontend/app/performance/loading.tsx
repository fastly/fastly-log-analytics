import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Performance"
      description="Analyze latency, cache efficiency, and origin vs edge processing time."
    />
  )
}
