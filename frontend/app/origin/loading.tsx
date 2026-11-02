import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Origin Performance"
      description="Real-time backend health, fetch timing, and error analysis."
    />
  )
}
