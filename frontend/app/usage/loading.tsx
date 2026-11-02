import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="System Usage"
      description="Estimate Fastly Object Storage and CDN logging costs."
    />
  )
}
