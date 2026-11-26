import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="System Usage"
      description="Measure Fastly Object Storage impact, Class A/B operations, CDN egress bandwidth, and log ingestion activity across your active service, and estimate monthly costs."
    />
  )
}
