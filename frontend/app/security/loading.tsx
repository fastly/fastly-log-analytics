import { DashboardSkeleton } from '@/components/ui/skeleton'
export default function Loading() {
  return (
    <DashboardSkeleton
      title="Security"
      description="Monitor TLS cipher and protocol health, verify well-known bot and AI crawler identities, analyze request anomalies, inspect proxy exposure, and track client fingerprint coverage."
    />
  )
}
