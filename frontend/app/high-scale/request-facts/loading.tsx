import { TableSkeleton } from '@/components/ReportLayout'

export default function Loading() {
  return (
    <div
      data-testid="high-scale-request-facts-route-loading"
      className="min-h-[720px] [content-visibility:auto] [contain-intrinsic-size:auto_720px]"
    >
      <TableSkeleton
        title="High-scale request facts"
        description="Explicit opt-in access to the isolated high-scale request-facts API."
      />
    </div>
  )
}
