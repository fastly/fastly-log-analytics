import { FormSkeleton } from '@/components/skeletons/PageSkeleton'

export default function Loading() {
  return (
    <div
      className="space-y-6"
      data-testid="admin-overview-loading"
      role="status"
      aria-live="polite"
    >
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>Inspecting background scheduler state...</span>
        <span>Loading service configs...</span>
      </div>
      <div className="min-h-[108px] [contain-intrinsic-size:auto_108px] [content-visibility:auto]">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-[108px] rounded-xl border bg-card/50 animate-pulse" />
          ))}
        </div>
      </div>
      <div className="min-h-[260px] [contain-intrinsic-size:auto_260px] [content-visibility:auto]">
        <FormSkeleton />
      </div>
    </div>
  )
}
