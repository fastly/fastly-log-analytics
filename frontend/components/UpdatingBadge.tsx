'use client'

export function UpdatingBadge() {
  return (
    <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-primary/10 text-primary text-[10px] font-bold uppercase tracking-wider animate-pulse">
      <span className="w-1.5 h-1.5 rounded-full bg-primary" />
      Updating
    </div>
  )
}
