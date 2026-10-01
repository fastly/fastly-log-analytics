'use client'

import { ShieldCheck } from 'lucide-react'

// Content security (anti-piracy) metrics reported by the player.
// Placeholder until the player metrics pipeline lands.
export default function SecurityTab() {
  return (
    <div className="flex flex-col items-center justify-center gap-4 py-16 text-center">
      <ShieldCheck className="h-12 w-12 text-muted-foreground" />
      <div>
        <h3 className="text-lg font-medium">Content security metrics coming soon</h3>
        <p className="text-sm text-muted-foreground mt-1">
          Anti-piracy metrics submitted by the player will be reported here.
        </p>
      </div>
    </div>
  )
}
