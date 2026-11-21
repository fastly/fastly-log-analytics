'use client'

import { useBootstrapCacheSelector } from '@/hooks/useBootstrap'
import type { BootstrapResponse } from '@/types/api'

export function CloudVersionFooter() {
  const buildSha = useBootstrapCacheSelector(
    (bootstrap) => (bootstrap as BootstrapResponse | undefined)?.build_sha ?? 'unknown',
    'unknown',
  )

  return (
    <footer className="mt-12 py-4 border-t border-border/50 text-center text-[11px] font-mono text-muted-foreground select-none">
      Admin View • cloud-compute • fastly-wasm • commit:{buildSha}
    </footer>
  )
}
