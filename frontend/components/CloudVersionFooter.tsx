'use client'

import * as React from 'react'

export function CloudVersionFooter() {
  const [buildSha, setBuildSha] = React.useState<string>('unknown')

  React.useEffect(() => {
    let active = true
    fetch('/version', { credentials: 'include' })
      .then((res) => (res.ok ? res.json() : null))
      .then((data: { build_sha?: string } | null) => {
        if (active && data?.build_sha) {
          setBuildSha(data.build_sha)
        }
      })
      .catch(() => {})
    return () => {
      active = false
    }
  }, [])

  return (
    <footer className="mt-12 py-4 border-t border-border/50 text-center text-[11px] font-mono text-muted-foreground select-none">
      Admin View • cloud-compute • fastly-wasm • commit:{buildSha}
    </footer>
  )
}
