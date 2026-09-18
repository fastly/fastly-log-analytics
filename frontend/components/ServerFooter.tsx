import * as os from 'os'

export function ServerFooter() {
  const isDev = process.env.NODE_ENV !== 'production'
  const cluster = process.env.ELEVATION_CLUSTER_NAME || (process.env.KUBERNETES_PORT ? 'k8s' : 'local')
  const pod = process.env.POD_NAME || os.hostname()

  return (
    <footer className="mt-12 py-4 border-t border-border/50 text-center text-[11px] font-mono text-muted-foreground/60 select-none">
      Admin View • {cluster} • {pod}
    </footer>
  )
}
