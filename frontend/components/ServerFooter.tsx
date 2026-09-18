import * as os from 'os'

export function ServerFooter() {
  let envName = 'local'
  if (process.env.ELEVATION_CLUSTER_NAME) {
    envName = `elevation (${process.env.ELEVATION_CLUSTER_NAME})`
  } else if (process.env.KUBERNETES_PORT) {
    envName = 'k8s'
  } else if (process.env.GCE_INSTANCE || os.hostname().includes('gce') || os.hostname().includes('instance')) {
    envName = 'gce'
  } else if (process.env.NODE_ENV === 'production') {
    envName = 'production'
  }

  const machineName = process.env.POD_NAME || os.hostname()

  return (
    <footer className="mt-12 py-4 border-t border-border/50 text-center text-[11px] font-mono text-muted-foreground/60 select-none">
      Admin View • {envName} • {machineName}
    </footer>
  )
}
