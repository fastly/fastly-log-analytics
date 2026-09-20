import * as os from 'os'

export function ServerFooter() {
  let envName = 'local'
  if (process.env.ENV_NAME) {
    envName = process.env.ENV_NAME
  } else if (process.env.ELEVATION_CLUSTER_NAME) {
    envName = `elevation (${process.env.ELEVATION_CLUSTER_NAME})`
  } else if (process.env.KUBERNETES_PORT) {
    envName = 'k8s'
  } else if (process.env.GCE_INSTANCE || os.hostname().includes('gce') || os.hostname().includes('instance')) {
    envName = 'gce'
  } else if (process.env.NODE_ENV === 'production') {
    envName = 'production'
  }

  const machineName = process.env.POD_NAME || os.hostname()

  const isHighScale =
    ['1', 'true', 'yes', 'on'].includes((process.env.HIGH_SCALE_ENABLED || '').trim().toLowerCase()) ||
    (process.env.DEPLOYMENT_MODE || '').trim().toLowerCase() === 'high_throughput'
  const architecture = isHighScale ? 'high-scale' : 'standard'
  const commitHash = process.env.COMMIT_HASH || 'unknown'

  return (
    <footer className="mt-12 py-4 border-t border-border/50 text-center text-[11px] font-mono text-muted-foreground select-none">
      Admin View • {architecture} • {envName} • {machineName} • commit:{commitHash}
    </footer>
  )
}
