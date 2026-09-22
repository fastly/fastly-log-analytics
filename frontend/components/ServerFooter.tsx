import * as os from 'os'

async function fetchGceMetadata(path: string): Promise<string | null> {
  try {
    // 169.254.169.254 / metadata.google.internal is standard Google Cloud Metadata Server IP
    const res = await fetch(`http://169.254.169.254/computeMetadata/v1/${path}`, {
      headers: { 'Metadata-Flavor': 'Google' },
      next: { revalidate: 3600 }, // Cache for 1 hour
      signal: AbortSignal.timeout(500), // Fail-open after 500ms if not on GCE
    })
    if (res.ok) {
      return (await res.text()).trim()
    }
  } catch {}
  return null
}

export async function ServerFooter() {
  let envName = 'local'
  let machineName = process.env.POD_NAME || os.hostname()

  // Dynamic Google Compute Engine Metadata Detection
  const gceHostname = await fetchGceMetadata('instance/name')
  const gceProject = await fetchGceMetadata('project/project-id')

  if (gceHostname) {
    envName = 'gce'
    if (gceProject) {
      envName = `gce (${gceProject})`
    }
    machineName = gceHostname
  } else if (process.env.ELEVATION_CLUSTER_NAME) {
    envName = `elevation (${process.env.ELEVATION_CLUSTER_NAME})`
  } else if (process.env.KUBERNETES_SERVICE_HOST) {
    envName = 'k8s'
  } else if (process.env.ENV_NAME) {
    envName = process.env.ENV_NAME
  } else if (process.env.NODE_ENV === 'production') {
    envName = 'production'
  }

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
