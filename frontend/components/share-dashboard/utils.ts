export type RateLimitFailure = { ip: string; count: number; window_s: number }
export type RateLimitLockout = { ip: string; remaining_s: number }
export type TunnelHistoryEntry = {
  started_at: string
  ended_at: string
  duration_s: number
  reason: string
}

export type ShareStatus = {
  sharing_active: boolean
  use_tunnel: boolean
  tunnel_url: string | null
  public_endpoint: string | null
  public_url: string | null
  forward_port: number | null
  started_at: string | null
  max_concurrent_sessions: number
  active_session_count: number
  services: { service_id: string; name: string }[]
  invites: any[]
  sessions: any[]
  audit_logs: any[]
  rate_limits?: { failures: RateLimitFailure[]; lockouts: RateLimitLockout[] }
  telemetry?: {
    heartbeat_unauth_count: number
    current_uptime_s: number | null
    tunnel_uptime_history: TunnelHistoryEntry[]
  }
}

export function formatUptime(seconds: number | null | undefined): string {
  if (seconds == null || seconds < 0) return '—'
  if (seconds < 60) return `${seconds}s`
  const m = Math.floor(seconds / 60)
  if (m < 60) return `${m}m ${seconds % 60}s`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ${m % 60}m`
  const d = Math.floor(h / 24)
  return `${d}d ${h % 24}h`
}

export function formatStamp(s: string | null | undefined): string {
  if (!s) return '—'
  try {
    return new Date(s).toLocaleString()
  } catch {
    return s
  }
}
