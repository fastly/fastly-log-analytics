import { AlertTriangle, Info } from 'lucide-react'

export function ClickHouseStatusNotice({
  enabled,
  health,
}: {
  enabled?: boolean
  health?: string
}) {
  const disabled = enabled === false
  const unavailable = enabled === true && health === 'unavailable'

  return (
    <div className="grid">
      {/* Both notices size the same grid cell before status arrives, including wrapped text. */}
      <div
        data-testid="admin-clickhouse-disabled-banner"
        aria-hidden={!disabled}
        className={`col-start-1 row-start-1 rounded-md border border-blue-500/30 bg-blue-500/10 px-4 py-3 text-xs text-blue-700 dark:text-blue-300 flex items-start gap-2.5 ${disabled ? '' : 'invisible'}`}
      >
        <Info className="h-4 w-4 shrink-0 mt-0.5" />
        <div>
          <p className="font-semibold">ClickHouse Disabled</p>
          <p className="mt-0.5 text-muted-foreground">
            ClickHouse cluster connection is not currently enabled (<code>enabled: false</code>,{' '}
            <code>health: &quot;disabled&quot;</code>).
          </p>
        </div>
      </div>
      <div
        data-testid="admin-clickhouse-unavailable-banner"
        aria-hidden={!unavailable}
        className={`col-start-1 row-start-1 rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-xs text-destructive flex items-start gap-2.5 ${unavailable ? '' : 'invisible'}`}
      >
        <AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
        <div>
          <p className="font-semibold">ClickHouse Cluster or Manifest Schema Unavailable (HTTP 503)</p>
          <p className="mt-0.5">
            The ClickHouse health check failed or the recorded <code>clickhouse_schema_state</code> version does
            not match the expected runtime schema version. Inspect cluster connectivity and schema initialization
            before executing replays.
          </p>
        </div>
      </div>
    </div>
  )
}
