'use client'

import * as React from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, FileWarning, Trash2 } from 'lucide-react'

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { adminFetch } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import type { components } from '@/types/api.generated'

type QuarantineSummary = components['schemas']['QuarantineSummary']
type QuarantineListResponse = components['schemas']['QuarantineListResponse']

export function QuarantineSection() {
  const activeServiceId = useServiceStore(s => s.activeServiceId)
  const queryClient = useQueryClient()
  const [showDetails, setShowDetails] = React.useState(false)
  const [purging, setPurging] = React.useState(false)

  const { data: summary } = useQuery<QuarantineSummary>({
    queryKey: ['admin', 'quarantine', 'summary', activeServiceId],
    queryFn: async ({ signal }) => {
      const r = await adminFetch('/api/admin/quarantine/summary', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: !!activeServiceId,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  })

  const { data: listData } = useQuery<QuarantineListResponse>({
    queryKey: ['admin', 'quarantine', 'list', activeServiceId],
    queryFn: async ({ signal }) => {
      const r = await adminFetch('/api/admin/quarantine?limit=20', { signal })
      if (!r.ok) throw new Error(`status ${r.status}`)
      return r.json()
    },
    enabled: !!activeServiceId && showDetails,
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
  })

  const totalFiles = summary?.total_files ?? 0

  if (!activeServiceId) return null
  if (totalFiles === 0 && !showDetails) return null

  const handlePurge = async () => {
    setPurging(true)
    try {
      await adminFetch('/api/admin/quarantine/purge', { method: 'POST' })
      queryClient.invalidateQueries({ queryKey: ['admin', 'quarantine'] })
    } finally {
      setPurging(false)
    }
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-base">
            {totalFiles > 0 ? (
              <AlertTriangle className="h-4 w-4 text-amber-500" />
            ) : (
              <FileWarning className="h-4 w-4 text-muted-foreground" />
            )}
            Ingest Quarantine
            {totalFiles > 0 && (
              <Badge variant="outline" className="text-xs text-amber-700 dark:text-amber-400 border-amber-500/40">
                {totalFiles} file{totalFiles !== 1 ? 's' : ''}
              </Badge>
            )}
          </CardTitle>
          <div className="flex items-center gap-2">
            {totalFiles > 0 && (
              <Button
                variant="ghost"
                size="sm"
                className="text-xs"
                onClick={() => setShowDetails(prev => !prev)}
              >
                {showDetails ? 'Hide' : 'Details'}
              </Button>
            )}
            {totalFiles > 0 && (
              <Button
                variant="outline"
                size="sm"
                className="text-xs"
                onClick={handlePurge}
                disabled={purging}
              >
                <Trash2 className="h-3 w-3 mr-1" />
                {purging ? 'Purging…' : 'Purge all'}
              </Button>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent className="pt-0">
        {totalFiles === 0 ? (
          <p className="text-sm text-muted-foreground">No quarantined files.</p>
        ) : (
          <div className="space-y-3">
            <div className="flex gap-6 text-sm">
              <div>
                <span className="text-muted-foreground">Corrupt lines: </span>
                <span className="font-medium tabular-nums">{(summary?.total_corrupt_rows ?? 0).toLocaleString()}</span>
              </div>
              {summary?.oldest_at && (
                <div>
                  <span className="text-muted-foreground">Since: </span>
                  <span className="font-medium">{new Date(summary.oldest_at + 'Z').toLocaleString()}</span>
                </div>
              )}
              {summary?.newest_at && (
                <div>
                  <span className="text-muted-foreground">Latest: </span>
                  <span className="font-medium">{new Date(summary.newest_at + 'Z').toLocaleString()}</span>
                </div>
              )}
            </div>

            {showDetails && listData?.files && listData.files.length > 0 && (
              <div className="border rounded-md overflow-hidden">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="text-left px-3 py-2 font-medium">File</th>
                      <th className="text-right px-3 py-2 font-medium">Valid</th>
                      <th className="text-right px-3 py-2 font-medium">Corrupt</th>
                      <th className="text-right px-3 py-2 font-medium">Quarantined</th>
                    </tr>
                  </thead>
                  <tbody>
                    {listData.files.map(f => (
                      <tr key={f.id} className="border-b last:border-0">
                        <td className="px-3 py-2 font-mono truncate max-w-[300px]" title={f.file_name}>
                          {f.file_name}
                        </td>
                        <td className="text-right px-3 py-2 tabular-nums">{f.valid_rows}</td>
                        <td className="text-right px-3 py-2 tabular-nums text-amber-600 dark:text-amber-400">
                          {f.corrupt_rows}
                        </td>
                        <td className="text-right px-3 py-2 text-muted-foreground">
                          {new Date(f.quarantined_at + 'Z').toLocaleString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {showDetails && listData?.files && listData.files.length > 0 && (
              <CorruptSamples files={listData.files} />
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function CorruptSamples({ files }: { files: NonNullable<QuarantineListResponse['files']> }) {
  const withSamples = files.filter(f => f.corrupt_samples && f.corrupt_samples.length > 0)
  if (withSamples.length === 0) return null

  return (
    <div className="space-y-2">
      <p className="text-xs font-medium text-muted-foreground">Sample corrupt lines</p>
      <div className="bg-muted/50 rounded-md p-3 space-y-2 max-h-48 overflow-y-auto">
        {withSamples.slice(0, 5).flatMap(f =>
          (f.corrupt_samples ?? []).slice(0, 3).map(line => (
            <div key={`${f.id}-${line.slice(0, 40)}`} className="text-[11px] font-mono text-muted-foreground break-all">
              <span className="text-foreground/60">[{f.file_name}]</span>{' '}
              {line.length > 300 ? line.slice(0, 300) + '…' : line}
            </div>
          ))
        )}
      </div>
    </div>
  )
}
