'use client'

import React from 'react'
import {
  Table,  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from "@/components/ui/table"
import { Skeleton } from '@/components/ui/skeleton'

export function SchemaTab({
  schemaData,
  isLoadingSchema,
}: {
  schemaData: any
  isLoadingSchema: boolean
}) {
  return (
    <>
      <div className="p-4 border-b flex justify-between items-center">
        <div>
          <h3 className="text-sm font-medium">DuckDB Table Schema & Statistics</h3>
          <p className="text-xs text-muted-foreground mt-1">Based on a fast statistical sample of your logs.</p>
        </div>
      </div>
      <div className="overflow-x-auto">
        <Table>
          <caption className="sr-only">DuckDB Table Schema and Statistics</caption>
          <TableHeader>
            <TableRow>
              <TableHead>Column Name</TableHead>
              <TableHead>DuckDB Type</TableHead>
              <TableHead className="text-right">Populated %</TableHead>
              <TableHead className="text-right">Approx Unique</TableHead>
              <TableHead className="max-w-[200px]">Min Value</TableHead>
              <TableHead className="max-w-[200px]">Max Value</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {isLoadingSchema ? (
              [1, 2, 3, 4, 5].map(i => (
                <TableRow key={i}>
                  <TableCell><Skeleton className="h-4 w-32" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-12 ml-auto" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-16 ml-auto" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                  <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                </TableRow>
              ))
            ) : (schemaData as any)?.schema.map((col: any) => {
              const hasStats = col.null_percentage !== undefined
              const populatedPct = hasStats ? Math.max(0, 100 - col.null_percentage).toFixed(1) : '—'

              return (
                <TableRow key={col.name}>
                  <TableCell className="font-mono text-xs font-bold">{col.name}</TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">{col.type}</TableCell>
                  <TableCell className="font-mono text-xs text-right tabular-nums">
                    {hasStats ? (
                      <span className={populatedPct === '0.0' ? 'text-muted-foreground/30' : ''}>
                        {populatedPct}%
                      </span>
                    ) : '—'}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-right text-muted-foreground tabular-nums">
                    {hasStats ? col.approx_unique?.toLocaleString() ?? '—' : '—'}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground truncate max-w-[200px]" title={col.min}>
                    {hasStats ? col.min ?? '—' : '—'}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground truncate max-w-[200px]" title={col.max}>
                    {hasStats ? col.max ?? '—' : '—'}
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </div>
    </>
  )
}
