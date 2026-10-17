import { ColumnDef } from '@tanstack/react-table'
import type { components } from '@/types/api.generated'
import { DateTimeCell } from '@/components/DataTable/DateTimeCell'
import { formatBytes, formatCount } from '@/lib/format'
export const ingestedFilesColumns: ColumnDef<components['schemas']['IngestedFile']>[] = [
  {
    accessorKey: 'file_name',
    id: 'file_name',
    meta: { label: 'File Name' },
    header: 'File Name',
    cell: ({ row }) => <span className="font-mono text-xs">{row.original.file_name}</span>,
  },
  {
    accessorKey: 'ingested_at',
    id: 'ingested_at',
    meta: { label: 'Ingested At' },
    header: 'Ingested At',
    cell: ({ row }) => <DateTimeCell iso={row.original.ingested_at} />,
  },
  {
    accessorKey: 'row_count',
    id: 'row_count',
    meta: { label: 'Rows' },
    header: 'Rows',
    cell: ({ row }) => (
      <span className="font-mono text-muted-foreground tabular-nums text-xs">
        {formatCount(row.original.row_count || 0)}
      </span>
    ),
  },
  {
    accessorKey: 'file_size_bytes',
    id: 'file_size_bytes',
    meta: { label: 'Size' },
    header: 'Size',
    cell: ({ row }) => {
      const bytes = row.original.file_size_bytes
      return (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {bytes ? formatBytes(bytes) : '—'}
        </span>
      )
    },
  },
]
