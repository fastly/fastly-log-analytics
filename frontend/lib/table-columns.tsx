import { ColumnDef } from '@tanstack/react-table'
import { DateTimeCell } from '@/components/DataTable/DateTimeCell'
import { formatBytes } from '@/lib/format'

export const ingestedFilesColumns: ColumnDef<unknown>[] = [
  {
    accessorKey: 'file_name',
    id: 'file_name',
    meta: { label: 'File Name' },
    header: 'File Name',
    cell: ({ row }) => <span className="font-mono text-xs">{(row.original as { file_name: string }).file_name}</span>,
  },
  {
    accessorKey: 'ingested_at',
    id: 'ingested_at',
    meta: { label: 'Ingested At' },
    header: 'Ingested At',
    cell: ({ row }) => <DateTimeCell iso={(row.original as { ingested_at: string }).ingested_at} />,
  },
  {
    accessorKey: 'row_count',
    id: 'row_count',
    meta: { label: 'Rows' },
    header: 'Rows',
    cell: ({ row }) => (
      <span className="font-mono text-muted-foreground tabular-nums text-xs">
        {((row.original as { row_count?: number }).row_count || 0).toLocaleString()}
      </span>
    ),
  },
  {
    accessorKey: 'file_size_bytes',
    id: 'file_size_bytes',
    meta: { label: 'Size' },
    header: 'Size',
    cell: ({ row }) => {
      const bytes = (row.original as { file_size_bytes?: number }).file_size_bytes
      return (
        <span className="font-mono text-muted-foreground tabular-nums text-xs">
          {bytes ? formatBytes(bytes) : '—'}
        </span>
      )
    },
  },
]
