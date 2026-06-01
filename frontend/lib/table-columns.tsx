import { ColumnDef, HeaderContext } from '@tanstack/react-table'
import { ArrowUpDown } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { DateTimeCell } from '@/components/DataTable/DateTimeCell'
import { formatBytes } from '@/lib/utils'

const SortableHeader = ({ column, label }: { column: HeaderContext<unknown, unknown>['column']; label: string }) => (
  <Button
    variant="ghost"
    onClick={() => column.toggleSorting(column.getIsSorted() === 'asc')}
    className="-ml-2.5 h-8 data-[state=open]:bg-accent"
  >
    {label}
    <ArrowUpDown className="ml-2 h-4 w-4" />
  </Button>
)

export const ingestedFilesColumns: ColumnDef<unknown>[] = [
  {
    accessorKey: 'file_name',
    id: 'file_name',
    meta: { label: 'File Name' },
    header: ({ column }) => <SortableHeader column={column} label="File Name" />,
    cell: ({ row }) => <span className="font-mono text-xs">{(row.original as { file_name: string }).file_name}</span>,
  },
  {
    accessorKey: 'ingested_at',
    id: 'ingested_at',
    meta: { label: 'Ingested At' },
    header: ({ column }) => <SortableHeader column={column} label="Ingested At" />,
    cell: ({ row }) => <DateTimeCell iso={(row.original as { ingested_at: string }).ingested_at} />,
  },
  {
    accessorKey: 'row_count',
    id: 'row_count',
    meta: { label: 'Rows' },
    header: ({ column }) => <SortableHeader column={column} label="Rows" />,
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
    header: ({ column }) => <SortableHeader column={column} label="Size" />,
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
