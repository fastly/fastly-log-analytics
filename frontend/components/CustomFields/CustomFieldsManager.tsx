import React, { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { customFieldsApi, type CustomField } from '@/lib/api/custom-fields'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Plus, Edit, Trash2, Download, Upload } from 'lucide-react'
import { CustomFieldDrawer } from './CustomFieldDrawer'
import { queryKeys } from '@/lib/query-keys'
import { downloadBlob } from '@/lib/utils'
import { extractApiError } from '@/lib/api'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'

export function CustomFieldsManager({ serviceId }: { serviceId: string }) {
  const queryClient = useQueryClient()
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [editingField, setEditingField] = useState<CustomField | null>(null)

  const { data: fieldsData, isLoading } = useQuery({
    queryKey: ['custom-fields', serviceId],
    queryFn: () => customFieldsApi.listCustomFields(serviceId)
  })

  const deleteMutation = useMutation({
    mutationFn: (fieldName: string) => customFieldsApi.deleteCustomField(serviceId, fieldName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['custom-fields', serviceId] })
      queryClient.invalidateQueries({ queryKey: queryKeys.logFieldsCatalog(serviceId) })
    }
  })

  const handleEdit = (field: CustomField) => {
    setEditingField(field)
    setDrawerOpen(true)
  }

  const handleCreate = () => {
    setEditingField(null)
    setDrawerOpen(true)
  }

  const handleDelete = (fieldName: string) => {
    if (window.confirm(`Are you sure you want to delete custom field '${fieldName}'?`)) {
      deleteMutation.mutate(fieldName)
    }
  }

  const handleExport = async () => {
    try {
      const blob = await customFieldsApi.exportCustomFields(serviceId);
      downloadBlob(blob, `custom_fields_${serviceId}.json`);
    } catch (e) {
      alert("Failed to export fields.");
    }
  }

  const handleImport = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    try {
      const text = await file.text();
      const json = JSON.parse(text);
      if (json.custom_fields && Array.isArray(json.custom_fields)) {
        await customFieldsApi.importCustomFields(serviceId, json.custom_fields);
        queryClient.invalidateQueries({ queryKey: ['custom-fields', serviceId] });
        queryClient.invalidateQueries({ queryKey: queryKeys.logFieldsCatalog(serviceId) });
        alert(`Successfully imported custom fields.`);
      } else {
        alert("Invalid export file format.");
      }
    } catch (err) {
      alert(`Failed to import: ${extractApiError(err)}`);
    }
    e.target.value = '';
  }

  const fields = fieldsData?.fields || []

  const columns: ColumnDef<CustomField>[] = [
    {
      id: 'name',
      header: 'Name',
      accessorFn: (f) => f.name,
      size: 240,
      cell: ({ row }) => (
        <span className="font-mono text-xs">
          {row.original.name}
          {!row.original.enabled && <Badge variant="secondary" className="ml-2 text-[10px] uppercase shadow-none scale-90">Disabled</Badge>}
        </span>
      ),
    },
    {
      id: 'label',
      header: 'Label',
      accessorFn: (f) => f.label,
      size: 200,
      cell: ({ row }) => <span className="text-sm">{row.original.label}</span>,
    },
    {
      id: 'type',
      header: 'Type',
      accessorFn: (f) => f.duckdb_type,
      size: 120,
      cell: ({ row }) => <span className="text-xs font-mono">{row.original.duckdb_type}</span>,
    },
    {
      id: 'dashboard',
      header: 'Dashboard',
      accessorFn: (f) => (f.show_in_dashboard ? 1 : 0),
      size: 110,
      cell: ({ row }) => <span className="block text-center">{row.original.show_in_dashboard ? '✓' : '✗'}</span>,
    },
    {
      id: 'logs',
      header: 'Logs',
      accessorFn: (f) => (f.show_in_logs ? 1 : 0),
      size: 90,
      cell: ({ row }) => <span className="block text-center">{row.original.show_in_logs ? '✓' : '✗'}</span>,
    },
    {
      id: 'actions',
      header: () => <span className="sr-only">Actions</span>,
      size: 110,
      enableSorting: false,
      cell: ({ row }) => (
        <div className="text-right space-x-2">
          <Button variant="ghost" size="sm" onClick={() => handleEdit(row.original)}>
            <Edit className="h-3.5 w-3.5" />
          </Button>
          <Button variant="ghost" size="sm" onClick={() => handleDelete(row.original.name)} className="text-destructive hover:bg-destructive/10" title="Delete">
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      ),
    },
  ]


  return (
    <div className="space-y-4">
      <div className="bg-muted/30 p-3 rounded-md text-sm border flex justify-between items-center">
        <div>
           <p className="font-medium">Define Custom Log Fields</p>
           <p className="text-muted-foreground text-xs mt-0.5">Collect additional edge or origin data for analysis.</p>
        </div>
        <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={handleExport} title="Export Custom Fields">
               <Download className="h-4 w-4" />
            </Button>
            <div className="relative">
                <Button variant="outline" size="sm" title="Import Custom Fields" className="cursor-pointer">
                   <Upload className="h-4 w-4" />
                   <input
                       type="file"
                       accept=".json"
                       className="absolute inset-0 opacity-0 cursor-pointer"
                       onChange={handleImport}
                   />
                </Button>
            </div>
            <Button size="sm" onClick={handleCreate}>
              <Plus className="h-4 w-4 mr-1.5" /> Add Field
            </Button>
        </div>
      </div>

      {/* M-6 (audit, mobile UX): overflow-x-auto so the 6-column custom
          fields table scrolls on narrow viewports instead of clipping
          (overflow-hidden was hiding the actions column on phones). */}
      <DataTableReadonly
        columns={columns}
        data={fields}
        isLoading={isLoading}
        hideToolbar
        showPagination={false}
        tableCaption="Custom log fields"
        emptyMessage="No custom fields defined yet."
        getRowClassName={(field) => (!field.enabled ? 'opacity-50 grayscale' : '')}
      />

      <CustomFieldDrawer
        serviceId={serviceId}
        field={editingField}
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        onSave={() => {
            queryClient.invalidateQueries({ queryKey: ['custom-fields', serviceId] })
            queryClient.invalidateQueries({ queryKey: queryKeys.logFieldsCatalog(serviceId) })
            setDrawerOpen(false)
        }}
      />
    </div>
  )
}
