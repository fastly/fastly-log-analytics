import React, { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { customFieldsApi, type CustomField } from '@/lib/api/custom-fields'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Plus, Edit, Trash2, Download, Upload } from 'lucide-react'
import { CustomFieldDrawer } from './CustomFieldDrawer'
import { queryKeys } from '@/lib/query-keys'
import { downloadBlob } from '@/lib/utils'

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
    } catch (err: any) {
      alert(`Failed to import: ${err.message}`);
    }
    e.target.value = '';
  }

  const fields = fieldsData?.fields || []

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
      <div className="border rounded-md overflow-x-auto text-sm">
        <table className="w-full">
          <thead className="bg-muted/40">
            <tr>
              <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Name</th>
              <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Label</th>
              <th className="text-left px-3 py-2 text-xs font-medium text-muted-foreground">Type</th>
              <th className="text-center px-3 py-2 text-xs font-medium text-muted-foreground">Dashboard</th>
              <th className="text-center px-3 py-2 text-xs font-medium text-muted-foreground">Logs</th>
              <th className="px-3 py-2 text-right"></th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
               <tr><td colSpan={6} className="px-3 py-4 text-center text-muted-foreground animate-pulse">Loading custom fields...</td></tr>
            ) : fields.length === 0 ? (
               <tr><td colSpan={6} className="px-3 py-8 text-center text-muted-foreground bg-muted/10 border-dashed">No custom fields defined yet.</td></tr>
            ) : (
                fields.map((field) => (
                  <tr key={field.name} className={`border-t ${!field.enabled ? 'opacity-50 grayscale' : ''}`}>
                    <td className="px-3 py-2 font-mono text-xs">
                        {field.name}
                        {!field.enabled && <Badge variant="secondary" className="ml-2 text-[10px] uppercase shadow-none scale-90">Disabled</Badge>}
                    </td>
                    <td className="px-3 py-2">{field.label}</td>
                    <td className="px-3 py-2 text-xs font-mono">{field.duckdb_type}</td>
                    <td className="px-3 py-2 text-center">{field.show_in_dashboard ? '✓' : '✗'}</td>
                    <td className="px-3 py-2 text-center">{field.show_in_logs ? '✓' : '✗'}</td>
                    <td className="px-3 py-2 text-right space-x-2">
                        <Button variant="ghost" size="sm" onClick={() => handleEdit(field)}>
                            <Edit className="h-3.5 w-3.5" />
                        </Button>
                        <Button variant="ghost" size="sm" onClick={() => handleDelete(field.name)} className="text-destructive hover:bg-destructive/10" title="Delete">
                            <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                    </td>
                  </tr>
                ))
            )}
          </tbody>
        </table>
      </div>

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
