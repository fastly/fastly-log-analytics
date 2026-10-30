import React, { useState, useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useDebounce } from '@/hooks/useDashboardCards'
import { useOpenGeneration } from '@/hooks/useSSE'
import { downloadBlob } from '@/lib/utils'
import { queryKeys, extractApiError, customFieldsApi, type CustomField, type VclLintResult } from "@/lib/api";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Loader2, Plus, Edit, Trash2, Download, Upload } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import {
  type FormState,
  customFieldFormSchema,
  DEFAULT_FIELD,
  CustomFieldBasicSection,
  CustomFieldVclSection,
  CustomFieldStorageSection,
} from './CustomFieldFormSections'

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
      const blob = await customFieldsApi.exportCustomFields(serviceId)
      downloadBlob(blob, `custom_fields_${serviceId}.json`)
    } catch {
      alert("Failed to export fields.")
    }
  }

  const handleImport = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    try {
      const text = await file.text()
      const json = JSON.parse(text)
      if (json.custom_fields && Array.isArray(json.custom_fields)) {
        await customFieldsApi.importCustomFields(serviceId, json.custom_fields)
        queryClient.invalidateQueries({ queryKey: ['custom-fields', serviceId] })
        queryClient.invalidateQueries({ queryKey: queryKeys.logFieldsCatalog(serviceId) })
        alert(`Successfully imported custom fields.`)
      } else {
        alert("Invalid export file format.")
      }
    } catch (err) {
      alert(`Failed to import: ${extractApiError(err)}`)
    }
    e.target.value = ''
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

interface CustomFieldDrawerProps {
  serviceId: string
  field: CustomField | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onSave: () => void
}

export function CustomFieldDrawer(props: CustomFieldDrawerProps) {
  const generation = useOpenGeneration(props.open)
  return <CustomFieldDrawerBody key={`${props.field?.name ?? '__new__'}|${generation}`} {...props} />
}

function CustomFieldDrawerBody({ serviceId, field, open, onOpenChange, onSave }: CustomFieldDrawerProps) {
  const isEditing = !!field
  const initialValues: FormState = field
    ? {
        ...field,
        collection_stage: field.collection_stage || 'edge',
      }
    : { ...DEFAULT_FIELD }
  const form = useForm<z.infer<typeof customFieldFormSchema>>({
    resolver: zodResolver(customFieldFormSchema),
    defaultValues: initialValues as z.infer<typeof customFieldFormSchema>,
  })
  const [formData, setFormData] = useState<FormState>(() => initialValues)
  const [validationError, setValidationError] = useState<string | null>(null)
  const [lintResult, setLintResult] = useState<VclLintResult | null>(null)
  const [isLinting, setIsLinting] = useState(false)
  // Distinct from `lintResult` (which carries the *parsed* validation
  // verdict): this captures the case where the validation API CALL
  // itself failed (network error, 500), so the user sees a visible
  // hint instead of an empty pane after the spinner disappears.
  const [lintFetchError, setLintFetchError] = useState<string | null>(null)

  const debouncedVcl = useDebounce(formData.vcl_log_expression, 500)
  const debouncedStage = useDebounce(formData.collection_stage, 500)

  // VCL Validation effect
  useEffect(() => {
    let active = true
    async function validate() {
      if (!open || !debouncedVcl) {
        setLintResult(null)
        return
      }
      setIsLinting(true)
      setLintFetchError(null)
      try {
        const result = await customFieldsApi.validateCustomVcl(serviceId, {
          vcl_log_expression: debouncedVcl,
          collection_stage: debouncedStage
        })
        if (active) {
          setLintResult(result)
        }
      } catch (err) {
        if (active) {
          if (process.env.NODE_ENV === 'development') {
            console.error("VCL validation failed", err)
          }
          setLintResult(null)
          setLintFetchError(
            (err as Error)?.message
              ? `Validation could not run: ${(err as Error).message}`
              : "Validation could not run. Check your connection and try again."
          )
        }
      } finally {
        if (active) {
          setIsLinting(false)
        }
      }
    }
    validate()
    return () => { active = false }
  }, [debouncedVcl, debouncedStage, serviceId, open])

  const saveMutation = useMutation({
    mutationFn: async () => {
      if (isEditing) {
        return customFieldsApi.updateCustomField(serviceId, field!.name, formData)
      } else {
        return customFieldsApi.createCustomField(serviceId, formData)
      }
    },
    onSuccess: () => onSave(),
    onError: (error: Error) => setValidationError(error.message || "Failed to save field")
  })

  const handleChange = <K extends keyof FormState>(key: K, value: FormState[K]) => {
    setFormData(prev => {
      const next = { ...prev, [key]: value };
      // Auto-slug label to name if not editing
      if (!isEditing && key === 'label' && typeof value === 'string') {
        next.name = value
          .toLowerCase()
          .replace(/[^a-z0-9_]/g, '_')
          .replace(/_+/g, '_')
          .replace(/^[^a-z]+/, '')
          .substring(0, 48);
      }
      form.reset(next as z.infer<typeof customFieldFormSchema>)
      return next;
    })
  }

  const isValidName = /^[a-z][a-z0-9_]{0,47}$/.test(formData.name)
  const isLintingStale = formData.vcl_log_expression !== debouncedVcl || formData.collection_stage !== debouncedStage
  const isSaveDisabled =
    saveMutation.isPending ||
    isLinting ||
    isLintingStale ||
    !isValidName ||
    !formData.label.trim() ||
    !formData.vcl_log_expression.trim() ||
    Boolean(lintFetchError) ||
    Boolean(lintResult && !lintResult.valid)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader className="mb-6">
          <DialogTitle>{isEditing ? `Edit Custom Field: ${field!.name}` : 'Create Custom Field'}</DialogTitle>
          <DialogDescription>
            Define a new field to capture from your Fastly logs using VCL.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-8">
          <CustomFieldBasicSection
            formData={formData}
            isValidName={isValidName}
            onChange={handleChange}
          />
          <CustomFieldVclSection
            formData={formData}
            isEditing={isEditing}
            isLinting={isLinting}
            lintFetchError={lintFetchError}
            lintResult={lintResult}
            onChange={handleChange}
          />
          <CustomFieldStorageSection
            formData={formData}
            onChange={handleChange}
          />
        </div>

        <DialogFooter className="mt-6 border-t pt-4">
            <div className="flex flex-col w-full gap-4">
                {validationError && (
                    <div className="p-3 bg-destructive/10 border border-destructive/20 text-destructive rounded-md text-sm w-full font-medium">
                        {validationError}
                    </div>
                )}
                <div className="flex flex-col sm:flex-row justify-between items-center w-full gap-4">
                    <div className="text-xs text-muted-foreground">
                        {lintResult?.format_length ? (
                           <span>Total Fastly log format length: <strong className={lintResult.format_length > 8000 ? "text-destructive" : ""}>{lintResult.format_length}</strong> / 8000 chars</span>
                        ) : null}
                    </div>
                    <div className="flex gap-2">
                        <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
                        <Button disabled={isSaveDisabled || false} onClick={() => saveMutation.mutate()}>
                           {saveMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : 'Save Field'}
                        </Button>
                    </div>
                </div>
            </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
