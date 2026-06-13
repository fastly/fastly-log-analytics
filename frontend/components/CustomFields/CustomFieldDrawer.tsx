import React, { useState, useEffect } from 'react'
import { useDebounce } from '@/hooks/useDebounce'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { LabelWithInfo } from '@/components/ui/label-with-info'
import { customFieldsApi, type CustomField, type VclLintRequest, type VclLintResult } from '@/lib/api/custom-fields'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, CheckCircle2, Loader2, Activity } from 'lucide-react'

interface CustomFieldDrawerProps {
  serviceId: string
  field: CustomField | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onSave: () => void
}

const DEFAULT_FIELD: Omit<CustomField, "created_at" | "updated_at"> = {
  name: "",
  label: "",
  description: "",
  vcl_log_expression: "",
  collection_stage: "edge",
  origin_log_frequency: "all",
  duckdb_type: "VARCHAR",
  value_type: "string",
  bytes_estimate: 20,
  nullable: true,
  enabled: true,
  show_in_dashboard: false,
  show_in_logs: true,
  filterable: true
}

export function CustomFieldDrawer({ serviceId, field, open, onOpenChange, onSave }: CustomFieldDrawerProps) {
  const isEditing = !!field
  const [formData, setFormData] = useState<Omit<CustomField, "created_at" | "updated_at">>(DEFAULT_FIELD)
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
          collection_stage: debouncedStage as any
        })
        if (active) {
          setLintResult(result)
        }
      } catch (err) {
        if (active) {
          // Surface to the user — silent console.error left them staring
          // at an empty validation pane after the spinner disappeared,
          // unsure whether their VCL was good or whether the lint had
          // run at all. Keep the console output for devtools triage too.
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
        return customFieldsApi.updateCustomField(serviceId, field!.name, formData as any)
      } else {
        return customFieldsApi.createCustomField(serviceId, formData)
      }
    },
    onSuccess: () => onSave(),
    onError: (error: any) => setValidationError(error.message || "Failed to save field")
  })

  // Reset form when the drawer opens or the edited field changes. The deps
  // intentionally exclude `saveMutation` — TanStack Query's mutation object
  // is recreated on every render, so listing it would re-fire this effect on
  // every keystroke and clobber the user's in-progress edits. We only want
  // to reset on the open/field transition; `saveMutation.reset()` is safe to
  // call against the current ref each time.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (open) {
      setFormData(field ? {
        ...field,
        collection_stage: field.collection_stage || "edge"
      } : { ...DEFAULT_FIELD })
      setValidationError(null)
      setLintResult(null)
      setLintFetchError(null)
      saveMutation.reset()
    }
  }, [open, field])

  const handleChange = (key: keyof CustomField, value: any) => {
    setFormData(prev => {
      const next = { ...prev, [key]: value };
      // Auto-slug label to name if not editing
      if (!isEditing && key === 'label') {
        next.name = value
          .toLowerCase()
          .replace(/[^a-z0-9_]/g, '_')
          .replace(/_+/g, '_')
          .replace(/^[^a-z]+/, '')
          .substring(0, 48);
      }
      return next;
    })
  }

  const isValidName = /^[a-z][a-z0-9_]{0,47}$/.test(formData.name)
  const isLintingStale = formData.vcl_log_expression !== debouncedVcl || formData.collection_stage !== debouncedStage
  const isSaveDisabled = saveMutation.isPending || isLinting || isLintingStale || !isValidName || !formData.label || !formData.vcl_log_expression || (lintResult && !lintResult.valid)

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
          {/* Basic Info */}
          <section className="space-y-4">
            <h3 className="text-sm font-semibold border-b pb-2">Basic Information</h3>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="cf-label">Label <span className="text-destructive">*</span></Label>
                <Input
                  id="cf-label"
                  placeholder="e.g. Referrer Domain"
                  value={formData.label}
                  onChange={e => handleChange('label', e.target.value)}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="cf-name">Field Name <span className="text-destructive">*</span></Label>
                <Input
                  id="cf-name"
                  placeholder="e.g. referrer_domain"
                  value={formData.name}
                  disabled={true}
                  className="font-mono text-sm bg-muted opacity-70"
                />
                {!isValidName && formData.name && (
                   <p className="text-[10px] text-destructive">Must start with a letter, lowercase alphanumeric and underscores only, max 48 chars.</p>
                )}
              </div>
            </div>
            <div className="space-y-2">
                <Label>Description</Label>
                <Textarea
                    placeholder="Describe what this field captures..."
                    value={formData.description || ''}
                    onChange={e => handleChange('description', e.target.value)}
                    className="h-20 text-sm"
                />
            </div>
          </section>

          {/* VCL Expression */}
          <section className="space-y-4">
            <div className="flex justify-between items-center border-b pb-2">
                <h3 className="text-sm font-semibold">VCL Log Expression <span className="text-destructive">*</span></h3>
            </div>

            <div className="space-y-3 p-4 bg-muted/30 border rounded-md">
              <Label className="text-xs">Collection Stage</Label>
              <div className="grid grid-cols-2 gap-3">
                <div
                  className={`flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors ${formData.collection_stage === 'edge' ? 'border-blue-500 bg-blue-500/5' : 'hover:bg-muted/50'}`}
                  onClick={() => handleChange('collection_stage', 'edge')}
                >
                  <span className="font-medium flex items-center gap-2 text-sm">
                    <Activity className="h-4 w-4 text-blue-500" />
                    Edge
                  </span>
                  <span className="text-[10px] text-muted-foreground mt-1">Captured before shielding or backend fetches. Best for client data.</span>
                </div>
                <div
                  className={`flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors ${formData.collection_stage === 'origin' ? 'border-purple-500 bg-purple-500/5' : 'hover:bg-muted/50'}`}
                  onClick={() => handleChange('collection_stage', 'origin')}
                >
                  <span className="font-medium flex items-center gap-2 text-sm">
                    <Activity className="h-4 w-4 text-purple-500" />
                    Origin
                  </span>
                  <span className="text-[10px] text-muted-foreground mt-1">Captured from the backend response. Best for origin timing or status.</span>
                </div>
              </div>
            </div>

            {formData.collection_stage === 'origin' && (
              <div className="space-y-3 p-4 bg-muted/30 border rounded-md mt-3">
                <Label className="text-xs">Origin Log Frequency</Label>
                <div className="grid grid-cols-2 gap-3">
                  <div
                    className={`flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors ${formData.origin_log_frequency === 'all' ? 'border-primary bg-primary/5' : 'hover:bg-muted/50'}`}
                    onClick={() => handleChange('origin_log_frequency', 'all')}
                  >
                    <span className="font-medium text-sm">All requests</span>
                    <span className="text-[10px] text-muted-foreground mt-1">Log the origin value even on cache hits.</span>
                  </div>
                  <div
                    className={`flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors ${formData.origin_log_frequency === 'miss_pass' ? 'border-primary bg-primary/5' : 'hover:bg-muted/50'}`}
                    onClick={() => handleChange('origin_log_frequency', 'miss_pass')}
                  >
                    <span className="font-medium text-sm">Only miss and pass</span>
                    <span className="text-[10px] text-muted-foreground mt-1">Log only when the request actively fetches from the origin.</span>
                  </div>
                </div>
              </div>
            )}

            <p className="text-xs text-muted-foreground mt-4">The VCL expression used to collect the value. This will be automatically injected into the selected collection stage.</p>
            <div className="bg-muted p-4 rounded-md font-mono text-sm border relative">
                <div className="flex items-center text-muted-foreground mb-2">
                    <code>set {formData.collection_stage === 'origin' ? 'beresp' : 'req'}.http.x-fos-{formData.collection_stage}-data:{formData.name || 'field_name'} =</code>
                </div>
                <Input
                  id="cf-vcl-expression"
                  aria-label="VCL Log Expression"
                  value={formData.vcl_log_expression}
                  onChange={e => handleChange('vcl_log_expression', e.target.value)}
                  placeholder="req.http.Host"
                  className="font-mono bg-background"
                />
            </div>

            {isEditing && (
              <p className="text-[10px] text-amber-600 bg-amber-500/10 p-2 rounded">
                 Warning: Changing the VCL expression affects new log data only. Historical data retains the previous value.
              </p>
            )}

            {/* Lint Results */}
            {isLinting ? (
                <div className="flex items-center gap-2 text-xs text-muted-foreground animate-pulse">
                    <Loader2 className="h-3 w-3 animate-spin" /> Validating VCL...
                </div>
            ) : lintFetchError ? (
                <div className="bg-amber-500/10 border border-amber-500/20 rounded p-3 text-xs text-amber-700 dark:text-amber-500 space-y-1">
                    <p className="font-semibold flex items-center gap-1.5">
                        <AlertTriangle className="h-3.5 w-3.5" /> {lintFetchError}
                    </p>
                    <p className="text-amber-700/80 dark:text-amber-500/80">
                        The expression will be re-validated automatically when the connection recovers.
                    </p>
                </div>
            ) : lintResult ? (
                <div className="space-y-2">
                    {lintResult.errors?.length > 0 && (
                        <div className="bg-destructive/10 border border-destructive/20 rounded p-3 text-xs text-destructive space-y-1">
                            <p className="font-semibold flex items-center gap-1.5"><AlertTriangle className="h-3.5 w-3.5" /> Validation Errors</p>
                            <ul className="list-disc pl-5 space-y-1">
                                {lintResult.errors.map((err) => <li key={err}>{err}</li>)}
                            </ul>
                        </div>
                    )}
                    {lintResult.warnings?.length > 0 && (
                        <div className="bg-amber-500/10 border border-amber-500/20 rounded p-3 text-xs text-amber-700 dark:text-amber-500 space-y-1">
                            <p className="font-semibold flex items-center gap-1.5"><AlertTriangle className="h-3.5 w-3.5" /> Warnings</p>
                            <ul className="list-disc pl-5 space-y-1">
                                {lintResult.warnings.map((warn) => <li key={warn}>{warn}</li>)}
                            </ul>
                        </div>
                    )}
                    {lintResult.valid && lintResult.warnings?.length === 0 && (
                        <p className="text-xs text-green-600 dark:text-green-500 flex items-center gap-1.5 font-medium">
                            <CheckCircle2 className="h-3.5 w-3.5" /> VCL Expression is valid.
                        </p>
                    )}
                </div>
            ) : null}
          </section>

          {/* Storage & Display */}
          <section className="space-y-4">
             <h3 className="text-sm font-semibold border-b pb-2">Storage & Display</h3>
             <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
                 <div className="space-y-2">
                     <LabelWithInfo labelClassName="text-xs" info="The underlying DuckDB column type. VARCHAR is safest for strings, while numeric types (BIGINT, DOUBLE) save space and enable mathematical operations." label="Data Type (DuckDB)" />
                     <Select value={formData.duckdb_type} onValueChange={v => handleChange('duckdb_type', v)}>
                        <SelectTrigger className="h-8 text-xs">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value="VARCHAR">VARCHAR (String)</SelectItem>
                            <SelectItem value="INTEGER">INTEGER (32-bit)</SelectItem>
                            <SelectItem value="BIGINT">BIGINT (64-bit)</SelectItem>
                            <SelectItem value="DOUBLE">DOUBLE (Float)</SelectItem>
                            <SelectItem value="BOOLEAN">BOOLEAN</SelectItem>
                        </SelectContent>
                     </Select>
                 </div>
                 <div className="space-y-2">
                     <LabelWithInfo labelClassName="text-xs" info="Determines how values are formatted and filtered in the UI. For example, 'IP Address' enables CIDR range filtering." label="Value Type (UI Handling)" />
                     <Select value={formData.value_type} onValueChange={v => handleChange('value_type', v)}>
                        <SelectTrigger className="h-8 text-xs">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value="string">Generic String</SelectItem>
                            <SelectItem value="numeric">Numeric Range</SelectItem>
                            <SelectItem value="boolean">Boolean (True/False)</SelectItem>
                            <SelectItem value="ip">IP Address</SelectItem>
                            <SelectItem value="url">URL Path</SelectItem>
                        </SelectContent>
                     </Select>
                 </div>
                 <div className="space-y-2">
                     <LabelWithInfo labelClassName="text-xs" info="An estimate of the size of this field per log line, used for calculating total storage costs and requirements." label="Bytes Estimate (per log)" />
                     <Input
                        type="number"
                        min="1"
                        max="1024"
                        className="h-8 text-xs"
                        value={formData.bytes_estimate}
                        onChange={e => handleChange('bytes_estimate', parseInt(e.target.value) || 20)}
                     />
                 </div>
             </div>

             <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mt-4 p-4 bg-muted/30 rounded-lg border">
                <div className="flex items-center justify-between">
                    <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-enabled" info="If disabled, this field will not be collected or processed from the logs." label="Enabled" />
                    <Switch id="toggle-enabled" checked={formData.enabled} onCheckedChange={v => handleChange('enabled', v)} />
                </div>
                <div className="flex items-center justify-between">
                    <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-nullable" info="If checked, this field can be empty. If unchecked, a default value or empty string will be used if the VCL expression returns no value." label="Nullable" />
                    <Switch id="toggle-nullable" checked={formData.nullable} onCheckedChange={v => handleChange('nullable', v)} />
                </div>
                <div className="flex items-center justify-between">
                    <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-dashboard" info="Enables this field for use in high-level charts and metrics. Recommended for fields with low cardinality like status codes or regions." label="Show in Dashboard" />
                    <Switch id="toggle-dashboard" checked={formData.show_in_dashboard} onCheckedChange={v => handleChange('show_in_dashboard', v)} />
                </div>
                <div className="flex items-center justify-between">
                    <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-logs" info="Displays this field in the raw log viewer for individual request inspection." label="Show in Log Explorer" />
                    <Switch id="toggle-logs" checked={formData.show_in_logs} onCheckedChange={v => handleChange('show_in_logs', v)} />
                </div>
                <div className="flex items-center justify-between">
                    <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-filterable" info="Allows this field to be used as a global filter across all pages of the dashboard." label="Filterable globally" />
                    <Switch id="toggle-filterable" checked={formData.filterable} onCheckedChange={v => handleChange('filterable', v)} />
                </div>
             </div>
          </section>

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
