import React from 'react'
import { z } from 'zod'
import { oneOf } from '@/lib/utils'
import { type CustomField, type VclLintResult } from '@/lib/api'
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
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import { AlertTriangle, CheckCircle2, Loader2, Activity } from 'lucide-react'

export type FormState = Omit<CustomField, "created_at" | "updated_at">

export const DUCKDB_TYPES: readonly CustomField['duckdb_type'][] = ['VARCHAR', 'INTEGER', 'BIGINT', 'DOUBLE', 'BOOLEAN']
export const VALUE_TYPES: readonly CustomField['value_type'][] = ['string', 'numeric', 'boolean', 'ip', 'url']

export const customFieldFormSchema = z.object({
  name: z.string().regex(/^[a-z][a-z0-9_]{0,47}$/, 'Invalid snake_case field name'),
  label: z.string().min(1, 'Label is required'),
  description: z.string().optional(),
  vcl_log_expression: z.string().min(1, 'VCL expression is required'),
  collection_stage: z.enum(['edge', 'origin']),
  origin_log_frequency: z.enum(['all', 'once']).optional(),
  duckdb_type: z.enum(['VARCHAR', 'INTEGER', 'BIGINT', 'DOUBLE', 'BOOLEAN']),
  value_type: z.enum(['string', 'numeric', 'boolean', 'ip', 'url']),
  bytes_estimate: z.number().int().min(1),
  nullable: z.boolean(),
  enabled: z.boolean(),
  show_in_dashboard: z.boolean(),
  show_in_logs: z.boolean(),
  filterable: z.boolean(),
})

export const DEFAULT_FIELD: FormState = {
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

export interface CustomFieldBasicSectionProps {
  formData: FormState
  isValidName: boolean
  onChange: <K extends keyof FormState>(key: K, value: FormState[K]) => void
}

export function CustomFieldBasicSection({
  formData,
  isValidName,
  onChange,
}: CustomFieldBasicSectionProps) {
  return (
    <section className="space-y-4">
      <h3 className="text-sm font-semibold border-b pb-2">Basic Information</h3>
      <div className="grid grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="cf-label">Label <span className="text-destructive">*</span></Label>
          <Input
            id="cf-label"
            placeholder="e.g. Referrer Domain"
            value={formData.label}
            onChange={e => onChange('label', e.target.value)}
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
          onChange={e => onChange('description', e.target.value)}
          className="h-20 text-sm"
        />
      </div>
    </section>
  )
}

export interface CustomFieldVclSectionProps {
  formData: FormState
  isEditing: boolean
  isLinting: boolean
  lintFetchError: string | null
  lintResult: VclLintResult | null
  onChange: <K extends keyof FormState>(key: K, value: FormState[K]) => void
}

export function CustomFieldVclSection({
  formData,
  isEditing,
  isLinting,
  lintFetchError,
  lintResult,
  onChange,
}: CustomFieldVclSectionProps) {
  return (
    <section className="space-y-4">
      <div className="flex justify-between items-center border-b pb-2">
        <h3 className="text-sm font-semibold">VCL Log Expression <span className="text-destructive">*</span></h3>
      </div>

      <div className="space-y-3 p-4 bg-muted/30 border rounded-md">
        <Label id="cf-stage-label" className="text-xs">Collection Stage</Label>
        <div role="radiogroup" aria-labelledby="cf-stage-label" className="grid grid-cols-2 gap-3">
          {/* a11y: <div onClick> radio cards were keyboard-invisible
              and not announced as a radio group. Each card is now a
              proper <button role="radio">. Tab moves to the group,
              arrow keys / Space / Enter activate per ARIA APG. */}
          <button
            type="button"
            role="radio"
            aria-checked={formData.collection_stage === 'edge'}
            className={`text-left flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none ${formData.collection_stage === 'edge' ? 'border-blue-500 bg-blue-500/5' : 'hover:bg-muted/50'}`}
            onClick={() => onChange('collection_stage', 'edge')}
          >
            <span className="font-medium flex items-center gap-2 text-sm">
              <Activity className="h-4 w-4 text-blue-500" />
              Edge
            </span>
            <span className="text-[10px] text-muted-foreground mt-1">Captured before shielding or backend fetches. Best for client data.</span>
          </button>
          <button
            type="button"
            role="radio"
            aria-checked={formData.collection_stage === 'origin'}
            className={`text-left flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none ${formData.collection_stage === 'origin' ? 'border-purple-500 bg-purple-500/5' : 'hover:bg-muted/50'}`}
            onClick={() => onChange('collection_stage', 'origin')}
          >
            <span className="font-medium flex items-center gap-2 text-sm">
              <Activity className="h-4 w-4 text-purple-500" />
              Origin
            </span>
            <span className="text-[10px] text-muted-foreground mt-1">Captured from the backend response. Best for origin timing or status.</span>
          </button>
        </div>
      </div>

      {formData.collection_stage === 'origin' && (
        <div className="space-y-3 p-4 bg-muted/30 border rounded-md mt-3">
          <Label id="cf-freq-label" className="text-xs">Origin Log Frequency</Label>
          <div role="radiogroup" aria-labelledby="cf-freq-label" className="grid grid-cols-2 gap-3">
            <button
              type="button"
              role="radio"
              aria-checked={formData.origin_log_frequency === 'all'}
              className={`text-left flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none ${formData.origin_log_frequency === 'all' ? 'border-primary bg-primary/5' : 'hover:bg-muted/50'}`}
              onClick={() => onChange('origin_log_frequency', 'all')}
            >
              <span className="font-medium text-sm">All requests</span>
              <span className="text-[10px] text-muted-foreground mt-1">Log the origin value even on cache hits.</span>
            </button>
            <button
              type="button"
              role="radio"
              aria-checked={formData.origin_log_frequency === 'miss_pass'}
              className={`text-left flex flex-col space-y-1 border rounded-md p-3 cursor-pointer transition-colors focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none ${formData.origin_log_frequency === 'miss_pass' ? 'border-primary bg-primary/5' : 'hover:bg-muted/50'}`}
              onClick={() => onChange('origin_log_frequency', 'miss_pass')}
            >
              <span className="font-medium text-sm">Only miss and pass</span>
              <span className="text-[10px] text-muted-foreground mt-1">Log only when the request actively fetches from the origin.</span>
            </button>
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
          onChange={e => onChange('vcl_log_expression', e.target.value)}
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
  )
}

export interface CustomFieldStorageSectionProps {
  formData: FormState
  onChange: <K extends keyof FormState>(key: K, value: FormState[K]) => void
}

export function CustomFieldStorageSection({
  formData,
  onChange,
}: CustomFieldStorageSectionProps) {
  return (
    <section className="space-y-4">
      <h3 className="text-sm font-semibold border-b pb-2">Storage & Display</h3>
      <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
        <div className="space-y-2">
          <LabelWithInfo labelClassName="text-xs" info="The underlying DuckDB column type. VARCHAR is safest for strings, while numeric types (BIGINT, DOUBLE) save space and enable mathematical operations." label="Data Type (DuckDB)" />
          <Select value={formData.duckdb_type} onValueChange={v => { const t = oneOf(DUCKDB_TYPES, v); if (t) onChange('duckdb_type', t) }}>
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
          <Select value={formData.value_type} onValueChange={v => { const t = oneOf(VALUE_TYPES, v); if (t) onChange('value_type', t) }}>
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
            onChange={e => onChange('bytes_estimate', parseInt(e.target.value) || 20)}
          />
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mt-4 p-4 bg-muted/30 rounded-lg border">
        <div className="flex items-center justify-between">
          <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-enabled" info="If disabled, this field will not be collected or processed from the logs." label="Enabled" />
          <Switch id="toggle-enabled" checked={formData.enabled} onCheckedChange={v => onChange('enabled', v)} />
        </div>
        <div className="flex items-center justify-between">
          <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-nullable" info="If checked, this field can be empty. If unchecked, a default value or empty string will be used if the VCL expression returns no value." label="Nullable" />
          <Switch id="toggle-nullable" checked={formData.nullable} onCheckedChange={v => onChange('nullable', v)} />
        </div>
        <div className="flex items-center justify-between">
          <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-dashboard" info="Displays a leaderboard card on the dashboard. Enabling this automatically pre-calculates hourly rollups for instant, high-performance loading." label="Show in Dashboard" />
          <Switch id="toggle-dashboard" checked={formData.show_in_dashboard} onCheckedChange={v => onChange('show_in_dashboard', v)} />
        </div>
        <div className="flex items-center justify-between">
          <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-logs" info="Displays this field in the raw log viewer for individual request inspection." label="Show in Log Explorer" />
          <Switch id="toggle-logs" checked={formData.show_in_logs} onCheckedChange={v => onChange('show_in_logs', v)} />
        </div>
        <div className="flex items-center justify-between">
          <LabelWithInfo labelClassName="text-sm font-normal cursor-pointer" htmlFor="toggle-filterable" info="Allows this field to be used as a global filter across all pages of the dashboard." label="Filterable globally" />
          <Switch id="toggle-filterable" checked={formData.filterable} onCheckedChange={v => onChange('filterable', v)} />
        </div>
      </div>
    </section>
  )
}
