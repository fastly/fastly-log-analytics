'use client'

import * as React from 'react'
import { type UseFormRegisterReturn } from 'react-hook-form'
import { Loader2, Search } from 'lucide-react'
import { cn, formatCount, isIpFamilyField } from '@/lib/utils'
import { useDebounce } from '@/hooks/useDashboardCards'
import { useFieldValues, useLogFieldsCatalog, useMaskIps } from '@/hooks/useBootstrap'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

export const COMMON_FIELD_IDS = [
  'ip', 'url', 'host', 'method', 'status', 'ua', 'country', 'city',
  'asn', 'p_type', 'p_desc', 'ja4', 'ja3', 'cache', 'edge', 'pop',
  'backend', 'proto', 'tls', 'referer', 'waf', 'waf_resp',
]

export function useCommonFilterFields() {
  const maskIps = useMaskIps()
  const { data: catalog } = useLogFieldsCatalog()
  const commonFields = React.useMemo(
    () =>
      COMMON_FIELD_IDS
        .filter((id) => !(maskIps && isIpFamilyField(id)))
        .map((id) => ({
          value: id,
          label: catalog?.fields?.find((f) => f.id === id)?.label ?? id,
        })),
    [maskIps, catalog]
  )
  return { maskIps, commonFields }
}

export interface TokenAutocompleteProps {
  field: string
  value: string
  valueRegister: UseFormRegisterReturn
  commonFields: { value: string; label: string }[]
  onFieldChange: (nextField: string) => void
  onSelectSuggestion: (val: string) => void
}

export function TokenAutocomplete({
  field,
  value,
  valueRegister,
  commonFields,
  onFieldChange,
  onSelectSuggestion,
}: TokenAutocompleteProps) {
  const debouncedValue = useDebounce(value)
  const { data: suggestions, isLoading: isLoadingSuggestions } = useFieldValues({
    field,
    search: debouncedValue,
    limit: 10,
    enabled: debouncedValue.length > 0,
  })

  return (
    <>
      <div className="grid gap-2">
        <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
          Field
        </Label>
        <Select value={field} onValueChange={(val) => val && onFieldChange(val)}>
          <SelectTrigger className="bg-background shadow-sm h-10 w-full justify-between pr-3">
            <SelectValue placeholder="Select field">
              {(val) => commonFields.find((f) => f.value === val)?.label ?? String(val)}
            </SelectValue>
          </SelectTrigger>
          <SelectContent align="start" className="max-h-[300px]">
            {commonFields.map((f) => (
              <SelectItem key={f.value} value={f.value}>
                {f.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="grid gap-2">
        <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
          Match Value
        </Label>
        <div className="relative group">
          <div className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none group-focus-within:text-primary transition-colors">
            <Search className="h-3.5 w-3.5" />
          </div>
          <Input
            placeholder={`Type a ${field} value...`}
            {...valueRegister}
            className="bg-background shadow-sm h-10 pl-9"
            autoFocus
            autoComplete="off"
          />
          {isLoadingSuggestions && (
            <div className="absolute right-3 top-1/2 -translate-y-1/2">
              <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
            </div>
          )}
        </div>

        {value.length > 0 && suggestions?.values && suggestions.values.length > 0 && (
          <div className="mt-1 border rounded-md bg-background shadow-lg overflow-hidden animate-in fade-in zoom-in-95 duration-100">
            <ScrollArea
              className={cn('max-h-[160px]', suggestions.values.length > 5 ? 'h-[160px]' : '')}
            >
              <div className="p-1">
                {suggestions.values.map((v) => (
                  <button
                    key={String(v.value)}
                    type="button"
                    onClick={() => onSelectSuggestion(String(v.value))}
                    className="w-full flex items-center justify-between px-3 py-2 text-xs hover:bg-muted rounded-sm transition-colors text-left group"
                  >
                    <span className="truncate max-w-[240px] font-medium">
                      {v.value === null ? (
                        <em className="text-muted-foreground">NULL</em>
                      ) : (
                        String(v.label || v.value)
                      )}
                    </span>
                    <span className="text-[10px] text-muted-foreground group-hover:text-foreground tabular-nums">
                      {formatCount(v.count)} hits
                    </span>
                  </button>
                ))}
              </div>
            </ScrollArea>
          </div>
        )}

        <p className="text-[10px] text-muted-foreground mt-1 leading-tight flex items-start gap-1">
          <span className="text-primary font-bold">Pro Tip:</span>
          <span>
            Exact matches by default. Use{' '}
            <code className="bg-background border rounded-sm px-1 font-mono text-[9px]">*</code> for
            wildcards.
          </span>
        </p>
      </div>
    </>
  )
}
