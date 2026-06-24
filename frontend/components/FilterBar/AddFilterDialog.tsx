'use client'

import React, { useState } from 'react'
import { Plus, Filter, Search, Loader2, Check, X } from 'lucide-react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useFilterStore } from '@/stores/filterStore'
import { cn } from '@/lib/utils'
import { useDebounce } from '@/hooks/useDebounce'
import { useFieldValues } from '@/hooks/useFieldValues'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'

const COMMON_FIELD_IDS = [
  'ip', 'url', 'host', 'method', 'status', 'ua', 'country', 'city',
  'asn', 'p_type', 'p_desc', 'ja4', 'ja3', 'cache', 'edge', 'pop',
  'backend', 'proto', 'tls', 'referer', 'waf', 'waf_resp',
]

export function AddFilterDialog() {
  const [open, setOpen] = useState(false)
  const [field, setField] = useState('ip')
  const [value, setValue] = useState('')
  const [mode, setMode] = useState<'include' | 'exclude'>('include')
  const debouncedValue = useDebounce(value)

  const { data: catalog } = useLogFieldsCatalog()
  const commonFields = COMMON_FIELD_IDS.map(id => ({
    value: id,
    label: catalog?.fields?.find((f: any) => f.id === id)?.label ?? id,
  }))

  const addFilter = useFilterStore(s => s.addFilter)

  const { data: suggestions, isLoading: isLoadingSuggestions } = useFieldValues({
    field,
    search: debouncedValue,
    limit: 10,
    enabled: open && debouncedValue.length > 0,
  })

  const handleAdd = (e?: React.FormEvent) => {
    e?.preventDefault()
    if (field && value) {
      addFilter(field, value, mode)
      setValue('')
      setOpen(false)
    }
  }

  const handleSelectSuggestion = (val: string | number) => {
    setValue(String(val))
    // We don't automatically add, let the user confirm or edit
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button variant="outline" size="sm" className="h-9 sm:h-7 gap-1 pr-2.5 sm:pr-2 pl-2 sm:pl-1.5 border-dashed text-xs sm:text-[11px]">
            <Plus className="h-3 w-3" />
            <span>Add Filter</span>
          </Button>
        }
      />
      <DialogContent className="sm:max-w-[420px] p-0 overflow-hidden gap-0">
        <form onSubmit={handleAdd}>
          <div className="p-5 pb-4">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2.5 text-lg font-semibold">
                <div className="flex h-8 w-8 items-center justify-center rounded-md bg-primary/10 border border-primary/20 shrink-0">
                  <Filter className="h-4 w-4 text-primary" />
                </div>
                Add Custom Filter
              </DialogTitle>
            </DialogHeader>
          </div>

          <div className="px-5 py-6 bg-muted/30 border-y grid gap-5">
            <div className="grid gap-2">
              <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
                Filter Type
              </Label>
              <Tabs value={mode} onValueChange={(v: any) => setMode(v)} className="w-full">
                <TabsList className="grid w-full grid-cols-2 h-9">
                  <TabsTrigger value="include" className="text-xs gap-1.5">
                    <Check className="h-3 w-3 text-green-500" /> Include
                  </TabsTrigger>
                  <TabsTrigger value="exclude" className="text-xs gap-1.5">
                    <X className="h-3 w-3 text-red-500" /> Exclude
                  </TabsTrigger>
                </TabsList>
              </Tabs>
            </div>

            <div className="grid gap-2">
              <Label className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
                Field
              </Label>
              <Select value={field} onValueChange={val => val && setField(val)}>
                <SelectTrigger className="bg-background shadow-sm h-10 w-full justify-between pr-3">
                  <SelectValue placeholder="Select field">
                    {(val) => {
                      const found = commonFields.find(f => f.value === val)
                      return found ? found.label : String(val)
                    }}
                  </SelectValue>
                </SelectTrigger>
                <SelectContent align="start" className="max-h-[300px]">
                  {commonFields.map(f => (
                    <SelectItem key={f.value} value={f.value}>{f.label}</SelectItem>
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
                  value={value}
                  onChange={e => setValue(e.target.value)}
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
                  <ScrollArea className={cn("max-h-[160px]", suggestions.values.length > 5 ? "h-[160px]" : "")}>
                    <div className="p-1">
                      {suggestions.values.map((v: any, i: number) => (
                        <button
                          key={i}
                          type="button"
                          onClick={() => handleSelectSuggestion(v.value)}
                          className="w-full flex items-center justify-between px-3 py-2 text-xs hover:bg-muted rounded-sm transition-colors text-left group"
                        >
                          <span className="truncate max-w-[240px] font-medium">
                            {v.value === null ? <em className="text-muted-foreground">NULL</em> : String(v.label || v.value)}
                          </span>
                          <span className="text-[10px] text-muted-foreground group-hover:text-foreground tabular-nums">
                            {v.count.toLocaleString()} hits
                          </span>
                        </button>
                      ))}
                    </div>
                  </ScrollArea>
                </div>
              )}

              <p className="text-[10px] text-muted-foreground mt-1 leading-tight flex items-start gap-1">
                <span className="text-primary font-bold">Pro Tip:</span>
                <span>Exact matches by default. Use <code className="bg-background border rounded-[3px] px-1 font-mono text-[9px]">*</code> for wildcards.</span>
              </p>
            </div>
          </div>

          <div className="p-4 bg-background border-t flex justify-end">
            <Button type="submit" disabled={!value} className="w-full sm:w-auto shadow-md gap-2 h-10 px-6">
              <Plus className="h-4 w-4" />
              Apply Filter
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  )
}
