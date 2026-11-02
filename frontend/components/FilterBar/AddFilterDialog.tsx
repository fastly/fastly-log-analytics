'use client'

import * as React from 'react'
import { useForm, useWatch } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { Filter, Plus } from 'lucide-react'
import { useFilterStore } from '@/stores/filterStore'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { OperatorSelector } from './OperatorSelector'
import { TokenAutocomplete, useCommonFilterFields } from './TokenAutocomplete'

export const addFilterSchema = z.object({
  field: z.string().min(1),
  value: z.string().min(1),
  mode: z.enum(['include', 'exclude']),
})

export type AddFilterValues = z.infer<typeof addFilterSchema>

export function AddFilterDialogContent({ onClose }: { onClose: () => void }) {
  const { maskIps, commonFields } = useCommonFilterFields()
  const {
    register,
    handleSubmit,
    setValue,
    control,
    formState: { isValid },
  } = useForm<AddFilterValues>({
    resolver: zodResolver(addFilterSchema),
    mode: 'onChange',
    defaultValues: { field: maskIps ? 'url' : 'ip', value: '', mode: 'include' },
  })
  const { field, value, mode } = useWatch({ control }) as AddFilterValues

  const addFilter = useFilterStore((s) => s.addFilter)

  const handleAdd = handleSubmit((values) => {
    addFilter(values.field, values.value, values.mode)
    setValue('value', '')
    onClose()
  })

  return (
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
          <OperatorSelector
            mode={mode}
            onModeChange={(nextMode) => setValue('mode', nextMode)}
          />

          <TokenAutocomplete
            field={field}
            value={value}
            valueRegister={register('value')}
            commonFields={commonFields}
            onFieldChange={(val) => setValue('field', val)}
            onSelectSuggestion={(val) => setValue('value', val, { shouldValidate: true })}
          />
        </div>

        <div className="p-4 bg-background border-t flex justify-end">
          <Button
            type="submit"
            disabled={!isValid}
            className="w-full sm:w-auto shadow-md gap-2 h-10 px-6"
          >
            <Plus className="h-4 w-4" />
            Apply Filter
          </Button>
        </div>
      </form>
    </DialogContent>
  )
}

export function AddFilterDialog() {
  const [open, setOpen] = React.useState(false)
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          <Button
            data-testid="add-filter-stub"
            variant="outline"
            size="sm"
            className="h-9 sm:h-7 gap-1 pr-2.5 sm:pr-2 pl-2 sm:pl-1.5 border-dashed text-xs sm:text-[11px]"
          >
            <Plus className="h-3 w-3" />
            <span>Add Filter</span>
          </Button>
        }
      />
      {open && <AddFilterDialogContent onClose={() => setOpen(false)} />}
    </Dialog>
  )
}
