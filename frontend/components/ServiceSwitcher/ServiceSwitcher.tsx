'use client'

import * as React from 'react'
import { Check, ChevronsUpDown } from 'lucide-react'
import { useRouter, usePathname } from 'next/navigation'

import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import { useServiceStore } from '@/stores/serviceStore'

import { buttonVariants } from '@/components/ui/button'

// Pre-fix this component also called useBootstrap() to "ensure services are
// loaded". AppLayout (which wraps this) already calls useBootstrap and
// populates useServiceStore as a side effect — having ServiceSwitcher call
// it too added a second hook subscription that triggered extra renders on
// every bootstrap settle, even though React Query deduped the network
// request itself. The store is the right read source.
export function ServiceSwitcher() {
  const [open, setOpen] = React.useState(false)
  const { services, activeServiceId, setActiveServiceId } = useServiceStore()
  const router = useRouter()
  const pathname = usePathname()

  const activeService = services.find((s) => s.id === activeServiceId)

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        role="combobox"
        aria-expanded={open}
        aria-label="Active service"
        className={cn(buttonVariants({ variant: "outline" }), "w-[250px] justify-between")}
      >
        <span className="flex min-w-0 flex-1 items-center justify-between">
          <span className="truncate">
            {activeService ? activeService.name : 'Select service...'}
          </span>
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </span>
      </PopoverTrigger>
      <PopoverContent className="w-[250px] p-0">
        <Command>
          <CommandInput placeholder="Search service..." />
          <CommandList>
            <CommandEmpty>No service found.</CommandEmpty>
            <CommandGroup>
              {services.map((service) => (
                <CommandItem
                  key={service.id}
                  value={service.name}
                  onSelect={() => {
                    setActiveServiceId(service.id)
                    setOpen(false)
                    router.push(`${pathname}?service=${service.id}`)
                  }}
                >
                  <Check
                    className={cn(
                      'mr-2 h-4 w-4',
                      activeServiceId === service.id ? 'opacity-100' : 'opacity-0'
                    )}
                  />
                  {service.name}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
