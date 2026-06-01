'use client'

import React from 'react'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { LayoutDashboard, EyeOff } from 'lucide-react'

interface DashboardHeaderProps {
  visibleCardsCount: number
  allCards: Array<{ id: string, label: string, inActiveFormat?: boolean }>
  visibleCards: Set<string>
  onToggleCard: (id: string) => void
  onShowAll: () => void
  onResetCards: () => void
}

export function DashboardHeader({
  visibleCardsCount,
  allCards,
  visibleCards,
  onToggleCard,
  onShowAll,
  onResetCards
}: DashboardHeaderProps) {
  return (
    <Popover>
      <PopoverTrigger render={
        <Button variant="outline" size="sm" className="h-9 gap-1.5">
          <span className="flex items-center gap-1.5">
            <LayoutDashboard className="h-4 w-4" />
            <span className="hidden sm:inline text-xs">Cards</span>
            <Badge variant="secondary" className="h-4 text-[10px] px-1.5">
              {visibleCardsCount}
            </Badge>
          </span>
        </Button>
      } />
      <PopoverContent align="end" className="w-72 p-3">
        <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-3">
          Visible cards
        </p>
        <div className="grid grid-cols-2 gap-x-4 gap-y-2">
          {allCards.map(card => (
            <div key={card.id} className="flex items-center gap-2">
              <Checkbox
                id={`card-${card.id}`}
                checked={visibleCards.has(card.id)}
                onCheckedChange={() => React.startTransition(() => onToggleCard(card.id))}
              />
              <Label htmlFor={`card-${card.id}`} className="text-xs cursor-pointer leading-tight inline-flex items-center gap-1">
                {card.label}
                {card.inActiveFormat === false && (
                  <Tooltip>
                    <TooltipTrigger render={<span className="inline-flex items-center" />}>
                      <EyeOff className="h-3 w-3 text-muted-foreground/70" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p className="text-[10px]">Not in active log format.</p>
                    </TooltipContent>
                  </Tooltip>
                )}
              </Label>
            </div>
          ))}
        </div>
        <div className="flex gap-2 mt-4 pt-3 border-t">
          <Button
            variant="ghost" size="sm" className="text-xs h-7 flex-1"
            onClick={() => React.startTransition(() => onShowAll())}
          >
            Show all
          </Button>
          <Button
            variant="ghost" size="sm" className="text-xs h-7 flex-1"
            onClick={() => React.startTransition(() => onResetCards())}
          >
            Reset
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  )
}
