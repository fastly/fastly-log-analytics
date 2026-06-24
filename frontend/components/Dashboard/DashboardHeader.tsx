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
      {/* aria-label lives on the PopoverTrigger directly (not on
          the Button render-prop) so it survives SSR — Base UI's
          render-prop merge drops aria-label from the inner element
          on the server pass, leaving the SSR'd button with no
          accessible name. The visible "Cards" text is
          `hidden sm:inline` so on <640px viewports it's empty too —
          the label is the only reliable name across both states. */}
      <PopoverTrigger
        aria-label="Toggle visible dashboard cards"
        render={
          <Button variant="outline" size="sm" className="h-9 gap-1.5">
            <span className="flex items-center gap-1.5">
              <LayoutDashboard className="h-4 w-4" />
              <span className="hidden sm:inline text-xs">Cards</span>
              <Badge variant="secondary" className="h-4 text-[10px] px-1.5">
                {visibleCardsCount}
              </Badge>
            </span>
          </Button>
        }
      />
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
                    {/* A-8 (a11y, WCAG 2.1.1): tabIndex + role="button" so
                        keyboard users can focus the EyeOff icon and reveal
                        the tooltip. A Button here would intercept clicks
                        meant for the parent Label/Checkbox. */}
                    <TooltipTrigger
                      render={
                        <span
                          className="inline-flex items-center rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          tabIndex={0}
                          role="button"
                          aria-label="Not in active log format"
                        />
                      }
                    >
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
