'use client'

import React from 'react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Filter, Code2 } from 'lucide-react'
import type { QueryMode } from '../_sql_builder'

interface ModeToggleProps {
  mode: QueryMode
  onModeChange: (next: QueryMode) => void
}

/**
 * Mode toggle. Renders above the editor/preview so it's the first thing
 * a user sees on the page and stays in a predictable spot when toggling.
 */
export function ModeToggle({ mode, onModeChange }: ModeToggleProps) {
  return (
    <Tabs value={mode} onValueChange={(v) => onModeChange(v as QueryMode)}>
      <TabsList>
        <TabsTrigger value="structured">
          <Filter className="h-3.5 w-3.5" />
          Structured
        </TabsTrigger>
        <TabsTrigger value="raw">
          <Code2 className="h-3.5 w-3.5" />
          Edit Raw SQL
        </TabsTrigger>
      </TabsList>
    </Tabs>
  )
}
