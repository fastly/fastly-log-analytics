'use client'

import { useState } from 'react'
import { Checkbox } from '@/components/ui/checkbox'
import { Label } from '@/components/ui/label'
import { ChevronRight, ChevronDown } from 'lucide-react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

interface CmcdConfigSectionProps {
  enabled: boolean
  onEnabledChange: (enabled: boolean) => void
  mode: string
  onModeChange: (mode: string) => void
  version: number
  onVersionChange: (version: number) => void
  disabled?: boolean
}

export function CmcdConfigSection({
  enabled,
  onEnabledChange,
  mode,
  onModeChange,
  version,
  onVersionChange,
  disabled,
}: CmcdConfigSectionProps) {
  const [isOpen, setIsOpen] = useState(false)

  return (
    <div className="border border-border/60 rounded-lg overflow-hidden bg-card/50">
      <div className="w-full flex items-center justify-between p-3 bg-muted/20 hover:bg-muted/40 transition-colors text-left">
        <div className="flex items-center gap-3">
          <Checkbox
            checked={enabled}
            onCheckedChange={(checked) => onEnabledChange(checked as boolean)}
            disabled={disabled}
            className="mr-1"
          />
          <button
            type="button"
            onClick={() => setIsOpen(!isOpen)}
            aria-expanded={isOpen}
            className="flex items-center gap-2 text-left cursor-pointer bg-transparent border-0 p-0"
          >
            <h4 className="text-xs font-bold tracking-tight uppercase text-foreground/80">
              CMCD Streaming Metrics
            </h4>
            <span className="text-[10px] text-muted-foreground ml-1">+~348 bytes</span>
          </button>
        </div>
        <button
          type="button"
          onClick={() => setIsOpen(!isOpen)}
          aria-label={isOpen ? "Collapse group" : "Expand group"}
          className="text-muted-foreground bg-transparent border-0 p-0 cursor-pointer"
        >
          {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </button>
      </div>

      {isOpen && (
        <div className="p-4 pt-2 border-t border-border/40 bg-card">
          <p className="text-[11px] text-muted-foreground mb-3 leading-relaxed">
            Capture Common Media Client Data (CTA-5004) fields from streaming video players.
            Extracts session, bitrate, buffer, and quality metrics at the edge.
            <span className="block mt-1.5 text-amber-600 dark:text-amber-500 font-medium italic">
              ⚠ Enabling this group deploys a VCL snippet to your service to extract CMCD fields from query strings or HTTP headers.
            </span>
          </p>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="cmcd-version" className="text-[11px] font-medium">CMCD Version</Label>
              <Select
                value={String(version)}
                onValueChange={(v) => v && onVersionChange(Number(v))}
                disabled={disabled || !enabled}
              >
                <SelectTrigger id="cmcd-version" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1">v1 (CTA-5004)</SelectItem>
                  <SelectItem value="2">v2 (CTA-5004-2)</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="cmcd-mode" className="text-[11px] font-medium">Transport Mode</Label>
              <Select
                value={mode}
                onValueChange={(v) => v && onModeChange(v)}
                disabled={disabled || !enabled}
              >
                <SelectTrigger id="cmcd-mode" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="query_string">Query String (?CMCD=...)</SelectItem>
                  <SelectItem value="headers">HTTP Headers (CMCD-*)</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
