'use client'

import React, { useState, useEffect } from 'react'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Loader2, CloudDownload, Calendar } from 'lucide-react'
import { formatDateTime, cn } from '@/lib/utils';
import { formatBytes } from '@/lib/format'
import { adminFetch } from '@/lib/api'
import { formatForInput, parseFromInput } from '@/lib/date'
import {
  panelDialogContent,
  panelDialogHeaderMuted,
} from '@/lib/panel-dialog'
import { useTimezoneStore } from '@/stores/timezoneStore'
import { useServiceStore } from '@/stores/serviceStore'

interface SyncFromCloudModalProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onStartSync: (range?: { start: string; end: string }) => void
}

export function SyncFromCloudModal({ open, onOpenChange, onStartSync }: SyncFromCloudModalProps) {
  const { timezone } = useTimezoneStore()
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  const [lakeInfo, setLakeInfo] = useState<any>(null)
  const [lakeError, setLakeError] = useState<string | null>(null)
  const [isAnalyzing, setIsAnalyzing] = useState(false)

  const [importMode, setImportMode] = useState<"all" | "range">("all")
  const [importRange, setImportRange] = useState({ start: "", end: "" })

  useEffect(() => {
    if (open && activeServiceId) {
      handleAnalyzeLake()
    }
    if (!open) {
      setImportMode("all")
      setImportRange({ start: "", end: "" })
    }
  }, [open, activeServiceId])

  const estimatedImportSize = React.useMemo(() => {
    if (!lakeInfo?.calendar) return 0;
    let total = 0;
    const start = importRange.start;
    const end = importRange.end;

    for (const [dateStr, stats] of Object.entries(lakeInfo.calendar)) {
      if (dateStr === "unknown") continue;

      if (importMode === "range") {
        if (start && dateStr < start.split('T')[0]) continue;
        if (end && dateStr > end.split('T')[0]) continue;
      }

      total += (stats as any).size_bytes || 0;
    }
    return total;
  }, [lakeInfo, importMode, importRange]);

  const handleAnalyzeLake = async () => {
    if (!activeServiceId) return;
    setIsAnalyzing(true);
    setLakeError(null);
    try {
      // Raw fetch: the lake-info route resolves service_id via a FastAPI
      // dependency, so OpenAPI declares `path?: never` and the typed
      // client can't substitute the param. The URL path is still tightly
      // coupled to the backend route. Add a response_model on the
      // handler (and lift service_id into an explicit path arg) to make
      // this typed.
      const resp = await adminFetch(`/api/services/${activeServiceId}/lake-info`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
      }
      const data = await resp.json();
      if (data.ok) {
        setLakeInfo(data);
        if (data.range?.start && data.range?.end) {
          setImportRange({ start: data.range.start, end: data.range.end });
        }
      } else {
        setLakeError(data.error || data.message || "Failed to analyze data lake.");
      }
    } catch (e: any) {
      console.error("Failed to analyze data lake:", e);
      setLakeError(e.message || "Connection error while analyzing data lake.");
    } finally {
      setIsAnalyzing(false);
    }
  };

  const handleStartSync = () => {
    if (importMode === "range") {
      onStartSync({ start: importRange.start, end: importRange.end });
    } else {
      onStartSync();
    }
    onOpenChange(false);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className={cn("sm:max-w-xl", panelDialogContent)}>
        <DialogHeader className={panelDialogHeaderMuted}>
          <div className="flex items-center justify-between">
            <DialogTitle className="flex items-center gap-2">
              <CloudDownload className="h-5 w-5 text-primary" />
              Sync from Cloud
            </DialogTitle>
          </div>
          <div className="text-sm text-muted-foreground mt-1">
            Import missing historical data from the shared Iceberg table.
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto p-6 space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-sm font-bold uppercase tracking-widest text-foreground/80">Available Cloud Data</h3>
              <p className="text-[10px] text-muted-foreground mt-1">Select the range of data you want to import into your local cache.</p>
            </div>
            {lakeInfo && !isAnalyzing && (
              <Badge variant="secondary" className="font-mono bg-muted/50 border shadow-sm">
                ~{formatBytes(estimatedImportSize)}
              </Badge>
            )}
          </div>

          {isAnalyzing ? (
            <div className="p-4 border border-dashed rounded-lg bg-muted/5 flex flex-col items-center justify-center space-y-3 py-12">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
              <div className="text-center">
                <p className="text-sm font-medium">Scanning Cloud Catalog...</p>
                <p className="text-xs text-muted-foreground">Checking Iceberg metadata for available snapshots.</p>
              </div>
            </div>
          ) : lakeError ? (
            <div className="p-4 border border-destructive/20 rounded-lg bg-destructive/5 text-center space-y-2">
              <p className="text-xs text-destructive font-medium">Analysis Failed</p>
              <p className="text-[10px] text-muted-foreground">{lakeError}</p>
              <Button variant="outline" size="sm" onClick={handleAnalyzeLake} className="h-7 text-[10px] mt-2">
                Try Again
              </Button>
            </div>
          ) : lakeInfo?.table_exists ? (
            <div className="space-y-4">
              <div className="bg-background/50 border rounded-lg p-3 grid grid-cols-2 gap-4">
                <div className="space-y-1">
                  <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">Available From</span>
                  <div className="text-xs font-mono font-semibold flex items-center gap-1.5">
                    <Calendar className="h-3 w-3 text-primary" />
                    {formatDateTime(lakeInfo.range?.start, timezone)}
                  </div>
                </div>
                <div className="space-y-1">
                  <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">Available To</span>
                  <div className="text-xs font-mono font-semibold flex items-center gap-1.5">
                    <Calendar className="h-3 w-3 text-primary" />
                    {formatDateTime(lakeInfo.range?.end, timezone)}
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3">
                <button
                  onClick={() => setImportMode("all")}
                  className={`flex flex-col items-center gap-2 p-3 border rounded-lg transition-all text-center ${importMode === "all" ? "border-primary bg-primary/5 ring-1 ring-primary/20" : "border-muted hover:bg-muted/50"}`}
                >
                  <div className="font-bold text-xs">Import All</div>
                  <p className="text-[9px] text-muted-foreground">Sync every available file</p>
                </button>
                <button
                  onClick={() => setImportMode("range")}
                  className={`flex flex-col items-center gap-2 p-3 border rounded-lg transition-all text-center ${importMode === "range" ? "border-primary bg-primary/5 ring-1 ring-primary/20" : "border-muted hover:bg-muted/50"}`}
                >
                  <div className="font-bold text-xs">Custom Range</div>
                  <p className="text-[9px] text-muted-foreground">Choose specific times</p>
                </button>
              </div>

              {importMode === "range" && (
                <div className="p-4 border rounded-lg bg-muted/5 grid grid-cols-2 gap-4">
                  <div className="space-y-1.5">
                    <Label className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">Start Time</Label>
                    <input
                      type="datetime-local"
                      step="1"
                      value={formatForInput(importRange.start, timezone)}
                      min={formatForInput(lakeInfo.range?.start, timezone)}
                      max={formatForInput(importRange.end || lakeInfo.range?.end, timezone)}
                      onChange={(e) => setImportRange(prev => ({ ...prev, start: parseFromInput(e.target.value, timezone) ?? '' }))}
                      className="flex h-8 w-full rounded-md border border-input bg-background px-3 py-1 text-xs shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono"
                    />
                  </div>
                  <div className="space-y-1.5">
                    <Label className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">End Time</Label>
                    <input
                      type="datetime-local"
                      step="1"
                      value={formatForInput(importRange.end, timezone)}
                      min={formatForInput(importRange.start || lakeInfo.range?.start, timezone)}
                      max={formatForInput(lakeInfo.range?.end, timezone)}
                      onChange={(e) => setImportRange(prev => ({ ...prev, end: parseFromInput(e.target.value, timezone) ?? '' }))}
                      className="flex h-8 w-full rounded-md border border-input bg-background px-3 py-1 text-xs shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50 font-mono"
                    />
                  </div>
                </div>
              )}

              <Button onClick={handleStartSync} className="w-full h-8 text-xs mt-2" variant="default">
                Start Sync
              </Button>
            </div>
          ) : (
            <div className="p-4 border border-dashed rounded-lg bg-muted/5 text-center space-y-2">
              <p className="text-xs text-muted-foreground">No data found in the lake.</p>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
