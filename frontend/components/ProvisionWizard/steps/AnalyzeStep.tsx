"use client";

import React from "react";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { SectionHeader } from "@/components/ui/section-header";
import { LabelWithInfo } from "@/components/ui/label-with-info";
import {
  AlertCircle,
  ArrowRight,
  Calendar,
  CheckCircle2,
  Database,
  Search,
} from "lucide-react";
import { cn, formatBytes, formatDateTime } from "@/lib/utils";
import { formatForInput, parseFromInput } from "@/lib/date";
import type { WizardState } from "../useWizardState";

export function AnalyzeStep({ s }: { s: WizardState }) {
  const { lakeInfo, importMode, setImportMode, importRange, setImportRange } =
    s;
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-8 pb-12 max-w-3xl mx-auto">
        <div className="space-y-4">
          <SectionHeader title="Analyze Data Lake" icon={Search} />
          {lakeInfo?.table_exists ? (
            <div className="space-y-6">
              <div className="bg-emerald-500/5 border border-emerald-500/20 rounded-xl p-6 space-y-4">
                <div className="flex items-center gap-3 text-emerald-600 dark:text-emerald-400">
                  <CheckCircle2 className="h-6 w-6" />
                  <h4 className="text-lg font-bold">
                    Found existing Iceberg Table
                  </h4>
                </div>
                <p className="text-sm text-muted-foreground leading-relaxed">
                  We found an active data lake in this bucket with{" "}
                  <strong>{lakeInfo.info.data_files}</strong> data files and{" "}
                  <strong>{lakeInfo.info.snapshots}</strong> snapshots.
                </p>

                <div className="grid grid-cols-2 gap-4 pt-2">
                  <div className="bg-background/50 border rounded-lg p-4 space-y-1">
                    <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                      Available From
                    </span>
                    <div className="flex flex-col font-mono text-sm font-semibold">
                      <div className="flex items-center gap-2">
                        <Calendar className="h-3.5 w-3.5 text-primary" />
                        {formatDateTime(lakeInfo.range.start, s.timezone)}
                      </div>
                    </div>
                  </div>
                  <div className="bg-background/50 border rounded-lg p-4 space-y-1">
                    <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                      Available To
                    </span>
                    <div className="flex flex-col font-mono text-sm font-semibold">
                      <div className="flex items-center gap-2">
                        <Calendar className="h-3.5 w-3.5 text-primary" />
                        {formatDateTime(lakeInfo.range.end, s.timezone)}
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <LabelWithInfo
                    label="Data Import Strategy"
                    info="Choose how much historical data you want to sync to your local machine. You can always sync more later."
                  />
                  <Badge
                    variant="secondary"
                    className="font-mono bg-muted/50 border shadow-sm"
                  >
                    ~{formatBytes(s.estimatedImportSize)}
                  </Badge>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <button
                    onClick={() => setImportMode("all")}
                    className={cn(
                      "flex flex-col items-center gap-3 p-6 border-2 rounded-xl transition-all text-left",
                      importMode === "all"
                        ? "border-primary bg-primary/5 ring-4 ring-primary/10"
                        : "border-muted hover:bg-muted/50",
                    )}
                  >
                    <Database className="h-6 w-6 text-primary" />
                    <div className="text-center">
                      <div className="font-bold text-sm">Import All Data</div>
                      <p className="text-[10px] text-muted-foreground mt-1">
                        Sync every available log file
                      </p>
                    </div>
                  </button>
                  <button
                    onClick={() => setImportMode("range")}
                    className={cn(
                      "flex flex-col items-center gap-3 p-6 border-2 rounded-xl transition-all text-left",
                      importMode === "range"
                        ? "border-primary bg-primary/5 ring-4 ring-primary/10"
                        : "border-muted hover:bg-muted/50",
                    )}
                  >
                    <Calendar className="h-6 w-6 text-primary" />
                    <div className="text-center">
                      <div className="font-bold text-sm">Select Range</div>
                      <p className="text-[10px] text-muted-foreground mt-1">
                        Choose specific dates to import
                      </p>
                    </div>
                  </button>
                </div>
              </div>

              {importMode === "range" && (
                <div className="p-6 border rounded-xl bg-muted/5 space-y-4 animate-in fade-in slide-in-from-top-2">
                  <div className="flex items-center gap-4">
                    <div className="space-y-1.5 flex-1">
                      <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
                        Start Time
                      </Label>
                      <Input
                        type="datetime-local"
                        step="1"
                        value={formatForInput(importRange.start, s.timezone)}
                        min={formatForInput(lakeInfo.range.start, s.timezone)}
                        max={formatForInput(
                          importRange.end || lakeInfo.range.end,
                          s.timezone,
                        )}
                        onChange={(e) =>
                          setImportRange((prev) => ({
                            ...prev,
                            start:
                              parseFromInput(e.target.value, s.timezone) ?? "",
                          }))
                        }
                        className="h-9 font-mono"
                      />
                    </div>
                    <ArrowRight className="h-4 w-4 text-muted-foreground mt-6" />
                    <div className="space-y-1.5 flex-1">
                      <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
                        End Time
                      </Label>
                      <Input
                        type="datetime-local"
                        step="1"
                        value={formatForInput(importRange.end, s.timezone)}
                        min={formatForInput(
                          importRange.start || lakeInfo.range.start,
                          s.timezone,
                        )}
                        max={formatForInput(lakeInfo.range.end, s.timezone)}
                        onChange={(e) =>
                          setImportRange((prev) => ({
                            ...prev,
                            end:
                              parseFromInput(e.target.value, s.timezone) ?? "",
                          }))
                        }
                        className="h-9 font-mono"
                      />
                    </div>
                  </div>
                  <div className="flex items-center justify-between mt-2 pt-2 border-t border-muted/50">
                    <p className="text-[10px] text-muted-foreground italic">
                      Only data between these times will be downloaded
                      initially.
                    </p>
                  </div>
                </div>
              )}
            </div>
          ) : (
            <div className="p-12 border border-dashed rounded-xl bg-muted/5 text-center space-y-4">
              <div className="mx-auto w-12 h-12 rounded-full bg-amber-500/10 flex items-center justify-center">
                <AlertCircle className="h-6 w-6 text-amber-500" />
              </div>
              <div className="space-y-1">
                <h4 className="font-bold">No Data Found</h4>
                <p className="text-sm text-muted-foreground max-w-xs mx-auto">
                  We couldn't find an Iceberg table in this bucket. The admin
                  might not have started the ingestion yet.
                </p>
              </div>
              <p className="text-xs text-muted-foreground">
                You can still connect, but the dashboard will be empty until
                data is available.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
