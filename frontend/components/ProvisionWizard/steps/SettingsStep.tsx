"use client";

import React from "react";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Info } from "lucide-react";
import type { WizardState } from "../useWizardState";
import { SectionHeader } from "@/components/ui/section-header";
import { Settings } from "lucide-react";

export function SettingsStep({ s }: { s: WizardState }) {
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
        <div className="space-y-6">
          <SectionHeader title="Ingestion Settings" icon={Settings} />
          <p className="text-sm text-muted-foreground leading-relaxed">
            Configure how you want to handle ongoing updates from the data
            lake.
          </p>

          <div className="bg-muted/5 border rounded-xl overflow-hidden divide-y">
            <div className="p-6 flex items-center justify-between gap-8">
              <div className="space-y-1 flex-1">
                <div className="flex items-center gap-2">
                  <Label id="settings-auto-sync-label" className="text-sm font-bold tracking-tight">
                    Auto-Sync New Data
                  </Label>
                  <Badge
                    variant="secondary"
                    className="text-[9px] uppercase h-4"
                  >
                    Recommended
                  </Badge>
                </div>
                <p className="text-xs text-muted-foreground leading-relaxed">
                  Automatically poll for and download new processed log files
                  as they are committed to the cloud.
                </p>
              </div>
              <Switch
                aria-labelledby="settings-auto-sync-label"
                checked={s.syncEnabled}
                onCheckedChange={s.setSyncEnabled}
              />
            </div>

            {s.syncEnabled && (
              <div className="p-6 space-y-4 bg-background/30 animate-in fade-in slide-in-from-top-1">
                <div className="flex items-start justify-between gap-8">
                  <div className="space-y-1">
                    <Label htmlFor="settings-sync-interval" className="text-sm font-bold tracking-tight">
                      Cloud Sync Interval
                    </Label>
                    <p className="text-xs text-muted-foreground leading-relaxed">
                      How often to check for new cloud commits. More frequent =
                      fresher data.
                    </p>
                  </div>
                  <Select
                    value={s.syncIntervalMins}
                    onValueChange={(v) => v && s.setSyncIntervalMins(v)}
                  >
                    <SelectTrigger id="settings-sync-interval" className="h-9 w-[180px] shrink-0">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="1">Every 1 min</SelectItem>
                      <SelectItem value="2">Every 2 mins</SelectItem>
                      <SelectItem value="5">Every 5 mins</SelectItem>
                      <SelectItem value="15">Every 15 mins</SelectItem>
                      <SelectItem value="30">Every 30 mins</SelectItem>
                      <SelectItem value="60">Every 60 mins</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            )}
          </div>

          {!s.syncEnabled && (
            <div className="p-4 rounded-lg bg-amber-500/5 border border-amber-500/20 flex items-start gap-3">
              <Info className="h-4 w-4 text-amber-500 mt-0.5" />
              <p className="text-[11px] text-amber-700 dark:text-amber-400 leading-normal">
                With auto-sync disabled, your local dashboard will only show
                the data you import now. You will need to manually trigger a
                sync later to see newer logs.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
