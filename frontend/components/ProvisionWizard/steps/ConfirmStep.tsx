"use client";

import React from "react";
import {
  ReviewCard,
  ReviewHeader,
  ReviewContent,
  ReviewItem,
} from "@/components/ui/review-card";
import {
  Calendar,
  CheckCircle2,
  Cloud,
  Database,
  Settings,
  Sparkles,
  XCircle,
} from "lucide-react";
import { formatBytes, formatDateTime } from "@/lib/utils";
import type { WizardState } from "../useWizardState";

export function ConfirmStep({ s }: { s: WizardState }) {
  const { config, importMode, importRange, lakeInfo } = s;
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-8 pb-12 max-w-4xl mx-auto text-left">
        <div className="text-center space-y-2">
          <h3 className="text-2xl font-bold tracking-tight">
            Confirm Connection
          </h3>
          <p className="text-sm text-muted-foreground leading-relaxed">
            Review your connection and import settings before continuing.
          </p>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <ReviewCard>
            <ReviewHeader icon={Cloud}>Target Service</ReviewHeader>
            <ReviewContent>
              <ReviewItem
                label="Service Name"
                value={config.endpoint_name}
              />
              <ReviewItem
                label="Service ID"
                value={config.cdn_service_name}
              />
              <ReviewItem label="Mode" value="Read-Only Analyst" />
            </ReviewContent>
          </ReviewCard>

          <ReviewCard>
            <ReviewHeader icon={Database}>Data Lake</ReviewHeader>
            <ReviewContent>
              <ReviewItem label="Bucket" value={config.fos_bucket_name} />
              <ReviewItem label="Region" value={config.fos_region} />
              <ReviewItem
                label="Existing Data"
                value={lakeInfo?.table_exists ? "Available" : "Not Found"}
              />
            </ReviewContent>
          </ReviewCard>

          <ReviewCard>
            <ReviewHeader icon={Calendar}>Initial Import</ReviewHeader>
            <ReviewContent>
              <ReviewItem
                label="Strategy"
                value={importMode === "all" ? "Import All" : "Custom Range"}
              />
              {importMode === "range" ? (
                <>
                  <ReviewItem
                    label="Start Time"
                    value={formatDateTime(importRange.start, s.timezone)}
                  />
                  <ReviewItem
                    label="End Time"
                    value={formatDateTime(importRange.end, s.timezone)}
                  />
                </>
              ) : (
                <ReviewItem
                  label="Range"
                  value={`${formatDateTime(lakeInfo?.range?.start, s.timezone)} → ${formatDateTime(lakeInfo?.range?.end, s.timezone)}`}
                />
              )}
              <ReviewItem
                label="Est. Download Size"
                value={`~${formatBytes(s.estimatedImportSize)}`}
                className="text-primary font-medium"
              />
            </ReviewContent>
          </ReviewCard>

          <ReviewCard>
            <ReviewHeader icon={Settings}>Automation</ReviewHeader>
            <ReviewContent>
              <ReviewItem
                variant="between"
                label="Background Sync"
                value={
                  s.syncEnabled ? (
                    <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                  ) : (
                    <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                  )
                }
              />
              {s.syncEnabled && (
                <ReviewItem
                  label="Sync Interval"
                  value={`Every ${s.syncIntervalMins} minutes`}
                />
              )}
            </ReviewContent>
          </ReviewCard>
        </div>

        <div className="p-4 rounded-xl bg-primary/5 border border-primary/20 space-y-3">
          <div className="flex items-center gap-2 text-primary">
            <Sparkles className="h-4 w-4" />
            <span className="text-xs font-bold uppercase tracking-wider">
              What to expect
            </span>
          </div>
          <p className="text-xs text-muted-foreground leading-relaxed">
            After connecting, the system will begin downloading the requested
            Parquet data files to your local cache. This process happens in the
            background and may take a few minutes depending on the volume of
            data. Your dashboard will begin populating as files arrive.
          </p>
        </div>
      </div>
    </div>
  );
}
