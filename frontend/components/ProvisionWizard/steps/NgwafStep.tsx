"use client";

import React from "react";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { AlertCircle, Info, Loader2, Shield } from "lucide-react";
import type { WizardState } from "../useWizardState";

export function NgwafStep({ s }: { s: WizardState }) {
  const { config, setConfig } = s;
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-6 max-w-2xl mx-auto">
        <div className="flex items-center gap-2 pb-2 border-b">
          <Shield className="h-5 w-5 text-primary" />
          <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
            NGWAF Workspace
          </h3>
        </div>

        <p className="text-sm text-muted-foreground leading-relaxed">
          Link this service to an existing Fastly NGWAF workspace to enable WAF
          signal logging and bot detection. This step is optional — skip it if
          NGWAF is not deployed on this service.
        </p>

        {s.ngwafFetching ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading workspaces…
          </div>
        ) : s.ngwafFetchError ? (
          <div className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4 shrink-0" />
            {s.ngwafFetchError}
          </div>
        ) : s.ngwafWorkspaces.length > 0 ? (
          <div className="space-y-2">
            <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Workspace
            </Label>
            <Select
              value={config.ngwaf_workspace_id || "__none__"}
              onValueChange={(v: string | null) =>
                setConfig((prev) => {
                  const workspaceId = !v || v === "__none__" ? "" : v;
                  const update: typeof prev = {
                    ...prev,
                    ngwaf_workspace_id: workspaceId,
                  };
                  if (workspaceId) {
                    const groups: string[] = prev.log_fields?.groups ?? [];
                    if (!groups.includes("J")) {
                      update.log_fields = {
                        ...prev.log_fields,
                        groups: [...groups, "J"],
                      };
                    }
                  }
                  return update;
                })
              }
            >
              <SelectTrigger className="h-9 text-sm">
                <SelectValue placeholder="Select a workspace…" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__none__">
                  <span className="text-muted-foreground">
                    No NGWAF (skip)
                  </span>
                </SelectItem>
                {s.ngwafWorkspaces.map((ws) => (
                  <SelectItem key={ws.id} value={ws.id}>
                    {ws.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-sm text-muted-foreground bg-muted/30 p-3 rounded-lg border border-dashed">
              <Info className="h-4 w-4 shrink-0" />
              No NGWAF workspaces found in this account.
            </div>

            {s.ngwafFetchError && (
              <div className="text-xs text-amber-600 bg-amber-50 dark:bg-amber-950/20 p-3 rounded-lg border border-amber-200 dark:border-amber-900/50 flex gap-2">
                <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                <p className="leading-relaxed font-medium">
                  {s.ngwafFetchError}
                </p>
              </div>
            )}

            {s.ngwafDebugRaw && (
              <details className="text-[10px]">
                <summary className="cursor-pointer text-muted-foreground uppercase tracking-wider font-bold">
                  Raw API response (debug)
                </summary>
                <pre className="mt-1 p-2 bg-muted rounded text-xs overflow-auto max-h-32 whitespace-pre-wrap break-all">
                  {s.ngwafDebugRaw}
                </pre>
              </details>
            )}
          </div>
        )}

        <div className="p-4 rounded-xl bg-muted/30 border border-dashed space-y-1">
          <p className="text-xs font-semibold text-muted-foreground">
            WAF / NGWAF log fields (group J) will only be available in the next
            step if a workspace is selected here.
          </p>
        </div>
      </div>
    </div>
  );
}
