"use client";

import React from "react";
import { Search, Settings, Zap } from "lucide-react";
import { cn } from "@/lib/utils";
import type { WizardState } from "../useWizardState";
import { ANALYST_PATH_A_UNSUPPORTED_REASON } from "../types";

export function ModeStep({ s }: { s: WizardState }) {
  const joinDisabled = !s.analystPathASupported;
  const analystPathAReason =
    s.analystPathAReason ?? ANALYST_PATH_A_UNSUPPORTED_REASON;

  return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-10 text-center animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="space-y-3 max-w-lg">
        <h3 className="text-2xl font-bold tracking-tight">Select your role</h3>
        <p className="text-muted-foreground leading-relaxed">
          Choose how you want to set up this service.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full max-w-4xl">
        <button
          onClick={() => {
            s.setMode("provision");
            s.setStep("token");
          }}
          className="group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 hover:border-primary transition-all text-left"
        >
          <div className="w-16 h-16 rounded-2xl bg-primary/10 flex items-center justify-center group-hover:scale-110 transition-transform">
            <Zap className="h-8 w-8 text-primary" />
          </div>
          <div className="space-y-2 text-center">
            <h4 className="font-bold text-lg">Admin: Provision</h4>
            <p className="text-xs text-muted-foreground leading-relaxed">
              Deploy new Fastly Object Storage resources, logging endpoints, and
              a CDN proxy. Best for new projects.
            </p>
          </div>
        </button>

        <button
          onClick={() => {
            s.setMode("ingest");
            s.setStep("token");
          }}
          className="group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 hover:border-amber-500 transition-all text-left"
        >
          <div className="w-16 h-16 rounded-2xl bg-amber-500/10 flex items-center justify-center group-hover:scale-110 transition-transform">
            <Settings className="h-8 w-8 text-amber-500" />
          </div>
          <div className="space-y-2 text-center">
            <h4 className="font-bold text-lg">Admin: Connect Terraform</h4>
            <p className="text-xs text-muted-foreground leading-relaxed">
              Set up ingestion for a service that is already managed via
              Terraform. We'll skip creating Fastly resources but set up data
              sync.
            </p>
          </div>
        </button>

        <button
          type="button"
          disabled={joinDisabled}
          aria-describedby={joinDisabled ? "analyst-path-a-reason" : undefined}
          onClick={() => {
            s.setMode("join");
            s.setStep("join");
          }}
          className={cn(
            "group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 hover:border-blue-500 transition-all text-left",
            joinDisabled &&
              "cursor-not-allowed opacity-60 hover:bg-background hover:border-border",
          )}
        >
          <div className="w-16 h-16 rounded-2xl bg-blue-500/10 flex items-center justify-center group-hover:scale-110 transition-transform">
            <Search className="h-8 w-8 text-blue-500" />
          </div>
          <div className="space-y-2 text-center">
            <h4 className="font-bold text-lg">Analyst: Join</h4>
            <p className="text-xs text-muted-foreground leading-relaxed">
              Connect to an existing project. We'll only sync the processed data
              from the cloud. No new resources.
            </p>
            {joinDisabled && (
              <p
                id="analyst-path-a-reason"
                className="text-xs font-medium text-amber-600 dark:text-amber-400 leading-relaxed"
              >
                {analystPathAReason}
              </p>
            )}
          </div>
        </button>
      </div>
    </div>
  );
}
