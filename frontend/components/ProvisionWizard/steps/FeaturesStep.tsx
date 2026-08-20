"use client";

import React from "react";
import { Activity, Eye, Layers } from "lucide-react";
import { cn } from "@/lib/utils";
import type { WizardState } from "../useWizardState";

export function FeaturesStep({ s }: { s: WizardState }) {
  const { config, setConfig } = s;

  const currentSelection = React.useMemo(() => {
    if (config.logging_enabled && config.rum_enabled) return "both";
    if (config.rum_enabled && !config.logging_enabled) return "rum";
    return "logging";
  }, [config.logging_enabled, config.rum_enabled]);

  const selectOption = (opt: "logging" | "rum" | "both") => {
    setConfig((prev) => ({
      ...prev,
      logging_enabled: opt === "logging" || opt === "both",
      rum_enabled: opt === "rum" || opt === "both",
    }));
  };

  return (
    <div className="flex-1 flex flex-col items-center justify-center p-6 md:p-8 space-y-8 text-center animate-in fade-in slide-in-from-bottom-4 duration-500 max-w-4xl mx-auto w-full">
      <div className="space-y-3 max-w-lg">
        <span className="text-[10px] font-bold uppercase tracking-widest text-primary bg-primary/10 px-2.5 py-1 rounded-full">
          Step 4 of {s.STEPS.length}
        </span>
        <h3 className="text-2xl font-bold tracking-tight">Select features to deploy</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Choose the integration capabilities you want to configure on your Fastly service. You can adjust these settings later.
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full">
        {/* Option 1: Request Logging */}
        <button
          type="button"
          onClick={() => selectOption("logging")}
          className={cn(
            "group relative flex flex-col items-center gap-5 p-6 border-2 rounded-2xl bg-background transition-all text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
            currentSelection === "logging"
              ? "border-primary bg-primary/5 shadow-md shadow-primary/5"
              : "border-muted hover:border-primary/50 hover:bg-muted/30"
          )}
        >
          <div
            className={cn(
              "w-12 h-12 rounded-xl flex items-center justify-center transition-transform group-hover:scale-110",
              currentSelection === "logging" ? "bg-primary/20" : "bg-muted"
            )}
          >
            <Activity className={cn("h-6 w-6", currentSelection === "logging" ? "text-primary" : "text-muted-foreground")} />
          </div>
          <div className="space-y-1.5 text-center">
            <h4 className="font-bold text-base">Request Logging</h4>
            <p className="text-xs text-muted-foreground leading-relaxed">
              Stream edge requests directly to Object Storage. Power charts for traffic, status codes, and security.
            </p>
          </div>
          {currentSelection === "logging" && (
            <div className="absolute top-3 right-3 w-2 h-2 rounded-full bg-primary" />
          )}
        </button>

        {/* Option 2: RUM (Real User Monitoring) */}
        <button
          type="button"
          onClick={() => selectOption("rum")}
          className={cn(
            "group relative flex flex-col items-center gap-5 p-6 border-2 rounded-2xl bg-background transition-all text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
            currentSelection === "rum"
              ? "border-amber-500 bg-amber-500/5 shadow-md shadow-amber-500/5"
              : "border-muted hover:border-amber-500/50 hover:bg-muted/30"
          )}
        >
          <div
            className={cn(
              "w-12 h-12 rounded-xl flex items-center justify-center transition-transform group-hover:scale-110",
              currentSelection === "rum" ? "bg-amber-500/20" : "bg-muted"
            )}
          >
            <Eye className={cn("h-6 w-6", currentSelection === "rum" ? "text-amber-500" : "text-muted-foreground")} />
          </div>
          <div className="space-y-1.5 text-center">
            <h4 className="font-bold text-base">RUM only</h4>
            <p className="text-xs text-muted-foreground leading-relaxed">
              Deploy client-side Core Web Vitals, page performance, and error analytics. Skips standard request logs.
            </p>
          </div>
          {currentSelection === "rum" && (
            <div className="absolute top-3 right-3 w-2 h-2 rounded-full bg-amber-500" />
          )}
        </button>

        {/* Option 3: Both */}
        <button
          type="button"
          onClick={() => selectOption("both")}
          className={cn(
            "group relative flex flex-col items-center gap-5 p-6 border-2 rounded-2xl bg-background transition-all text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
            currentSelection === "both"
              ? "border-emerald-500 bg-emerald-500/5 shadow-md shadow-emerald-500/5"
              : "border-muted hover:border-emerald-500/50 hover:bg-muted/30"
          )}
        >
          <div
            className={cn(
              "w-12 h-12 rounded-xl flex items-center justify-center transition-transform group-hover:scale-110",
              currentSelection === "both" ? "bg-emerald-500/20" : "bg-muted"
            )}
          >
            <Layers className={cn("h-6 w-6", currentSelection === "both" ? "text-emerald-500" : "text-muted-foreground")} />
          </div>
          <div className="space-y-1.5 text-center">
            <h4 className="font-bold text-base">Shared Bundle (Both)</h4>
            <p className="text-xs text-muted-foreground leading-relaxed">
              Enable Request Logging and Real User Monitoring side-by-side, sharing a single FOS storage bucket.
            </p>
          </div>
          {currentSelection === "both" && (
            <div className="absolute top-3 right-3 w-2 h-2 rounded-full bg-emerald-500" />
          )}
        </button>
      </div>
    </div>
  );
}
