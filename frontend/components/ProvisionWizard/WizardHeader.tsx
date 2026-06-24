"use client";

import React from "react";
import {
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { CheckCircle2, Plus } from "lucide-react";
import { cn } from "@/lib/utils";
import type { WizardState } from "./useWizardState";

export function WizardHeader({ s }: { s: WizardState }) {
  const { step, STEPS } = s;
  return (
    <DialogHeader className="px-6 pt-6 pb-4 border-b">
      <DialogTitle className="flex items-center gap-2 text-xl font-bold">
        <Plus className="h-5 w-5 text-primary" />
        Provision New Service
      </DialogTitle>
      <div className="flex items-center justify-center gap-4 mt-6 overflow-x-auto w-full">
        {STEPS.map((s2, i) => {
          const stepIndex = STEPS.findIndex((x) => x.id === step);
          return (
            <React.Fragment key={s2.id}>
              <div className="flex items-center gap-2 shrink-0">
                <div
                  className={cn(
                    "w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold transition-colors",
                    step === s2.id
                      ? "bg-primary text-primary-foreground"
                      : stepIndex > i
                        ? "bg-green-500 text-white"
                        : "bg-muted text-muted-foreground",
                  )}
                >
                  {stepIndex > i ? (
                    <CheckCircle2 className="w-4 h-4" />
                  ) : (
                    i + 1
                  )}
                </div>
                <span
                  className={cn(
                    "text-xs font-semibold whitespace-nowrap",
                    step === s2.id
                      ? "text-foreground"
                      : "text-muted-foreground",
                  )}
                >
                  {s2.label}
                </span>
              </div>
              {i < STEPS.length - 1 && (
                <div className="h-px w-6 bg-muted shrink-0" />
              )}
            </React.Fragment>
          );
        })}
      </div>
    </DialogHeader>
  );
}
