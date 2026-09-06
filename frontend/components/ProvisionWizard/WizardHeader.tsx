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
  const stepIndex = STEPS.findIndex((x) => x.id === step);

  return (
    <DialogHeader className="px-6 pt-6 pb-7 border-b">
      <DialogTitle className="flex items-center gap-2 text-xl font-bold">
        <Plus className="h-5 w-5 text-primary" />
        Provision New Service
      </DialogTitle>

      {/* Premium Stepper Timeline */}
      <div className="flex items-center justify-center w-full mt-6 px-1 sm:px-4 max-w-2xl mx-auto">
        {STEPS.map((s2, i) => {
          const isActive = step === s2.id;
          const isCompleted = stepIndex > i;

          return (
            <React.Fragment key={s2.id}>
              {/* Step Node */}
              <div className="flex flex-col items-center shrink-0 relative">
                <div
                  className={cn(
                    "w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold transition-all duration-300 border-2",
                    isActive
                      ? "bg-primary border-primary text-primary-foreground shadow-md shadow-primary/20 scale-110"
                      : isCompleted
                        ? "bg-green-500 border-green-500 text-white"
                        : "bg-background border-muted text-muted-foreground",
                  )}
                  title={s2.label}
                >
                  {isCompleted ? (
                    <CheckCircle2 className="w-4 h-4" />
                  ) : (
                    i + 1
                  )}
                </div>

                {/* Responsive Label - hidden on small screens unless active to save horizontal space */}
                <span
                  className={cn(
                    "text-[10px] font-semibold mt-1.5 absolute top-7 whitespace-nowrap transition-all duration-300",
                    isActive
                      ? "text-foreground font-bold opacity-100 scale-100"
                      : "text-muted-foreground opacity-60 hidden md:block",
                  )}
                >
                  {s2.label}
                </span>
              </div>

              {/* Connecting Line Segment */}
              {i < STEPS.length - 1 && (
                <div className="flex-1 min-w-[8px] max-w-[48px] h-[2px] mx-1 sm:mx-2 bg-muted relative">
                  <div
                    className={cn(
                      "absolute inset-0 bg-primary transition-all duration-500 ease-in-out",
                      stepIndex > i ? "w-full" : "w-0",
                    )}
                  />
                </div>
              )}
            </React.Fragment>
          );
        })}
      </div>

      {/* Dynamic step name details line */}
      <div className="text-center mt-7 text-xs text-muted-foreground font-medium">
        Step <span className="text-foreground font-semibold">{stepIndex + 1}</span> of {STEPS.length}
      </div>
    </DialogHeader>
  );
}
