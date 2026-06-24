"use client";

import React from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { RotateCcw } from "lucide-react";
import { getStepsForMode, type WizardDraft } from "./types";

function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "earlier";
  const secs = Math.floor((Date.now() - then) / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function stepLabelFor(draft: WizardDraft): string {
  const steps = getStepsForMode(draft.mode);
  const found = steps.find((s) => s.id === draft.currentStep);
  return found?.label ?? draft.currentStep;
}

export interface ResumeBannerProps {
  draft: WizardDraft;
  onResume: () => void;
  onStartFresh: () => void;
}

export function ResumeBanner({ draft, onResume, onStartFresh }: ResumeBannerProps) {
  const label = stepLabelFor(draft);
  const when = relativeTime(draft.updatedAt);
  return (
    <div className="px-6 pt-4">
      <Alert>
        <RotateCcw className="h-4 w-4" />
        <AlertTitle>Resume previous wizard?</AlertTitle>
        <AlertDescription>
          You left off on the <span className="font-medium">{label}</span> step
          {" "}({when}). Fastly API token and storage credentials were cleared
          for safety and will need to be re-entered.
        </AlertDescription>
        <div className="mt-3 flex flex-wrap gap-2 group-has-[>svg]/alert:col-start-2">
          <Button size="sm" onClick={onResume} data-testid="resume-banner-resume">
            Resume from {label}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={onStartFresh}
            data-testid="resume-banner-start-fresh"
          >
            Start fresh
          </Button>
        </div>
      </Alert>
    </div>
  );
}
