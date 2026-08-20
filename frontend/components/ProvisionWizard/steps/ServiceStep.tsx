"use client";

import React from "react";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { ChevronRight, Loader2, Search } from "lucide-react";
import { cn } from "@/lib/utils";
import type { WizardState } from "../useWizardState";

export function ServiceStep({ s }: { s: WizardState }) {
  return (
    <div className="flex-1 flex flex-col overflow-hidden p-6 md:p-8 max-w-3xl mx-auto w-full gap-4">
      <div className="flex items-center justify-between shrink-0">
        <div className="p-2 border rounded-lg bg-muted/10 flex items-center gap-3 px-4 flex-1">
          <Search className="h-5 w-5 text-muted-foreground" />
          <Input
            placeholder="Search your services..."
            className="h-10 border-none bg-transparent shadow-none focus-visible:ring-0 text-base"
            value={s.search}
            onChange={(e) => s.setSearch(e.target.value)}
          />
        </div>
        {s.tokenInfo && (
          <div className="ml-4 flex flex-col items-end shrink-0">
            <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
              Authenticated as
            </span>
            <div className="flex items-center gap-1.5">
              <span className="text-xs font-semibold">{s.tokenInfo.name}</span>
              <Badge
                variant={s.tokenInfo.type === "user" ? "default" : "outline"}
                className="text-[9px] h-3.5 px-1 uppercase"
              >
                {s.tokenInfo.type}
              </Badge>
            </div>
          </div>
        )}
      </div>
      <div className="flex-1 overflow-y-auto min-h-0 border rounded-lg shadow-sm">
        <div className="divide-y divide-muted/50 bg-background">
          {s.isLoadingServices ? (
            <div className="py-12 flex flex-col items-center justify-center gap-3 text-muted-foreground text-sm">
              <Loader2 className="h-6 w-6 animate-spin text-primary" />
              <span>Loading services...</span>
            </div>
          ) : s.filteredServices.length > 0 ? (
            s.filteredServices.map((svc: any) => (
              // a11y: was a <div onClick> — keyboard-invisible. Native
              // button with disabled state on provisioned services and a
              // descriptive aria-label that includes the service id.
              <button
                key={svc.id}
                type="button"
                disabled={svc.provisioned}
                aria-label={svc.provisioned ? `${svc.name} — already provisioned` : `Select service ${svc.name}, id ${svc.id}`}
                className={cn(
                  "w-full text-left p-4 flex items-center justify-between transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
                  svc.provisioned
                    ? "opacity-40 grayscale bg-muted/5 cursor-not-allowed"
                    : "hover:bg-muted/50 cursor-pointer active:bg-muted",
                )}
                onClick={() => !svc.provisioned && s.handleServiceSelect(svc)}
              >
                <div className="space-y-1">
                  <div className="font-semibold text-sm flex items-center gap-2">
                    {svc.name}
                    {svc.provisioned && (
                      <Badge
                        variant="secondary"
                        className="text-[10px] h-4 px-1 leading-none font-bold uppercase tracking-tight"
                      >
                        Active
                      </Badge>
                    )}
                  </div>
                  <div className="text-xs font-mono text-muted-foreground">
                    {svc.id}
                  </div>
                </div>
                {!svc.provisioned && (
                  <div className="flex items-center text-primary">
                    {s.validateMutation.isPending &&
                    s.selectedService?.id === svc.id ? (
                      <Loader2 className="h-5 w-5 animate-spin" />
                    ) : (
                      <ChevronRight className="h-5 w-5" />
                    )}
                  </div>
                )}
              </button>
            ))
          ) : (
            <div className="py-12 text-center text-muted-foreground text-sm italic">
              No services found matching &quot;{s.search}&quot;
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
