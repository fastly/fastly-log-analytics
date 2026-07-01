"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { AlertCircle, Loader2, Lock } from "lucide-react";
import type { WizardState } from "../useWizardState";

// Read the machine-readable error code the services query attaches (see
// useWizardState) without an `any` cast, so we can render a richer message for
// specific backend errors.
function errorCode(e: unknown): string | undefined {
  return e && typeof e === "object" && "code" in e
    ? String((e as Record<string, unknown>).code)
    : undefined;
}

export function TokenStep({ s }: { s: WizardState }) {
  return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-6 text-center">
      <div className="space-y-2 max-w-md">
        <h3 className="text-xl font-semibold tracking-tight">
          Enter Fastly API Token
        </h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          We need a token with <code>superuser</code> permissions to create and
          configure your services. An <code>engineer</code> token cannot create
          new services.
        </p>
        <div className="pt-2">
          <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-amber-500/10 text-amber-600 dark:text-amber-500 border border-amber-500/20 text-[10px] font-bold uppercase tracking-wider">
            <AlertCircle className="h-3 w-3 shrink-0" />
            <a
              href="https://www.fastly.com/documentation/reference/api/auth-tokens/user/"
              target="_blank"
              rel="noreferrer"
              className="hover:underline hover:text-amber-700 dark:hover:text-amber-400 transition-colors"
            >
              Personal API Tokens required for NGWAF
            </a>
          </div>
        </div>
      </div>
      <div className="space-y-4 w-full max-w-sm text-left">
        <div className="space-y-2">
          <Label
            htmlFor="token"
            className="flex items-center gap-2 text-sm font-medium"
          >
            <Lock className="h-3.5 w-3.5" /> API Token
          </Label>
          <Input
            id="token"
            type="password"
            value={s.token}
            onChange={(e) => s.setToken(e.target.value.trim())}
            placeholder=""
            className="font-mono text-center"
          />
        </div>
        {s.servicesError && (
          <div className="p-3 bg-destructive/10 text-destructive text-xs rounded-md border border-destructive/20 flex gap-2 animate-in fade-in slide-in-from-top-1 text-left">
            <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
            {errorCode(s.servicesError) === "object_storage_not_enabled" ? (
              <span>
                Object Storage isn&apos;t enabled on this Fastly account, and
                it&apos;s required to store logs.{" "}
                <a
                  href="https://manage.fastly.com/products/object-storage"
                  target="_blank"
                  rel="noreferrer"
                  className="font-semibold underline underline-offset-2 hover:opacity-80"
                >
                  Enable Object Storage
                </a>{" "}
                for your account, then click Fetch Services again.
              </span>
            ) : s.servicesError instanceof Error ? (
              s.servicesError.message
            ) : (
              "Failed to fetch services"
            )}
          </div>
        )}
        <Button
          className="w-full"
          size="lg"
          onClick={s.handleTokenSubmit}
          disabled={!s.token || s.isLoadingServices}
        >
          {s.isLoadingServices && (
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          )}
          Fetch Services
        </Button>
      </div>
    </div>
  );
}
