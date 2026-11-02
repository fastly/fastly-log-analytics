"use client"

import React from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SectionHeader, LabelWithInfo } from '@/components/ui/confirm-dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Eye,
  Globe,
  Info,
  Layers,
  Loader2,
  Search,
  Shield,
} from 'lucide-react'
import { cn, SHIELD_LABELS } from '@/lib/utils'
import type { WizardState } from '../types'

const FEATURE_OPTIONS = [
  {
    id: "logging" as const,
    icon: Activity,
    title: "Request Logging",
    description:
      "Stream edge requests directly to Object Storage. Power charts for traffic, status codes, and security.",
    activeClass: "border-primary bg-primary/5 shadow-md shadow-primary/5",
    hoverClass: "border-muted hover:border-primary/50 hover:bg-muted/30",
    iconBgClass: "bg-primary/20",
    iconColorClass: "text-primary",
    dotClass: "bg-primary",
  },
  {
    id: "rum" as const,
    icon: Eye,
    title: "RUM only",
    description:
      "Deploy client-side Core Web Vitals, page performance, and error analytics. Skips standard request logs.",
    activeClass: "border-amber-500 bg-amber-500/5 shadow-md shadow-amber-500/5",
    hoverClass: "border-muted hover:border-amber-500/50 hover:bg-muted/30",
    iconBgClass: "bg-amber-500/20",
    iconColorClass: "text-amber-500",
    dotClass: "bg-amber-500",
  },
  {
    id: "both" as const,
    icon: Layers,
    title: "Shared Bundle (Both)",
    description:
      "Enable Request Logging and Real User Monitoring side-by-side, sharing a single FOS storage bucket.",
    activeClass: "border-emerald-500 bg-emerald-500/5 shadow-md shadow-emerald-500/5",
    hoverClass: "border-muted hover:border-emerald-500/50 hover:bg-muted/30",
    iconBgClass: "bg-emerald-500/20",
    iconColorClass: "text-emerald-500",
    dotClass: "bg-emerald-500",
  },
] as const

export function FeaturesStep({ s }: { s: WizardState }) {
  const { config, setConfig } = s
  const currentSelection =
    config.logging_enabled && config.rum_enabled
      ? "both"
      : config.rum_enabled && !config.logging_enabled
        ? "rum"
        : "logging"
  return (
    <div className="flex-1 flex flex-col items-center justify-center p-6 md:p-8 space-y-8 text-center animate-in fade-in slide-in-from-bottom-4 duration-500 max-w-4xl mx-auto w-full">
      <div className="space-y-3 max-w-lg">
        <span className="text-[10px] font-bold uppercase tracking-widest text-primary bg-primary/10 px-2.5 py-1 rounded-full">
          Step {s.STEPS.findIndex((x) => x.id === "features") + 1} of {s.STEPS.length}
        </span>
        <h3 className="text-2xl font-bold tracking-tight">Select features to deploy</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Choose the integration capabilities you want to configure on your Fastly service. You can adjust
          these settings later.
        </p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full">
        {FEATURE_OPTIONS.map((opt) => {
          const active = currentSelection === opt.id
          const Icon = opt.icon
          return (
            <button
              key={opt.id}
              type="button"
              onClick={() =>
                setConfig((prev) => ({
                  ...prev,
                  logging_enabled: opt.id === "logging" || opt.id === "both",
                  rum_enabled: opt.id === "rum" || opt.id === "both",
                }))
              }
              className={cn(
                "group relative flex flex-col items-center gap-5 p-6 border-2 rounded-2xl bg-background transition-all text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                active ? opt.activeClass : opt.hoverClass,
              )}
            >
              <div
                className={cn(
                  "w-12 h-12 rounded-xl flex items-center justify-center transition-transform group-hover:scale-110",
                  active ? opt.iconBgClass : "bg-muted",
                )}
              >
                <Icon className={cn("h-6 w-6", active ? opt.iconColorClass : "text-muted-foreground")} />
              </div>
              <div className="space-y-1.5 text-center">
                <h4 className="font-bold text-base">{opt.title}</h4>
                <p className="text-xs text-muted-foreground leading-relaxed">{opt.description}</p>
              </div>
              {active && <div className={cn("absolute top-3 right-3 w-2 h-2 rounded-full", opt.dotClass)} />}
            </button>
          )
        })}
      </div>
    </div>
  )
}

export function NgwafStep({ s }: { s: WizardState }) {
  const { config, setConfig } = s
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
          Link this service to an existing Fastly NGWAF workspace to enable WAF signal logging and bot
          detection. This step is optional — skip it if NGWAF is not deployed on this service.
        </p>
        {s.ngwafFetching ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading workspaces…
          </div>
        ) : s.ngwafFetchError ? (
          <div className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4 shrink-0" /> {s.ngwafFetchError}
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
                  const workspaceId = !v || v === "__none__" ? "" : v
                  const groups: string[] = prev.log_fields?.groups ?? []
                  return {
                    ...prev,
                    ngwaf_workspace_id: workspaceId,
                    ...(workspaceId && !groups.includes("J")
                      ? { log_fields: { ...prev.log_fields, groups: [...groups, "J"] } }
                      : {}),
                  }
                })
              }
            >
              <SelectTrigger className="h-9 text-sm">
                <SelectValue placeholder="Select a workspace…" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__none__">
                  <span className="text-muted-foreground">No NGWAF (skip)</span>
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
              <Info className="h-4 w-4 shrink-0" /> No NGWAF workspaces found in this account.
            </div>
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
            WAF / NGWAF log fields (group J) will only be available in the next step if a workspace is
            selected here.
          </p>
        </div>
      </div>
    </div>
  )
}

export interface CdnFrontSectionProps {
  cdnPrefix: string
  cdnShield: string
  domainStatus: WizardState['domainStatus']
  domainMessage: string
  onChangePrefix: (prefix: string) => void
  onChangeShield: (shield: string) => void
  onCheckDomain: (prefix: string) => void
}

export function CdnFrontSection({
  cdnPrefix,
  cdnShield,
  domainStatus,
  domainMessage,
  onChangePrefix,
  onChangeShield,
  onCheckDomain,
}: CdnFrontSectionProps) {
  return (
    <div className="space-y-5">
      <SectionHeader title="CDN Performance Front" icon={Globe} />
      <p className="text-xs text-muted-foreground leading-relaxed">
        Highly recommended. Provision a secondary Fastly service to front the Object Storage bucket for
        faster dashboard queries and secure access.
      </p>
      <div className="grid grid-cols-2 gap-6 pt-1">
        <div className="space-y-1.5">
          <LabelWithInfo
            label="Domain Prefix"
            info="The domain name for the secondary CDN service that sits in front of your Object Storage bucket."
          />
          <div className="space-y-1.5">
            <div className="flex items-center gap-1.5">
              <Input
                value={cdnPrefix}
                onChange={(e) => onChangePrefix(e.target.value.toLowerCase())}
                className={cn(
                  "h-9 font-mono text-sm",
                  domainStatus === "available" && "border-green-500 focus-visible:ring-green-500",
                  domainStatus === "taken" && "border-red-500 focus-visible:ring-red-500",
                )}
              />
              <span className="text-[10px] font-mono text-muted-foreground opacity-70">
                .global.ssl.fastly.net
              </span>
              <Button
                variant="outline"
                size="sm"
                className="h-9 px-3 shrink-0 text-xs"
                onClick={() => onCheckDomain(cdnPrefix)}
                disabled={domainStatus === "checking" || !cdnPrefix}
                title="Check Domain Availability"
              >
                <Search className="h-4 w-4 mr-1.5" />
                Check Domain
              </Button>
            </div>
            {domainStatus === "checking" && (
              <p className="text-[10px] animate-pulse text-muted-foreground">Checking availability...</p>
            )}
            {domainStatus === "available" && (
              <p className="text-[10px] text-green-600 font-medium flex items-center gap-1">
                <CheckCircle2 className="h-3 w-3" /> {domainMessage}
              </p>
            )}
            {domainStatus === "taken" && (
              <p className="text-[10px] text-red-600 font-medium flex items-center gap-1">
                <AlertCircle className="h-3 w-3" /> {domainMessage}
              </p>
            )}
          </div>
        </div>
        <div className="space-y-1.5">
          <LabelWithInfo
            label="Origin Shield"
            info="The Fastly POP that will act as a shield between the edge nodes and your bucket, reducing direct bucket reads and improving performance."
          />
          <Select value={cdnShield} onValueChange={(v) => v && onChangeShield(v)}>
            <SelectTrigger className="h-9">
              <SelectValue>{(val) => SHIELD_LABELS[String(val)] || val}</SelectValue>
            </SelectTrigger>
            <SelectContent>
              {Object.entries(SHIELD_LABELS).map(([k, label]) => (
                <SelectItem key={k} value={k}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  )
}

export const DomainStep = FeaturesStep
