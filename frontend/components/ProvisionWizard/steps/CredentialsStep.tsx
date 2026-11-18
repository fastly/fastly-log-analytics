"use client"

import React from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  AlertCircle,
  ChevronRight,
  Loader2,
  Lock,
  Search,
  Settings,
  Zap,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import type { WizardState } from '../types'
import { isCloudMode } from '@/lib/cloud-engine/mode'
import { ATTACH_ENDPOINT_NAME, attachUnavailableMessage } from '../attach'

function errorCode(e: unknown): string | undefined {
  return e && typeof e === "object" && "code" in e
    ? String((e as Record<string, unknown>).code)
    : undefined
}

export function ModeStep({ s }: { s: WizardState }) {
  const cloud = isCloudMode()
  const modeCards = [
    {
      mode: "provision" as const,
      nextStep: "token" as const,
      icon: Zap,
      iconBg: "bg-primary/10",
      iconColor: "text-primary",
      hoverBorder: "hover:border-primary",
      title: "Admin: Provision",
      description: cloud
        ? "Deploy new Fastly Object Storage resources and logging endpoints. Best for new projects."
        : "Deploy new Fastly Object Storage resources, logging endpoints, and a CDN proxy. Best for new projects.",
    },
    ...(cloud ? [] : [{
      mode: "ingest" as const,
      nextStep: "token" as const,
      icon: Settings,
      iconBg: "bg-amber-500/10",
      iconColor: "text-amber-500",
      hoverBorder: "hover:border-amber-500",
      title: "Admin: Connect Terraform",
      description:
        "Set up ingestion for a service that is already managed via Terraform. We'll skip creating Fastly resources but set up data sync.",
    }]),
  ]

  return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-10 text-center animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="space-y-3 max-w-lg">
        <h3 className="text-2xl font-bold tracking-tight">Select your role</h3>
        <p className="text-muted-foreground leading-relaxed">Choose how you want to set up this service.</p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-3xl">
        {modeCards.map((card) => {
          const Icon = card.icon
          return (
            <button
              key={card.mode}
              type="button"
              onClick={() => {
                s.setMode(card.mode)
                s.setStep(card.nextStep)
              }}
              className={cn(
                "group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 transition-all text-left",
                card.hoverBorder,
              )}
            >
              <div
                className={cn(
                  "w-16 h-16 rounded-2xl flex items-center justify-center group-hover:scale-110 transition-transform",
                  card.iconBg,
                )}
              >
                <Icon className={cn("h-8 w-8", card.iconColor)} />
              </div>
              <div className="space-y-2 text-center">
                <h4 className="font-bold text-lg">{card.title}</h4>
                <p className="text-xs text-muted-foreground leading-relaxed">{card.description}</p>
              </div>
            </button>
          )
        })}
      </div>
    </div>
  )
}

export function TokenStep({ s }: { s: WizardState }) {
  const cloud = isCloudMode()
  return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-6 text-center">
      <div className="space-y-2 max-w-md">
        <h3 className="text-xl font-semibold tracking-tight">Enter Fastly API Token</h3>
        {cloud ? (
          <p className="text-sm text-muted-foreground leading-relaxed">
            We need an <code>engineer</code> token with access to all services, or a <code>superuser</code> token,
            to configure logging on your service and set up Fastly Object Storage.
          </p>
        ) : (
          <p className="text-sm text-muted-foreground leading-relaxed">
            We need an <code>engineer</code> token with access to all services, or a <code>superuser</code> token,
            to create and configure your services.
          </p>
        )}
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
          <Label htmlFor="token" className="flex items-center gap-2 text-sm font-medium">
            <Lock className="h-3.5 w-3.5" /> API Token
          </Label>
          <Input
            id="token"
            type="password"
            autoFocus
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
                Object Storage isn&apos;t enabled on this Fastly account, and it&apos;s required to store logs.{" "}
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
      </div>
    </div>
  )
}

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
            s.filteredServices.map((svc) => (
              <button
                key={svc.id}
                type="button"
                disabled={svc.provisioned}
                aria-label={
                  svc.provisioned
                    ? `${svc.name} — already provisioned`
                    : `Select service ${svc.name}, id ${svc.id}`
                }
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
                  <div className="text-xs font-mono text-muted-foreground">{svc.id}</div>
                </div>
                {!svc.provisioned && (
                  <div className="flex items-center text-primary">
                    {s.validateMutation.isPending && s.selectedService?.id === svc.id ? (
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
      <AttachPanel s={s} />
    </div>
  )
}

function AttachPanel({ s }: { s: WizardState }) {
  const offer = s.attachOffer
  if (s.isCheckingAttach) {
    return (
      <div className="shrink-0 flex items-center gap-2 text-sm text-muted-foreground" role="status">
        <Loader2 className="h-4 w-4 animate-spin" /> Another install manages this service. Checking whether Cloud can attach…
      </div>
    )
  }
  if (!offer || offer.serviceId !== s.selectedService?.id) return null
  if (!offer.available) {
    return (
      <div className="shrink-0 flex items-start gap-2 p-3 border border-destructive/30 rounded-lg text-sm text-destructive" role="alert">
        <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" /> {attachUnavailableMessage(offer.reason)}
      </div>
    )
  }
  return (
    <div className="shrink-0 p-4 border rounded-lg bg-muted/10 space-y-3 text-sm" role="region" aria-label="Attach alongside the existing install">
      <p className="font-semibold">Another install already manages this service&apos;s logging VCL.</p>
      <p className="text-muted-foreground leading-relaxed">
        Cloud can attach alongside it and reuse its capture VCL. Cloud adds only its own owner marker, the
        logging endpoint &quot;{ATTACH_ENDPOINT_NAME}&quot; and its response condition, and leaves everything
        else on the service alone. Request logs only: RUM and CMCD stay with the other install.
      </p>
      {offer.missingFields.length > 0 && (
        <p className="text-muted-foreground">
          The existing capture VCL does not set these fields, so they will be blank:{" "}
          <span className="font-mono">{offer.missingFields.join(", ")}</span>
        </p>
      )}
      <Button className="h-9 text-xs" onClick={s.acceptAttach}>
        Attach alongside the existing install
      </Button>
    </div>
  )
}

export const CredentialsStep = TokenStep
