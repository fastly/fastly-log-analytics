"use client";

import React, { useState, useEffect, useEffectEvent, useRef } from 'react'
import { useMutation, useQueryClient, useQuery, type QueryClient } from '@tanstack/react-query'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { SectionHeader, LabelWithInfo, ReviewCard, ReviewHeader, ReviewContent, ReviewItem, RetentionSelect } from '@/components/ui/confirm-dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { CollapsibleGroup, CmcdConfigSection } from '@/components/LogSettingsModal/LogSettingsModal'
import { SSEProgressView } from '@/components/SSEModal'
import { Activity, AlertCircle, CheckCircle2, ChevronLeft, ChevronRight, Cloud, Copy, Database, Eye, FileJson, FileText, Globe, Info, Layers, Loader2, Lock, Plus, RotateCcw, Search, Settings, Shield, Sparkles, XCircle, Zap, RefreshCw, TriangleAlert } from 'lucide-react'
import { cn, copyToClipboard, panelDialogContent, panelDialogFooter, REGION_LABELS, SHIELD_LABELS, ELAPSED_RELATIVE_AGO, formatBytes, formatElapsed, PERIOD_LABELS, downloadBlob, SHIELD_MAP, showToast } from '@/lib/utils'
import { evaluateLogFieldPresetSummary, resolveEnabledGroupsWithDeps, client, unwrapClient, queryKeys, adminFetch, extractApiError, estimateLogFieldBytesPerLine, isLogFieldPresetActive, toggleLogFieldGroup, toggleLogFieldOverride, toggleLogFieldPreset, updateLogFieldLimit } from '@/lib/api'
import { type Service, useServiceStore, useTimezoneStore } from '@/stores/filterStore'
import { type SSELine, type SSEStatus, useSSE } from '@/hooks/useSSE'
import type { Schema } from '@/types/api'
import { useForm, type PathValue } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { Skeleton } from '@/components/ui/skeleton'
import type { components } from '@/types/api.generated'

export type ProvisionService = Schema<"ProvisionService">;
export type LogFieldsCatalog = Schema<"LogFieldsCatalogResponse">; type LogFieldsConfig = Schema<"LogFieldsConfig">;
type ValidateResponse = Schema<"ProvisionValidateResponse">; export const WIZARD_DRAFT_VERSION = 2; export type Step =
  | "mode" | "token" | "service" | "features" | "storage" | "ngwaf"
  | "fields" | "execute" | "terraform" | "connect"; export type WizardMode = "provision" | "ingest" | null;
export type FosStatus = "idle" | "checking" | "success" | "error"; export type DomainStatus = "idle" | "checking" | "available" | "taken" | "error";
export type SetupPhase = "form" | "done";
export interface TokenInfo { id: string; name: string; type: "user" | "automation"; }
export interface ProvisionConfig { endpoint_name: string; fos_region: string; fos_endpoint: string; fos_bucket_name: string; fos_prefix: string;
  fos_access_key: string; fos_secret_key: string; sample_rate: number; edge_only: boolean; custom_condition: string;
  log_period: number; cdn_service_name: string; cdn_prefix: string; cdn_shield: string; cdn_url: string;
  cdn_secret: string; enable_cron_sync: boolean; delete_after: boolean; commit_interval_mins: number;
  enable_cron_compact: boolean; log_fields: LogFieldsConfig; ngwaf_workspace_id: string; cmcd_enabled: boolean;
  cmcd_mode: string; cmcd_version: number; logging_enabled: boolean; rum_enabled: boolean; rum_log_period: number;
  log_retention_days: number; rum_retention_days: number; cache_retention_days: number; rollup_retention_months: number;
  rum_custom_condition: string; faro_version: string | null; }
export const INITIAL_CONFIG: ProvisionConfig = { endpoint_name: "", fos_region: "us-east-1", fos_endpoint: "", fos_bucket_name: "", fos_prefix: "",
  fos_access_key: "", fos_secret_key: "", sample_rate: 100, edge_only: true, custom_condition: "",
  log_period: 60, rum_log_period: 300, cdn_service_name: "", cdn_prefix: "", cdn_shield: "iad-va-us",
  cdn_url: "", cdn_secret: "", enable_cron_sync: true, delete_after: true, commit_interval_mins: 5,
  enable_cron_compact: true, log_fields: { groups: [], field_overrides: {} }, ngwaf_workspace_id: "",
  cmcd_enabled: false, cmcd_mode: "query_string", cmcd_version: 1, logging_enabled: true, rum_enabled: false,
  log_retention_days: 30, rum_retention_days: 30, cache_retention_days: 14, rollup_retention_months: 12, rum_custom_condition: "", faro_version: null, };
export type PersistedConfig = Omit<ProvisionConfig, "fos_access_key" | "fos_secret_key" | "cdn_secret">; export interface WizardDraft {
  version: number; draftId: string; mode: Exclude<WizardMode, null>; step: Step; currentStep: Step;
  selectedServiceId: string | null; selectedServiceName: string | null; selectedCdnServiceId: string | null; selectedCdnServiceName: string | null;
  tokenInfo: TokenInfo | null; config: PersistedConfig; updatedAt: string; createdAt: string;
}
export function getStepsForMode( mode: WizardMode, config?: { logging_enabled?: boolean }, ): { id: Step; label: string }[] {
  const steps: { id: Step; label: string }[] = [ { id: "mode", label: "Role" }, { id: "token", label: "Auth" },
    { id: "service", label: "Service" }, ];
  if (mode === "ingest") steps.push({ id: "connect", label: "Connect" });
  steps.push({ id: "features", label: "Features" }, { id: "storage", label: "Storage" });
  if (!config || config.logging_enabled !== false) { steps.push({ id: "ngwaf", label: "NGWAF" }, { id: "fields", label: "Log Fields" }); }
  steps.push({ id: "execute", label: "Review" }); return steps; }
export interface ProvisionWizardProps { open: boolean; onOpenChange: (open: boolean) => void; }
export interface WizardState { setActiveServiceId: (id: string) => void; setServices: (services: Service[]) => void; services: Service[];
  timezone: string; queryClient: ReturnType<typeof useQueryClient>; step: Step; setStep: (s: Step) => void;
  mode: WizardMode; setMode: (m: WizardMode) => void; token: string; setToken: (t: string) => void;
  tokenInfo: TokenInfo | null; search: string; setSearch: (s: string) => void;
  selectedService: ProvisionService | null; setSelectedService: (s: ProvisionService | null) => void;
  selectedCdnService: ProvisionService | null; setSelectedCdnService: (s: ProvisionService | null) => void;
  isDeploying: boolean; fosStatus: FosStatus; fosError: string; terraformFiles: Record<string, string>;
  selectedTfFile: string; setSelectedTfFile: (f: string) => void; isFetchingTerraform: boolean;
  configStatus: Schema<"ProvisionCheckConfigResponse"> | null; isCheckingConfig: boolean;
  handleCheckConfig: () => Promise<void>; ngwafWorkspaces: { id: string; name: string }[]; ngwafFetching: boolean; ngwafFetchError: string; ngwafDebugRaw: string;
  lines: SSELine[]; status: SSEStatus;
  isDone: boolean; sseError: string | null; stop: () => void; handleModalClose: (isOpen: boolean) => void;
  onOpenChange: (open: boolean) => void; config: ProvisionConfig; setConfig: React.Dispatch<React.SetStateAction<ProvisionConfig>>;
  catalog: LogFieldsCatalog | undefined; isLoadingCatalog: boolean; toggleGroup: (groupId: string, checked: boolean) => void;
  toggleField: (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) => void; updateFieldLimit: (fieldId: string, limit?: number) => void;
  togglePreset: (presetGroups: string[]) => void; isPresetActive: (groups: string[]) => boolean;
  estimatedBytes: number; servicesData: ProvisionService[] | undefined; servicesError: Error | null;
  isLoadingServices: boolean; filteredServices: ProvisionService[]; handleTokenSubmit: () => Promise<void>;
  validateMutation: ReturnType<typeof useMutation<ValidateResponse | undefined, Error, string>>; handleServiceSelect: (service: ProvisionService) => void;
  handleCheckFos: (vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string }) => Promise<void>;
  checkDomain: (prefix: string) => Promise<void>; domainStatus: DomainStatus; domainMessage: string;
  setupPhase: SetupPhase; handleDeploy: () => void; fetchTerraformPreview: () => Promise<void>;
  handleExportTerraform: () => Promise<void>; handleAdminIngest: () => Promise<void>; STEPS: { id: Step; label: string }[]; pendingDraft: WizardDraft | null;
  resumeDraft: () => void; discardDraft: () => void; }
function relativeTime(iso: string): string { const then = new Date(iso).getTime();
  return Number.isNaN(then) ? "earlier" : formatElapsed(Math.floor((Date.now() - then) / 1000), ELAPSED_RELATIVE_AGO); }
export function ResumeBanner({ draft, onResume, onStartFresh, }: { draft: WizardDraft; onResume: () => void; onStartFresh: () => void }) {
  const label = getStepsForMode(draft.mode, draft.config).find((s) => s.id === draft.currentStep)?.label ?? draft.currentStep;
  const when = relativeTime(draft.updatedAt); return ( <div className="px-6 pt-4">
      <Alert>
        <RotateCcw className="h-4 w-4" />
        <AlertTitle>Resume previous wizard?</AlertTitle>
        <AlertDescription>
          You left off on the <span className="font-medium">{label}</span> step ({when}). Fastly API token and storage credentials were cleared for safety and will need to be re-entered.
        </AlertDescription>
        <div className="mt-3 flex flex-wrap gap-2 group-has-[>svg]/alert:col-start-2">
          <Button size="sm" onClick={onResume} data-testid="resume-banner-resume">Resume from {label}</Button>
          <Button size="sm" variant="ghost" onClick={onStartFresh} data-testid="resume-banner-start-fresh">Start fresh</Button>
        </div>
      </Alert>
    </div> ); }
export function WizardHeader({ s }: { s: WizardState }) { const { step, STEPS } = s; const stepIndex = STEPS.findIndex((x) => x.id === step); return (
    <DialogHeader className="px-6 pt-6 pb-7 border-b">
      <DialogTitle className="flex items-center gap-2 text-xl font-bold">
        <Plus className="h-5 w-5 text-primary" />
        Provision New Service
      </DialogTitle>
      <div className="flex items-center justify-center w-full mt-6 px-1 sm:px-4 max-w-2xl mx-auto">
        {STEPS.map((s2, i) => { const isActive = step === s2.id; const isCompleted = stepIndex > i; return ( <React.Fragment key={s2.id}>
              <div className="flex flex-col items-center shrink-0 relative">
                <div className={cn( "w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold transition-all duration-300 border-2", isActive
                      ? "bg-primary border-primary text-primary-foreground shadow-md shadow-primary/20 scale-110"
                      : isCompleted
                        ? "bg-green-500 border-green-500 text-white"
                        : "bg-background border-muted text-muted-foreground", )}
                  title={s2.label} >
                  {isCompleted ? <CheckCircle2 className="w-4 h-4" /> : i + 1}
                </div>
                <span className={cn( "text-[10px] font-semibold mt-1.5 absolute top-7 whitespace-nowrap transition-all duration-300",
                    isActive ? "text-foreground font-bold opacity-100 scale-100" : "text-muted-foreground opacity-60 hidden md:block", )} >
                  {s2.label}
                </span>
              </div>
              {i < STEPS.length - 1 && ( <div className="flex-1 min-w-[8px] max-w-[48px] h-[2px] mx-1 sm:mx-2 bg-muted relative">
                  <div className={cn("absolute inset-0 bg-primary transition-all duration-500 ease-in-out", stepIndex > i ? "w-full" : "w-0")} />
                </div>
              )}
            </React.Fragment> ); })}
      </div>
      <div className="text-center mt-7 text-xs text-muted-foreground font-medium">
        Step <span className="text-foreground font-semibold">{stepIndex + 1}</span> of {STEPS.length}
      </div>
    </DialogHeader> ); }
export function WizardFooter({ s }: { s: WizardState }) { const { step, setStep, mode, isDeploying, status, isDone, handleModalClose,
    token, isLoadingServices, handleTokenSubmit, selectedService, validateMutation, domainStatus, config, fetchTerraformPreview, handleDeploy, handleAdminIngest,
    fosStatus, setupPhase, stop, } = s; const handleBack = () => {
    const baseSteps = getStepsForMode(mode, config).map((x) => x.id);
    const order = baseSteps.includes("execute") ? [...baseSteps, "terraform" as Step] : baseSteps; const idx = order.indexOf(step);
    if (idx > 0) setStep(order[idx - 1]); }; return ( <DialogFooter className={panelDialogFooter}>
      {!isDeploying && step !== "mode" && step !== "terraform" && !(step === "connect" && setupPhase === "done") && (
        <Button variant="ghost" className="mr-auto h-9 text-xs" onClick={handleBack}>
          <ChevronLeft className="h-4 w-4 mr-1" /> Back
        </Button>
      )}
      {status !== "streaming" && ( <Button variant="outline" className="h-9 text-xs" onClick={() => handleModalClose(false)}>
          {status === "done" || isDone ? "Close & Reload" : "Cancel"}
        </Button>
      )}
      {!isDeploying && ( <>
          {step === "mode" && <Button disabled className="h-9 text-xs">Select a Role</Button>}
          {step === "token" && ( <Button disabled={!token || isLoadingServices} onClick={handleTokenSubmit} className="h-9 text-xs">
              {isLoadingServices && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Fetch Services
            </Button>
          )}
          {step === "service" && ( <Button
              disabled={!selectedService || validateMutation.isPending}
              onClick={() => selectedService && validateMutation.mutate(selectedService.id)}
              className="h-9 text-xs" >
              {validateMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Continue
            </Button>
          )}
          {step === "features" && ( <Button onClick={() => setStep("storage")} disabled={!config.logging_enabled && !config.rum_enabled} className="h-9 text-xs">
              Continue
            </Button>
          )}
          {step === "storage" && ( <Button
              onClick={() => setStep(config.logging_enabled ? "ngwaf" : "execute")}
              disabled={domainStatus === "taken" || domainStatus === "checking"}
              className="h-9 text-xs" >
              Continue
            </Button>
          )}
          {step === "ngwaf" && ( <Button onClick={() => setStep("fields")} className="h-9 text-xs">
              {config.ngwaf_workspace_id ? "Continue" : "Skip"}
            </Button>
          )}
          {step === "fields" && ( <Button onClick={() => setStep("execute")} className="h-9 text-xs">Review Settings</Button>
          )}
          {step === "execute" && ( <div className="flex gap-2">
              <Button variant="secondary" className="h-9 font-bold" onClick={() => { fetchTerraformPreview(); setStep("terraform"); }}>
                View & Export Terraform
              </Button>
              {mode !== "ingest" ? (
                <Button size="lg" disabled={domainStatus === "taken" || !config.fos_bucket_name} className="h-9 px-6 font-bold" onClick={handleDeploy}>
                  Deploy to Fastly
                </Button>
              ) : ( <Button size="lg" disabled={!config.fos_bucket_name} className="h-9 px-6 font-bold" onClick={handleAdminIngest}>
                  Complete Setup
                </Button>
              )}
            </div>
          )}
          {step === "terraform" && ( <Button size="lg" className="h-9 px-6 font-bold" onClick={() => setStep("execute")}>Back to Review</Button>
          )}
          {step === "connect" && setupPhase === "form" && ( <Button
              size="lg"
              disabled={!config.endpoint_name || !config.cdn_service_name || fosStatus !== "success"}
              className="h-9 px-6 font-bold"
              onClick={() => setStep("features")} >
              Continue
            </Button>
          )}
        </>
      )}
      {status === "streaming" && <Button variant="outline" onClick={stop} className="h-9 text-xs">Stop</Button>}
    </DialogFooter> ); }
function errorCode(e: unknown): string | undefined { return e && typeof e === "object" && "code" in e ? String((e as Record<string, unknown>).code) : undefined; }
function StatusCheckIcon({ enabled }: { enabled: boolean }) {
  return enabled ? <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" /> : <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />; }
function FeatureSummaryBadge({ icon: Icon, title, subtitle }: { icon: React.ComponentType<{ className?: string }>; title: string; subtitle: string }) { return (
    <div className="flex-1 flex items-center gap-3 border rounded-xl p-3 bg-primary/[0.03] border-primary/10 shadow-sm">
      <div className="h-8 w-8 rounded-lg bg-primary/10 flex items-center justify-center text-primary shrink-0"><Icon className="h-4 w-4" /></div>
      <div className="flex flex-col min-w-0">
        <span className="text-xs font-semibold text-foreground">{title}</span>
        <span className="text-[10px] text-muted-foreground leading-snug">{subtitle}</span>
      </div>
    </div> ); }
export function ExecuteStep({ s }: { s: WizardState }) { const { config, catalog, selectedService } = s;
  const enabledGroups = React.useMemo(() => resolveEnabledGroupsWithDeps(config.log_fields?.groups, catalog), [config.log_fields?.groups, catalog]);
  const presetSummary = React.useMemo(() => evaluateLogFieldPresetSummary(config.log_fields, catalog), [config.log_fields, catalog]); const automationChecks = [
    { label: `Sync every ${config.log_period >= 120 ? Math.floor(config.log_period / 120) + "m" : Math.floor(config.log_period / 2) + "s"}`, enabled: config.enable_cron_sync },
    { label: `Commit to DuckLake every ${config.commit_interval_mins}m`, enabled: config.enable_cron_sync },
    { label: "Auto-delete Raw Logs", enabled: config.delete_after && config.enable_cron_sync },
    { label: "Daily DuckLake Optimization", enabled: config.enable_cron_compact && config.enable_cron_sync }, ]; return (
    <div className="flex-1 overflow-y-auto min-h-0 flex flex-col p-8 items-center text-left">
      <div className="w-full max-w-2xl space-y-8">
        {s.isDeploying ? ( <div className="space-y-6 w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="text-center space-y-2">
              <h3 className="text-2xl font-semibold tracking-tight">Provisioning: {selectedService?.name}</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                Setting up Fastly Object Storage, logging endpoints, and CDN proxy...
              </p>
            </div>
            <SSEProgressView lines={s.lines} status={s.status} error={s.sseError} className="h-[400px]" progressLabel="Progress"
              doneMessage="Provisioning completed successfully! You may now close this window." />
          </div>
        ) : ( <>
            <div className="text-center space-y-2">
              <h3 className="text-2xl font-semibold tracking-tight">Review & Deploy</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">You are about to provision the following resources.</p>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <ReviewCard className="col-span-2 space-y-3">
                <ReviewHeader icon={Sparkles}>Enabled Features</ReviewHeader>
                <div className="flex flex-col sm:flex-row gap-3 pt-1">
                  {config.logging_enabled !== false && (
                    <FeatureSummaryBadge icon={Database} title="Request Logging" subtitle="In-depth HTTP logs, edge metrics, and WAF protection." />
                  )}
                  {config.rum_enabled && (
                    <FeatureSummaryBadge icon={Globe} title="Real User Monitoring (RUM)" subtitle="Client-side web vitals, speed insights, and session analytics." />
                  )}
                </div>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Cloud}>Target Service</ReviewHeader>
                <ReviewContent>
                  <ReviewItem label="Service Name" value={selectedService?.name} />
                  {config.logging_enabled !== false && ( <>
                      <ReviewItem label="Log Endpoint" value={config.endpoint_name} />
                      <ReviewItem label="Sampling Rate / Period" value={`${config.sample_rate}% / ${config.log_period}s`} />
                    </>
                  )}
                  {config.logging_enabled !== false && config.custom_condition && (
                    <ReviewItem label="Custom Condition" value={config.custom_condition} className="truncate font-mono text-[10px]" />
                  )}
                </ReviewContent>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Globe}>CDN Edge Proxy</ReviewHeader>
                <ReviewContent>
                  <ReviewItem label="Domain" value={`${config.cdn_prefix}.global.ssl.fastly.net`} />
                  <ReviewItem label="Shield POP" value={SHIELD_LABELS[config.cdn_shield] || "None"} />
                </ReviewContent>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Database}>Object Storage</ReviewHeader>
                <ReviewContent>
                  <ReviewItem label="Bucket" value={config.fos_bucket_name} />
                  <ReviewItem label="Region" value={REGION_LABELS[config.fos_region]} />
                  <ReviewItem label="Edge Only" value={config.edge_only ? "Yes" : "No"} />
                </ReviewContent>
              </ReviewCard>
              <ReviewCard>
                <ReviewHeader icon={Settings}>Automation</ReviewHeader>
                <ReviewContent className="gap-2.5">
                  {automationChecks.map(({ label, enabled }) => (
                    <ReviewItem key={label} variant="between" label={label} className={cn(!enabled && "text-muted-foreground")} value={<StatusCheckIcon enabled={enabled} />} />
                  ))}
                  {config.logging_enabled !== false && (
                    <ReviewItem variant="between" label="Log Retention" className={cn(!config.enable_cron_sync && "text-muted-foreground")} value={config.log_retention_days === 0 ? "Forever" : `${config.log_retention_days} days`} />
                  )}
                  {config.rum_enabled && (
                    <ReviewItem variant="between" label="RUM Retention" className={cn(!config.enable_cron_sync && "text-muted-foreground")} value={config.rum_retention_days === 0 ? "Forever" : `${config.rum_retention_days} days`} />
                  )}
                </ReviewContent>
              </ReviewCard>
              {config.logging_enabled !== false && ( <ReviewCard className="col-span-2 space-y-3">
                  <div className="flex justify-between items-center">
                    <ReviewHeader icon={FileJson}>Log Configuration</ReviewHeader>
                    <span className="font-mono text-[10px] bg-muted px-2 py-0.5 rounded text-muted-foreground border">~{formatBytes(s.estimatedBytes)} / line</span>
                  </div>
                  <div className="flex flex-wrap gap-1.5 pt-1">
                    <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                      {presetSummary.bestPresetName ? `${presetSummary.bestPresetName} Preset` : "Custom Configuration"}
                    </div>
                    {!presetSummary.bestPresetName && catalog?.groups?.map((g) => {
                      const isEnabled = g.locked || (g.id != null && presetSummary.enabledGroupsSet.has(g.id)); return isEnabled ? (
                        <div key={g.id || "core"} className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-primary/10 text-primary border border-primary/20">
                          {g.label}
                        </div>
                      ) : null; })}
                    {presetSummary.disabledCount > 0 && (
                      <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                        +{presetSummary.disabledCount} disabled
                      </div>
                    )}
                  </div>
                </ReviewCard>
              )}
              {config.logging_enabled !== false && ( <ReviewCard className="col-span-2 space-y-3">
                  <div className="flex justify-between items-center">
                    <ReviewHeader icon={Sparkles}>Automated Insights</ReviewHeader>
                    <span className="text-[10px] text-muted-foreground">Derived from logs</span>
                  </div>
                  <div className="grid grid-cols-2 gap-3 pt-1">
                    {catalog?.insights?.map((insight) => { const isEnabled = insight.required_groups.every((rg) => enabledGroups.has(rg)); return (
                        <div key={insight.id} className={cn("flex items-start gap-3 border rounded-lg p-2.5 bg-background shadow-sm transition-all", !isEnabled && "opacity-50 grayscale")}>
                          <div className="mt-0.5 shrink-0">
                            {isEnabled ? <CheckCircle2 className="h-4 w-4 text-emerald-500" /> : <XCircle className="h-4 w-4 text-muted-foreground" />}
                          </div>
                          <div className="flex flex-col min-w-0">
                            <span className={cn("text-xs font-semibold truncate", !isEnabled && "line-through text-muted-foreground")}>{insight.title}</span>
                            <span className="text-[10px] text-muted-foreground line-clamp-2 leading-tight mt-0.5" title={insight.description}>{insight.description}</span>
                          </div>
                        </div> ); })}
                  </div>
                </ReviewCard>
              )}
            </div>
          </>
        )}
      </div>
    </div> ); }
export function TokenStep({ s }: { s: WizardState }) { return ( <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-6 text-center">
      <div className="space-y-2 max-w-md">
        <h3 className="text-xl font-semibold tracking-tight">Enter Fastly API Token</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          We need a token with <code>superuser</code> permissions to create and configure your services. An <code>engineer</code> token cannot create new services.
        </p>
        <div className="pt-2">
          <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-amber-500/10 text-amber-600 dark:text-amber-500 border border-amber-500/20 text-[10px] font-bold uppercase tracking-wider">
            <AlertCircle className="h-3 w-3 shrink-0" />
            <a href="https://www.fastly.com/documentation/reference/api/auth-tokens/user/" target="_blank" rel="noreferrer" className="hover:underline hover:text-amber-700 dark:hover:text-amber-400 transition-colors">
              Personal API Tokens required for NGWAF
            </a>
          </div>
        </div>
      </div>
      <div className="space-y-4 w-full max-w-sm text-left">
        <div className="space-y-2">
          <Label htmlFor="token" className="flex items-center gap-2 text-sm font-medium"><Lock className="h-3.5 w-3.5" /> API Token</Label>
          <Input id="token" type="password" value={s.token} onChange={(e) => s.setToken(e.target.value.trim())} placeholder="" className="font-mono text-center" />
        </div>
        {s.servicesError && (
          <div className="p-3 bg-destructive/10 text-destructive text-xs rounded-md border border-destructive/20 flex gap-2 animate-in fade-in slide-in-from-top-1 text-left">
            <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
            {errorCode(s.servicesError) === "object_storage_not_enabled" ? ( <span>
                Object Storage isn&apos;t enabled on this Fastly account, and it&apos;s required to store logs.{" "}
                <a href="https://manage.fastly.com/products/object-storage" target="_blank" rel="noreferrer" className="font-semibold underline underline-offset-2 hover:opacity-80">
                  Enable Object Storage
                </a>{" "}
                for your account, then click Fetch Services again.
              </span>
            ) : s.servicesError instanceof Error ? s.servicesError.message : "Failed to fetch services"}
          </div>
        )}
      </div>
    </div> ); }
export function ServiceStep({ s }: { s: WizardState }) { return ( <div className="flex-1 flex flex-col overflow-hidden p-6 md:p-8 max-w-3xl mx-auto w-full gap-4">
      <div className="flex items-center justify-between shrink-0">
        <div className="p-2 border rounded-lg bg-muted/10 flex items-center gap-3 px-4 flex-1">
          <Search className="h-5 w-5 text-muted-foreground" />
          <Input placeholder="Search your services..." className="h-10 border-none bg-transparent shadow-none focus-visible:ring-0 text-base" value={s.search}
            onChange={(e) => s.setSearch(e.target.value)} />
        </div>
        {s.tokenInfo && ( <div className="ml-4 flex flex-col items-end shrink-0">
            <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">Authenticated as</span>
            <div className="flex items-center gap-1.5">
              <span className="text-xs font-semibold">{s.tokenInfo.name}</span>
              <Badge variant={s.tokenInfo.type === "user" ? "default" : "outline"} className="text-[9px] h-3.5 px-1 uppercase">
                {s.tokenInfo.type}
              </Badge>
            </div>
          </div>
        )}
      </div>
      <div className="flex-1 overflow-y-auto min-h-0 border rounded-lg shadow-sm">
        <div className="divide-y divide-muted/50 bg-background">
          {s.isLoadingServices ? ( <div className="py-12 flex flex-col items-center justify-center gap-3 text-muted-foreground text-sm">
              <Loader2 className="h-6 w-6 animate-spin text-primary" />
              <span>Loading services...</span>
            </div>
          ) : s.filteredServices.length > 0 ? ( s.filteredServices.map((svc) => ( <button
                key={svc.id}
                type="button"
                disabled={svc.provisioned}
                aria-label={svc.provisioned ? `${svc.name} — already provisioned` : `Select service ${svc.name}, id ${svc.id}`}
                className={cn(
                  "w-full text-left p-4 flex items-center justify-between transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
                  svc.provisioned ? "opacity-40 grayscale bg-muted/5 cursor-not-allowed" : "hover:bg-muted/50 cursor-pointer active:bg-muted", )}
                onClick={() => !svc.provisioned && s.handleServiceSelect(svc)} >
                <div className="space-y-1">
                  <div className="font-semibold text-sm flex items-center gap-2">
                    {svc.name}
                    {svc.provisioned && ( <Badge variant="secondary" className="text-[10px] h-4 px-1 leading-none font-bold uppercase tracking-tight">Active</Badge>
                    )}
                  </div>
                  <div className="text-xs font-mono text-muted-foreground">{svc.id}</div>
                </div>
                {!svc.provisioned && ( <div className="flex items-center text-primary">
                    {s.validateMutation.isPending && s.selectedService?.id === svc.id ? ( <Loader2 className="h-5 w-5 animate-spin" />
                    ) : ( <ChevronRight className="h-5 w-5" />
                    )}
                  </div>
                )}
              </button>
            ))
          ) : ( <div className="py-12 text-center text-muted-foreground text-sm italic">No services found matching &quot;{s.search}&quot;</div>
          )}
        </div>
      </div>
    </div> ); }
export function NgwafStep({ s }: { s: WizardState }) { const { config, setConfig } = s; return ( <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-6 max-w-2xl mx-auto">
        <div className="flex items-center gap-2 pb-2 border-b">
          <Shield className="h-5 w-5 text-primary" />
          <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">NGWAF Workspace</h3>
        </div>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Link this service to an existing Fastly NGWAF workspace to enable WAF signal logging and bot detection. This step is optional — skip it if NGWAF is not deployed on this service.
        </p>
        {s.ngwafFetching ? ( <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading workspaces…
          </div>
        ) : s.ngwafFetchError ? ( <div className="flex items-center gap-2 text-sm text-destructive">
            <AlertCircle className="h-4 w-4 shrink-0" /> {s.ngwafFetchError}
          </div>
        ) : s.ngwafWorkspaces.length > 0 ? ( <div className="space-y-2">
            <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Workspace</Label>
            <Select value={config.ngwaf_workspace_id || "__none__"} onValueChange={(v: string | null) => setConfig((prev) => {
                  const workspaceId = !v || v === "__none__" ? "" : v; const groups: string[] = prev.log_fields?.groups ?? []; return { ...prev,
                    ngwaf_workspace_id: workspaceId,
                    ...(workspaceId && !groups.includes("J") ? { log_fields: { ...prev.log_fields, groups: [...groups, "J"] } } : {}), }; })
              } >
              <SelectTrigger className="h-9 text-sm"><SelectValue placeholder="Select a workspace…" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="__none__"><span className="text-muted-foreground">No NGWAF (skip)</span></SelectItem>
                {s.ngwafWorkspaces.map((ws) => <SelectItem key={ws.id} value={ws.id}>{ws.name}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
        ) : ( <div className="space-y-3">
            <div className="flex items-center gap-2 text-sm text-muted-foreground bg-muted/30 p-3 rounded-lg border border-dashed">
              <Info className="h-4 w-4 shrink-0" /> No NGWAF workspaces found in this account.
            </div>
            {s.ngwafDebugRaw && ( <details className="text-[10px]">
                <summary className="cursor-pointer text-muted-foreground uppercase tracking-wider font-bold">Raw API response (debug)</summary>
                <pre className="mt-1 p-2 bg-muted rounded text-xs overflow-auto max-h-32 whitespace-pre-wrap break-all">{s.ngwafDebugRaw}</pre>
              </details>
            )}
          </div>
        )}
        <div className="p-4 rounded-xl bg-muted/30 border border-dashed space-y-1">
          <p className="text-xs font-semibold text-muted-foreground">
            WAF / NGWAF log fields (group J) will only be available in the next step if a workspace is selected here.
          </p>
        </div>
      </div>
    </div> ); }
const FEATURE_OPTIONS = [ { id: "logging" as const, icon: Activity, title: "Request Logging",
    description: "Stream edge requests directly to Object Storage. Power charts for traffic, status codes, and security.",
    activeClass: "border-primary bg-primary/5 shadow-md shadow-primary/5", hoverClass: "border-muted hover:border-primary/50 hover:bg-muted/30",
    iconBgClass: "bg-primary/20", iconColorClass: "text-primary", dotClass: "bg-primary", }, { id: "rum" as const, icon: Eye, title: "RUM only",
    description: "Deploy client-side Core Web Vitals, page performance, and error analytics. Skips standard request logs.",
    activeClass: "border-amber-500 bg-amber-500/5 shadow-md shadow-amber-500/5", hoverClass: "border-muted hover:border-amber-500/50 hover:bg-muted/30",
    iconBgClass: "bg-amber-500/20", iconColorClass: "text-amber-500", dotClass: "bg-amber-500", }, {
    id: "both" as const, icon: Layers, title: "Shared Bundle (Both)",
    description: "Enable Request Logging and Real User Monitoring side-by-side, sharing a single FOS storage bucket.",
    activeClass: "border-emerald-500 bg-emerald-500/5 shadow-md shadow-emerald-500/5", hoverClass: "border-muted hover:border-emerald-500/50 hover:bg-muted/30",
    iconBgClass: "bg-emerald-500/20", iconColorClass: "text-emerald-500", dotClass: "bg-emerald-500", }, ] as const;
export function FeaturesStep({ s }: { s: WizardState }) { const { config, setConfig } = s;
  const currentSelection = config.logging_enabled && config.rum_enabled ? "both" : config.rum_enabled && !config.logging_enabled ? "rum" : "logging"; return (
    <div className="flex-1 flex flex-col items-center justify-center p-6 md:p-8 space-y-8 text-center animate-in fade-in slide-in-from-bottom-4 duration-500 max-w-4xl mx-auto w-full">
      <div className="space-y-3 max-w-lg">
        <span className="text-[10px] font-bold uppercase tracking-widest text-primary bg-primary/10 px-2.5 py-1 rounded-full">Step {s.STEPS.findIndex((x) => x.id === "features") + 1} of {s.STEPS.length}</span>
        <h3 className="text-2xl font-bold tracking-tight">Select features to deploy</h3>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Choose the integration capabilities you want to configure on your Fastly service. You can adjust these settings later.
        </p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full">
        {FEATURE_OPTIONS.map((opt) => { const active = currentSelection === opt.id; const Icon = opt.icon; return ( <button
              key={opt.id}
              type="button"
              onClick={() => setConfig((prev) => ({ ...prev, logging_enabled: opt.id === "logging" || opt.id === "both", rum_enabled: opt.id === "rum" || opt.id === "both" }))}
              className={cn(
                "group relative flex flex-col items-center gap-5 p-6 border-2 rounded-2xl bg-background transition-all text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                active ? opt.activeClass : opt.hoverClass, )} >
              <div className={cn("w-12 h-12 rounded-xl flex items-center justify-center transition-transform group-hover:scale-110", active ? opt.iconBgClass : "bg-muted")}>
                <Icon className={cn("h-6 w-6", active ? opt.iconColorClass : "text-muted-foreground")} />
              </div>
              <div className="space-y-1.5 text-center">
                <h4 className="font-bold text-base">{opt.title}</h4>
                <p className="text-xs text-muted-foreground leading-relaxed">{opt.description}</p>
              </div>
              {active && <div className={cn("absolute top-3 right-3 w-2 h-2 rounded-full", opt.dotClass)} />}
            </button> ); })}
      </div>
    </div> ); }
export function FieldsStep({ s }: { s: WizardState }) { const { config, setConfig, catalog, isLoadingCatalog } = s; return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-6 max-w-4xl mx-auto">
        <div className="flex items-center justify-between pb-2 border-b">
          <div className="flex items-center gap-2">
            <FileJson className="h-5 w-5 text-primary" />
            <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">Log Fields</h3>
          </div>
          {!isLoadingCatalog && ( <div className="text-xs font-mono text-muted-foreground bg-muted/50 px-3 py-1 rounded-md border">
              Est. ~{formatBytes(s.estimatedBytes)} / line
            </div>
          )}
        </div>
        {isLoadingCatalog ? ( <div className="h-[200px] flex items-center justify-center bg-muted/10 rounded-lg border border-dashed">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : ( <div className="space-y-6">
            <div className="space-y-4">
              <p className="text-sm text-muted-foreground">
                Select the data fields to capture at the edge. More fields provide richer insights but increase storage and bandwidth costs.
              </p>
              <div className="p-3 bg-blue-500/10 border border-blue-500/20 text-blue-700 dark:text-blue-400 rounded-md text-xs">
                <strong>Note:</strong> Custom log fields (e.g. tracking specific HTTP headers or application IDs) can be configured from the Admin dashboard after initial provisioning is complete.
              </div>
              {catalog?.presets && ( <div className="flex flex-wrap gap-2 pt-2 items-center">
                  {Object.entries(catalog.presets).map(([key, preset]) => { const isMinimal = key === "minimal";
                    const active = isMinimal || s.isPresetActive(preset.groups); return ( <Button
                        key={key}
                        variant={active ? "default" : "outline"}
                        size="sm"
                        className={cn("h-8 text-xs font-semibold transition-all", active && "ring-2 ring-primary/20", isMinimal && "opacity-80")}
                        title={preset.description}
                        onClick={() => !isMinimal && s.togglePreset(preset.groups || [])}
                        disabled={isMinimal} >
                        {preset.label || key}
                      </Button> ); })}
                  <Button variant="ghost" size="sm" className="h-8 text-xs font-semibold text-muted-foreground hover:text-foreground ml-auto"
                    onClick={() => setConfig((prev) => ({ ...prev, log_fields: { ...prev.log_fields, groups: [], field_overrides: {}, field_limits: prev.log_fields.field_limits } }))}
                  >
                    Clear All
                  </Button>
                </div>
              )}
            </div>
            {!config.ngwaf_workspace_id && (
              <div className="flex items-center gap-2 text-xs text-muted-foreground bg-muted/30 border border-dashed rounded-lg px-3 py-2">
                <Shield className="h-3.5 w-3.5 shrink-0" />
                WAF / NGWAF fields (group J) are hidden — no NGWAF workspace selected.
              </div>
            )}
            <div className="grid grid-cols-1 gap-3 pb-8">
              {catalog && (catalog.groups ?? []).filter((g) => config.ngwaf_workspace_id || g.id !== "J").map((g) => ( <CollapsibleGroup
                  key={g.id}
                  group={g}
                  catalog={catalog}
                  config={config.log_fields}
                  toggleGroup={s.toggleGroup}
                  toggleField={s.toggleField}
                  updateFieldLimit={s.updateFieldLimit} />
              ))}
              <CmcdConfigSection enabled={config.cmcd_enabled} onEnabledChange={(v) => setConfig((prev) => ({ ...prev, cmcd_enabled: v }))} mode={config.cmcd_mode}
                onModeChange={(v) => setConfig((prev) => ({ ...prev, cmcd_mode: v }))}
                version={config.cmcd_version}
                onVersionChange={(v) => setConfig((prev) => ({ ...prev, cmcd_version: v }))} />
            </div>
          </div>
        )}
      </div>
    </div> ); }
export function ModeStep({ s }: { s: WizardState }) { const modeCards = [ {
      mode: "provision" as const, nextStep: "token" as const, icon: Zap, iconBg: "bg-primary/10", iconColor: "text-primary", hoverBorder: "hover:border-primary",
      title: "Admin: Provision", description: "Deploy new Fastly Object Storage resources, logging endpoints, and a CDN proxy. Best for new projects.", }, {
      mode: "ingest" as const, nextStep: "token" as const, icon: Settings,
      iconBg: "bg-amber-500/10", iconColor: "text-amber-500", hoverBorder: "hover:border-amber-500", title: "Admin: Connect Terraform",
      description: "Set up ingestion for a service that is already managed via Terraform. We'll skip creating Fastly resources but set up data sync.", }, ]; return (
    <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-10 text-center animate-in fade-in slide-in-from-bottom-4 duration-500">
      <div className="space-y-3 max-w-lg">
        <h3 className="text-2xl font-bold tracking-tight">Select your role</h3>
        <p className="text-muted-foreground leading-relaxed">Choose how you want to set up this service.</p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 w-full max-w-3xl">
        {modeCards.map((card) => { const Icon = card.icon; return ( <button
              key={card.mode}
              type="button"
              onClick={() => { s.setMode(card.mode); s.setStep(card.nextStep); }}
              className={cn( "group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 transition-all text-left", card.hoverBorder, )} >
              <div className={cn("w-16 h-16 rounded-2xl flex items-center justify-center group-hover:scale-110 transition-transform", card.iconBg)}>
                <Icon className={cn("h-8 w-8", card.iconColor)} />
              </div>
              <div className="space-y-2 text-center">
                <h4 className="font-bold text-lg">{card.title}</h4>
                <p className="text-xs text-muted-foreground leading-relaxed">{card.description}</p>
              </div>
            </button> ); })}
      </div>
    </div> ); }
const TERRAFORM_TABS = [ { value: "instructions", label: "Instructions", icon: FileText, defaultFile: "instructions", match: (f: string) => f === "instructions" }, {
    value: "logging", label: "Logging Service", icon: Zap, defaultFile: "logging_service.tf",
    match: (f: string) => f === "logging_service.tf" || f === "log_format.vcl" || f.startsWith("capture_snippets/"), }, {
    value: "cdn", label: "CDN & Storage", icon: Globe, defaultFile: "fos.tf",
    match: (f: string) => f === "fos.tf" || f === "cdn_proxy.tf" || f === "cdn_proxy.vcl" || f.startsWith("cdn_snippets/"), },
  { value: "all", label: "All Files", icon: FileJson, defaultFile: undefined, match: () => true }, ] as const;
export function TerraformStep({ s }: { s: WizardState }) { const { terraformFiles, selectedTfFile, setSelectedTfFile } = s; return (
    <div className="flex-1 overflow-hidden p-8 flex flex-col">
      <div className="w-full max-w-6xl mx-auto flex flex-col h-full space-y-6">
        <div className="flex items-center justify-between pb-4 border-b shrink-0">
          <div className="space-y-1">
            <h3 className="text-lg font-bold tracking-tight flex items-center gap-2">
              <FileJson className="h-5 w-5 text-primary" /> Terraform & VCL Preview
            </h3>
            <p className="text-sm text-muted-foreground">Review and export the generated configuration files.</p>
          </div>
          <Button onClick={s.handleExportTerraform} className="h-9 font-bold">Export as ZIP</Button>
        </div>
        {s.isFetchingTerraform ? ( <div className="flex-1 flex items-center justify-center bg-muted/10 rounded-lg border border-dashed">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : ( <Tabs
            defaultValue="instructions"
            className="flex-1 flex flex-col min-h-0"
            onValueChange={(tab) => { const found = TERRAFORM_TABS.find((t) => t.value === tab); if (found?.defaultFile) setSelectedTfFile(found.defaultFile); }} >
            <TabsList className="grid w-full grid-cols-4 shrink-0">
              {TERRAFORM_TABS.map(({ value, label, icon: Icon }) => ( <TabsTrigger key={value} value={value} className="flex items-center gap-2">
                  <Icon className="w-3.5 h-3.5" /> {label}
                </TabsTrigger>
              ))}
            </TabsList>
            {TERRAFORM_TABS.map(({ value, match }) => ( <TabsContent key={value} value={value} className="flex-1 flex gap-4 min-h-0 pt-4 mt-0">
                <div className="w-64 shrink-0 flex flex-col gap-1 overflow-y-auto pr-2 custom-scrollbar border-r">
                  {Object.keys(terraformFiles)
                    .filter(match)
                    .sort((a, b) => (a.endsWith(".tf") !== b.endsWith(".tf") ? (a.endsWith(".tf") ? -1 : 1) : a.localeCompare(b)))
                    .map((fileName) => ( <button
                        key={fileName}
                        onClick={() => setSelectedTfFile(fileName)}
                        className={cn( "text-left px-3 py-2 rounded-md text-[11px] font-mono transition-colors truncate",
                          selectedTfFile === fileName ? "bg-primary text-primary-foreground font-bold shadow-sm" : "hover:bg-muted text-muted-foreground", )} >
                        {fileName}
                      </button>
                    ))}
                </div>
                <div className="flex-1 bg-muted rounded-lg border overflow-hidden flex flex-col">
                  <div className="px-4 py-2 border-b bg-muted/50 flex items-center justify-between shrink-0">
                    <span className="text-[10px] font-mono text-muted-foreground">{selectedTfFile}</span>
                    <Button variant="ghost" size="icon"
                      aria-label={`Copy ${selectedTfFile} to clipboard`}
                      className="h-6 w-6 hover:bg-muted-foreground/10"
                      onClick={() => void copyToClipboard(terraformFiles[selectedTfFile])} >
                      <Copy className="h-3 w-3" />
                    </Button>
                  </div>
                  <div className="flex-1 overflow-auto p-4 custom-scrollbar">
                    <pre className="text-xs font-mono text-muted-foreground whitespace-pre leading-relaxed">
                      {terraformFiles[selectedTfFile] || "Select a file on the left to preview its content."}
                    </pre>
                  </div>
                </div>
              </TabsContent>
            ))}
          </Tabs>
        )}
      </div>
    </div> ); }
const STEP_COMPONENTS: Record<WizardState["step"], React.ComponentType<{ s: WizardState }>> = {
  mode: ModeStep, token: TokenStep, service: ServiceStep, storage: StorageStep, features: FeaturesStep,
  connect: ConnectStep,
  ngwaf: NgwafStep, fields: FieldsStep, execute: ExecuteStep, terraform: TerraformStep, };
export function ProvisionWizard({ open, onOpenChange }: ProvisionWizardProps) { const s = useWizardState(open, onOpenChange);
  const StepComponent = STEP_COMPONENTS[s.step]; return ( <Dialog open={open} onOpenChange={s.handleModalClose}>
      <DialogContent className={cn("sm:max-w-5xl h-[85vh]", panelDialogContent)} showCloseButton={s.status !== "streaming" && s.setupPhase !== "done"} >
        {s.pendingDraft && s.mode === null && ( <ResumeBanner draft={s.pendingDraft} onResume={s.resumeDraft} onStartFresh={s.discardDraft} />
        )}
        <WizardHeader s={s} />
        <div className="flex-1 overflow-hidden flex flex-col">
          {StepComponent && <StepComponent s={s} />}
        </div>
        <WizardFooter s={s} />
      </DialogContent>
    </Dialog> ); }
// Lightweight per-field validators that fire on blur so wizard users see
// errors as they tab through, not only when the validate-mutation rejects
// on Next-click. Each returns null on valid input, an error string otherwise.
function validateBucketName(v: string): string | null { if (!v) return null
  // RFC 1123-ish: lowercase letters, digits, hyphens; 3-63 chars; no leading/
  // trailing hyphen; no consecutive dots. FOS rejects names that fail this.
  if (v.length < 3 || v.length > 63) return 'Bucket name must be 3–63 chars.'
  if (!/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/.test(v)) { return 'Bucket name must be lowercase alphanumeric with hyphens (no leading/trailing -).'
  }
  return null
}
function validateAccessKey(v: string): string | null { if (!v) return null
  // AWS access-key IDs are 20 chars starting with AKIA (long-lived) or ASIA
  // (temporary session). FOS keys follow the same pattern.
  if (!/^(AKIA|ASIA)[A-Z0-9]{16}$/.test(v.trim())) { return 'Access key should start with AKIA or ASIA and be 20 chars total.'
  }
  return null
}
export function ConnectStep({ s }: { s: WizardState }) {
  // Local per-field error state. Lives in ConnectStep, not WizardState, since
  // it's a UI affordance only — the backend validates authoritatively on
  // submit. Map ties field-key to its blur-validation message.
  const [fieldErrors, setFieldErrors] = React.useState<Record<string, string | null>>({});
  const setError = (key: string, msg: string | null) => setFieldErrors((prev) => ({ ...prev, [key]: msg }));
  if (s.setupPhase === "done") {
    return (
      <div className="flex-1 overflow-y-auto min-h-0 p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
        <div className="text-center space-y-1">
          <h3 className="text-lg font-semibold tracking-tight">Setup Complete</h3>
          <p className="text-sm text-muted-foreground">Your service is connected and background ingestion is configured.</p>
        </div>
      </div>
    );
  }
  // form phase
  const { config, setConfig } = s;
  const renderServicePicker = (selected: ProvisionService | null, onSelect: (svc: ProvisionService) => void, placeholder: string) => (
    <Select value={selected?.id || ""} onValueChange={(id) => { const svc = (s.servicesData ?? []).find((item) => item.id === id); if (svc) onSelect(svc); }}>
      <SelectTrigger className="h-9 font-mono text-sm"><SelectValue placeholder={placeholder} /></SelectTrigger>
      <SelectContent>{(s.servicesData ?? []).map((svc) => <SelectItem key={svc.id} value={svc.id}>{svc.name} ({svc.id})</SelectItem>)}</SelectContent>
    </Select>
  );
  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
        <div className="space-y-5">
          <SectionHeader title="Connect to Existing Storage" icon={Database} />
          <p className="text-sm text-muted-foreground leading-relaxed">
            Enter the credentials for your existing Fastly Object Storage bucket and CDN proxy. We will use these to set up background ingestion.
          </p>
          <div className="grid grid-cols-2 gap-6 pt-2">
            <div className="space-y-1.5">
              <LabelWithInfo label="Logging Service" info="The Fastly service that is streaming logs to Object Storage." />
              {renderServicePicker(s.selectedService, s.setSelectedService, "Select logging service...")}
            </div>
            <div className="space-y-1.5">
              <LabelWithInfo label="CDN Proxy Service" info="The Fastly service used to front the Object Storage bucket." />
              {renderServicePicker(s.selectedCdnService, s.setSelectedCdnService, "Select CDN service...")}
            </div>
          </div>
          <div className="space-y-4 pt-2 border-t">
              <div className="flex items-center justify-between">
                <div className="text-sm text-muted-foreground italic">We will verify that both services have the correct resources and VCL snippets.</div>
                <Button variant="secondary" size="sm" disabled={s.isCheckingConfig || !s.selectedService || !s.selectedCdnService || !config.fos_bucket_name} onClick={s.handleCheckConfig}>
                  {s.isCheckingConfig && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}Verify Configuration
                </Button>
              </div>
              {s.configStatus && (
                <div className="grid grid-cols-2 gap-4">
                  {([["Logging Service", s.configStatus.logging_service], ["CDN Proxy Service", s.configStatus.cdn_service]] as const).map(([title, item]) => (
                    <div key={title} className={cn("p-3 rounded-lg border text-xs space-y-1", item.ok ? "bg-emerald-500/5 border-emerald-500/20" : "bg-destructive/5 border-destructive/20")}>
                      <div className="flex items-center gap-2 font-bold">{item.ok ? <CheckCircle2 className="w-3 h-3 text-emerald-500" /> : <XCircle className="w-3 h-3 text-destructive" />}{title}</div>
                      <p className="text-muted-foreground leading-relaxed">{item.details}</p>
                    </div>
                  ))}
                </div>
              )}
          </div>
          <div className="grid grid-cols-2 gap-6">
            <div className="space-y-1.5">
              <LabelWithInfo label="FOS Bucket Name" info="The name of the existing Fastly Object Storage bucket." />
              <Input value={config.fos_bucket_name} onChange={(e) => setConfig({ ...config, fos_bucket_name: e.target.value.toLowerCase() })} onBlur={(e) => setError("fos_bucket_name", validateBucketName(e.target.value))} aria-invalid={fieldErrors.fos_bucket_name != null} className="h-9 font-mono text-sm" placeholder="e.g. my-service-logs" />
              {fieldErrors.fos_bucket_name && <p role="alert" className="text-[11px] text-destructive">{fieldErrors.fos_bucket_name}</p>}
            </div>
            <div className="space-y-1.5">
              <LabelWithInfo label="FOS Region" info="The region where the bucket is located." />
              <Select value={config.fos_region} onValueChange={(v) => v && setConfig({ ...config, fos_region: v })}>
                <SelectTrigger className="h-9"><SelectValue>{(val) => REGION_LABELS[String(val)] || val}</SelectValue></SelectTrigger>
                <SelectContent>{Object.entries(REGION_LABELS).map(([k, label]) => <SelectItem key={k} value={k}>{label}</SelectItem>)}</SelectContent>
              </Select>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-6">
            <div className="space-y-1.5">
              <LabelWithInfo label="Access Key" info="An access key with read permissions for the bucket." />
              <Input value={config.fos_access_key || ""} onChange={(e) => setConfig({ ...config, fos_access_key: e.target.value })} onBlur={(e) => setError("fos_access_key", validateAccessKey(e.target.value))} aria-invalid={fieldErrors.fos_access_key != null} className="h-9 font-mono text-sm" placeholder="e.g. AKIA..." />
              {fieldErrors.fos_access_key && <p role="alert" className="text-[11px] text-destructive">{fieldErrors.fos_access_key}</p>}
            </div>
            <div className="space-y-1.5">
              <LabelWithInfo label="Secret Key" info="The secret key associated with the access key." />
              <Input type="password" value={config.fos_secret_key || ""} onChange={(e) => setConfig({ ...config, fos_secret_key: e.target.value })} className="h-9 font-mono text-sm" placeholder="e.g. wJalrXUtnFEMI..." />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-6">
            <div className="space-y-1.5">
              <LabelWithInfo label="CDN API URL (Optional)" info="The Fastly CDN URL used to proxy API requests (bypasses CORS)." />
              <Input value={config.cdn_url || ""} onChange={(e) => setConfig({ ...config, cdn_url: e.target.value })} className="h-9 font-mono text-sm" placeholder="e.g. https://fos-xyz.global.ssl.fastly.net" />
            </div>
            <div className="space-y-1.5">
              <LabelWithInfo label="CDN Secret (Optional)" info="The pre-shared secret required by the CDN API proxy." />
              <Input type="password" value={config.cdn_secret || ""} onChange={(e) => setConfig({ ...config, cdn_secret: e.target.value })} className="h-9 font-mono text-sm" placeholder="e.g. s3cr3t..." />
            </div>
          </div>
        </div>
        <div className="space-y-4 pt-4 border-t">
          <div className="flex items-center justify-between">
            {s.fosStatus === "idle" || s.fosStatus === "checking" ? (
              <div className="text-sm text-muted-foreground">Please verify your credentials before continuing.</div>
            ) : s.fosStatus === "success" ? (
              <div className="flex items-center gap-2 text-emerald-500 font-semibold"><CheckCircle2 className="h-5 w-5" /><h4>Ready to Continue</h4></div>
            ) : (
              <div className="flex items-center gap-2 text-destructive font-semibold"><div className="h-5 w-5 rounded-full bg-destructive/10 flex items-center justify-center text-xs">!</div><h4>Connection Failed</h4></div>
            )}
            <Button variant={s.fosStatus === "success" ? "outline" : "secondary"} size="sm" onClick={() => s.handleCheckFos()} disabled={s.fosStatus === "checking" || !config.fos_bucket_name || !config.fos_region || !config.fos_access_key || !config.fos_secret_key}>
              {s.fosStatus === "checking" && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}Verify Access
            </Button>
          </div>
          {s.fosStatus === "error" && <div className="text-sm text-destructive bg-destructive/10 p-3 rounded-md">{s.fosError}</div>}
          {s.fosStatus === "success" && (
            <p className="text-xs text-muted-foreground leading-relaxed animate-in fade-in slide-in-from-top-1">
              We will connect to this service in <strong>Admin</strong> mode. We will set up background ingestion and metadata management.
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

type RumVersionsResponse = components["schemas"]["RumVersionsResponse"];
const storageStepSchema = z.object({
  endpoint_name: z.string().min(1), fos_region: z.string().min(1), fos_bucket_name: z.string().min(1),
  log_period: z.number().int().min(1), edge_only: z.boolean(), sample_rate: z.number().min(1).max(100),
  custom_condition: z.string(), rum_log_period: z.number().int().min(1), cdn_prefix: z.string(),
  cdn_shield: z.string().min(1), enable_cron_sync: z.boolean(), delete_after: z.boolean(),
  commit_interval_mins: z.number().int().min(1), log_retention_days: z.number().int().min(0),
  rum_retention_days: z.number().int().min(0), faro_version: z.string().nullable(),
  rum_custom_condition: z.string(), cache_retention_days: z.number().int().min(0),
  rollup_retention_months: z.number().int().min(0), enable_cron_compact: z.boolean(),
});
type StorageStepFormValues = z.infer<typeof storageStepSchema>;

interface RumVersionPickerProps {
  serviceId: string; value: string | null; onChange: (version: string | null) => void;
  availableVersions?: string[]; currentVersion?: string | null; latestVersion?: string | null;
}

const INFO_TEXT = "The self-hosted Grafana Faro Web SDK build served from your Object Storage bucket. Pin a version to control exactly which RUM client ships to real users; leave it unpinned and the service self-hosts the default vetted version instead.";
const RUM_PERIOD_OPTIONS = [["10", "10 seconds (Ultra-Fast / High-Load)"], ["60", "1 minute (Fastest)"], ["300", "5 minutes (Recommended)"], ["600", "10 minutes"], ["900", "15 minutes"]] as const;
const COMMIT_INTERVAL_OPTIONS = [["1", "Every 1 min — most real-time"], ["2", "Every 2 min"], ["3", "Every 3 min"], ["5", "Every 5 min — recommended"], ["15", "Every 15 min"], ["30", "Every 30 min"], ["60", "Every 60 min — fewest snapshots"]] as const;
const ROLLUP_MONTH_OPTIONS = [["1", "1 month"], ["3", "3 months"], ["6", "6 months"], ["12", "12 months"], ["24", "24 months"], ["0", "Forever"]] as const;

export function RumVersionPicker({ serviceId, value, onChange, availableVersions, currentVersion, latestVersion }: RumVersionPickerProps) {
  const hasPrefetched = availableVersions !== undefined;
  const { data, isLoading, isError, refetch, isFetching } = useQuery<RumVersionsResponse>({
    queryKey: queryKeys.rumVersions(serviceId),
    queryFn: async ({ signal }) => unwrapClient(client.GET("/api/services/{service_id}/rum/versions", { params: { path: { service_id: serviceId } }, signal })),
    enabled: !!serviceId && !hasPrefetched, retry: false, staleTime: Infinity,
  });
  const available = availableVersions ?? data?.available ?? [];
  const latest = latestVersion !== undefined ? latestVersion : (data?.latest ?? null);
  const current = currentVersion !== undefined ? currentVersion : (data?.current ?? null);
  const degraded = !hasPrefetched && (isError || (!isLoading && available.length === 0));
  const seedLatest = React.useEffectEvent(() => { if (latest && value === null) onChange(latest); });
  React.useEffect(() => { seedLatest(); }, [latest]);

  const header = (
    <div className="flex items-center justify-between">
      <LabelWithInfo label="Faro Web SDK Version" info={INFO_TEXT} />
      {!hasPrefetched && isLoading
        ? <span className="text-xs text-muted-foreground animate-pulse font-semibold">Release Notes &rarr;</span>
        : <a href="https://github.com/grafana/faro-web-sdk/releases" target="_blank" rel="noopener noreferrer" className="text-xs text-primary hover:underline flex items-center gap-1 font-semibold">Release Notes &rarr;</a>}
    </div>
  );
  if (!hasPrefetched && isLoading) return <div className="space-y-1.5">{header}<Skeleton className="h-8 w-full" /></div>;
  if (degraded) {
    return (
      <div className="space-y-1.5">
        {header}
        <Alert variant="destructive">
          <TriangleAlert /><AlertTitle>{isError ? "Couldn't reach the npm registry" : "No versions available"}</AlertTitle>
          <AlertDescription className="flex items-center justify-between gap-2">
            <span>RUM will provision using the default self-hosted version — you can pin a specific one later from the service&apos;s RUM settings.</span>
            <Button type="button" variant="outline" size="sm" className="h-7 text-xs shrink-0" onClick={() => refetch()} disabled={isFetching}>
              <RefreshCw className={cn("h-3 w-3 mr-1", isFetching && "animate-spin")} />{isFetching ? "Retrying…" : "Retry"}
            </Button>
          </AlertDescription>
        </Alert>
      </div>
    );
  }
  return (
    <div className="space-y-1.5">
      {header}
      <Select value={value ?? undefined} onValueChange={(v) => onChange(v)}>
        <SelectTrigger className="h-8 text-xs w-full" aria-label="Faro Web SDK Version"><SelectValue placeholder="Select a version" /></SelectTrigger>
        <SelectContent>
          {available.map((v) => {
            const tags = [v === latest && "latest", v === current && "current"].filter(Boolean);
            return <SelectItem key={v} value={v} className="text-xs">{v}{tags.length > 0 ? ` (${tags.join(", ")})` : ""}</SelectItem>;
          })}
        </SelectContent>
      </Select>
    </div>
  );
}

export function StorageStep({ s }: { s: WizardState }) {
  const { config, setConfig, domainStatus, domainMessage, checkDomain } = s;
  const { setValue } = useForm<StorageStepFormValues>({ resolver: zodResolver(storageStepSchema), values: config });
  const update = <K extends keyof StorageStepFormValues>(key: K, val: StorageStepFormValues[K]) => {
    setValue(key, val as PathValue<StorageStepFormValues, K>, { shouldDirty: true, shouldValidate: true });
    setConfig({ ...config, [key]: val });
  };
  const isLogging = config.logging_enabled !== false;
  const syncDisabledClass = !config.enable_cron_sync ? "opacity-30 pointer-events-none" : undefined;

  const fosRegionField = (
    <div className="space-y-1.5">
      <LabelWithInfo label="FOS Region" info="The geographical region where your Fastly Object Storage bucket will be created. We recommend matching this with your primary user base." />
      <Select value={config.fos_region} onValueChange={(v) => v && update("fos_region", v)}>
        <SelectTrigger className="h-9"><SelectValue>{(val) => REGION_LABELS[String(val)] || val}</SelectValue></SelectTrigger>
        <SelectContent>{Object.entries(REGION_LABELS).map(([k, label]) => <SelectItem key={k} value={k}>{label}</SelectItem>)}</SelectContent>
      </Select>
    </div>
  );
  const bucketNameField = (
    <div className="space-y-1.5">
      <LabelWithInfo label="Bucket Name" info="The name of the Fastly Object Storage bucket. Must be unique across all Fastly customers." />
      <Input value={config.fos_bucket_name} onChange={(e) => update("fos_bucket_name", e.target.value.toLowerCase())} className="h-9 font-mono text-sm" />
    </div>
  );
  const renderRumPeriodField = (infoText: string, compact = false) => (
    <div className="space-y-1.5">
      <LabelWithInfo label="RUM Log Period" info={infoText} />
      <Select value={String(config.rum_log_period || 300)} onValueChange={(v) => update("rum_log_period", Number(v) || 300)}>
        <SelectTrigger className={compact ? "h-8 text-xs" : "h-9"}><SelectValue>{(val) => PERIOD_LABELS[String(val)] || val}</SelectValue></SelectTrigger>
        <SelectContent>{RUM_PERIOD_OPTIONS.map(([val, label]) => <SelectItem key={val} value={val} className={compact ? "text-xs" : undefined}>{label}</SelectItem>)}</SelectContent>
      </Select>
    </div>
  );
  const pollFreqShort = isLogging ? (config.log_period >= 120 ? `${Math.floor(config.log_period / 120)}m` : config.log_period >= 60 ? `${Math.floor(config.log_period / 2)}s` : `${Math.max(10, config.log_period)}s`) : "1m";
  const pollFreqLong = isLogging ? (config.log_period >= 120 ? `${Math.floor(config.log_period / 120)} min` : config.log_period >= 60 ? `${Math.floor(config.log_period / 2)}s` : `${Math.max(10, config.log_period)}s`) : "1 min";

  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
        {/* Section: Logging / Telemetry Storage */}
        <div className="space-y-5">
          {isLogging ? (
            <>
              <SectionHeader title="Logging Setup" icon={Zap} />
              <div className="grid grid-cols-2 gap-6">
                <div className="space-y-1.5">
                  <LabelWithInfo label="Endpoint Name" info="The name of the logging endpoint that will be created on your Fastly service. This is just for your reference." />
                  <Input value={config.endpoint_name} onChange={(e) => update("endpoint_name", e.target.value)} className="h-9" />
                </div>
                {fosRegionField}
              </div>
              <div className="grid grid-cols-2 gap-6">
                {bucketNameField}
                <div className="space-y-1.5">
                  <LabelWithInfo label="Log Period" info="How often Fastly will write log files to the bucket. A shorter period means more real-time data but creates more files." />
                  <Select value={String(config.log_period)} onValueChange={(v) => update("log_period", Number(v) || 60)}>
                    <SelectTrigger className="h-9"><SelectValue>{(val) => PERIOD_LABELS[String(val)] || val}</SelectValue></SelectTrigger>
                    <SelectContent>{["1", "5", "10", "20", "30", "60", "120", "300"].map((val) => <SelectItem key={val} value={val}>{PERIOD_LABELS[val]}</SelectItem>)}</SelectContent>
                  </Select>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-6 items-center">
                <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
                  <div className="space-y-0.5">
                    <LabelWithInfo label="Edge Only" info="When enabled, only edge nodes write logs, skipping shield nodes and cache restarts. This prevents duplicate log entries." />
                    <p className="text-[10px] text-muted-foreground">Skip shield/restart logs</p>
                  </div>
                  <Switch aria-label="Edge Only" checked={config.edge_only} onCheckedChange={(v) => update("edge_only", v)} />
                </div>
                <div className="space-y-1.5">
                  <LabelWithInfo label="Sample Rate (%)" info="The percentage of requests to log. Set to 100% to log everything, or lower it for high-traffic services to save storage." />
                  <Input type="number" min={1} max={100} value={config.sample_rate} onChange={(e) => update("sample_rate", Number(e.target.value))} className="h-9" />
                </div>
              </div>
              <div className="space-y-1.5">
                <LabelWithInfo htmlFor="customCondition" label="Optional Log Condition" info="An additional VCL condition to filter logs (e.g., req.url !~ '\.(jpg|png)$'). The expression will be wrapped in parentheses and added to the logging condition logic." />
                <Input id="customCondition" placeholder="e.g. std.tolower(req.url) !~ '\.(jpg|png|css|js)$'" value={config.custom_condition} onChange={(e) => update("custom_condition", e.target.value)} className="h-9 font-mono text-xs" />
              </div>
            </>
          ) : (
            <>
              <SectionHeader title="RUM Telemetry Storage" icon={Database} />
              <div className="grid grid-cols-2 gap-6">{fosRegionField}{bucketNameField}</div>
              <div className="grid grid-cols-2 gap-6 pt-1">{renderRumPeriodField("How often Fastly will flush RUM beacon logs to your bucket. 5 minutes is recommended for balanced latency and storage efficiency.")}</div>
            </>
          )}
        </div>

        {/* Section: CDN Access */}
        <div className="space-y-5">
          <SectionHeader title="CDN Performance Front" icon={Globe} />
          <p className="text-xs text-muted-foreground leading-relaxed">Highly recommended. Provision a secondary Fastly service to front the Object Storage bucket for faster dashboard queries and secure access.</p>
          <div className="grid grid-cols-2 gap-6 pt-1">
            <div className="space-y-1.5">
              <LabelWithInfo label="Domain Prefix" info="The domain name for the secondary CDN service that sits in front of your Object Storage bucket." />
              <div className="space-y-1.5">
                <div className="flex items-center gap-1.5">
                  <Input value={config.cdn_prefix} onChange={(e) => update("cdn_prefix", e.target.value.toLowerCase())} className={cn("h-9 font-mono text-sm", domainStatus === "available" && "border-green-500 focus-visible:ring-green-500", domainStatus === "taken" && "border-red-500 focus-visible:ring-red-500")} />
                  <span className="text-[10px] font-mono text-muted-foreground opacity-70">.global.ssl.fastly.net</span>
                  <Button variant="outline" size="sm" className="h-9 px-3 shrink-0 text-xs" onClick={() => checkDomain(config.cdn_prefix)} disabled={domainStatus === "checking" || !config.cdn_prefix} title="Check Domain Availability">
                    <Search className="h-4 w-4 mr-1.5" />Check Domain
                  </Button>
                </div>
                {domainStatus === "checking" && <p className="text-[10px] animate-pulse text-muted-foreground">Checking availability...</p>}
                {domainStatus === "available" && <p className="text-[10px] text-green-600 font-medium flex items-center gap-1"><CheckCircle2 className="h-3 w-3" /> {domainMessage}</p>}
                {domainStatus === "taken" && <p className="text-[10px] text-red-600 font-medium flex items-center gap-1"><AlertCircle className="h-3 w-3" /> {domainMessage}</p>}
              </div>
            </div>
            <div className="space-y-1.5">
              <LabelWithInfo label="Origin Shield" info="The Fastly POP that will act as a shield between the edge nodes and your bucket, reducing direct bucket reads and improving performance." />
              <Select value={config.cdn_shield} onValueChange={(v) => v && update("cdn_shield", v)}>
                <SelectTrigger className="h-9"><SelectValue>{(val) => SHIELD_LABELS[String(val)] || val}</SelectValue></SelectTrigger>
                <SelectContent>{Object.entries(SHIELD_LABELS).map(([k, label]) => <SelectItem key={k} value={k}>{label}</SelectItem>)}</SelectContent>
              </Select>
            </div>
          </div>
        </div>

        {/* Section: Automation */}
        <div className="space-y-5">
          <SectionHeader title="Automation" icon={Settings} />
          <div className="grid grid-cols-2 gap-4">
            <div className="flex items-center justify-between p-3 border rounded-md bg-muted/5">
              <div className="space-y-0.5">
                <LabelWithInfo label="Background Sync" info={`Automatically polls FOS for new ${isLogging ? "log" : "RUM telemetry"} files (every ${pollFreqLong}) and writes them into the local buffer. The buffer is then committed to the shared DuckLake table at the Cloud Commit Interval below.`} />
                <p className="text-[10px] text-muted-foreground">Polls FOS every {pollFreqShort}</p>
              </div>
              <Switch aria-label="Background Sync" checked={config.enable_cron_sync} onCheckedChange={(v) => update("enable_cron_sync", v)} />
            </div>
            <div className={cn("flex items-center justify-between p-3 border rounded-md bg-muted/5 transition-opacity", syncDisabledClass)}>
              <div className="space-y-0.5">
                <LabelWithInfo label="Auto-Delete Raw Logs" info={`Deletes the raw .gz ${isLogging ? "log" : "RUM beacon"} files from FOS after they are ingested into DuckLake. Recommended — the DuckLake table holds the same data in a more efficient format.`} />
                <p className="text-[10px] text-muted-foreground">{isLogging ? "Remove .gz files after ingest" : "Remove .gz RUM files after ingest"}</p>
              </div>
              <Switch aria-label="Auto-Delete Raw Logs" checked={config.delete_after} onCheckedChange={(v) => update("delete_after", v)} />
            </div>
          </div>

          {/* Cloud commit interval — separate row, full width */}
          <div className={cn("p-4 border rounded-md bg-muted/5 space-y-3 transition-opacity", syncDisabledClass)}>
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <LabelWithInfo label="Cloud Commit Interval" info="How often the local buffer is flushed to the shared DuckLake table. This determines how quickly data becomes visible to other users or tools querying the DuckLake table directly. More frequent commits mean fresher data but create more small files — the daily DuckLake optimization consolidates them." />
                <p className="text-[10px] text-muted-foreground leading-relaxed">Controls data freshness for shared access. Every commit creates one DuckLake snapshot.</p>
              </div>
              <Select value={String(config.commit_interval_mins)} onValueChange={(v) => v && update("commit_interval_mins", Number(v))}>
                <SelectTrigger className="h-8 w-[220px] shrink-0 text-xs"><SelectValue /></SelectTrigger>
                <SelectContent>{COMMIT_INTERVAL_OPTIONS.map(([val, label]) => <SelectItem key={val} value={val} className="text-xs">{label}</SelectItem>)}</SelectContent>
              </Select>
            </div>
            <div className="text-[10px] text-muted-foreground bg-muted/30 rounded px-3 py-2 leading-relaxed">
              {isLogging ? `With a ${config.log_period >= 60 ? `${config.log_period / 60}-minute` : `${config.log_period}-second`} log period` : "With a 1-minute RUM poll period"}{" "}
              and a {config.commit_interval_mins}-minute commit interval, the system will create ~{Math.round(1440 / config.commit_interval_mins)} DuckLake snapshots per day before the daily optimization consolidates them.
            </div>
          </div>

          {/* Section: Data Retention Policies */}
          <div className={cn("p-4 border rounded-md bg-muted/5 space-y-4 transition-opacity", syncDisabledClass)}>
            <div>
              <LabelWithInfo label="Data Retention Policy" info="Control how long data is stored in your DuckLake table before being deleted to manage storage size and billing. General edge logs are high-volume, while RUM beacons are low-volume." />
              <p className="text-[10px] text-muted-foreground leading-relaxed">Configure separate retention windows to optimize storage billing and historical trend depth.</p>
            </div>
            <div className={cn("grid gap-6", isLogging && config.rum_enabled ? "grid-cols-2" : "grid-cols-1")}>
              {isLogging && (
                <div className="space-y-1.5">
                  <LabelWithInfo label="Request Log Retention" info="Prunes high-volume CDN HTTP logs older than this threshold to minimize your Object Storage charges. RUM records are kept safe." />
                  <RetentionSelect value={String(config.log_retention_days)} onValueChange={(v) => update("log_retention_days", Number(v))} triggerClassName="h-8 text-xs" itemClassName="text-xs" />
                </div>
              )}
              {config.rum_enabled && (
                <div className="space-y-1.5">
                  <LabelWithInfo label="RUM Telemetry Retention" info="Keeps client-side performance beacons (Web Vitals) for a longer period of time. Since RUM volume is very small, a larger retention window has negligible storage costs." />
                  <RetentionSelect value={String(config.rum_retention_days)} onValueChange={(v) => update("rum_retention_days", Number(v))} triggerClassName="h-8 text-xs" itemClassName="text-xs" />
                </div>
              )}
            </div>
            {config.rum_enabled && isLogging && <div className="grid grid-cols-2 gap-6 pt-1">{renderRumPeriodField("Fastly edge rotation period for RUM beacons. Ingestion polls at a derived frequency (e.g. 5m flush polls every 150s). You can configure a longer period than request logs to batch beacons and optimize file writes.", true)}</div>}
            {config.rum_enabled && s.selectedService?.id && <div className="pt-1"><RumVersionPicker serviceId={s.selectedService.id} value={config.faro_version} onChange={(v) => update("faro_version", v)} /></div>}
            {config.rum_enabled && (
              <div className="space-y-1.5 pt-2">
                <LabelWithInfo htmlFor="rumCustomCondition" label="Optional RUM Log Condition" info="An additional VCL condition to filter RUM beacons (e.g., req.url.path !~ '^/api/'). The expression will be wrapped in parentheses and added to the RUM beacon condition logic." />
                <Input id="rumCustomCondition" placeholder="e.g. req.url.path !~ '^/api/'" value={config.rum_custom_condition} onChange={(e) => update("rum_custom_condition", e.target.value)} className="h-9 font-mono text-xs" />
              </div>
            )}
          </div>

          {/* Section: Local Cache Retention Policies */}
          <div className={cn("p-4 border rounded-md bg-muted/5 space-y-4 transition-opacity", syncDisabledClass)}>
            <div>
              <LabelWithInfo label="Local Dashboard Cache Retention" info="Control how long data is stored on this server's local NVMe disk for instantaneous dashboard querying. Data older than this is seamlessly queried directly from S3 (slower, but saves local disk space)." />
              <p className="text-[10px] text-muted-foreground leading-relaxed">Configure local hot-cache windows. Rollups are tiny and should be kept long-term; raw logs are huge and can be evicted early. If your local disk gets &gt;85% full, the oldest cache is automatically evicted regardless of these settings to prevent disk exhaustion.</p>
            </div>
            <div className="grid grid-cols-2 gap-6">
              <div className="space-y-1.5">
                <LabelWithInfo label="Local Raw Log Cache" info="How many days of raw logs to keep on the local NVMe disk. Older logs are automatically queried directly from S3 Object Storage." />
                <RetentionSelect value={String(config.cache_retention_days)} onValueChange={(v) => update("cache_retention_days", Number(v))} triggerClassName="h-8 text-xs" itemClassName="text-xs" />
              </div>
              <div className="space-y-1.5">
                <LabelWithInfo label="Local Rollups & Metrics" info="How many MONTHS of pre-aggregated time-series rollups to keep locally. These are very small and allow instant loading of 90-day dashboard charts." />
                <Select value={String(config.rollup_retention_months)} onValueChange={(v) => update("rollup_retention_months", Number(v))}>
                  <SelectTrigger className="h-8 text-xs"><SelectValue /></SelectTrigger>
                  <SelectContent>{ROLLUP_MONTH_OPTIONS.map(([val, label]) => <SelectItem key={val} value={val} className="text-xs">{label}</SelectItem>)}</SelectContent>
                </Select>
              </div>
            </div>
          </div>

          <div className={cn("flex items-center justify-between p-3 border rounded-md bg-muted/5 transition-opacity", syncDisabledClass)}>
            <div className="space-y-0.5">
              <LabelWithInfo label="Daily DuckLake Optimization" info="Every night at 04:00 UTC, rewrites many small DuckLake snapshot files into larger, optimized Parquet files. This is storage-side housekeeping that keeps cloud storage costs down — dashboard query speed is handled separately by always-on local compaction. Strongly recommended when using frequent commit intervals." />
              <p className="text-[10px] text-muted-foreground">Runs at 04:00 UTC — consolidates daily snapshots</p>
            </div>
            <Switch aria-label="Daily DuckLake Optimization" checked={config.enable_cron_compact} onCheckedChange={(v) => update("enable_cron_compact", v)} />
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Folded from wizard-deploy.ts ──────────────────────────────────────────

function buildTerraformPayload(token: string, selectedService: ProvisionService, config: ProvisionConfig) {
  return {
    token, logging_service_id: selectedService.id, service_name: selectedService.name,
    endpoint_name: config.endpoint_name, fos_region: config.fos_region, fos_bucket_name: config.fos_bucket_name,
    fos_prefix: config.fos_prefix, sample_rate: String(config.sample_rate), edge_only: config.edge_only,
    custom_condition: config.custom_condition, log_period: String(config.log_period),
    rum_log_period: String(config.rum_log_period || 300), cdn_service_name: config.cdn_service_name,
    cdn_prefix: config.cdn_prefix, cdn_shield: config.cdn_shield, log_fields: config.log_fields,
  };
}

interface FetchTerraformPreviewArgs {
  token: string; selectedService: ProvisionService | null; config: ProvisionConfig;
  setIsFetchingTerraform: (b: boolean) => void; setTerraformFiles: (f: Record<string, string>) => void; setSelectedTfFile: (f: string) => void;
}

export async function runFetchTerraformPreview(args: FetchTerraformPreviewArgs) {
  const { token, selectedService, config, setIsFetchingTerraform, setTerraformFiles, setSelectedTfFile } = args;
  if (!selectedService) return;
  setIsFetchingTerraform(true);
  try {
    const files = await unwrapClient(client.POST("/api/provision/terraform/preview", { body: buildTerraformPayload(token, selectedService, config) }));
    if (files) {
      setTerraformFiles(files);
      const preferred = files["instructions"] ? "instructions" : files["main.tf"] ? "main.tf" : Object.keys(files)[0];
      if (preferred) setSelectedTfFile(preferred);
    }
  } catch (e) {
    console.error(e);
  } finally {
    setIsFetchingTerraform(false);
  }
}

export async function runExportTerraform(args: { token: string; selectedService: ProvisionService | null; config: ProvisionConfig }) {
  const { token, selectedService, config } = args;
  if (!selectedService) return;
  try {
    const response = await adminFetch("/api/provision/terraform/export", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildTerraformPayload(token, selectedService, config)),
    });
    if (!response.ok) throw new Error("Export failed");
    downloadBlob(await response.blob(), "fastly-log-analysis-terraform.zip");
  } catch (e) {
    console.error("Failed to export Terraform", e);
  }
}

interface ModalCloseDeps {
  status: string; isDone: boolean; onOpenChange: (open: boolean) => void; selectedService: ProvisionService | null;
  setActiveServiceId: (id: string) => void; queryClient: Pick<QueryClient, "invalidateQueries">;
  setStep: (s: Step) => void; setMode: (m: WizardMode) => void; setSearch: (s: string) => void;
  setSelectedService: (s: ProvisionService | null) => void; setIsDeploying: (b: boolean) => void;
  setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void; setSetupPhase: (p: SetupPhase) => void;
  reset: () => void; resetConfig: () => void; setNgwafWorkspaces: (w: { id: string; name: string }[]) => void;
  setNgwafFetching: (b: boolean) => void; setNgwafFetchError: (s: string) => void;
}

export function buildHandleModalClose(deps: ModalCloseDeps) {
  return (isOpen: boolean) => {
    if (deps.status === "streaming") return;
    deps.onOpenChange(isOpen);
    if (!isOpen) {
      if (deps.status === "done" || deps.isDone) {
        if (deps.selectedService?.id) deps.setActiveServiceId(deps.selectedService.id);
        deps.queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() });
        clearDraft();
        window.location.reload();
      } else {
        setTimeout(() => {
          deps.setStep("mode"); deps.setMode(null); deps.setSearch(""); deps.setSelectedService(null);
          deps.setIsDeploying(false); deps.setFosStatus("idle"); deps.setFosError(""); deps.setSetupPhase("form");
          deps.reset(); deps.resetConfig(); deps.setNgwafWorkspaces([]);
          deps.setNgwafFetching(false); deps.setNgwafFetchError("");
        }, 300);
      }
    }
  };
}

interface DeployArgs {
  token: string; selectedService: ProvisionService | null; config: ProvisionConfig;
  setIsDeploying: (b: boolean) => void; start: (urlPath: string, body?: Record<string, unknown>) => void;
}

export function runDeploy({ token, selectedService, config, setIsDeploying, start }: DeployArgs) {
  if (!selectedService) return;
  setIsDeploying(true);
  const body: Record<string, unknown> = {
    token, service_id: selectedService.id, service_name: selectedService.name,
    endpoint_name: config.endpoint_name, fos_region: config.fos_region, fos_bucket_name: config.fos_bucket_name,
    fos_prefix: config.fos_prefix, sample_rate: String(config.sample_rate), edge_only: config.edge_only,
    custom_condition: config.custom_condition, log_period: String(config.log_period),
    rum_log_period: String(config.rum_log_period || 300), cdn_service_name: config.cdn_service_name,
    cdn_shield: config.cdn_shield, enable_cron_sync: config.enable_cron_sync, delete_after: config.delete_after,
    commit_interval_mins: Number(config.commit_interval_mins), enable_cron_compact: config.enable_cron_compact,
    log_retention_days: Number(config.log_retention_days), rum_retention_days: Number(config.rum_retention_days),
    log_fields: config.log_fields ? JSON.stringify(config.log_fields) : null,
    cmcd_enabled: config.cmcd_enabled, cmcd_mode: config.cmcd_mode, cmcd_version: config.cmcd_version,
    logging_enabled: config.logging_enabled, rum_enabled: config.rum_enabled,
    rum_custom_condition: config.rum_custom_condition, faro_version: config.faro_version || null,
  };
  if (config.cdn_prefix) body.cdn_url = `https://${config.cdn_prefix}.global.ssl.fastly.net`;
  start("/api/provision/execute", body);
}

function registerServiceAndInvalidate<T extends { id: string; name: string }>(
  id: string, name: string,
  services: T[], setServices: (s: T[]) => void,
  setActiveServiceId: (id: string) => void,
  queryClient: Pick<QueryClient, "invalidateQueries">,
) {
  if (id) {
    setActiveServiceId(id);
    if (!services.some((s) => s.id === id)) setServices([...services, { id, name } as T]);
  }
  queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() });
}

interface AdminIngestArgs {
  token: string; selectedService: ProvisionService | null; selectedCdnService: ProvisionService | null;
  config: ProvisionConfig; services: Service[]; setIsDeploying: (b: boolean) => void;
  setActiveServiceId: (id: string) => void;
  setServices: (services: Service[]) => void; queryClient: Pick<QueryClient, "invalidateQueries">;
  setSetupPhase: (p: SetupPhase) => void; setStep: (s: Step) => void;
}

export async function runAdminIngest(args: AdminIngestArgs) {
  const {
    token, selectedService, selectedCdnService, config, services, setIsDeploying,
    setActiveServiceId, setServices, queryClient, setSetupPhase, setStep,
  } = args;
  if (!selectedService) return;
  setIsDeploying(true);
  try {
    const data = await unwrapClient(
      client.POST("/api/provision/ingest", {
        body: {
          token, service_id: selectedService.id, service_name: selectedService.name,
          endpoint_name: config.endpoint_name, fos_region: config.fos_region, fos_bucket_name: config.fos_bucket_name,
          fos_prefix: config.fos_prefix, sample_rate: String(config.sample_rate), edge_only: config.edge_only,
          custom_condition: config.custom_condition, log_period: String(config.log_period),
          cdn_service_id: selectedCdnService?.id || config.cdn_service_name,
          cdn_service_name: selectedCdnService?.name || config.cdn_service_name,
          cdn_url: config.cdn_url || (config.cdn_prefix ? `https://${config.cdn_prefix}.global.ssl.fastly.net` : ""),
          cdn_shield: config.cdn_shield, enable_cron_sync: config.enable_cron_sync, delete_after: config.delete_after,
          commit_interval_mins: config.commit_interval_mins, enable_cron_compact: config.enable_cron_compact,
          log_retention_days: config.log_retention_days, rum_retention_days: config.rum_retention_days,
          log_fields: config.log_fields, rum_custom_condition: config.rum_custom_condition,
          fos_access_key: config.fos_access_key, fos_secret_key: config.fos_secret_key,
        },
      }),
    );
    if (data?.ok) {
      registerServiceAndInvalidate(
        selectedService.id, selectedService.name,
        services, setServices, setActiveServiceId, queryClient,
      );
      setSetupPhase("done");
      setStep("connect");
    }
  } catch (e) {
    console.error("Ingest failed", e);
  } finally {
    setIsDeploying(false);
  }
}

// ── Effects & Helpers ─────────────────────────────────────────────────────

interface WizardEffectsArgs {
  open: boolean; step: Step; config: ProvisionConfig; setConfig: React.Dispatch<React.SetStateAction<ProvisionConfig>>;
  fosStatus: FosStatus; setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void;
  setStep: (s: Step) => void; setMode: (m: WizardMode) => void; setSearch: (s: string) => void;
  setSelectedService: (s: ProvisionService | null) => void; setIsDeploying: (b: boolean) => void;
  reset: () => void; resetConfig: () => void; selectedService: ProvisionService | null; token: string;
  setNgwafWorkspaces: (w: { id: string; name: string }[]) => void;
  setNgwafFetchError: (s: string) => void; setNgwafDebugRaw: (s: string) => void;
  setNgwafFetching: (b: boolean) => void; isDone: boolean;
  checkDomain: (prefix: string) => void; pendingDraft?: WizardDraft | null;
}

export function useWizardEffects(args: WizardEffectsArgs) {
  const {
    open, step, config, setConfig, fosStatus, setFosStatus, setFosError, setStep, setMode, setSearch,
    setSelectedService, setIsDeploying, reset, resetConfig,
    selectedService, token, setNgwafWorkspaces, setNgwafFetchError, setNgwafDebugRaw, setNgwafFetching,
    isDone, checkDomain, pendingDraft,
  } = args;

  useEffect(() => {
    const shield = SHIELD_MAP[config.fos_region];
    if (shield && shield !== config.cdn_shield) setConfig((prev) => ({ ...prev, cdn_shield: shield }));
  }, [config.fos_region]);

  useEffect(() => {
    if (open && !pendingDraft) {
      setStep("mode"); setMode(null); setSearch(""); setSelectedService(null);
      setIsDeploying(false); setFosStatus("idle"); setFosError(""); reset(); resetConfig();
    }
  }, [open, reset, pendingDraft]);

  useEffect(() => {
    if (step === "storage" && config.cdn_prefix) {
      const timer = setTimeout(() => checkDomain(config.cdn_prefix), 500);
      return () => clearTimeout(timer);
    }
  }, [config.cdn_prefix, step]);

  useEffect(() => {
    if (fosStatus !== "idle" && fosStatus !== "checking") { setFosStatus("idle"); setFosError(""); }
  }, [config.fos_bucket_name, config.fos_region, config.fos_access_key, config.fos_secret_key]);

  useEffect(() => {
    if (step !== "ngwaf" || !selectedService?.id) return;
    setNgwafWorkspaces([]); setNgwafFetchError(""); setNgwafDebugRaw(""); setNgwafFetching(true);
    client
      .GET("/api/provision/ngwaf-workspaces", { params: { query: { service_id: selectedService.id, token: token || undefined } } })
      .then((r) => {
        if (r.error) {
          setNgwafFetchError(extractApiError(r.error) || "Could not load workspaces");
        } else {
          const data = r.data;
          setNgwafWorkspaces((data?.workspaces || []).flatMap((w) => (w.id && w.name ? [{ id: w.id, name: w.name }] : [])));
          if (data?._debug_raw) setNgwafDebugRaw(data._debug_raw);
          if (data?.error_hint) setNgwafFetchError(data.error_hint);
        }
      })
      .catch((e: unknown) => setNgwafFetchError(extractApiError(e) || "Could not load workspaces"))
      .finally(() => setNgwafFetching(false));
  }, [step, selectedService?.id, token]);

  const persistNgwafWorkspace = useEffectEvent(() => {
    if (isDone && step === "execute" && config.ngwaf_workspace_id && selectedService?.id) {
      client
        .PATCH("/api/provision/services/{service_id}/ngwaf-workspace", {
          params: { path: { service_id: selectedService.id } },
          headers: { Authorization: `Bearer ${token}` },
          body: { ngwaf_workspace_id: config.ngwaf_workspace_id },
        })
        .then((r) => { if (r?.error) console.error("[wizard] failed to persist ngwaf_workspace_id:", r.error); })
        .catch((e: unknown) => console.error("[wizard] failed to persist ngwaf_workspace_id:", e));
    }
  });
  useEffect(() => { persistNgwafWorkspace(); }, [isDone]);
}

interface ValidateMutationDeps {
  token: string; setTokenInfo: (info: { id: string; name: string; type: "user" | "automation" }) => void;
  setConfig: (updater: (prev: ProvisionConfig) => ProvisionConfig) => void; setStep: (s: Step) => void; mode: string | null;
}

export const validateMutationFn = (token: string) => async (serviceId: string) =>
  await unwrapClient(client.POST("/api/provision/validate", { body: { token, service_id: serviceId } }));

export function buildValidateOnSuccess(deps: ValidateMutationDeps) {
  return (data: ValidateResponse | undefined) => {
    const tokenInfo = data?.token_info;
    if (tokenInfo?.id && tokenInfo.name && tokenInfo.type) {
      deps.setTokenInfo({ id: tokenInfo.id, name: tokenInfo.name, type: tokenInfo.type });
    }
    const defaults = data?.defaults;
    if (defaults) {
      deps.setConfig((prev) => ({
        ...prev,
        endpoint_name: defaults.endpoint_name || "Fastly Object Storage Logs",
        fos_region: defaults.fos_region || "us-east-1",
        fos_bucket_name: defaults.fos_bucket_name?.toLowerCase() || "",
        fos_prefix: defaults.fos_prefix || "",
        cdn_service_name: defaults.cdn_service_name || `${data.service_name} (CDN)`,
        cdn_prefix: (
          defaults.cdn_prefix || (defaults.fos_bucket_name ? `fos-${defaults.fos_bucket_name.split("-").slice(0, 2).join("-")}` : "")
        ).toLowerCase(),
      }));
    }
    deps.setStep(deps.mode === "ingest" ? "connect" : "features");
  };
}

interface CheckConfigArgs {
  token: string; selectedService: ProvisionService | null; selectedCdnService: ProvisionService | null;
  config: ProvisionConfig; setIsCheckingConfig: (b: boolean) => void;
  setConfigStatus: (s: components["schemas"]["ProvisionCheckConfigResponse"] | null) => void;
}

export async function runCheckConfig(args: CheckConfigArgs) {
  const { token, selectedService, selectedCdnService, config, setIsCheckingConfig, setConfigStatus } = args;
  if (!selectedService || !selectedCdnService || !config.fos_bucket_name) return;
  setIsCheckingConfig(true);
  try {
    const data = await unwrapClient(
      client.GET("/api/provision/check-config", {
        params: { query: { token, service_id: selectedService.id, cdn_service_id: selectedCdnService.id, bucket: config.fos_bucket_name } },
      }),
    );
    if (data) setConfigStatus(data);
  } catch (e) {
    console.error("Failed to check config", e);
  } finally {
    setIsCheckingConfig(false);
  }
}

interface CheckFosArgs {
  vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string };
  config: ProvisionConfig; setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void;
}

export async function runCheckFos({ vals, config, setFosStatus, setFosError }: CheckFosArgs) {
  const bucket = vals?.bucket ?? config.fos_bucket_name;
  const region = vals?.region ?? config.fos_region;
  const access_key = vals?.access_key ?? config.fos_access_key;
  const secret_key = vals?.secret_key ?? config.fos_secret_key;
  if (!bucket || !region || !access_key || !secret_key) return;
  setFosStatus("checking"); setFosError("");
  try {
    const data = await unwrapClient(client.POST("/api/provision/check-fos", { body: { bucket, region, access_key, secret_key } }));
    if (data?.ok) setFosStatus("success");
    else { setFosStatus("error"); setFosError(data?.error || "Failed to connect."); }
  } catch (err) {
    setFosStatus("error"); setFosError(err instanceof Error && err.message ? err.message : "An error occurred.");
  }
}

export async function runCheckDomain({
  prefix, setDomainStatus, setDomainMessage,
}: {
  prefix: string; setDomainStatus: (s: "idle" | "checking" | "available" | "taken" | "error") => void; setDomainMessage: (m: string) => void;
}) {
  if (!prefix || prefix.length < 3) return;
  setDomainStatus("checking");
  try {
    const data = await unwrapClient(client.GET("/api/provision/check-domain", { params: { query: { prefix } } }));
    if (data?.available) { setDomainStatus("available"); setDomainMessage("Domain available!"); }
    else { setDomainStatus("taken"); setDomainMessage("This domain prefix is already in use."); }
  } catch {
    setDomainStatus("error");
  }
}

export const WIZARD_DRAFT_KEY = "provision-wizard-draft-v2";
const LEGACY_WIZARD_DRAFT_KEYS = ["provision-wizard-draft-v1"];
const SECRET_KEYS = ["fos_access_key", "fos_secret_key", "cdn_secret"] as const;

export function stripSecretsFromConfig(config: ProvisionConfig): PersistedConfig {
  const out: Partial<ProvisionConfig> = { ...config };
  for (const key of SECRET_KEYS) delete out[key];
  return out as PersistedConfig;
}

export function mergePersistedConfig(persisted: PersistedConfig): ProvisionConfig {
  return { ...INITIAL_CONFIG, ...persisted, fos_access_key: "", fos_secret_key: "", cdn_secret: "" };
}

function safeLocalStorage(): Storage | null {
  try { return typeof window === "undefined" ? null : window.localStorage; } catch { return null; }
}

const WizardDraftHeaderSchema = z.object({
  version: z.number(), mode: z.enum(["provision", "ingest"]), step: z.string(),
});

export function loadDraft(): WizardDraft | null {
  const ls = safeLocalStorage();
  if (!ls) return null;
  for (const legacyKey of LEGACY_WIZARD_DRAFT_KEYS) { try { ls.removeItem(legacyKey); } catch { /* ignore */ } }
  try {
    const raw = ls.getItem(WIZARD_DRAFT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as WizardDraft;
    if (!parsed || typeof parsed !== "object") return null;
    if (parsed.version !== WIZARD_DRAFT_VERSION) {
      try { ls.removeItem(WIZARD_DRAFT_KEY); } catch { /* ignore */ }
      return null;
    }
    const check = WizardDraftHeaderSchema.safeParse(parsed);
    return !check.success || parsed.step === "mode" ? null : parsed;
  } catch {
    return null;
  }
}

export function saveDraft(draft: WizardDraft): void {
  const ls = safeLocalStorage();
  if (!ls) return;
  try {
    ls.setItem(WIZARD_DRAFT_KEY, JSON.stringify({ ...draft, config: stripSecretsFromConfig({ ...INITIAL_CONFIG, ...(draft.config as ProvisionConfig) }) }));
  } catch { /* private-mode browsers throw; intentional no-op */ }
}

export function clearDraft(): void {
  const ls = safeLocalStorage();
  if (ls) try { ls.removeItem(WIZARD_DRAFT_KEY); } catch { /* ignore */ }
}

export function subscribeDraft(cb: (next: WizardDraft | null) => void): () => void {
  if (typeof window === "undefined") return () => undefined;
  const handler = (e: StorageEvent) => {
    if (e.key !== WIZARD_DRAFT_KEY) return;
    if (e.newValue === null) { cb(null); return; }
    try {
      const parsed = JSON.parse(e.newValue) as WizardDraft;
      cb(parsed && parsed.version === WIZARD_DRAFT_VERSION ? parsed : null);
    } catch { cb(null); }
  };
  window.addEventListener("storage", handler);
  return () => window.removeEventListener("storage", handler);
}

export function makeDraftId(): string {
  return typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `draft-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export function useWizardState(open: boolean, onOpenChange: (open: boolean) => void): WizardState {
  const setActiveServiceId = useServiceStore((s) => s.setActiveServiceId);
  const setServices = useServiceStore((s) => s.setServices);
  const services = useServiceStore((s) => s.services);
  const { timezone } = useTimezoneStore();
  const queryClient = useQueryClient();
  const [step, setStep] = useState<Step>("mode");
  const [mode, setMode] = useState<WizardMode>(null);
  const [token, setToken] = useState("");
  const [submittedToken, setSubmittedToken] = useState("");
  const [prevOpenForToken, setPrevOpenForToken] = useState(open);
  if (open !== prevOpenForToken) { setPrevOpenForToken(open); if (!open) setSubmittedToken(""); }

  const [tokenInfo, setTokenInfo] = useState<TokenInfo | null>(null);
  const [search, setSearch] = useState("");
  const [selectedService, setSelectedService] = useState<ProvisionService | null>(null);
  const [isDeploying, setIsDeploying] = useState(false);
  const [fosStatus, setFosStatus] = useState<FosStatus>("idle");
  const [fosError, setFosError] = useState("");
  const [terraformFiles, setTerraformFiles] = useState<Record<string, string>>({});
  const [selectedTfFile, setSelectedTfFile] = useState<string>("logging_service.tf");
  const [isFetchingTerraform, setIsFetchingTerraform] = useState(false);
  const [selectedCdnService, setSelectedCdnService] = useState<ProvisionService | null>(null);
  const [configStatus, setConfigStatus] = useState<Schema<"ProvisionCheckConfigResponse"> | null>(null);
  const [isCheckingConfig, setIsCheckingConfig] = useState(false);
  const [config, setConfig] = useState<ProvisionConfig>(INITIAL_CONFIG);
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<{ id: string; name: string }[]>([]);
  const [ngwafFetching, setNgwafFetching] = useState(false);
  const [ngwafFetchError, setNgwafFetchError] = useState("");
  const [ngwafDebugRaw, setNgwafDebugRaw] = useState("");
  const [setupPhase, setSetupPhase] = useState<SetupPhase>("form");

  const [pendingDraft, setPendingDraft] = useState<WizardDraft | null>(() => (open ? loadDraft() : null));
  const draftIdRef = useRef<string | null>(null);
  const draftCreatedAtRef = useRef<string | null>(null);
  const { lines, status, isDone, error: sseError, start, stop, reset } = useSSE();

  const handleModalClose = buildHandleModalClose({
    status, isDone, onOpenChange, selectedService, setActiveServiceId, queryClient,
    setStep, setMode, setSearch, setSelectedService, setIsDeploying, setFosStatus,
    setFosError, setSetupPhase, reset,
    resetConfig: () => setConfig({ ...INITIAL_CONFIG }), setNgwafWorkspaces, setNgwafFetching, setNgwafFetchError,
  });

  const [domainStatus, setDomainStatus] = useState<DomainStatus>("idle");
  const [domainMessage, setDomainMessage] = useState("");

  const { data: servicesData, error: servicesError, isLoading: isLoadingServices } = useQuery({
    queryKey: ["provision-services", submittedToken],
    queryFn: async () => {
      if (!submittedToken) return [];
      const { data, error } = await client.GET("/api/provision/services", { params: { query: { token: submittedToken } } });
      if (error) {
        const err = new Error(extractApiError(error)) as Error & { code?: string };
        err.code = (error as { detail?: { error?: string } })?.detail?.error;
        throw err;
      }
      return data ?? [];
    },
    enabled: !!submittedToken,
    retry: false,
  });

  const [prevServicesData, setPrevServicesData] = useState(servicesData);
  if (servicesData !== prevServicesData) {
    setPrevServicesData(servicesData);
    if (servicesData && Array.isArray(servicesData) && step === "token") setStep("service");
  }

  const { data: catalog, isLoading: isLoadingCatalog } = useQuery({
    queryKey: ["services", "catalog"],
    queryFn: async () => unwrapClient(client.GET("/api/log-fields/catalog")),
    enabled: step === "fields",
  });

  const updateLogFields = (fn: (lf: ProvisionConfig["log_fields"]) => ProvisionConfig["log_fields"]) =>
    setConfig((prev) => ({ ...prev, log_fields: fn(prev.log_fields) }));
  const toggleGroup = (groupId: string, checked: boolean) => updateLogFields((lf) => toggleLogFieldGroup(lf, catalog, groupId, checked));
  const toggleField = (fieldId: string, checked: boolean, defaultEnabledByGroup: boolean) =>
    updateLogFields((lf) => toggleLogFieldOverride(lf, fieldId, checked, defaultEnabledByGroup));
  const updateFieldLimit = (fieldId: string, limit?: number) => updateLogFields((lf) => updateLogFieldLimit(lf, fieldId, limit));
  const isPresetActive = (groups: string[]) => isLogFieldPresetActive(config.log_fields, groups);
  const togglePreset = (presetGroups: string[]) => updateLogFields((lf) => toggleLogFieldPreset(lf, catalog, presetGroups, isPresetActive));

  const estimatedBytes = React.useMemo(() => estimateLogFieldBytesPerLine(config.log_fields, catalog), [catalog, config.log_fields]);
  const handleTokenSubmit = async () => { if (token) setSubmittedToken(token); };

  const validateMutation = useMutation({
    mutationFn: validateMutationFn(token),
    onSuccess: buildValidateOnSuccess({ token, setTokenInfo, setConfig, setStep, mode }),
  });

  const handleServiceSelect = (service: ProvisionService) => {
    if (service.provisioned) return;
    setSelectedService(service);
    validateMutation.mutate(service.id);
  };

  const handleCheckConfig = () => runCheckConfig({ token, selectedService, selectedCdnService, config, setIsCheckingConfig, setConfigStatus });
  const handleCheckFos = (vals?: { bucket?: string; region?: string; access_key?: string; secret_key?: string }) =>
    runCheckFos({ vals, config, setFosStatus, setFosError });
  const checkDomain = (prefix: string) => runCheckDomain({ prefix, setDomainStatus, setDomainMessage });

  const resumeDraft = () => {
    if (!pendingDraft) return;
    setMode(pendingDraft.mode);
    setConfig(mergePersistedConfig(pendingDraft.config));
    const toSvc = (id: string | null, name: string | null) => (id ? ({ id, name: name || id } as ProvisionService) : null);
    setSelectedService(toSvc(pendingDraft.selectedServiceId, pendingDraft.selectedServiceName));
    setSelectedCdnService(toSvc(pendingDraft.selectedCdnServiceId, pendingDraft.selectedCdnServiceName));
    let next: Step = pendingDraft.currentStep === "execute" ? "fields" : pendingDraft.currentStep;
    if ((["service", "connect", "storage", "ngwaf", "fields", "execute"] as Step[]).includes(next)) next = "token";
    setStep(next);
    draftIdRef.current = pendingDraft.draftId;
    draftCreatedAtRef.current = pendingDraft.createdAt;
    setPendingDraft(null);
  };

  const discardDraft = () => { clearDraft(); draftIdRef.current = null; draftCreatedAtRef.current = null; setPendingDraft(null); };

  const STEPS = getStepsForMode(mode, config);

  useWizardEffects({
    open, step, config, setConfig, fosStatus, setFosStatus, setFosError, setStep, setMode, setSearch,
    setSelectedService, setIsDeploying, reset, resetConfig: () => setConfig({ ...INITIAL_CONFIG }),
    selectedService, token, setNgwafWorkspaces,
    setNgwafFetchError, setNgwafDebugRaw, setNgwafFetching, isDone, checkDomain, pendingDraft,
  });

  const handleDeploy = () => runDeploy({ token, selectedService, config, setIsDeploying, start });
  const fetchTerraformPreview = () =>
    runFetchTerraformPreview({ token, selectedService, config, setIsFetchingTerraform, setTerraformFiles, setSelectedTfFile });
  const handleExportTerraform = () => runExportTerraform({ token, selectedService, config });
  const handleAdminIngest = () =>
    runAdminIngest({ token, selectedService, selectedCdnService, config, services, setIsDeploying, setActiveServiceId, setServices, queryClient, setSetupPhase, setStep });

  const [draftOpen, setDraftOpen] = useState(false);
  if (open !== draftOpen) { setDraftOpen(open); setPendingDraft(open ? loadDraft() : null); }

  useEffect(() => {
    if (!open || !mode || step === "mode" || pendingDraft) return;
    const timer = setTimeout(() => {
      if (!draftIdRef.current) draftIdRef.current = makeDraftId();
      if (!draftCreatedAtRef.current) draftCreatedAtRef.current = new Date().toISOString();
      saveDraft({
        version: WIZARD_DRAFT_VERSION, draftId: draftIdRef.current,
        mode: mode as Exclude<WizardMode, null>, step, currentStep: step,
        selectedServiceId: selectedService?.id ?? null, selectedServiceName: selectedService?.name ?? null,
        selectedCdnServiceId: selectedCdnService?.id ?? null, selectedCdnServiceName: selectedCdnService?.name ?? null,
        tokenInfo, config: stripSecretsFromConfig(config),
        updatedAt: new Date().toISOString(), createdAt: draftCreatedAtRef.current,
      });
    }, 500);
    return () => clearTimeout(timer);
  }, [
    open, pendingDraft, step, mode, selectedService?.id, selectedService?.name,
    selectedCdnService?.id, selectedCdnService?.name, tokenInfo, config,
  ]);

  useEffect(() => {
    if (!open) return;
    return subscribeDraft((next) => {
      if (next && draftIdRef.current && next.draftId !== draftIdRef.current) {
        showToast("Wizard updated in another tab. Close and reopen to load latest.", "warn");
      }
    });
  }, [open]);

  useEffect(() => {
    if (status === "done" || isDone) {
      clearDraft(); draftIdRef.current = null; draftCreatedAtRef.current = null;
      queryClient.invalidateQueries({ queryKey: queryKeys.services() });
    }
  }, [status, isDone, queryClient]);

  const filteredServices = Array.isArray(servicesData)
    ? servicesData.filter((s) => s.name.toLowerCase().includes(search.toLowerCase()) || s.id.toLowerCase().includes(search.toLowerCase()))
    : [];

  return {
    setActiveServiceId, setServices, services, timezone, queryClient, step, setStep, mode, setMode,
    token, setToken, tokenInfo, search, setSearch, selectedService, setSelectedService,
    selectedCdnService, setSelectedCdnService, isDeploying, fosStatus, fosError, terraformFiles,
    selectedTfFile, setSelectedTfFile, isFetchingTerraform, configStatus, isCheckingConfig,
    handleCheckConfig, ngwafWorkspaces, ngwafFetching, ngwafFetchError, ngwafDebugRaw,
    lines, status, isDone, sseError,
    stop, handleModalClose, onOpenChange, config, setConfig, catalog, isLoadingCatalog,
    toggleGroup, toggleField, updateFieldLimit, togglePreset, isPresetActive, estimatedBytes,
    servicesData, servicesError, isLoadingServices, filteredServices, handleTokenSubmit,
    validateMutation, handleServiceSelect, handleCheckFos, checkDomain, domainStatus,
    domainMessage, setupPhase, handleDeploy, fetchTerraformPreview, handleExportTerraform,
    handleAdminIngest, STEPS, pendingDraft, resumeDraft, discardDraft,
  };
}
