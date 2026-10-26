"use client";

import React, { useEffect, useEffectEvent, useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { adminFetch, client, extractApiError, unwrapClient, queryKeys, estimateLogFieldBytesPerLine, isLogFieldPresetActive, toggleLogFieldGroup, toggleLogFieldOverride, toggleLogFieldPreset, updateLogFieldLimit } from "@/lib/api";
import { useServiceStore, useTimezoneStore, type Service } from "@/stores/filterStore";
import { useSSE } from "@/hooks/useSSE";
import { downloadBlob, SHIELD_MAP, showToast } from "@/lib/utils";
import type { Schema } from "@/types/api";
import type { components } from "@/types/api.generated";
import {
  INITIAL_CONFIG, WIZARD_DRAFT_VERSION, ANALYST_PATH_A_UNSUPPORTED_REASON, getStepsForMode,
  type DomainStatus, type FosStatus, type JoinPhase, type LakeInfo, type PersistedConfig,
  type ProvisionConfig, type ProvisionService, type Step, type TokenInfo, type WizardDraft,
  type WizardMode, type WizardState,
} from "./ProvisionWizard";

export type { WizardState } from "./ProvisionWizard";

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
  setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void; setLakeInfo: (l: LakeInfo | null) => void;
  setIsAnalyzing: (b: boolean) => void; setImportMode: (m: "all" | "range") => void; setSyncEnabled: (b: boolean) => void;
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
          deps.setIsDeploying(false); deps.setFosStatus("idle"); deps.setFosError(""); deps.setLakeInfo(null);
          deps.setIsAnalyzing(false); deps.setImportMode("all"); deps.setSyncEnabled(true);
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

interface JoinArgs {
  config: ProvisionConfig; analystPathASupported: boolean; syncIntervalMins: string; syncEnabled: boolean;
  icebergMetadataLocation: string; importMode: "all" | "range"; importRange: { start: string; end: string };
  setIsDeploying: (b: boolean) => void; setJoinPhase: (p: JoinPhase) => void; setStep: (s: Step) => void;
  reset: () => void; start: (urlPath: string, body?: Record<string, unknown>) => void;
}

export function runJoin(args: JoinArgs) {
  const {
    config, analystPathASupported, syncIntervalMins, syncEnabled, icebergMetadataLocation,
    importMode, importRange, setIsDeploying, setJoinPhase, setStep, reset, start,
  } = args;
  if (
    !analystPathASupported || !config.endpoint_name || !config.cdn_service_name ||
    !config.fos_bucket_name || !config.fos_region || !config.fos_access_key || !config.fos_secret_key
  ) return;
  setIsDeploying(true); setJoinPhase("connecting"); setStep("join"); reset();
  const params: Record<string, string> = {
    service_id: config.cdn_service_name, service_name: config.endpoint_name,
    fos_bucket_name: config.fos_bucket_name, fos_region: config.fos_region,
    fos_endpoint: config.fos_endpoint || "", fos_access_key: config.fos_access_key,
    fos_secret_key: config.fos_secret_key, cdn_url: config.cdn_url || "",
    cdn_service_id: config.cdn_service_name || "", cdn_secret: config.cdn_secret || "",
    sync_interval_mins: syncIntervalMins, sync_enabled: String(syncEnabled),
    iceberg_metadata_location: icebergMetadataLocation || "",
  };
  if (importMode === "range") {
    if (importRange.start) params.start_time = importRange.start;
    if (importRange.end) params.end_time = importRange.end;
  }
  start(`/api/provision/join?${new URLSearchParams(params).toString()}`);
}

function registerServiceAndInvalidate<T extends { id: string; name: string; accessLevel?: string }>(
  id: string, name: string, accessLevel: "read_write" | "read_only",
  services: T[], setServices: (s: T[]) => void,
  setActiveServiceId: (id: string) => void, setJoinedServiceId: (id: string) => void,
  queryClient: Pick<QueryClient, "invalidateQueries">,
) {
  setJoinedServiceId(id);
  if (id) {
    setActiveServiceId(id);
    if (!services.some((s) => s.id === id)) setServices([...services, { id, name, accessLevel } as T]);
  }
  queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() });
}

interface AdminIngestArgs {
  token: string; selectedService: ProvisionService | null; selectedCdnService: ProvisionService | null;
  config: ProvisionConfig; services: Service[]; setIsDeploying: (b: boolean) => void;
  setJoinedServiceId: (id: string) => void; setActiveServiceId: (id: string) => void;
  setServices: (services: Service[]) => void; queryClient: Pick<QueryClient, "invalidateQueries">;
  setJoinPhase: (p: JoinPhase) => void; setStep: (s: Step) => void;
}

export async function runAdminIngest(args: AdminIngestArgs) {
  const {
    token, selectedService, selectedCdnService, config, services, setIsDeploying,
    setJoinedServiceId, setActiveServiceId, setServices, queryClient, setJoinPhase, setStep,
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
        selectedService.id, selectedService.name, "read_write",
        services, setServices, setActiveServiceId, setJoinedServiceId, queryClient,
      );
      setJoinPhase("done");
      setStep("join");
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
  reset: () => void; resetConfig: () => void; joinPhase: JoinPhase; isAnalyzing: boolean;
  handleAnalyzeLake: () => void; selectedService: ProvisionService | null; token: string;
  setNgwafWorkspaces: (w: { id: string; name: string }[]) => void;
  setNgwafFetchError: (s: string) => void; setNgwafDebugRaw: (s: string) => void;
  setNgwafFetching: (b: boolean) => void; isDone: boolean;
  checkDomain: (prefix: string) => void; pendingDraft?: WizardDraft | null;
}

export function useWizardEffects(args: WizardEffectsArgs) {
  const {
    open, step, config, setConfig, fosStatus, setFosStatus, setFosError, setStep, setMode, setSearch,
    setSelectedService, setIsDeploying, reset, resetConfig, joinPhase, isAnalyzing, handleAnalyzeLake,
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

  const autoAnalyzeLake = useEffectEvent(() => {
    if (fosStatus === "success" && step === "join" && joinPhase === "form" && config.endpoint_name && config.cdn_service_name && !isAnalyzing) {
      handleAnalyzeLake();
    }
  });
  useEffect(() => { autoAnalyzeLake(); }, [fosStatus]);

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

interface JoinCompletionEffectArgs {
  joinPhase: JoinPhase; status: string; config: ProvisionConfig;
  setIsDeploying: (b: boolean) => void; setJoinedServiceId: (id: string) => void; setActiveServiceId: (id: string) => void;
  services: { id: string; name: string; accessLevel?: string }[];
  setServices: (s: { id: string; name: string; accessLevel?: string }[]) => void;
  queryClient: Pick<QueryClient, "invalidateQueries">; setJoinPhase: (p: JoinPhase) => void; reset: () => void;
}

export function useJoinCompletionEffect(args: JoinCompletionEffectArgs) {
  const {
    joinPhase, status, config, setIsDeploying, setJoinedServiceId,
    setActiveServiceId, services, setServices, queryClient, setJoinPhase, reset,
  } = args;
  useEffect(() => {
    if (joinPhase !== "connecting") return;
    if (status === "done") {
      setIsDeploying(false);
      registerServiceAndInvalidate(
        config.cdn_service_name, config.cdn_service_name, "read_only",
        services, setServices, setActiveServiceId, setJoinedServiceId, queryClient,
      );
      setJoinPhase("done");
    } else if (status === "error") {
      setIsDeploying(false); setJoinPhase("form"); reset();
    }
  }, [joinPhase, status, config.cdn_service_name, setActiveServiceId, setServices, services, queryClient, reset]);
}

type ValidateResponse = components["schemas"]["ProvisionValidateResponse"];

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
    deps.setStep(deps.mode === "join" || deps.mode === "ingest" ? "join" : "features");
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

interface AnalyzeLakeArgs {
  config: ProvisionConfig; icebergMetadataLocation: string; setIsAnalyzing: (b: boolean) => void;
  setLakeInfo: (l: LakeInfo) => void; setImportRange: (r: { start: string; end: string }) => void;
  setStep: (s: Step) => void; setFosStatus: (s: FosStatus) => void; setFosError: (s: string) => void;
}

export async function runAnalyzeLake(args: AnalyzeLakeArgs) {
  const { config, icebergMetadataLocation, setIsAnalyzing, setLakeInfo, setImportRange, setStep, setFosStatus, setFosError } = args;
  setIsAnalyzing(true);
  try {
    const data = await unwrapClient(
      client.POST("/api/provision/lake-info", {
        body: {
          bucket: config.fos_bucket_name, region: config.fos_region, access_key: config.fos_access_key,
          secret_key: config.fos_secret_key, prefix: config.fos_prefix,
          endpoint: config.fos_endpoint || undefined, iceberg_metadata_location: icebergMetadataLocation || undefined,
        },
      }),
    );
    if (data?.ok) {
      setLakeInfo(data);
      if (data.range?.start && data.range?.end) setImportRange({ start: data.range.start, end: data.range.end });
      setStep("analyze");
    } else {
      setFosStatus("error"); setFosError(data?.error || "Failed to analyze data lake.");
    }
  } catch (e) {
    setFosStatus("error"); setFosError(e instanceof Error ? e.message : String(e));
  } finally {
    setIsAnalyzing(false);
  }
}

export const WIZARD_DRAFT_KEY = "provision-wizard-draft-v1";
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
  version: z.number(), mode: z.enum(["provision", "join", "ingest"]), step: z.string(),
});

export function loadDraft(): WizardDraft | null {
  const ls = safeLocalStorage();
  if (!ls) return null;
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
  const activeServiceId = useServiceStore((s) => s.activeServiceId);
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
  const [lakeInfo, setLakeInfo] = useState<LakeInfo | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [importMode, setImportMode] = useState<"all" | "range">("all");
  const [importRange, setImportRange] = useState<{ start: string; end: string }>({ start: "", end: "" });
  const [syncEnabled, setSyncEnabled] = useState(true);

  const capabilityService = React.useMemo(() => {
    if (!services.length) return null;
    return !activeServiceId ? (services[0] ?? null) : (services.find((service) => service.id === activeServiceId) ?? null);
  }, [activeServiceId, services]);
  const analystPathASupported = capabilityService?.analystPathASupported ?? true;
  const analystPathAReason = analystPathASupported ? null : (capabilityService?.analystPathAReason ?? ANALYST_PATH_A_UNSUPPORTED_REASON);

  const [pendingDraft, setPendingDraft] = useState<WizardDraft | null>(() => (open ? loadDraft() : null));
  const draftIdRef = useRef<string | null>(null);
  const draftCreatedAtRef = useRef<string | null>(null);
  const { lines, status, isDone, error: sseError, start, stop, reset } = useSSE();

  const handleModalClose = buildHandleModalClose({
    status, isDone, onOpenChange, selectedService, setActiveServiceId, queryClient,
    setStep, setMode, setSearch, setSelectedService, setIsDeploying, setFosStatus,
    setFosError, setLakeInfo, setIsAnalyzing, setImportMode, setSyncEnabled, reset,
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

  const [joinPhase, setJoinPhase] = useState<JoinPhase>("form");
  const [joinedServiceId, setJoinedServiceId] = useState("");
  const [syncIntervalMins, setSyncIntervalMins] = useState("2");
  const [icebergMetadataLocation, setIcebergMetadataLocation] = useState("");

  const resumeDraft = () => {
    if (!pendingDraft) return;
    setMode(pendingDraft.mode);
    setConfig(mergePersistedConfig(pendingDraft.config));
    const toSvc = (id: string | null, name: string | null) => (id ? ({ id, name: name || id } as ProvisionService) : null);
    setSelectedService(toSvc(pendingDraft.selectedServiceId, pendingDraft.selectedServiceName));
    setSelectedCdnService(toSvc(pendingDraft.selectedCdnServiceId, pendingDraft.selectedCdnServiceName));
    setImportMode(pendingDraft.importMode);
    setImportRange(pendingDraft.importRange);
    setSyncEnabled(pendingDraft.syncEnabled);
    setSyncIntervalMins(pendingDraft.syncIntervalMins);
    setIcebergMetadataLocation(pendingDraft.icebergMetadataLocation);
    let next: Step = pendingDraft.currentStep === "execute" ? "fields" : pendingDraft.currentStep;
    if ((["service", "storage", "ngwaf", "fields", "execute"] as Step[]).includes(next)) next = "token";
    setStep(next);
    draftIdRef.current = pendingDraft.draftId;
    draftCreatedAtRef.current = pendingDraft.createdAt;
    setPendingDraft(null);
  };

  const discardDraft = () => { clearDraft(); draftIdRef.current = null; draftCreatedAtRef.current = null; setPendingDraft(null); };

  const estimatedImportSize = React.useMemo(() => {
    if (!lakeInfo?.calendar) return 0;
    let total = 0;
    const { start, end } = importRange;
    for (const [dateStr, stats] of Object.entries(lakeInfo.calendar)) {
      if (dateStr === "unknown") continue;
      if (importMode === "range" && ((start && dateStr < start.split("T")[0]) || (end && dateStr > end.split("T")[0]))) continue;
      total += stats.size_bytes;
    }
    return total;
  }, [lakeInfo, importMode, importRange]);

  const handleAnalyzeLake = () =>
    runAnalyzeLake({ config, icebergMetadataLocation, setIsAnalyzing, setLakeInfo, setImportRange, setStep, setFosStatus, setFosError });
  const handleJoin = () =>
    runJoin({ config, analystPathASupported, syncIntervalMins, syncEnabled, icebergMetadataLocation, importMode, importRange, setIsDeploying, setJoinPhase, setStep, reset, start });

  useJoinCompletionEffect({ joinPhase, status, config, setIsDeploying, setJoinedServiceId, setActiveServiceId, services, setServices, queryClient, setJoinPhase, reset });

  const handleFinishJoin = () => { onOpenChange(false); window.location.reload(); };
  const STEPS = getStepsForMode(mode, config, step);

  useWizardEffects({
    open, step, config, setConfig, fosStatus, setFosStatus, setFosError, setStep, setMode, setSearch,
    setSelectedService, setIsDeploying, reset, resetConfig: () => setConfig({ ...INITIAL_CONFIG }),
    joinPhase, isAnalyzing, handleAnalyzeLake, selectedService, token, setNgwafWorkspaces,
    setNgwafFetchError, setNgwafDebugRaw, setNgwafFetching, isDone, checkDomain, pendingDraft,
  });

  const handleDeploy = () => runDeploy({ token, selectedService, config, setIsDeploying, start });
  const fetchTerraformPreview = () =>
    runFetchTerraformPreview({ token, selectedService, config, setIsFetchingTerraform, setTerraformFiles, setSelectedTfFile });
  const handleExportTerraform = () => runExportTerraform({ token, selectedService, config });
  const handleAdminIngest = () =>
    runAdminIngest({ token, selectedService, selectedCdnService, config, services, setIsDeploying, setJoinedServiceId, setActiveServiceId, setServices, queryClient, setJoinPhase, setStep });

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
        tokenInfo, config: stripSecretsFromConfig(config), importMode, importRange,
        syncEnabled, syncIntervalMins, icebergMetadataLocation,
        updatedAt: new Date().toISOString(), createdAt: draftCreatedAtRef.current,
      });
    }, 500);
    return () => clearTimeout(timer);
  }, [
    open, pendingDraft, step, mode, selectedService?.id, selectedService?.name,
    selectedCdnService?.id, selectedCdnService?.name, tokenInfo, config,
    importMode, importRange, syncEnabled, syncIntervalMins, icebergMetadataLocation,
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
    lakeInfo, isAnalyzing, analystPathASupported, analystPathAReason, importMode, setImportMode,
    importRange, setImportRange, syncEnabled, setSyncEnabled, lines, status, isDone, sseError,
    stop, handleModalClose, onOpenChange, config, setConfig, catalog, isLoadingCatalog,
    toggleGroup, toggleField, updateFieldLimit, togglePreset, isPresetActive, estimatedBytes,
    servicesData, servicesError, isLoadingServices, filteredServices, handleTokenSubmit,
    validateMutation, handleServiceSelect, handleCheckFos, checkDomain, domainStatus,
    domainMessage, joinPhase, joinedServiceId, syncIntervalMins, setSyncIntervalMins,
    icebergMetadataLocation, setIcebergMetadataLocation, estimatedImportSize, handleAnalyzeLake,
    handleJoin, handleFinishJoin, handleDeploy, fetchTerraformPreview, handleExportTerraform,
    handleAdminIngest, STEPS, pendingDraft, resumeDraft, discardDraft,
  };
}
