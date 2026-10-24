"use client";

import type { QueryClient } from "@tanstack/react-query";
import { adminFetch, client } from "@/lib/api";
import { unwrapClient } from "@/lib/api-client";
import { queryKeys } from "@/lib/query-keys";
import { downloadBlob } from "@/lib/utils";
import type { Service } from "@/stores/serviceStore";
import type {
  FosStatus,
  JoinPhase,
  LakeInfo,
  ProvisionConfig,
  ProvisionService,
  Step,
  WizardMode,
} from "./types";
import { clearDraft } from "./wizard-draft";

// ── fetchTerraformPreview ──
interface FetchTerraformPreviewArgs {
  token: string;
  selectedService: ProvisionService | null;
  config: ProvisionConfig;
  setIsFetchingTerraform: (b: boolean) => void;
  setTerraformFiles: (f: Record<string, string>) => void;
  setSelectedTfFile: (f: string) => void;
}

export async function runFetchTerraformPreview(
  args: FetchTerraformPreviewArgs,
) {
  const {
    token,
    selectedService,
    config,
    setIsFetchingTerraform,
    setTerraformFiles,
    setSelectedTfFile,
  } = args;
  if (!selectedService) return;
  setIsFetchingTerraform(true);
  try {
    const data = await unwrapClient(
      client.POST("/api/provision/terraform/preview", {
        body: {
          token,
          logging_service_id: selectedService.id,
          service_name: selectedService.name,
          endpoint_name: config.endpoint_name,
          fos_region: config.fos_region,
          fos_bucket_name: config.fos_bucket_name,
          fos_prefix: config.fos_prefix,
          sample_rate: String(config.sample_rate),
          edge_only: config.edge_only,
          custom_condition: config.custom_condition,
          log_period: String(config.log_period),
          rum_log_period: String(config.rum_log_period || 300),
          cdn_service_name: config.cdn_service_name,
          cdn_prefix: config.cdn_prefix,
          cdn_shield: config.cdn_shield,
          log_fields: config.log_fields,
        },
      }),
    );
    if (data) {
      const files = data;
      setTerraformFiles(files);
      // Instructions is the default (first) tab, so select its file on load
      // — otherwise the Instructions tab would open showing a stale .tf file.
      if (files["instructions"]) {
        setSelectedTfFile("instructions");
      } else if (files["main.tf"]) {
        setSelectedTfFile("main.tf");
      } else {
        const firstFile = Object.keys(files)[0];
        if (firstFile) setSelectedTfFile(firstFile);
      }
    }
  } catch (e) {
    console.error(e);
  } finally {
    setIsFetchingTerraform(false);
  }
}

// ── handleExportTerraform ──
interface ExportTerraformArgs {
  token: string;
  selectedService: ProvisionService | null;
  config: ProvisionConfig;
}

export async function runExportTerraform(args: ExportTerraformArgs) {
  const { token, selectedService, config } = args;
  if (!selectedService) return;

  const payload = {
    token,
    logging_service_id: selectedService.id,
    service_name: selectedService.name,
    endpoint_name: config.endpoint_name,
    fos_region: config.fos_region,
    fos_bucket_name: config.fos_bucket_name,
    fos_prefix: config.fos_prefix,
    sample_rate: String(config.sample_rate),
    edge_only: config.edge_only,
    custom_condition: config.custom_condition,
    log_period: String(config.log_period),
    rum_log_period: String(config.rum_log_period || 300),
    cdn_service_name: config.cdn_service_name,
    cdn_prefix: config.cdn_prefix,
    cdn_shield: config.cdn_shield,
    log_fields: config.log_fields,
  };

  try {
    // Raw fetch (not typed `client`): this endpoint streams a binary
    // zip; openapi-fetch's JSON deserialization would corrupt it.
    // ``adminFetch`` stamps the standard client-context headers.
    const response = await adminFetch("/api/provision/terraform/export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) throw new Error("Export failed");

    const blob = await response.blob();
    downloadBlob(blob, "fastly-log-analysis-terraform.zip");
  } catch (e) {
    console.error("Failed to export Terraform", e);
  }
}

// ── buildHandleModalClose ──
interface ModalCloseDeps {
  status: string;
  isDone: boolean;
  onOpenChange: (open: boolean) => void;
  selectedService: ProvisionService | null;
  setActiveServiceId: (id: string) => void;
  queryClient: Pick<QueryClient, "invalidateQueries">;
  setStep: (s: Step) => void;
  setMode: (m: WizardMode) => void;
  setSearch: (s: string) => void;
  setSelectedService: (s: ProvisionService | null) => void;
  setIsDeploying: (b: boolean) => void;
  setFosStatus: (s: FosStatus) => void;
  setFosError: (s: string) => void;
  setLakeInfo: (l: LakeInfo | null) => void;
  setIsAnalyzing: (b: boolean) => void;
  setImportMode: (m: "all" | "range") => void;
  setSyncEnabled: (b: boolean) => void;
  reset: () => void;
  resetConfig: () => void;
  setNgwafWorkspaces: (w: { id: string; name: string }[]) => void;
  setNgwafFetching: (b: boolean) => void;
  setNgwafFetchError: (s: string) => void;
}

export function buildHandleModalClose(deps: ModalCloseDeps) {
  return (isOpen: boolean) => {
    if (deps.status === "streaming") return; // Prevent closing while streaming
    deps.onOpenChange(isOpen);
    if (!isOpen) {
      if (deps.status === "done" || deps.isDone) {
        if (deps.selectedService?.id) {
          deps.setActiveServiceId(deps.selectedService.id);
        }
        deps.queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() });
        clearDraft();
        window.location.reload();
      } else {
        setTimeout(() => {
          deps.setStep("mode");
          deps.setMode(null);
          deps.setSearch("");
          deps.setSelectedService(null);
          deps.setIsDeploying(false);
          deps.setFosStatus("idle");
          deps.setFosError("");
          deps.setLakeInfo(null);
          deps.setIsAnalyzing(false);
          deps.setImportMode("all");
          deps.setSyncEnabled(true);
          deps.reset();
          deps.resetConfig();
          deps.setNgwafWorkspaces([]);
          deps.setNgwafFetching(false);
          deps.setNgwafFetchError("");
        }, 300);
      }
    }
  };
}

// ── runDeploy (SSE-streamed provisioning) ──
interface DeployArgs {
  token: string;
  selectedService: ProvisionService | null;
  config: ProvisionConfig;
  setIsDeploying: (b: boolean) => void;
  start: (urlPath: string, body?: Record<string, unknown>) => void;
}

export function runDeploy(args: DeployArgs) {
  const { token, selectedService, config, setIsDeploying, start } = args;
  if (!selectedService) return;
  setIsDeploying(true);
  const body: Record<string, unknown> = {
    token,
    service_id: selectedService.id,
    service_name: selectedService.name,
    endpoint_name: config.endpoint_name,
    fos_region: config.fos_region,
    fos_bucket_name: config.fos_bucket_name,
    fos_prefix: config.fos_prefix,
    sample_rate: String(config.sample_rate),
    edge_only: config.edge_only,
    custom_condition: config.custom_condition,
    log_period: String(config.log_period),
    rum_log_period: String(config.rum_log_period || 300),
    cdn_service_name: config.cdn_service_name,
    cdn_shield: config.cdn_shield,
    enable_cron_sync: config.enable_cron_sync,
    delete_after: config.delete_after,
    commit_interval_mins: Number(config.commit_interval_mins),
    enable_cron_compact: config.enable_cron_compact,
    log_retention_days: Number(config.log_retention_days),
    rum_retention_days: Number(config.rum_retention_days),
    log_fields: config.log_fields ? JSON.stringify(config.log_fields) : null,
    cmcd_enabled: config.cmcd_enabled,
    cmcd_mode: config.cmcd_mode,
    cmcd_version: config.cmcd_version,
    logging_enabled: config.logging_enabled,
    rum_enabled: config.rum_enabled,
    rum_custom_condition: config.rum_custom_condition,
    faro_version: config.faro_version || null,
  };
  if (config.cdn_prefix) {
    body.cdn_url = `https://${config.cdn_prefix}.global.ssl.fastly.net`;
  }
  start("/api/provision/execute", body);
}

// ── runJoin (kicks off analyst join SSE) ──
interface JoinArgs {
  config: ProvisionConfig;
  analystPathASupported: boolean;
  syncIntervalMins: string;
  syncEnabled: boolean;
  icebergMetadataLocation: string;
  importMode: "all" | "range";
  importRange: { start: string; end: string };
  setIsDeploying: (b: boolean) => void;
  setJoinPhase: (p: JoinPhase) => void;
  setStep: (s: Step) => void;
  reset: () => void;
  start: (urlPath: string, body?: Record<string, unknown>) => void;
}

export function runJoin(args: JoinArgs) {
  const {
    config,
    analystPathASupported,
    syncIntervalMins,
    syncEnabled,
    icebergMetadataLocation,
    importMode,
    importRange,
    setIsDeploying,
    setJoinPhase,
    setStep,
    reset,
    start,
  } = args;
  if (!analystPathASupported) return;
  if (
    !config.endpoint_name ||
    !config.cdn_service_name ||
    !config.fos_bucket_name ||
    !config.fos_region ||
    !config.fos_access_key ||
    !config.fos_secret_key
  )
    return;
  setIsDeploying(true);
  setJoinPhase("connecting");
  setStep("join");
  reset();

  const params: Record<string, string> = {
    service_id: config.cdn_service_name,
    service_name: config.endpoint_name,
    fos_bucket_name: config.fos_bucket_name,
    fos_region: config.fos_region,
    fos_endpoint: config.fos_endpoint || "",
    fos_access_key: config.fos_access_key,
    fos_secret_key: config.fos_secret_key,
    cdn_url: config.cdn_url || "",
    cdn_service_id: config.cdn_service_name || "",
    cdn_secret: config.cdn_secret || "",
    sync_interval_mins: syncIntervalMins,
    sync_enabled: String(syncEnabled),
    iceberg_metadata_location: icebergMetadataLocation || "",
  };

  if (importMode === "range") {
    if (importRange.start) params.start_time = importRange.start;
    if (importRange.end) params.end_time = importRange.end;
  }

  const qs = new URLSearchParams(params).toString();
  const url = `/api/provision/join?${qs}`;
  start(url);
}

// ── handleAdminIngest ──
interface AdminIngestArgs {
  token: string;
  selectedService: ProvisionService | null;
  selectedCdnService: ProvisionService | null;
  config: ProvisionConfig;
  services: Service[];
  setIsDeploying: (b: boolean) => void;
  setJoinedServiceId: (id: string) => void;
  setActiveServiceId: (id: string) => void;
  setServices: (services: Service[]) => void;
  queryClient: Pick<QueryClient, "invalidateQueries">;
  setJoinPhase: (p: JoinPhase) => void;
  setStep: (s: Step) => void;
}

export async function runAdminIngest(args: AdminIngestArgs) {
  const {
    token,
    selectedService,
    selectedCdnService,
    config,
    services,
    setIsDeploying,
    setJoinedServiceId,
    setActiveServiceId,
    setServices,
    queryClient,
    setJoinPhase,
    setStep,
  } = args;
  if (!selectedService) return;
  setIsDeploying(true);

  try {
    const data = await unwrapClient(
      client.POST("/api/provision/ingest", {
        body: {
          token,
          service_id: selectedService.id,
          service_name: selectedService.name,
          endpoint_name: config.endpoint_name,
          fos_region: config.fos_region,
          fos_bucket_name: config.fos_bucket_name,
          fos_prefix: config.fos_prefix,
          sample_rate: String(config.sample_rate),
          edge_only: config.edge_only,
          custom_condition: config.custom_condition,
          log_period: String(config.log_period),
          cdn_service_id: selectedCdnService?.id || config.cdn_service_name,
          cdn_service_name: selectedCdnService?.name || config.cdn_service_name,
          cdn_url:
            config.cdn_url ||
            (config.cdn_prefix
              ? `https://${config.cdn_prefix}.global.ssl.fastly.net`
              : ""),
          cdn_shield: config.cdn_shield,
          enable_cron_sync: config.enable_cron_sync,
          delete_after: config.delete_after,
          commit_interval_mins: config.commit_interval_mins,
          enable_cron_compact: config.enable_cron_compact,
          log_retention_days: config.log_retention_days,
          rum_retention_days: config.rum_retention_days,
          log_fields: config.log_fields,
          rum_custom_condition: config.rum_custom_condition,
          fos_access_key: config.fos_access_key,
          fos_secret_key: config.fos_secret_key,
        },
      }),
    );

    if (data?.ok) {
      setJoinedServiceId(selectedService.id);
      if (selectedService.id) {
        setActiveServiceId(selectedService.id);
        if (!services.some((s) => s.id === selectedService.id)) {
          setServices([
            ...services,
            {
              id: selectedService.id,
              name: selectedService.name,
              accessLevel: "read_write",
            },
          ]);
        }
      }
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() });
      setJoinPhase("done");
      setStep("join");
    }
  } catch (e) {
    console.error("Ingest failed", e);
  } finally {
    setIsDeploying(false);
  }
}
