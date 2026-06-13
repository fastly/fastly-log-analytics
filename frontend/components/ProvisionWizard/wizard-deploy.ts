"use client";

import { client } from "@/lib/api";
import { downloadBlob } from "@/lib/utils";
import type { Service } from "@/stores/serviceStore";
import type {
  FosStatus,
  JoinPhase,
  ProvisionConfig,
  ProvisionService,
  Step,
} from "./types";

// ── fetchTerraformPreview ──
export interface FetchTerraformPreviewArgs {
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
    const { data } = await client.POST("/api/provision/terraform/preview", {
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
        cdn_service_name: config.cdn_service_name,
        cdn_prefix: config.cdn_prefix,
        cdn_shield: config.cdn_shield,
        log_fields: config.log_fields,
      } as any,
    });
    if (data) {
      const files = data as Record<string, string>;
      setTerraformFiles(files);
      if (files["main.tf"]) {
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
export interface ExportTerraformArgs {
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
    cdn_service_name: config.cdn_service_name,
    cdn_prefix: config.cdn_prefix,
    cdn_shield: config.cdn_shield,
    log_fields: config.log_fields,
  };

  try {
    // Raw fetch (not typed `client`): this endpoint streams a binary
    // zip; openapi-fetch's JSON deserialization would corrupt it. The
    // path is still type-checked via the literal endpoint string.
    const response = await fetch("/api/provision/terraform/export", {
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
export interface ModalCloseDeps {
  status: string;
  isDone: boolean;
  onOpenChange: (open: boolean) => void;
  selectedService: ProvisionService | null;
  setActiveServiceId: (id: string) => void;
  queryClient: { invalidateQueries: (opts: any) => void };
  setStep: (s: Step) => void;
  setMode: (m: any) => void;
  setSearch: (s: string) => void;
  setSelectedService: (s: ProvisionService | null) => void;
  setIsDeploying: (b: boolean) => void;
  setFosStatus: (s: FosStatus) => void;
  setFosError: (s: string) => void;
  setLakeInfo: (l: any) => void;
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
        deps.queryClient.invalidateQueries({ queryKey: ["bootstrap"] });
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
export interface DeployArgs {
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
  const body: Record<string, any> = {
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
    cdn_service_name: config.cdn_service_name,
    cdn_shield: config.cdn_shield,
    enable_cron_sync: config.enable_cron_sync,
    delete_after: config.delete_after,
    commit_interval_mins: Number(config.commit_interval_mins),
    enable_cron_compact: config.enable_cron_compact,
    log_fields: config.log_fields ? JSON.stringify(config.log_fields) : null,
  };
  if (config.cdn_prefix) {
    body.cdn_url = `https://${config.cdn_prefix}.global.ssl.fastly.net`;
  }
  start("/api/provision/execute", body);
}

// ── runJoin (kicks off analyst join SSE) ──
export interface JoinArgs {
  config: ProvisionConfig;
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
export interface AdminIngestArgs {
  token: string;
  selectedService: ProvisionService | null;
  selectedCdnService: ProvisionService | null;
  config: ProvisionConfig;
  services: Service[];
  setIsDeploying: (b: boolean) => void;
  setJoinedServiceId: (id: string) => void;
  setActiveServiceId: (id: string) => void;
  setServices: (services: Service[]) => void;
  queryClient: { invalidateQueries: (opts: any) => void };
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
    const { data } = await client.POST("/api/provision/ingest", {
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
        log_fields: config.log_fields,
        fos_access_key: config.fos_access_key,
        fos_secret_key: config.fos_secret_key,
      } as any,
    });

    if ((data as any)?.ok) {
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
      queryClient.invalidateQueries({ queryKey: ["bootstrap"] });
      setJoinPhase("done");
      setStep("join");
    }
  } catch (e) {
    console.error("Ingest failed", e);
  } finally {
    setIsDeploying(false);
  }
}
