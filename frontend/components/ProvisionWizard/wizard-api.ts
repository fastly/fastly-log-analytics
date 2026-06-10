"use client";

import { client } from "@/lib/api";
import type {
  FosStatus,
  ProvisionConfig,
  ProvisionService,
  Step,
} from "./types";

// ── validate mutation factory ──
export interface ValidateMutationDeps {
  token: string;
  setTokenInfo: (info: {
    id: string;
    name: string;
    type: "user" | "automation";
  }) => void;
  setConfig: (updater: (prev: ProvisionConfig) => ProvisionConfig) => void;
  setStep: (s: Step) => void;
  mode: any;
}

export const validateMutationFn = (token: string) =>
  async (serviceId: string) => {
    const { data } = await client.POST("/api/provision/validate", {
      body: { token, service_id: serviceId } as any,
    });
    return data as any;
  };

export function buildValidateOnSuccess(deps: ValidateMutationDeps) {
  return (data: any) => {
    if (data?.token_info) {
      deps.setTokenInfo(data.token_info);
    }
    if (data?.defaults) {
      deps.setConfig((prev) => ({
        ...prev,
        endpoint_name:
          data.defaults.endpoint_name || "Fastly Object Storage Logs",
        fos_region: data.defaults.fos_region || "us-east-1",
        fos_bucket_name: data.defaults.fos_bucket_name?.toLowerCase() || "",
        fos_prefix: data.defaults.fos_prefix || "",
        cdn_service_name:
          data.defaults.cdn_service_name || `${data.service_name} (CDN)`,
        cdn_prefix: (
          data.defaults.cdn_prefix ||
          (data.defaults.fos_bucket_name
            ? `fos-${data.defaults.fos_bucket_name.split("-").slice(0, 2).join("-")}`
            : "")
        ).toLowerCase(),
      }));
    }
    deps.setStep(
      deps.mode === "join" || deps.mode === "ingest" ? "join" : "storage",
    );
  };
}

// ── handleCheckConfig ──
export interface CheckConfigArgs {
  token: string;
  selectedService: ProvisionService | null;
  selectedCdnService: ProvisionService | null;
  config: ProvisionConfig;
  setIsCheckingConfig: (b: boolean) => void;
  setConfigStatus: (
    s: {
      logging_service: { ok: boolean; details: string };
      cdn_service: { ok: boolean; details: string };
    } | null,
  ) => void;
}

export async function runCheckConfig(args: CheckConfigArgs) {
  const {
    token,
    selectedService,
    selectedCdnService,
    config,
    setIsCheckingConfig,
    setConfigStatus,
  } = args;
  if (!selectedService || !selectedCdnService || !config.fos_bucket_name)
    return;
  setIsCheckingConfig(true);
  try {
    const { data } = await client.GET("/api/provision/check-config", {
      params: {
        query: {
          token,
          service_id: selectedService.id,
          cdn_service_id: selectedCdnService.id,
          bucket: config.fos_bucket_name,
        } as any,
      },
    });
    setConfigStatus(data as any);
  } catch (e) {
    console.error("Failed to check config", e);
  } finally {
    setIsCheckingConfig(false);
  }
}

// ── handleCheckFos ──
export interface CheckFosArgs {
  vals?: {
    bucket?: string;
    region?: string;
    access_key?: string;
    secret_key?: string;
  };
  config: ProvisionConfig;
  setFosStatus: (s: FosStatus) => void;
  setFosError: (s: string) => void;
}

export async function runCheckFos(args: CheckFosArgs) {
  const { vals, config, setFosStatus, setFosError } = args;
  const bucket = vals?.bucket ?? config.fos_bucket_name;
  const region = vals?.region ?? config.fos_region;
  const access_key = vals?.access_key ?? config.fos_access_key;
  const secret_key = vals?.secret_key ?? config.fos_secret_key;
  if (!bucket || !region || !access_key || !secret_key) return;
  setFosStatus("checking");
  setFosError("");
  try {
    const { data } = await client.GET("/api/provision/check-fos", {
      params: { query: { bucket, region, access_key, secret_key } as any },
    });
    if ((data as any)?.ok) {
      setFosStatus("success");
    } else {
      setFosStatus("error");
      setFosError((data as any)?.error || "Failed to connect.");
    }
  } catch (err: any) {
    setFosStatus("error");
    setFosError(err.message || "An error occurred.");
  }
}

// ── checkDomain ──
export interface CheckDomainArgs {
  prefix: string;
  setDomainStatus: (
    s: "idle" | "checking" | "available" | "taken" | "error",
  ) => void;
  setDomainMessage: (m: string) => void;
}

export async function runCheckDomain(args: CheckDomainArgs) {
  const { prefix, setDomainStatus, setDomainMessage } = args;
  if (!prefix || prefix.length < 3) return;
  setDomainStatus("checking");
  try {
    const { data } = await client.GET("/api/provision/check-domain", {
      params: { query: { prefix } },
    });
    if ((data as any)?.available) {
      setDomainStatus("available");
      setDomainMessage("Domain available!");
    } else {
      setDomainStatus("taken");
      setDomainMessage("This domain prefix is already in use.");
    }
  } catch {
    setDomainStatus("error");
  }
}

// ── handleAnalyzeLake ──
export interface AnalyzeLakeArgs {
  config: ProvisionConfig;
  icebergMetadataLocation: string;
  setIsAnalyzing: (b: boolean) => void;
  setLakeInfo: (l: any) => void;
  setImportRange: (r: { start: string; end: string }) => void;
  setStep: (s: Step) => void;
  setFosStatus: (s: FosStatus) => void;
  setFosError: (s: string) => void;
}

export async function runAnalyzeLake(args: AnalyzeLakeArgs) {
  const {
    config,
    icebergMetadataLocation,
    setIsAnalyzing,
    setLakeInfo,
    setImportRange,
    setStep,
    setFosStatus,
    setFosError,
  } = args;
  setIsAnalyzing(true);
  try {
    const { data } = await client.GET("/api/provision/lake-info", {
      params: {
        query: {
          bucket: config.fos_bucket_name,
          region: config.fos_region,
          access_key: config.fos_access_key,
          secret_key: config.fos_secret_key,
          prefix: config.fos_prefix,
          endpoint: config.fos_endpoint || undefined,
          iceberg_metadata_location: icebergMetadataLocation || undefined,
        },
      },
    });
    if ((data as any)?.ok) {
      setLakeInfo(data as any);
      if ((data as any)?.range?.start && (data as any)?.range?.end) {
        setImportRange({
          start: (data as any).range.start,
          end: (data as any).range.end,
        });
      }
      setStep("analyze");
    } else {
      setFosStatus("error");
      setFosError((data as any)?.error || "Failed to analyze data lake.");
    }
  } catch (e: any) {
    setFosStatus("error");
    setFosError(e.message || String(e));
  } finally {
    setIsAnalyzing(false);
  }
}
