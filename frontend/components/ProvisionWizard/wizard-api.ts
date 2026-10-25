"use client";

import { client } from "@/lib/api";
import { unwrapClient } from '@/lib/api';
import type { components } from "@/types/api.generated";
import type {
  FosStatus,
  ProvisionConfig,
  ProvisionService,
  Step,
} from "./types";

type ValidateResponse = components["schemas"]["ProvisionValidateResponse"];
type LakeInfo = components["schemas"]["ProvisionLakeInfoResponse"];

// ── validate mutation factory ──
interface ValidateMutationDeps {
  token: string;
  setTokenInfo: (info: {
    id: string;
    name: string;
    type: "user" | "automation";
  }) => void;
  setConfig: (updater: (prev: ProvisionConfig) => ProvisionConfig) => void;
  setStep: (s: Step) => void;
  mode: string | null;
}

export const validateMutationFn = (token: string) =>
  async (serviceId: string) => {
    return await unwrapClient(
      client.POST("/api/provision/validate", {
        body: { token, service_id: serviceId },
      }),
    );
  };

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
        cdn_service_name:
          defaults.cdn_service_name || `${data.service_name} (CDN)`,
        cdn_prefix: (
          defaults.cdn_prefix ||
          (defaults.fos_bucket_name
            ? `fos-${defaults.fos_bucket_name.split("-").slice(0, 2).join("-")}`
            : "")
        ).toLowerCase(),
      }));
    }
    deps.setStep(
      deps.mode === "join" || deps.mode === "ingest" ? "join" : "features",
    );
  };
}

// ── handleCheckConfig ──
interface CheckConfigArgs {
  token: string;
  selectedService: ProvisionService | null;
  selectedCdnService: ProvisionService | null;
  config: ProvisionConfig;
  setIsCheckingConfig: (b: boolean) => void;
  setConfigStatus: (
    s: components["schemas"]["ProvisionCheckConfigResponse"] | null,
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
    const data = await unwrapClient(
      client.GET("/api/provision/check-config", {
        params: {
          query: {
            token,
            service_id: selectedService.id,
            cdn_service_id: selectedCdnService.id,
            bucket: config.fos_bucket_name,
          },
        },
      }),
    );
    if (data) setConfigStatus(data);
  } catch (e) {
    console.error("Failed to check config", e);
  } finally {
    setIsCheckingConfig(false);
  }
}

// ── handleCheckFos ──
interface CheckFosArgs {
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
    const data = await unwrapClient(
      client.POST("/api/provision/check-fos", {
        body: { bucket, region, access_key, secret_key },
      }),
    );
    if (data?.ok) {
      setFosStatus("success");
    } else {
      setFosStatus("error");
      setFosError(data?.error || "Failed to connect.");
    }
  } catch (err) {
    setFosStatus("error");
    setFosError(err instanceof Error && err.message ? err.message : "An error occurred.");
  }
}

// ── checkDomain ──
interface CheckDomainArgs {
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
    const data = await unwrapClient(
      client.GET("/api/provision/check-domain", {
        params: { query: { prefix } },
      }),
    );
    if (data?.available) {
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
interface AnalyzeLakeArgs {
  config: ProvisionConfig;
  icebergMetadataLocation: string;
  setIsAnalyzing: (b: boolean) => void;
  setLakeInfo: (l: LakeInfo) => void;
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
    const data = await unwrapClient(
      client.POST("/api/provision/lake-info", {
        body: {
          bucket: config.fos_bucket_name,
          region: config.fos_region,
          access_key: config.fos_access_key,
          secret_key: config.fos_secret_key,
          prefix: config.fos_prefix,
          endpoint: config.fos_endpoint || undefined,
          iceberg_metadata_location: icebergMetadataLocation || undefined,
        },
      }),
    );
    if (data?.ok) {
      setLakeInfo(data);
      if (data.range?.start && data.range?.end) {
        setImportRange({
          start: data.range.start,
          end: data.range.end,
        });
      }
      setStep("analyze");
    } else {
      setFosStatus("error");
      setFosError(data?.error || "Failed to analyze data lake.");
    }
  } catch (e) {
    setFosStatus("error");
    setFosError(e instanceof Error ? e.message : String(e));
  } finally {
    setIsAnalyzing(false);
  }
}
