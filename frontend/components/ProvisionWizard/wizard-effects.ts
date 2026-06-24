"use client";

import { useEffect } from "react";
import { client } from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";
import { SHIELD_MAP, type FosStatus, type JoinPhase, type ProvisionConfig, type ProvisionService, type Step, type WizardDraft } from "./types";

export interface WizardEffectsArgs {
  open: boolean;
  step: Step;
  config: ProvisionConfig;
  setConfig: React.Dispatch<React.SetStateAction<ProvisionConfig>>;
  fosStatus: FosStatus;
  setFosStatus: (s: FosStatus) => void;
  setFosError: (s: string) => void;
  setStep: (s: Step) => void;
  setMode: (m: any) => void;
  setSearch: (s: string) => void;
  setSelectedService: (s: ProvisionService | null) => void;
  setIsDeploying: (b: boolean) => void;
  reset: () => void;
  resetConfig: () => void;
  joinPhase: JoinPhase;
  isAnalyzing: boolean;
  handleAnalyzeLake: () => void;
  selectedService: ProvisionService | null;
  token: string;
  setNgwafWorkspaces: (
    w: { id: string; name: string }[],
  ) => void;
  setNgwafFetchError: (s: string) => void;
  setNgwafDebugRaw: (s: string) => void;
  setNgwafFetching: (b: boolean) => void;
  isDone: boolean;
  checkDomain: (prefix: string) => void;
  pendingDraft?: WizardDraft | null;
}

export function useWizardEffects(args: WizardEffectsArgs) {
  const {
    open,
    step,
    config,
    setConfig,
    fosStatus,
    setFosStatus,
    setFosError,
    setStep,
    setMode,
    setSearch,
    setSelectedService,
    setIsDeploying,
    reset,
    resetConfig,
    joinPhase,
    isAnalyzing,
    handleAnalyzeLake,
    selectedService,
    token,
    setNgwafWorkspaces,
    setNgwafFetchError,
    setNgwafDebugRaw,
    setNgwafFetching,
    isDone,
    checkDomain,
    pendingDraft,
  } = args;

  // Update shield when region changes
  useEffect(() => {
    const shield = SHIELD_MAP[config.fos_region];
    if (shield && shield !== config.cdn_shield) {
      setConfig((prev) => ({ ...prev, cdn_shield: shield }));
    }
  }, [config.fos_region]);

  // Reset form when modal opens (U-7: skip when a resumable draft is pending
  // so the Resume banner can show without flashing through default state).
  useEffect(() => {
    if (open && !pendingDraft) {
      setStep("mode");
      setMode(null);
      setSearch("");
      setSelectedService(null);
      setIsDeploying(false);
      setFosStatus("idle");
      setFosError("");
      reset();
      resetConfig();
    }
  }, [open, reset, pendingDraft]);

  // ── CDN Domain Check ──
  useEffect(() => {
    if (step === "storage" && config.cdn_prefix) {
      const timer = setTimeout(() => {
        checkDomain(config.cdn_prefix);
      }, 500);
      return () => clearTimeout(timer);
    }
  }, [config.cdn_prefix, step]);

  useEffect(() => {
    if (fosStatus !== "idle" && fosStatus !== "checking") {
      setFosStatus("idle");
      setFosError("");
    }
  }, [
    config.fos_bucket_name,
    config.fos_region,
    config.fos_access_key,
    config.fos_secret_key,
  ]);

  // After FOS check succeeds in the join flow, auto-proceed to lake analysis
  useEffect(() => {
    if (
      fosStatus === "success" &&
      step === "join" &&
      joinPhase === "form" &&
      config.endpoint_name &&
      config.cdn_service_name &&
      !isAnalyzing
    ) {
      handleAnalyzeLake();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fosStatus]);

  // Auto-fetch NGWAF workspaces when entering the ngwaf step
  useEffect(() => {
    if (step !== "ngwaf" || !selectedService?.id) return;
    setNgwafWorkspaces([]);
    setNgwafFetchError("");
    setNgwafDebugRaw("");
    setNgwafFetching(true);
    client
      .GET("/api/provision/ngwaf-workspaces" as any, {
        params: {
          query: { service_id: selectedService.id, token: token || undefined },
        },
      })
      .then((r) => {
        if ((r as any).error) {
          const errBody = (r as any).error;
          const msg =
            errBody?.detail?.error ||
            errBody?.error ||
            errBody?.message ||
            "Could not load workspaces";
          setNgwafFetchError(msg);
        } else {
          const data = r.data as any;
          setNgwafWorkspaces(data?.workspaces || []);
          if (data?._debug_raw) setNgwafDebugRaw(data._debug_raw);
          if (data?.error_hint) setNgwafFetchError(data.error_hint);
        }
      })
      .catch((e: any) =>
        setNgwafFetchError(e?.message || "Could not load workspaces"),
      )
      .finally(() => setNgwafFetching(false));
  }, [step, selectedService?.id, token]);

  // Save ngwaf_workspace_id to local config after provisioning completes
  useEffect(() => {
    if (
      isDone &&
      step === "execute" &&
      config.ngwaf_workspace_id &&
      selectedService?.id
    ) {
      client
        .PATCH(
          "/api/provision/services/{service_id}/ngwaf-workspace" as any,
          {
            params: { path: { service_id: selectedService.id } },
            body: { ngwaf_workspace_id: config.ngwaf_workspace_id } as any,
          },
        )
        .then((r: { error?: unknown }) => {
          if (r?.error) {
            // Don't fail silently — a dropped save means the workspace id
            // never lands in the service config and NGWAF stats stay empty.
            console.error("[wizard] failed to persist ngwaf_workspace_id:", r.error);
          }
        })
        .catch((e: unknown) => {
          console.error("[wizard] failed to persist ngwaf_workspace_id:", e);
        });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDone]);
}

// Effect specifically for the join SSE completion watcher
export interface JoinCompletionEffectArgs {
  joinPhase: JoinPhase;
  status: string;
  config: ProvisionConfig;
  setIsDeploying: (b: boolean) => void;
  setJoinedServiceId: (id: string) => void;
  setActiveServiceId: (id: string) => void;
  services: { id: string; name: string; accessLevel?: string }[];
  setServices: (s: { id: string; name: string; accessLevel?: string }[]) => void;
  queryClient: { invalidateQueries: (opts: any) => void };
  setJoinPhase: (p: JoinPhase) => void;
  reset: () => void;
}

export function useJoinCompletionEffect(args: JoinCompletionEffectArgs) {
  const {
    joinPhase,
    status,
    config,
    setIsDeploying,
    setJoinedServiceId,
    setActiveServiceId,
    services,
    setServices,
    queryClient,
    setJoinPhase,
    reset,
  } = args;
  useEffect(() => {
    if (joinPhase !== "connecting") return;
    if (status === "done") {
      setIsDeploying(false);
      setJoinedServiceId(config.cdn_service_name);
      if (config.cdn_service_name) {
        setActiveServiceId(config.cdn_service_name);
        // Optimistically add to the store so hasServices is true immediately
        // after reload — before bootstrap has a chance to respond.
        if (!services.some((s) => s.id === config.cdn_service_name)) {
          setServices([
            ...services,
            {
              id: config.cdn_service_name,
              name: config.cdn_service_name,
              accessLevel: "read_only",
            },
          ]);
        }
      }
      queryClient.invalidateQueries({ queryKey: queryKeys.bootstrap() });
      setJoinPhase("done");
    } else if (status === "error") {
      setIsDeploying(false);
      setJoinPhase("form");
      reset();
    }
  }, [
    joinPhase,
    status,
    config.cdn_service_name,
    setActiveServiceId,
    setServices,
    services,
    queryClient,
    reset,
  ]);
}
