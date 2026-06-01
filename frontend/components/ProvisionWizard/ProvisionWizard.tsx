"use client";

import React, { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { client } from "@/lib/api";
import { useServiceStore } from "@/stores/serviceStore";
import { useTimezoneStore } from "@/stores/timezoneStore";
import { CollapsibleGroup } from "@/components/LogSettingsModal/LogSettingsModal";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { useSSE } from "@/hooks/useSSE";
import { SSEProgressView } from "@/components/SSEModal";
import {
  ReviewCard,
  ReviewHeader,
  ReviewContent,
  ReviewItem,
} from "@/components/ui/review-card";
import { SectionHeader } from "@/components/ui/section-header";
import { LabelWithInfo } from "@/components/ui/label-with-info";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import {
  Plus,
  ChevronRight,
  ChevronLeft,
  Search,
  Globe,
  Settings,
  Zap,
  Lock,
  Loader2,
  CheckCircle2,
  AlertCircle,
  FileJson,
  Copy,
  Info,
  Database,
  Cloud,
  Sparkles,
  XCircle,
  Calendar,
  ArrowRight,
  Shield,
  FileText,
} from "lucide-react";
import { cn, formatBytes, formatDateTime, downloadBlob } from "@/lib/utils";
import { formatForInput, parseFromInput } from "@/lib/date";
import {
  panelDialogContent,
  panelDialogFooter,
} from "@/lib/panel-dialog";
import { Textarea } from "@/components/ui/textarea";
import type { components } from "@/types/api.generated";

type ProvisionService = components["schemas"]["ProvisionService"];

interface JsonImportSectionProps {
  onImport: (parsed: Record<string, string>) => void;
}

function JsonImportSection({ onImport }: JsonImportSectionProps) {
  const [open, setOpen] = useState(false);
  const [raw, setRaw] = useState("");
  const [parseError, setParseError] = useState("");
  const [imported, setImported] = useState(false);

  const handleImport = () => {
    setParseError("");
    try {
      const parsed = JSON.parse(raw);
      if (typeof parsed !== "object" || Array.isArray(parsed))
        throw new Error("Expected a JSON object");
      onImport(parsed);
      setImported(true);
      setOpen(false);
      setRaw("");
      setTimeout(() => setImported(false), 3000);
    } catch (e: any) {
      setParseError(e.message || "Invalid JSON");
    }
  };

  return (
    <div className="rounded-lg border bg-muted/20 p-4 space-y-3">
      <div
        className="flex items-center justify-between cursor-pointer select-none"
        onClick={() => setOpen((o) => !o)}
      >
        <div className="flex items-center gap-2">
          <FileJson className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Import config from admin</span>
          {imported && (
            <span className="text-xs text-emerald-500 font-medium">
              Fields populated!
            </span>
          )}
        </div>
        <span className="text-xs text-muted-foreground">
          {open ? "Cancel" : "Paste JSON"}
        </span>
      </div>
      {open && (
        <div className="space-y-3 animate-in fade-in slide-in-from-top-2 duration-200">
          <Textarea
            value={raw}
            onChange={(e) => {
              setRaw(e.target.value);
              setParseError("");
            }}
            placeholder={
              '{\n  "name": "...",\n  "service_id": "...",\n  ...\n}'
            }
            className="font-mono text-xs h-36 resize-none"
            autoFocus
          />
          {parseError && (
            <p className="text-xs text-destructive">{parseError}</p>
          )}
          <Button
            size="sm"
            disabled={!raw.trim()}
            onClick={handleImport}
            className="h-8"
          >
            Import
          </Button>
        </div>
      )}
    </div>
  );
}

interface ProvisionWizardProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

type Step =
  | "mode"
  | "token"
  | "service"
  | "storage"
  | "ngwaf"
  | "fields"
  | "execute"
  | "terraform"
  | "join"
  | "analyze"
  | "settings"
  | "confirm";

export function ProvisionWizard({ open, onOpenChange }: ProvisionWizardProps) {
  const { setActiveServiceId, setServices, services } = useServiceStore();
  const { timezone } = useTimezoneStore();
  const queryClient = useQueryClient();
  const [step, setStep] = useState<Step>("mode");
  const [mode, setMode] = useState<"provision" | "join" | "ingest" | null>(
    null,
  );
  const [token, setToken] = useState("");
  const [tokenInfo, setTokenInfo] = useState<{
    id: string;
    name: string;
    type: "user" | "automation";
  } | null>(null);
  const [search, setSearch] = useState("");
  const [selectedService, setSelectedService] =
    useState<ProvisionService | null>(null);
  const [isDeploying, setIsDeploying] = useState(false);
  const [fosStatus, setFosStatus] = useState<
    "idle" | "checking" | "success" | "error"
  >("idle");
  const [fosError, setFosError] = useState("");
  const [terraformFiles, setTerraformFiles] = useState<Record<string, string>>({});
  const [selectedTfFile, setSelectedTfFile] = useState<string>("logging_service.tf");
  const [isFetchingTerraform, setIsFetchingTerraform] = useState(false);
  const [selectedCdnService, setSelectedCdnService] =
    useState<ProvisionService | null>(null);
  const [configStatus, setConfigStatus] = useState<{
    logging_service: { ok: boolean; details: string };
    cdn_service: { ok: boolean; details: string };
  } | null>(null);
  const [isCheckingConfig, setIsCheckingConfig] = useState(false);

  const handleCheckConfig = async () => {
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
  };

  // NGWAF step state
  const [ngwafWorkspaces, setNgwafWorkspaces] = useState<
    { id: string; name: string }[]
  >([]);
  const [ngwafFetching, setNgwafFetching] = useState(false);
  const [ngwafFetchError, setNgwafFetchError] = useState("");
  const [ngwafDebugRaw, setNgwafDebugRaw] = useState("");

  // Analyst Flow state
  const [lakeInfo, setLakeInfo] = useState<any>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [importMode, setImportMode] = useState<"all" | "range">("all");
  const [importRange, setImportRange] = useState<{
    start: string;
    end: string;
  }>({
    start: "",
    end: "",
  });
  const [syncEnabled, setSyncEnabled] = useState(true);

  const {
    lines,
    status,
    isDone,
    error: sseError,
    start,
    stop,
    reset,
  } = useSSE();

  const handleModalClose = (isOpen: boolean) => {
    if (status === "streaming") return; // Prevent closing while streaming
    onOpenChange(isOpen);
    if (!isOpen) {
      if (status === "done" || isDone) {
        if (selectedService?.id) {
          setActiveServiceId(selectedService.id);
        }
        queryClient.invalidateQueries({ queryKey: ["bootstrap"] });
        window.location.reload();
      } else {
        setTimeout(() => {
          setStep("mode");
          setMode(null);
          setSearch("");
          setSelectedService(null);
          setIsDeploying(false);
          setFosStatus("idle");
          setFosError("");
          setLakeInfo(null);
          setIsAnalyzing(false);
          setImportMode("all");
          setSyncEnabled(true);
          reset();
          setConfig({
            endpoint_name: "",
            fos_region: "us-east-1",
            fos_endpoint: "",
            fos_bucket_name: "",
            fos_prefix: "",
            fos_access_key: "",
            fos_secret_key: "",
            sample_rate: 100,
            edge_only: true,
            custom_condition: "",
            log_period: 60,
            cdn_service_name: "",
            cdn_prefix: "",
            cdn_shield: "iad-va-us",
            cdn_url: "",
            cdn_secret: "",
            enable_cron_sync: true,
            delete_after: true,
            commit_interval_mins: 5,
            enable_cron_compact: true,
            log_fields: { groups: [], field_overrides: {} } as any,
            ngwaf_workspace_id: "",
          });
          setNgwafWorkspaces([]);
          setNgwafFetching(false);
          setNgwafFetchError("");
        }, 300);
      }
    }
  };

  // Config state
  const [config, setConfig] = useState({
    endpoint_name: "",
    fos_region: "us-east-1",
    fos_endpoint: "",
    fos_bucket_name: "",
    fos_prefix: "",
    fos_access_key: "",
    fos_secret_key: "",
    sample_rate: 100,
    edge_only: true,
    custom_condition: "",
    log_period: 60,
    cdn_service_name: "",
    cdn_prefix: "",
    cdn_shield: "iad-va-us",
    cdn_url: "",
    cdn_secret: "",
    enable_cron_sync: true,
    delete_after: true,
    commit_interval_mins: 5,
    enable_cron_compact: true,
    log_fields: { groups: [], field_overrides: {} } as any,
    ngwaf_workspace_id: "",
  });

  // Mapping from Fastly Object Storage region to Fastly Shield POP
  const SHIELD_MAP: Record<string, string> = {
    "us-east-1": "iad-va-us", // Ashburn, VA
    "us-west": "sea-wa-us", // Seattle, WA
    "us-central-1": "mdw-il-us", // Chicago, IL
    "eu-central": "fra-de-eu", // Frankfurt, Germany
    "eu-south-1": "mxp-it-eu", // Milan, Italy
    "uk-east-1": "lcy-gb-eu", // London, UK
    "jp-central-1": "tyo-jp-asia", // Tokyo, Japan
    "au-east-1": "syd-au-aus", // Sydney, Australia
  };

  const REGION_LABELS: Record<string, string> = {
    "us-east-1": "US East (Ashburn)",
    "us-west": "US West (Seattle)",
    "us-central-1": "US Central (Chicago)",
    "eu-central": "EU Central (Frankfurt)",
    "eu-south-1": "EU South (Milan)",
    "uk-east-1": "UK East (London)",
    "jp-central-1": "JP Central (Tokyo)",
    "au-east-1": "AU East (Sydney)",
  };

  const SHIELD_LABELS: Record<string, string> = {
    none: "None",
    "iad-va-us": "IAD (Ashburn)",
    "sea-wa-us": "SEA (Seattle)",
    "mdw-il-us": "MDW (Chicago)",
    "fra-de-eu": "FRA (Frankfurt)",
    "mxp-it-eu": "MXP (Milan)",
    "lcy-gb-eu": "LCY (London)",
    "tyo-jp-asia": "TYO (Tokyo)",
    "syd-au-aus": "SYD (Sydney)",
  };

  const PERIOD_LABELS: Record<string, string> = {
    "1": "1 second",
    "5": "5 seconds",
    "10": "10 seconds",
    "20": "20 seconds",
    "30": "30 seconds",
    "60": "1 minute",
    "120": "2 minutes",
    "300": "5 minutes",
  };

  // Update shield when region changes
  useEffect(() => {
    const shield = SHIELD_MAP[config.fos_region];
    if (shield && shield !== config.cdn_shield) {
      setConfig((prev) => ({ ...prev, cdn_shield: shield }));
    }
  }, [config.fos_region]);

  // Reset form when modal opens
  useEffect(() => {
    if (open) {
      setStep("mode");
      setMode(null);
      setSearch("");
      setSelectedService(null);
      setIsDeploying(false);
      setFosStatus("idle");
      setFosError("");
      reset();
      setConfig({
        endpoint_name: "",
        fos_region: "us-east-1",
        fos_endpoint: "",
        fos_bucket_name: "",
        fos_prefix: "",
        fos_access_key: "",
        fos_secret_key: "",
        sample_rate: 100,
        edge_only: true,
        custom_condition: "",
        log_period: 60,
        cdn_service_name: "",
        cdn_prefix: "",
        cdn_shield: "iad-va-us",
        cdn_url: "",
        cdn_secret: "",
        enable_cron_sync: true,
        delete_after: true,
        commit_interval_mins: 5,
        enable_cron_compact: true,
        log_fields: { groups: [], field_overrides: {} } as any,
        ngwaf_workspace_id: "",
      });
    }
  }, [open, reset]);

  const [domainStatus, setDomainStatus] = useState<
    "idle" | "checking" | "available" | "taken" | "error"
  >("idle");
  const [domainMessage, setDomainMessage] = useState("");

  // ── Step 1: Token ──
  const {
    data: servicesData,
    error: servicesError,
    isLoading: isLoadingServices,
    refetch: fetchServices,
  } = useQuery({
    queryKey: ["provision-services"],
    queryFn: async () => {
      const { data } = await client.GET("/api/provision/services", {
        params: { query: { token } },
      });
      return data as any;
    },
    enabled: false,
    retry: false,
  });

  // ── Step 4: Catalog ──
  const { data: catalog, isLoading: isLoadingCatalog } = useQuery({
    queryKey: ["services", "catalog"],
    queryFn: async () => {
      const { data } = await client.GET("/api/log-fields/catalog");
      return data as any;
    },
    enabled: step === "fields",
  });

  // Group and field toggle handlers for Provisioning
  const toggleGroup = (groupId: string, checked: boolean) => {
    setConfig((prev) => {
      const lf = { ...prev.log_fields };
      const nextGroups = new Set(lf.groups || []);
      if (checked) {
        nextGroups.add(groupId);
        let changed = true;
        while (changed) {
          changed = false;
          catalog?.groups.forEach((g: any) => {
            if (
              nextGroups.has(g.id) &&
              g.requires &&
              !nextGroups.has(g.requires)
            ) {
              nextGroups.add(g.requires);
              changed = true;
            }
          });
        }
      } else {
        nextGroups.delete(groupId);
      }
      return { ...prev, log_fields: { ...lf, groups: Array.from(nextGroups) } };
    });
  };

  const toggleField = (
    fieldId: string,
    checked: boolean,
    defaultEnabledByGroup: boolean,
  ) => {
    setConfig((prev) => {
      const lf = { ...prev.log_fields };
      const overrides = { ...(lf.field_overrides || {}) };
      if (checked === defaultEnabledByGroup) {
        delete overrides[fieldId];
      } else {
        overrides[fieldId] = checked;
      }
      return { ...prev, log_fields: { ...lf, field_overrides: overrides } };
    });
  };

  const updateFieldLimit = (fieldId: string, limit?: number) => {
    setConfig((prev) => {
      const lf = { ...prev.log_fields };
      const field_limits = { ...(lf.field_limits || {}) };
      if (limit === undefined) {
        delete field_limits[fieldId];
      } else {
        field_limits[fieldId] = limit;
      }
      return { ...prev, log_fields: { ...lf, field_limits } };
    });
  };

  const togglePreset = (presetGroups: string[]) => {
    setConfig((prev) => {
      const lf = { ...prev.log_fields };
      const currentGroups = new Set(lf.groups || []);
      const allActive = presetGroups.every((g) => currentGroups.has(g));

      const nextGroups = new Set(lf.groups || []);

      if (allActive) {
        // Toggle OFF: remove groups in this preset.
        // First, figure out which OTHER presets are currently active.
        const otherActivePresetsGroups = new Set<string>();
        if (catalog?.presets) {
          Object.entries(catalog.presets).forEach(
            ([key, preset]: [string, any]) => {
              // Don't check the preset we are currently toggling off
              // We can identify it by comparing the arrays, or checking if ALL its groups are in the current preset we're toggling
              // A safer way is: if a preset is active, and it's NOT the exact same set of groups we are toggling...
              if (
                preset.groups.length !== presetGroups.length ||
                !preset.groups.every((g: string) => presetGroups.includes(g))
              ) {
                if (isPresetActive(preset.groups)) {
                  preset.groups.forEach((g: string) =>
                    otherActivePresetsGroups.add(g),
                  );
                }
              }
            },
          );
        }

        presetGroups.forEach((g) => {
          // Only remove the group if it's NOT required by another currently active preset
          if (!otherActivePresetsGroups.has(g)) {
            nextGroups.delete(g);
            // Cascading disable: if another group depends on 'g', disable it too.
            catalog?.groups.forEach((cg: any) => {
              if (cg.requires === g && !otherActivePresetsGroups.has(cg.id)) {
                nextGroups.delete(cg.id);
              }
            });
          }
        });
      } else {
        // Toggle ON
        presetGroups.forEach((g) => nextGroups.add(g));

        let changed = true;
        while (changed) {
          changed = false;
          catalog?.groups.forEach((cg: any) => {
            if (
              nextGroups.has(cg.id) &&
              cg.requires &&
              !nextGroups.has(cg.requires)
            ) {
              nextGroups.add(cg.requires);
              changed = true;
            }
          });
        }
      }

      return { ...prev, log_fields: { ...lf, groups: Array.from(nextGroups) } };
    });
  };

  const isPresetActive = (groups: string[]) => {
    if (!groups.length) return false;
    const currentGroups = new Set(config.log_fields.groups || []);
    return groups.every((g) => currentGroups.has(g));
  };

  const estimatedBytes = React.useMemo(() => {
    if (!catalog?.fields) return 0;
    let total = 0;
    const enabledGroups = new Set(config.log_fields.groups || []);
    const overrides = config.log_fields.field_overrides || {};
    for (const field of catalog.fields) {
      const inGroup = field.group === null || enabledGroups.has(field.group);
      const override = overrides[field.id];
      if (override === true) {
        total += field.typical_bytes || 0;
        continue;
      }
      if (override === false) continue;
      if (inGroup) total += field.typical_bytes || 0;
    }
    return total;
  }, [catalog, config.log_fields]);

  const handleTokenSubmit = async () => {
    const res = await fetchServices();
    if (res.data && Array.isArray(res.data)) {
      setStep("service");
    }
  };

  // ── Step 2: Service ──
  const validateMutation = useMutation({
    mutationFn: async (serviceId: string) => {
      const { data } = await client.POST("/api/provision/validate", {
        body: { token, service_id: serviceId } as any,
      });
      return data as any;
    },
    onSuccess: (data) => {
      if (data?.token_info) {
        setTokenInfo(data.token_info);
      }
      if (data?.defaults) {
        setConfig((prev) => ({
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
      setStep(mode === "join" || mode === "ingest" ? "join" : "storage");
    },
  });

  const handleServiceSelect = (service: ProvisionService) => {
    if (service.provisioned) return;
    setSelectedService(service);
    validateMutation.mutate(service.id);
  };

  const handleCheckFos = async (vals?: {
    bucket?: string;
    region?: string;
    access_key?: string;
    secret_key?: string;
  }) => {
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
  };

  const checkDomain = async (prefix: string) => {
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
  };

  // join flow phases: form → connecting (SSE) → importing (SSE) → done
  const [joinPhase, setJoinPhase] = useState<
    "form" | "connecting" | "importing" | "done"
  >("form");
  const [joinedServiceId, setJoinedServiceId] = useState("");
  const [syncIntervalMins, setSyncIntervalMins] = useState("2");
  const [icebergMetadataLocation, setIcebergMetadataLocation] = useState("");

  const estimatedImportSize = React.useMemo(() => {
    if (!lakeInfo?.calendar) return 0;
    let total = 0;
    const start = importRange.start;
    const end = importRange.end;

    for (const [dateStr, stats] of Object.entries(lakeInfo.calendar)) {
      if (dateStr === "unknown") continue;

      if (importMode === "range") {
        if (start && dateStr < start.split("T")[0]) continue;
        if (end && dateStr > end.split("T")[0]) continue;
      }

      total += (stats as any).size_bytes || 0;
    }
    return total;
  }, [lakeInfo, importMode, importRange]);

  const handleAnalyzeLake = async () => {
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
  };

  const handleJoin = () => {
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
  };

  // Watch for join SSE completion
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
      queryClient.invalidateQueries({ queryKey: ["bootstrap"] });
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

  const handleFinishJoin = () => {
    onOpenChange(false);
    window.location.reload();
  };

  const STEPS: { id: Step; label: string }[] =
    mode === "join"
      ? [
          { id: "mode", label: "Role" },
          { id: "join", label: "Connect" },
          { id: "analyze", label: "Analyze" },
          { id: "settings", label: "Settings" },
          { id: "confirm", label: "Confirm" },
        ]
      : [
          { id: "mode", label: "Role" },
          { id: "token", label: "Auth" },
          { id: "service", label: "Service" },
          { id: "storage", label: "Storage" },
          { id: "ngwaf", label: "NGWAF" },
          { id: "fields", label: "Log Fields" },
          { id: "execute", label: "Review" },
        ];

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
      client.PATCH(
        "/api/provision/services/{service_id}/ngwaf-workspace" as any,
        {
          params: { path: { service_id: selectedService.id } },
          body: { ngwaf_workspace_id: config.ngwaf_workspace_id } as any,
        },
      );
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isDone]);

  const handleDeploy = () => {
    if (!selectedService) return;
    setIsDeploying(true);
    const params: Record<string, string> = {
      token,
      service_id: selectedService.id,
      service_name: selectedService.name,
      endpoint_name: config.endpoint_name,
      fos_region: config.fos_region,
      fos_bucket_name: config.fos_bucket_name,
      fos_prefix: config.fos_prefix,
      sample_rate: String(config.sample_rate),
      edge_only: String(config.edge_only),
      custom_condition: config.custom_condition,
      log_period: String(config.log_period),
      cdn_service_name: config.cdn_service_name,
      cdn_shield: config.cdn_shield,
      enable_cron_sync: String(config.enable_cron_sync),
      delete_after: String(config.delete_after),
      commit_interval_mins: String(config.commit_interval_mins),
      enable_cron_compact: String(config.enable_cron_compact),
      log_fields: JSON.stringify(config.log_fields),
    };
    if (config.cdn_prefix) {
      params.cdn_url = `https://${config.cdn_prefix}.global.ssl.fastly.net`;
    }
    const qs = new URLSearchParams(params).toString();
    const url = `/api/provision/execute?${qs}`;
    start(url);
  };

  const fetchTerraformPreview = async () => {
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
  };

  const handleExportTerraform = async () => {
    if (!selectedService) return;

    // Create form data manually to trigger a file download from browser
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
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) throw new Error("Export failed");

      const blob = await response.blob();
      downloadBlob(blob, "fastly-log-analysis-terraform.zip");
    } catch (e) {
      console.error("Failed to export Terraform", e);
    }
  };

  const handleAdminIngest = async () => {
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
          cdn_url: config.cdn_url || (config.cdn_prefix
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
  };

  const filteredServices = Array.isArray(servicesData)
    ? servicesData.filter(
        (s) =>
          s.name.toLowerCase().includes(search.toLowerCase()) ||
          s.id.toLowerCase().includes(search.toLowerCase()),
      )
    : [];

  return (
    <Dialog open={open} onOpenChange={handleModalClose}>
      <DialogContent
        className={cn("sm:max-w-5xl", panelDialogContent)}
        showCloseButton={status !== "streaming" && joinPhase !== "done"}
      >
        <DialogHeader className="px-6 pt-6 pb-4 border-b">
          <DialogTitle className="flex items-center gap-2 text-xl font-bold">
            <Plus className="h-5 w-5 text-primary" />
            Provision New Service
          </DialogTitle>
          <div className="flex items-center justify-center gap-4 mt-6 overflow-x-auto w-full">
            {STEPS.map((s, i) => {
              const stepIndex = STEPS.findIndex((x) => x.id === step);
              return (
                <React.Fragment key={s.id}>
                  <div className="flex items-center gap-2 shrink-0">
                    <div
                      className={cn(
                        "w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold transition-colors",
                        step === s.id
                          ? "bg-primary text-primary-foreground"
                          : stepIndex > i
                            ? "bg-green-500 text-white"
                            : "bg-muted text-muted-foreground",
                      )}
                    >
                      {stepIndex > i ? (
                        <CheckCircle2 className="w-4 h-4" />
                      ) : (
                        i + 1
                      )}
                    </div>
                    <span
                      className={cn(
                        "text-xs font-semibold whitespace-nowrap",
                        step === s.id
                          ? "text-foreground"
                          : "text-muted-foreground",
                      )}
                    >
                      {s.label}
                    </span>
                  </div>
                  {i < STEPS.length - 1 && (
                    <div className="h-px w-6 bg-muted shrink-0" />
                  )}
                </React.Fragment>
              );
            })}
          </div>
        </DialogHeader>

        <div className="flex-1 overflow-hidden flex flex-col">
          {step === "mode" && (
            <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-10 text-center animate-in fade-in slide-in-from-bottom-4 duration-500">
              <div className="space-y-3 max-w-lg">
                <h3 className="text-2xl font-bold tracking-tight">
                  Select your role
                </h3>
                <p className="text-muted-foreground leading-relaxed">
                  Choose how you want to set up this service.
                </p>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full max-w-4xl">
                <button
                  onClick={() => {
                    setMode("provision");
                    setStep("token");
                  }}
                  className="group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 hover:border-primary transition-all text-left"
                >
                  <div className="w-16 h-16 rounded-2xl bg-primary/10 flex items-center justify-center group-hover:scale-110 transition-transform">
                    <Zap className="h-8 w-8 text-primary" />
                  </div>
                  <div className="space-y-2 text-center">
                    <h4 className="font-bold text-lg">Admin: Provision</h4>
                    <p className="text-xs text-muted-foreground leading-relaxed">
                      Deploy new Fastly Object Storage resources, logging
                      endpoints, and a CDN proxy. Best for new projects.
                    </p>
                  </div>
                </button>

                <button
                  onClick={() => {
                    setMode("ingest");
                    setStep("token");
                  }}
                  className="group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 hover:border-amber-500 transition-all text-left"
                >
                  <div className="w-16 h-16 rounded-2xl bg-amber-500/10 flex items-center justify-center group-hover:scale-110 transition-transform">
                    <Settings className="h-8 w-8 text-amber-500" />
                  </div>
                  <div className="space-y-2 text-center">
                    <h4 className="font-bold text-lg">
                      Admin: Connect Terraform
                    </h4>
                    <p className="text-xs text-muted-foreground leading-relaxed">
                      Set up ingestion for a service that is already managed via
                      Terraform. We'll skip creating Fastly resources but set up
                      data sync.
                    </p>
                  </div>
                </button>

                <button
                  onClick={() => {
                    setMode("join");
                    setStep("join");
                  }}
                  className="group relative flex flex-col items-center gap-6 p-8 border-2 rounded-2xl bg-background hover:bg-muted/50 hover:border-blue-500 transition-all text-left"
                >
                  <div className="w-16 h-16 rounded-2xl bg-blue-500/10 flex items-center justify-center group-hover:scale-110 transition-transform">
                    <Search className="h-8 w-8 text-blue-500" />
                  </div>
                  <div className="space-y-2 text-center">
                    <h4 className="font-bold text-lg">Analyst: Join</h4>
                    <p className="text-xs text-muted-foreground leading-relaxed">
                      Connect to an existing project. We'll only sync the
                      processed data from the cloud. No new resources.
                    </p>
                  </div>
                </button>
              </div>
            </div>
          )}

          {step === "token" && (
            <div className="flex-1 flex flex-col items-center justify-center p-8 space-y-6 text-center">
              <div className="space-y-2 max-w-md">
                <h3 className="text-xl font-semibold tracking-tight">
                  Enter Fastly API Token
                </h3>
                <p className="text-sm text-muted-foreground leading-relaxed">
                  We need a token with <code>engineer</code> or{" "}
                  <code>superuser</code> permissions to list and configure your
                  services.
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
                    value={token}
                    onChange={(e) => setToken(e.target.value.trim())}
                    placeholder=""
                    className="font-mono text-center"
                  />
                </div>
                {servicesError && (
                  <div className="p-3 bg-destructive/10 text-destructive text-xs rounded-md border border-destructive/20 flex gap-2 animate-in fade-in slide-in-from-top-1 text-left">
                    <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                    {servicesError instanceof Error
                      ? servicesError.message
                      : "Failed to fetch services"}
                  </div>
                )}
                <Button
                  className="w-full"
                  size="lg"
                  onClick={handleTokenSubmit}
                  disabled={!token || isLoadingServices}
                >
                  {isLoadingServices && (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  )}
                  Fetch Services
                </Button>
              </div>
            </div>
          )}

          {step === "service" && (
            <div className="flex-1 flex flex-col overflow-hidden p-6 md:p-8 max-w-3xl mx-auto w-full gap-4">
              <div className="flex items-center justify-between shrink-0">
                <div className="p-2 border rounded-lg bg-muted/10 flex items-center gap-3 px-4 flex-1">
                  <Search className="h-5 w-5 text-muted-foreground" />
                  <Input
                    placeholder="Search your services..."
                    className="h-10 border-none bg-transparent shadow-none focus-visible:ring-0 text-base"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                  />
                </div>
                {tokenInfo && (
                  <div className="ml-4 flex flex-col items-end shrink-0">
                    <span className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">
                      Authenticated as
                    </span>
                    <div className="flex items-center gap-1.5">
                      <span className="text-xs font-semibold">
                        {tokenInfo.name}
                      </span>
                      <Badge
                        variant={
                          tokenInfo.type === "user" ? "default" : "outline"
                        }
                        className="text-[9px] h-3.5 px-1 uppercase"
                      >
                        {tokenInfo.type}
                      </Badge>
                    </div>
                  </div>
                )}
              </div>
              <div className="flex-1 overflow-y-auto min-h-0 border rounded-lg shadow-sm">
                <div className="divide-y divide-muted/50 bg-background">
                  {filteredServices.length > 0 ? (
                    filteredServices.map((s) => (
                      <div
                        key={s.id}
                        className={cn(
                          "p-4 flex items-center justify-between transition-all",
                          s.provisioned
                            ? "opacity-40 grayscale bg-muted/5 cursor-not-allowed"
                            : "hover:bg-muted/50 cursor-pointer active:bg-muted",
                        )}
                        onClick={() => !s.provisioned && handleServiceSelect(s)}
                      >
                        <div className="space-y-1">
                          <div className="font-semibold text-sm flex items-center gap-2">
                            {s.name}
                            {s.provisioned && (
                              <Badge
                                variant="secondary"
                                className="text-[10px] h-4 px-1 leading-none font-bold uppercase tracking-tight"
                              >
                                Active
                              </Badge>
                            )}
                          </div>
                          <div className="text-xs font-mono text-muted-foreground">
                            {s.id}
                          </div>
                        </div>
                        {!s.provisioned && (
                          <div className="flex items-center text-primary">
                            {validateMutation.isPending &&
                            selectedService?.id === s.id ? (
                              <Loader2 className="h-5 w-5 animate-spin" />
                            ) : (
                              <ChevronRight className="h-5 w-5" />
                            )}
                          </div>
                        )}
                      </div>
                    ))
                  ) : (
                    <div className="py-12 text-center text-muted-foreground text-sm italic">
                      No services found matching &quot;{search}&quot;
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {step === "storage" && (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
                {/* Section: Logging */}
                <div className="space-y-5">
                  <SectionHeader title="Logging Setup" icon={Zap} />
                  <div className="grid grid-cols-2 gap-6">
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="Endpoint Name"
                        info="The name of the logging endpoint that will be created on your Fastly service. This is just for your reference."
                      />
                      <Input
                        value={config.endpoint_name}
                        onChange={(e) =>
                          setConfig({
                            ...config,
                            endpoint_name: e.target.value,
                          })
                        }
                        className="h-9"
                      />
                    </div>
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="FOS Region"
                        info="The geographical region where your Fastly Object Storage bucket will be created. We recommend matching this with your primary user base."
                      />
                      <Select
                        value={config.fos_region}
                        onValueChange={(v) =>
                          v && setConfig({ ...config, fos_region: v })
                        }
                      >
                        <SelectTrigger className="h-9">
                          <SelectValue>
                            {(val) => REGION_LABELS[String(val)] || val}
                          </SelectValue>
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="us-east-1">
                            US East (Ashburn)
                          </SelectItem>{" "}
                          <SelectItem value="us-west">
                            US West (Seattle)
                          </SelectItem>
                          <SelectItem value="us-central-1">
                            US Central (Chicago)
                          </SelectItem>
                          <SelectItem value="eu-central">
                            EU Central (Frankfurt)
                          </SelectItem>
                          <SelectItem value="eu-south-1">
                            EU South (Milan)
                          </SelectItem>
                          <SelectItem value="uk-east-1">
                            UK East (London)
                          </SelectItem>
                          <SelectItem value="jp-central-1">
                            JP Central (Tokyo)
                          </SelectItem>
                          <SelectItem value="au-east-1">
                            AU East (Sydney)
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-6">
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="Bucket Name"
                        info="The name of the Fastly Object Storage bucket. Must be unique across all Fastly customers."
                      />
                      <Input
                        value={config.fos_bucket_name}
                        onChange={(e) =>
                          setConfig({
                            ...config,
                            fos_bucket_name: e.target.value.toLowerCase(),
                          })
                        }
                        className="h-9 font-mono text-sm"
                      />
                    </div>
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="Log Period"
                        info="How often Fastly will write log files to the bucket. A shorter period means more real-time data but creates more files."
                      />
                      <Select
                        value={String(config.log_period)}
                        onValueChange={(v) =>
                          setConfig({ ...config, log_period: Number(v) || 60 })
                        }
                      >
                        <SelectTrigger className="h-9">
                          <SelectValue>
                            {(val) => PERIOD_LABELS[String(val)] || val}
                          </SelectValue>
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="1">1 second</SelectItem>
                          <SelectItem value="5">5 seconds</SelectItem>
                          <SelectItem value="10">10 seconds</SelectItem>
                          <SelectItem value="20">20 seconds</SelectItem>
                          <SelectItem value="30">30 seconds</SelectItem>
                          <SelectItem value="60">1 minute</SelectItem>
                          <SelectItem value="120">2 minutes</SelectItem>
                          <SelectItem value="300">5 minutes</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-6 items-center">
                    <div className="flex items-center justify-between p-3 border rounded-md bg-muted/10">
                      <div className="space-y-0.5">
                        <LabelWithInfo
                          label="Edge Only"
                          info="When enabled, only edge nodes write logs, skipping shield nodes and cache restarts. This prevents duplicate log entries."
                        />
                        <p className="text-[10px] text-muted-foreground">
                          Skip shield/restart logs
                        </p>
                      </div>
                      <Switch
                        checked={config.edge_only}
                        onCheckedChange={(v) =>
                          setConfig({ ...config, edge_only: v })
                        }
                      />
                    </div>
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="Sample Rate (%)"
                        info="The percentage of requests to log. Set to 100% to log everything, or lower it for high-traffic services to save storage."
                      />
                      <Input
                        type="number"
                        min={1}
                        max={100}
                        value={config.sample_rate}
                        onChange={(e) =>
                          setConfig({
                            ...config,
                            sample_rate: Number(e.target.value),
                          })
                        }
                        className="h-9"
                      />
                    </div>
                  </div>
                  <div className="space-y-1.5">
                    <LabelWithInfo
                      htmlFor="customCondition"
                      label="Optional Log Condition"
                      info="An additional VCL condition to filter logs (e.g., req.url !~ '\.(jpg|png)$'). The expression will be wrapped in parentheses and added to the logging condition logic."
                    />
                    <Input
                      id="customCondition"
                      placeholder="e.g. std.tolower(req.url) !~ '\.(jpg|png|css|js)$'"
                      value={config.custom_condition}
                      onChange={(e) =>
                        setConfig({
                          ...config,
                          custom_condition: e.target.value,
                        })
                      }
                      className="h-9 font-mono text-xs"
                    />
                  </div>
                </div>

                {/* Section: CDN Access */}
                <div className="space-y-5">
                  <SectionHeader title="CDN Performance Front" icon={Globe} />
                  <p className="text-xs text-muted-foreground leading-relaxed">
                    Highly recommended. Provision a secondary Fastly service to
                    front the Object Storage bucket for faster dashboard queries
                    and secure access.
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
                            value={config.cdn_prefix}
                            onChange={(e) =>
                              setConfig({
                                ...config,
                                cdn_prefix: e.target.value.toLowerCase(),
                              })
                            }
                            className={cn(
                              "h-9 font-mono text-sm",
                              domainStatus === "available" &&
                                "border-green-500 focus-visible:ring-green-500",
                              domainStatus === "taken" &&
                                "border-red-500 focus-visible:ring-red-500",
                            )}
                          />
                          <span className="text-[10px] font-mono text-muted-foreground opacity-70">
                            .global.ssl.fastly.net
                          </span>
                          <Button
                            variant="outline"
                            size="sm"
                            className="h-9 px-3 shrink-0 text-xs"
                            onClick={() => checkDomain(config.cdn_prefix)}
                            disabled={
                              domainStatus === "checking" || !config.cdn_prefix
                            }
                            title="Check Domain Availability"
                          >
                            <Search className="h-4 w-4 mr-1.5" />
                            Check Domain
                          </Button>
                        </div>
                        {domainStatus === "checking" && (
                          <p className="text-[10px] animate-pulse text-muted-foreground">
                            Checking availability...
                          </p>
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
                      <Select
                        value={config.cdn_shield}
                        onValueChange={(v) =>
                          v && setConfig({ ...config, cdn_shield: v })
                        }
                      >
                        <SelectTrigger className="h-9">
                          <SelectValue>
                            {(val) => SHIELD_LABELS[String(val)] || val}
                          </SelectValue>
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="none">None</SelectItem>
                          <SelectItem value="iad-va-us">
                            IAD (Ashburn)
                          </SelectItem>
                          <SelectItem value="sea-wa-us">
                            SEA (Seattle)
                          </SelectItem>
                          <SelectItem value="mdw-il-us">
                            MDW (Chicago)
                          </SelectItem>
                          <SelectItem value="fra-de-eu">
                            FRA (Frankfurt)
                          </SelectItem>
                          <SelectItem value="mxp-it-eu">MXP (Milan)</SelectItem>
                          <SelectItem value="lcy-gb-eu">
                            LCY (London)
                          </SelectItem>
                          <SelectItem value="tyo-jp-asia">
                            TYO (Tokyo)
                          </SelectItem>
                          <SelectItem value="syd-au-aus">
                            SYD (Sydney)
                          </SelectItem>
                        </SelectContent>
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
                        <LabelWithInfo
                          label="Background Sync"
                          info={`Automatically polls FOS for new log files (every ${config.log_period >= 120 ? Math.floor(config.log_period / 120) + " min" : config.log_period >= 60 ? Math.floor(config.log_period / 2) + "s" : Math.max(10, config.log_period) + "s"}) and writes them into the local buffer. The buffer is then committed to the shared Iceberg table at the Cloud Commit Interval below.`}
                        />
                        <p className="text-[10px] text-muted-foreground">
                          Polls FOS every{" "}
                          {config.log_period >= 120
                            ? Math.floor(config.log_period / 120) + "m"
                            : config.log_period >= 60
                              ? Math.floor(config.log_period / 2) + "s"
                              : Math.max(10, config.log_period) + "s"}
                        </p>{" "}
                      </div>
                      <Switch
                        checked={config.enable_cron_sync}
                        onCheckedChange={(v) =>
                          setConfig({ ...config, enable_cron_sync: v })
                        }
                      />
                    </div>
                    <div
                      className={cn(
                        "flex items-center justify-between p-3 border rounded-md bg-muted/5 transition-opacity",
                        !config.enable_cron_sync &&
                          "opacity-30 pointer-events-none",
                      )}
                    >
                      <div className="space-y-0.5">
                        <LabelWithInfo
                          label="Auto-Delete Raw Logs"
                          info="Deletes the raw .gz log files from FOS after they are ingested into Iceberg. Recommended — the Iceberg table holds the same data in a more efficient format."
                        />
                        <p className="text-[10px] text-muted-foreground">
                          Remove .gz files after ingest
                        </p>
                      </div>
                      <Switch
                        checked={config.delete_after}
                        onCheckedChange={(v) =>
                          setConfig({ ...config, delete_after: v })
                        }
                      />
                    </div>
                  </div>

                  {/* Cloud commit interval — separate row, full width */}
                  <div
                    className={cn(
                      "p-4 border rounded-md bg-muted/5 space-y-3 transition-opacity",
                      !config.enable_cron_sync &&
                        "opacity-30 pointer-events-none",
                    )}
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="space-y-1">
                        <LabelWithInfo
                          label="Cloud Commit Interval"
                          info="How often the local buffer is flushed to the shared Iceberg table in Fastly Object Storage. This determines how quickly data becomes visible to other users or tools querying the Iceberg table directly. More frequent commits mean fresher data but create more small files — the daily Iceberg optimization consolidates them."
                        />
                        <p className="text-[10px] text-muted-foreground leading-relaxed">
                          Controls data freshness for shared access. Every
                          commit creates one Iceberg snapshot in FOS.
                        </p>
                      </div>
                      <Select
                        value={String(config.commit_interval_mins)}
                        onValueChange={(v) =>
                          v &&
                          setConfig({
                            ...config,
                            commit_interval_mins: Number(v),
                          })
                        }
                      >
                        <SelectTrigger className="h-8 w-[220px] shrink-0 text-xs">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="1" className="text-xs">
                            Every 1 min — most real-time
                          </SelectItem>
                          <SelectItem value="2" className="text-xs">
                            Every 2 min
                          </SelectItem>
                          <SelectItem value="3" className="text-xs">
                            Every 3 min
                          </SelectItem>
                          <SelectItem value="5" className="text-xs">
                            Every 5 min — recommended
                          </SelectItem>
                          <SelectItem value="15" className="text-xs">
                            Every 15 min
                          </SelectItem>
                          <SelectItem value="30" className="text-xs">
                            Every 30 min
                          </SelectItem>
                          <SelectItem value="60" className="text-xs">
                            Every 60 min — fewest snapshots
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="text-[10px] text-muted-foreground bg-muted/30 rounded px-3 py-2 leading-relaxed">
                      With a{" "}
                      {config.log_period >= 60
                        ? config.log_period / 60 + "-minute"
                        : config.log_period + "-second"}{" "}
                      log period and a {config.commit_interval_mins}-minute
                      commit interval, the system will create ~
                      {Math.round(1440 / config.commit_interval_mins)} Iceberg
                      snapshots per day before the daily optimization
                      consolidates them.
                    </div>
                  </div>

                  <div
                    className={cn(
                      "flex items-center justify-between p-3 border rounded-md bg-muted/5 transition-opacity",
                      !config.enable_cron_sync &&
                        "opacity-30 pointer-events-none",
                    )}
                  >
                    <div className="space-y-0.5">
                      <LabelWithInfo
                        label="Daily Iceberg Optimization"
                        info="Every night at 03:00 UTC, rewrites many small Iceberg snapshot files into larger, optimized Parquet files. This keeps query speed fast and controls FOS storage costs. Strongly recommended when using frequent commit intervals."
                      />
                      <p className="text-[10px] text-muted-foreground">
                        Runs at 03:00 UTC — consolidates daily snapshots
                      </p>
                    </div>
                    <Switch
                      checked={config.enable_cron_compact}
                      onCheckedChange={(v) =>
                        setConfig({ ...config, enable_cron_compact: v })
                      }
                    />
                  </div>
                </div>
              </div>
            </div>
          )}
          {step === "join" &&
            (joinPhase === "connecting" || joinPhase === "done") && (
              <div className="flex-1 overflow-y-auto min-h-0 p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
                <div className="text-center space-y-1">
                  <h3 className="text-lg font-semibold tracking-tight">
                    {joinPhase === "connecting"
                      ? `Connecting to ${config.endpoint_name}`
                      : "Setup Complete"}
                  </h3>
                  <p className="text-sm text-muted-foreground">
                    {joinPhase === "connecting"
                      ? "Please wait while we secure your connection and import initial data."
                      : "Your service is connected and the initial data import is complete."}
                  </p>
                </div>
                <SSEProgressView
                  lines={lines}
                  status={status}
                  error={sseError}
                  className="h-[320px]"
                  progressLabel="Progress"
                  doneMessage=""
                />
              </div>
            )}

          {step === "join" && joinPhase === "form" && (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div
                className={cn(
                  "p-8 space-y-10 pb-12 max-w-3xl mx-auto transition-opacity duration-300",
                  isAnalyzing && "pointer-events-none opacity-50",
                )}
              >
                <div className="space-y-5">
                  <SectionHeader
                    title="Connect to Existing Storage"
                    icon={Database}
                  />
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    {mode === "ingest"
                      ? "Enter the credentials for your existing Fastly Object Storage bucket and CDN proxy. We will use these to set up background ingestion."
                      : "Enter the Fastly Object Storage credentials for the service you want to analyze, or paste the JSON config your admin shared with you."}
                  </p>

                  <JsonImportSection
                    onImport={(parsed) => {
                      setConfig((prev) => ({
                        ...prev,
                        endpoint_name: parsed.name ?? prev.endpoint_name,
                        cdn_service_name:
                          parsed.cdn_service_id ??
                          parsed.service_id ??
                          prev.cdn_service_name,
                        fos_bucket_name:
                          parsed.fos_bucket ?? prev.fos_bucket_name,
                        fos_region: parsed.fos_region ?? prev.fos_region,
                        fos_endpoint: parsed.fos_endpoint ?? prev.fos_endpoint,
                        fos_prefix: parsed.fos_prefix ?? prev.fos_prefix,
                        fos_access_key:
                          parsed.access_key_id ??
                          parsed.fos_key_id ??
                          prev.fos_access_key,
                        fos_secret_key:
                          parsed.secret_key ??
                          parsed.fos_secret_key ??
                          prev.fos_secret_key,
                        cdn_url: parsed.cdn_url ?? prev.cdn_url,
                        cdn_secret: parsed.cdn_secret ?? prev.cdn_secret,
                      }));
                      if (parsed.iceberg_metadata_location) {
                        setIcebergMetadataLocation(
                          parsed.iceberg_metadata_location,
                        );
                      }
                      handleCheckFos({
                        bucket: parsed.fos_bucket,
                        region: parsed.fos_region,
                        access_key: parsed.access_key_id ?? parsed.fos_key_id,
                        secret_key: parsed.secret_key ?? parsed.fos_secret_key,
                      });
                    }}
                  />
                  <div className="grid grid-cols-2 gap-6 pt-2">
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label={mode === "ingest" ? "Logging Service" : "Display Name"}
                        info={mode === "ingest" ? "The Fastly service that is streaming logs to Object Storage." : "A friendly name for this service in your local dashboard."}
                      />
                      {mode === "ingest" ? (
                        <Select
                          value={selectedService?.id || ""}
                          onValueChange={(id) => {
                            const svc = (servicesData as any[]).find(s => s.id === id);
                            if (svc) setSelectedService(svc);
                          }}
                        >
                          <SelectTrigger className="h-9 font-mono text-sm">
                            <SelectValue placeholder="Select logging service..." />
                          </SelectTrigger>
                          <SelectContent>
                            {(servicesData as any[])?.map((svc) => (
                              <SelectItem key={svc.id} value={svc.id}>
                                {svc.name} ({svc.id})
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      ) : (
                        <Input
                          value={config.endpoint_name}
                          onChange={(e) =>
                            setConfig({
                              ...config,
                              endpoint_name: e.target.value,
                            })
                          }
                          className="h-9 font-mono text-sm"
                          placeholder="e.g. Production Logs"
                        />
                      )}
                    </div>
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label={mode === "ingest" ? "CDN Proxy Service" : "Fastly Service ID"}
                        info={mode === "ingest" ? "The Fastly service used to front the Object Storage bucket." : "The Fastly Service ID you are pulling logs for."}
                      />
                      {mode === "ingest" ? (
                        <Select
                          value={selectedCdnService?.id || ""}
                          onValueChange={(id) => {
                            const svc = (servicesData as any[]).find(s => s.id === id);
                            if (svc) setSelectedCdnService(svc);
                          }}
                        >
                          <SelectTrigger className="h-9 font-mono text-sm">
                            <SelectValue placeholder="Select CDN service..." />
                          </SelectTrigger>
                          <SelectContent>
                            {(servicesData as any[])?.map((svc) => (
                              <SelectItem key={svc.id} value={svc.id}>
                                {svc.name} ({svc.id})
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      ) : (
                        <Input
                          value={config.cdn_service_name}
                          onChange={(e) =>
                            setConfig({
                              ...config,
                              cdn_service_name: e.target.value,
                            })
                          }
                          className="h-9 font-mono text-sm"
                          placeholder="e.g. 5xXj0O1P2R..."
                        />
                      )}
                    </div>
                  </div>

                  {mode === "ingest" && (
                    <div className="space-y-4 pt-2 border-t">
                      <div className="flex items-center justify-between">
                        <div className="text-sm text-muted-foreground italic">
                          We will verify that both services have the correct resources and VCL snippets.
                        </div>
                        <Button
                          variant="secondary"
                          size="sm"
                          disabled={isCheckingConfig || !selectedService || !selectedCdnService || !config.fos_bucket_name}
                          onClick={handleCheckConfig}
                        >
                          {isCheckingConfig && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                          Verify Configuration
                        </Button>
                      </div>

                      {configStatus && (
                        <div className="grid grid-cols-2 gap-4">
                          <div className={cn(
                            "p-3 rounded-lg border text-xs space-y-1",
                            configStatus.logging_service.ok ? "bg-emerald-500/5 border-emerald-500/20" : "bg-destructive/5 border-destructive/20"
                          )}>
                            <div className="flex items-center gap-2 font-bold">
                              {configStatus.logging_service.ok ? <CheckCircle2 className="w-3 h-3 text-emerald-500" /> : <XCircle className="w-3 h-3 text-destructive" />}
                              Logging Service
                            </div>
                            <p className="text-muted-foreground leading-relaxed">{configStatus.logging_service.details}</p>
                          </div>
                          <div className={cn(
                            "p-3 rounded-lg border text-xs space-y-1",
                            configStatus.cdn_service.ok ? "bg-emerald-500/5 border-emerald-500/20" : "bg-destructive/5 border-destructive/20"
                          )}>
                            <div className="flex items-center gap-2 font-bold">
                              {configStatus.cdn_service.ok ? <CheckCircle2 className="w-3 h-3 text-emerald-500" /> : <XCircle className="w-3 h-3 text-destructive" />}
                              CDN Proxy Service
                            </div>
                            <p className="text-muted-foreground leading-relaxed">{configStatus.cdn_service.details}</p>
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  <div className="grid grid-cols-2 gap-6">
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="FOS Bucket Name"
                        info="The name of the existing Fastly Object Storage bucket."
                      />
                      <Input
                        value={config.fos_bucket_name}
                        onChange={(e) =>
                          setConfig({
                            ...config,
                            fos_bucket_name: e.target.value.toLowerCase(),
                          })
                        }
                        className="h-9 font-mono text-sm"
                        placeholder="e.g. my-service-logs"
                      />
                    </div>
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="FOS Region"
                        info="The region where the bucket is located."
                      />
                      <Select
                        value={config.fos_region}
                        onValueChange={(v) =>
                          v && setConfig({ ...config, fos_region: v })
                        }
                      >
                        <SelectTrigger className="h-9">
                          <SelectValue>
                            {(val) => REGION_LABELS[String(val)] || val}
                          </SelectValue>
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="us-east-1">
                            US East (Ashburn)
                          </SelectItem>
                          <SelectItem value="us-west">
                            US West (Seattle)
                          </SelectItem>
                          <SelectItem value="us-central-1">
                            US Central (Chicago)
                          </SelectItem>
                          <SelectItem value="eu-central">
                            EU Central (Frankfurt)
                          </SelectItem>
                          <SelectItem value="eu-south-1">
                            EU South (Milan)
                          </SelectItem>
                          <SelectItem value="uk-east-1">
                            UK East (London)
                          </SelectItem>
                          <SelectItem value="jp-central-1">
                            JP Central (Tokyo)
                          </SelectItem>
                          <SelectItem value="au-east-1">
                            AU East (Sydney)
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  <div className="space-y-1.5">
                    <LabelWithInfo
                      label="Iceberg Metadata Location (Optional)"
                      info="The full S3 URI to the latest .metadata.json file. Required for analysts without ListBucket permissions. If you used an invite link or JSON export, this is filled automatically."
                    />
                    <Input
                      value={icebergMetadataLocation}
                      onChange={(e) =>
                        setIcebergMetadataLocation(e.target.value)
                      }
                      className="h-9 font-mono text-xs"
                      placeholder="s3://bucket/iceberg/default/logs/metadata/..."
                    />
                  </div>

                  <div className="grid grid-cols-2 gap-6">
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="Access Key"
                        info="An access key with read permissions for the bucket."
                      />
                      <Input
                        value={config.fos_access_key || ""}
                        onChange={(e) =>
                          setConfig({
                            ...config,
                            fos_access_key: e.target.value,
                          })
                        }
                        className="h-9 font-mono text-sm"
                        placeholder="e.g. AKIA..."
                      />
                    </div>
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="Secret Key"
                        info="The secret key associated with the access key."
                      />
                      <Input
                        type="password"
                        value={config.fos_secret_key || ""}
                        onChange={(e) =>
                          setConfig({
                            ...config,
                            fos_secret_key: e.target.value,
                          })
                        }
                        className="h-9 font-mono text-sm"
                        placeholder="e.g. wJalrXUtnFEMI..."
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-6">
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="CDN API URL (Optional)"
                        info="The Fastly CDN URL used to proxy API requests (bypasses CORS)."
                      />
                      <Input
                        value={config.cdn_url || ""}
                        onChange={(e) =>
                          setConfig({ ...config, cdn_url: e.target.value })
                        }
                        className="h-9 font-mono text-sm"
                        placeholder="e.g. https://fos-xyz.global.ssl.fastly.net"
                      />
                    </div>
                    <div className="space-y-1.5">
                      <LabelWithInfo
                        label="CDN Secret (Optional)"
                        info="The pre-shared secret required by the CDN API proxy."
                      />
                      <Input
                        type="password"
                        value={config.cdn_secret || ""}
                        onChange={(e) =>
                          setConfig({ ...config, cdn_secret: e.target.value })
                        }
                        className="h-9 font-mono text-sm"
                        placeholder="e.g. s3cr3t..."
                      />
                    </div>
                  </div>
                </div>

                <div className="space-y-4 pt-4 border-t">
                  <div className="flex items-center justify-between">
                    {fosStatus === "idle" || fosStatus === "checking" ? (
                      <div className="text-sm text-muted-foreground">
                        Please verify your credentials before connecting.
                      </div>
                    ) : fosStatus === "success" ? (
                      <div className="flex items-center gap-2 text-emerald-500 font-semibold">
                        <CheckCircle2 className="h-5 w-5" />
                        <h4>Ready to Connect</h4>
                      </div>
                    ) : (
                      <div className="flex items-center gap-2 text-destructive font-semibold">
                        <div className="h-5 w-5 rounded-full bg-destructive/10 flex items-center justify-center text-xs">
                          !
                        </div>
                        <h4>Connection Failed</h4>
                      </div>
                    )}

                    <Button
                      variant={
                        fosStatus === "success" ? "outline" : "secondary"
                      }
                      size="sm"
                      onClick={() => handleCheckFos()}
                      disabled={
                        fosStatus === "checking" ||
                        !config.fos_bucket_name ||
                        !config.fos_region ||
                        !config.fos_access_key ||
                        !config.fos_secret_key
                      }
                    >
                      {fosStatus === "checking" && (
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      )}
                      Verify Access
                    </Button>
                  </div>

                  {fosStatus === "error" && (
                    <div className="text-sm text-destructive bg-destructive/10 p-3 rounded-md">
                      {fosError}
                    </div>
                  )}

                  {fosStatus === "success" && (
                    <p className="text-xs text-muted-foreground leading-relaxed animate-in fade-in slide-in-from-top-1">
                      {mode === "ingest" ? (
                        <>
                          We will connect to this service in{" "}
                          <strong>Admin</strong> mode. We will set up
                          background ingestion and metadata management.
                        </>
                      ) : (
                        <>
                          We will connect to this service in{" "}
                          <strong>Read-Only</strong> mode. We will not create
                          any resources or modify your logging configuration.
                        </>
                      )}
                    </p>
                  )}
                </div>
              </div>
            </div>
          )}

          {step === "analyze" && (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div className="p-8 space-y-8 pb-12 max-w-3xl mx-auto">
                <div className="space-y-4">
                  <SectionHeader title="Analyze Data Lake" icon={Search} />
                  {lakeInfo?.table_exists ? (
                    <div className="space-y-6">
                      <div className="bg-emerald-500/5 border border-emerald-500/20 rounded-xl p-6 space-y-4">
                        <div className="flex items-center gap-3 text-emerald-600 dark:text-emerald-400">
                          <CheckCircle2 className="h-6 w-6" />
                          <h4 className="text-lg font-bold">
                            Found existing Iceberg Table
                          </h4>
                        </div>
                        <p className="text-sm text-muted-foreground leading-relaxed">
                          We found an active data lake in this bucket with{" "}
                          <strong>{lakeInfo.info.data_files}</strong> data files
                          and <strong>{lakeInfo.info.snapshots}</strong>{" "}
                          snapshots.
                        </p>

                        <div className="grid grid-cols-2 gap-4 pt-2">
                          <div className="bg-background/50 border rounded-lg p-4 space-y-1">
                            <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                              Available From
                            </span>
                            <div className="flex flex-col font-mono text-sm font-semibold">
                              <div className="flex items-center gap-2">
                                <Calendar className="h-3.5 w-3.5 text-primary" />
                                {formatDateTime(lakeInfo.range.start, timezone)}
                              </div>
                            </div>
                          </div>
                          <div className="bg-background/50 border rounded-lg p-4 space-y-1">
                            <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                              Available To
                            </span>
                            <div className="flex flex-col font-mono text-sm font-semibold">
                              <div className="flex items-center gap-2">
                                <Calendar className="h-3.5 w-3.5 text-primary" />
                                {formatDateTime(lakeInfo.range.end, timezone)}
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="space-y-4">
                        <div className="flex items-center justify-between">
                          <LabelWithInfo
                            label="Data Import Strategy"
                            info="Choose how much historical data you want to sync to your local machine. You can always sync more later."
                          />
                          <Badge
                            variant="secondary"
                            className="font-mono bg-muted/50 border shadow-sm"
                          >
                            ~{formatBytes(estimatedImportSize)}
                          </Badge>
                        </div>
                        <div className="grid grid-cols-2 gap-4">
                          <button
                            onClick={() => setImportMode("all")}
                            className={cn(
                              "flex flex-col items-center gap-3 p-6 border-2 rounded-xl transition-all text-left",
                              importMode === "all"
                                ? "border-primary bg-primary/5 ring-4 ring-primary/10"
                                : "border-muted hover:bg-muted/50",
                            )}
                          >
                            <Database className="h-6 w-6 text-primary" />
                            <div className="text-center">
                              <div className="font-bold text-sm">
                                Import All Data
                              </div>
                              <p className="text-[10px] text-muted-foreground mt-1">
                                Sync every available log file
                              </p>
                            </div>
                          </button>
                          <button
                            onClick={() => setImportMode("range")}
                            className={cn(
                              "flex flex-col items-center gap-3 p-6 border-2 rounded-xl transition-all text-left",
                              importMode === "range"
                                ? "border-primary bg-primary/5 ring-4 ring-primary/10"
                                : "border-muted hover:bg-muted/50",
                            )}
                          >
                            <Calendar className="h-6 w-6 text-primary" />
                            <div className="text-center">
                              <div className="font-bold text-sm">
                                Select Range
                              </div>
                              <p className="text-[10px] text-muted-foreground mt-1">
                                Choose specific dates to import
                              </p>
                            </div>
                          </button>
                        </div>
                      </div>

                      {importMode === "range" && (
                        <div className="p-6 border rounded-xl bg-muted/5 space-y-4 animate-in fade-in slide-in-from-top-2">
                          <div className="flex items-center gap-4">
                            <div className="space-y-1.5 flex-1">
                              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
                                Start Time
                              </Label>
                              <Input
                                type="datetime-local"
                                step="1"
                                value={formatForInput(
                                  importRange.start,
                                  timezone,
                                )}
                                min={formatForInput(
                                  lakeInfo.range.start,
                                  timezone,
                                )}
                                max={formatForInput(
                                  importRange.end || lakeInfo.range.end,
                                  timezone,
                                )}
                                onChange={(e) =>
                                  setImportRange((prev) => ({
                                    ...prev,
                                    start:
                                      parseFromInput(
                                        e.target.value,
                                        timezone,
                                      ) ?? "",
                                  }))
                                }
                                className="h-9 font-mono"
                              />
                            </div>
                            <ArrowRight className="h-4 w-4 text-muted-foreground mt-6" />
                            <div className="space-y-1.5 flex-1">
                              <Label className="text-xs font-bold uppercase tracking-wider text-muted-foreground">
                                End Time
                              </Label>
                              <Input
                                type="datetime-local"
                                step="1"
                                value={formatForInput(
                                  importRange.end,
                                  timezone,
                                )}
                                min={formatForInput(
                                  importRange.start || lakeInfo.range.start,
                                  timezone,
                                )}
                                max={formatForInput(
                                  lakeInfo.range.end,
                                  timezone,
                                )}
                                onChange={(e) =>
                                  setImportRange((prev) => ({
                                    ...prev,
                                    end:
                                      parseFromInput(
                                        e.target.value,
                                        timezone,
                                      ) ?? "",
                                  }))
                                }
                                className="h-9 font-mono"
                              />
                            </div>
                          </div>
                          <div className="flex items-center justify-between mt-2 pt-2 border-t border-muted/50">
                            <p className="text-[10px] text-muted-foreground italic">
                              Only data between these times will be downloaded
                              initially.
                            </p>
                          </div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="p-12 border border-dashed rounded-xl bg-muted/5 text-center space-y-4">
                      <div className="mx-auto w-12 h-12 rounded-full bg-amber-500/10 flex items-center justify-center">
                        <AlertCircle className="h-6 w-6 text-amber-500" />
                      </div>
                      <div className="space-y-1">
                        <h4 className="font-bold">No Data Found</h4>
                        <p className="text-sm text-muted-foreground max-w-xs mx-auto">
                          We couldn't find an Iceberg table in this bucket. The
                          admin might not have started the ingestion yet.
                        </p>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        You can still connect, but the dashboard will be empty
                        until data is available.
                      </p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {step === "settings" && (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div className="p-8 space-y-10 pb-12 max-w-3xl mx-auto">
                <div className="space-y-6">
                  <SectionHeader title="Ingestion Settings" icon={Settings} />
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Configure how you want to handle ongoing updates from the
                    data lake.
                  </p>

                  <div className="bg-muted/5 border rounded-xl overflow-hidden divide-y">
                    <div className="p-6 flex items-center justify-between gap-8">
                      <div className="space-y-1 flex-1">
                        <div className="flex items-center gap-2">
                          <Label className="text-sm font-bold tracking-tight">
                            Auto-Sync New Data
                          </Label>
                          <Badge
                            variant="secondary"
                            className="text-[9px] uppercase h-4"
                          >
                            Recommended
                          </Badge>
                        </div>
                        <p className="text-xs text-muted-foreground leading-relaxed">
                          Automatically poll for and download new processed log
                          files as they are committed to the cloud.
                        </p>
                      </div>
                      <Switch
                        checked={syncEnabled}
                        onCheckedChange={setSyncEnabled}
                      />
                    </div>

                    {syncEnabled && (
                      <div className="p-6 space-y-4 bg-background/30 animate-in fade-in slide-in-from-top-1">
                        <div className="flex items-start justify-between gap-8">
                          <div className="space-y-1">
                            <Label className="text-sm font-bold tracking-tight">
                              Cloud Sync Interval
                            </Label>
                            <p className="text-xs text-muted-foreground leading-relaxed">
                              How often to check for new cloud commits. More
                              frequent = fresher data.
                            </p>
                          </div>
                          <Select
                            value={syncIntervalMins}
                            onValueChange={(v) => v && setSyncIntervalMins(v)}
                          >
                            <SelectTrigger className="h-9 w-[180px] shrink-0">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="1">Every 1 min</SelectItem>
                              <SelectItem value="2">Every 2 mins</SelectItem>
                              <SelectItem value="5">Every 5 mins</SelectItem>
                              <SelectItem value="15">Every 15 mins</SelectItem>
                              <SelectItem value="30">Every 30 mins</SelectItem>
                              <SelectItem value="60">Every 60 mins</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      </div>
                    )}
                  </div>

                  {!syncEnabled && (
                    <div className="p-4 rounded-lg bg-amber-500/5 border border-amber-500/20 flex items-start gap-3">
                      <Info className="h-4 w-4 text-amber-500 mt-0.5" />
                      <p className="text-[11px] text-amber-700 dark:text-amber-400 leading-normal">
                        With auto-sync disabled, your local dashboard will only
                        show the data you import now. You will need to manually
                        trigger a sync later to see newer logs.
                      </p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {step === "confirm" && (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div className="p-8 space-y-8 pb-12 max-w-4xl mx-auto text-left">
                <div className="text-center space-y-2">
                  <h3 className="text-2xl font-bold tracking-tight">
                    Confirm Connection
                  </h3>
                  <p className="text-sm text-muted-foreground leading-relaxed">
                    Review your connection and import settings before
                    continuing.
                  </p>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <ReviewCard>
                    <ReviewHeader icon={Cloud}>Target Service</ReviewHeader>
                    <ReviewContent>
                      <ReviewItem
                        label="Service Name"
                        value={config.endpoint_name}
                      />
                      <ReviewItem
                        label="Service ID"
                        value={config.cdn_service_name}
                      />
                      <ReviewItem label="Mode" value="Read-Only Analyst" />
                    </ReviewContent>
                  </ReviewCard>

                  <ReviewCard>
                    <ReviewHeader icon={Database}>Data Lake</ReviewHeader>
                    <ReviewContent>
                      <ReviewItem
                        label="Bucket"
                        value={config.fos_bucket_name}
                      />
                      <ReviewItem label="Region" value={config.fos_region} />
                      <ReviewItem
                        label="Existing Data"
                        value={
                          lakeInfo?.table_exists ? "Available" : "Not Found"
                        }
                      />
                    </ReviewContent>
                  </ReviewCard>

                  <ReviewCard>
                    <ReviewHeader icon={Calendar}>Initial Import</ReviewHeader>
                    <ReviewContent>
                      <ReviewItem
                        label="Strategy"
                        value={
                          importMode === "all" ? "Import All" : "Custom Range"
                        }
                      />
                      {importMode === "range" ? (
                        <>
                          <ReviewItem
                            label="Start Time"
                            value={formatDateTime(importRange.start, timezone)}
                          />
                          <ReviewItem
                            label="End Time"
                            value={formatDateTime(importRange.end, timezone)}
                          />
                        </>
                      ) : (
                        <ReviewItem
                          label="Range"
                          value={`${formatDateTime(lakeInfo?.range?.start, timezone)} → ${formatDateTime(lakeInfo?.range?.end, timezone)}`}
                        />
                      )}
                      <ReviewItem
                        label="Est. Download Size"
                        value={`~${formatBytes(estimatedImportSize)}`}
                        className="text-primary font-medium"
                      />
                    </ReviewContent>
                  </ReviewCard>

                  <ReviewCard>
                    <ReviewHeader icon={Settings}>Automation</ReviewHeader>
                    <ReviewContent>
                      <ReviewItem
                        variant="between"
                        label="Background Sync"
                        value={
                          syncEnabled ? (
                            <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                          ) : (
                            <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                          )
                        }
                      />
                      {syncEnabled && (
                        <ReviewItem
                          label="Sync Interval"
                          value={`Every ${syncIntervalMins} minutes`}
                        />
                      )}
                    </ReviewContent>
                  </ReviewCard>
                </div>

                <div className="p-4 rounded-xl bg-primary/5 border border-primary/20 space-y-3">
                  <div className="flex items-center gap-2 text-primary">
                    <Sparkles className="h-4 w-4" />
                    <span className="text-xs font-bold uppercase tracking-wider">
                      What to expect
                    </span>
                  </div>
                  <p className="text-xs text-muted-foreground leading-relaxed">
                    After connecting, the system will begin downloading the
                    requested Parquet data files to your local cache. This
                    process happens in the background and may take a few minutes
                    depending on the volume of data. Your dashboard will begin
                    populating as files arrive.
                  </p>
                </div>
              </div>
            </div>
          )}

          {step === "ngwaf" && (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div className="p-8 space-y-6 max-w-2xl mx-auto">
                <div className="flex items-center gap-2 pb-2 border-b">
                  <Shield className="h-5 w-5 text-primary" />
                  <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
                    NGWAF Workspace
                  </h3>
                </div>

                <p className="text-sm text-muted-foreground leading-relaxed">
                  Link this service to an existing Fastly NGWAF workspace to
                  enable WAF signal logging and bot detection. This step is
                  optional — skip it if NGWAF is not deployed on this service.
                </p>

                {ngwafFetching ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading workspaces…
                  </div>
                ) : ngwafFetchError ? (
                  <div className="flex items-center gap-2 text-sm text-destructive">
                    <AlertCircle className="h-4 w-4 shrink-0" />
                    {ngwafFetchError}
                  </div>
                ) : ngwafWorkspaces.length > 0 ? (
                  <div className="space-y-2">
                    <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                      Workspace
                    </Label>
                    <Select
                      value={config.ngwaf_workspace_id || "__none__"}
                      onValueChange={(v: string | null) =>
                        setConfig((prev) => {
                          const workspaceId = !v || v === "__none__" ? "" : v;
                          const update: typeof prev = {
                            ...prev,
                            ngwaf_workspace_id: workspaceId,
                          };
                          if (workspaceId) {
                            const groups: string[] =
                              prev.log_fields?.groups ?? [];
                            if (!groups.includes("J")) {
                              update.log_fields = {
                                ...prev.log_fields,
                                groups: [...groups, "J"],
                              };
                            }
                          }
                          return update;
                        })
                      }
                    >
                      <SelectTrigger className="h-9 text-sm">
                        <SelectValue placeholder="Select a workspace…" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="__none__">
                          <span className="text-muted-foreground">
                            No NGWAF (skip)
                          </span>
                        </SelectItem>
                        {ngwafWorkspaces.map((ws) => (
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
                      <Info className="h-4 w-4 shrink-0" />
                      No NGWAF workspaces found in this account.
                    </div>

                    {ngwafFetchError && (
                      <div className="text-xs text-amber-600 bg-amber-50 dark:bg-amber-950/20 p-3 rounded-lg border border-amber-200 dark:border-amber-900/50 flex gap-2">
                        <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                        <p className="leading-relaxed font-medium">
                          {ngwafFetchError}
                        </p>
                      </div>
                    )}

                    {ngwafDebugRaw && (
                      <details className="text-[10px]">
                        <summary className="cursor-pointer text-muted-foreground uppercase tracking-wider font-bold">
                          Raw API response (debug)
                        </summary>
                        <pre className="mt-1 p-2 bg-muted rounded text-xs overflow-auto max-h-32 whitespace-pre-wrap break-all">
                          {ngwafDebugRaw}
                        </pre>
                      </details>
                    )}
                  </div>
                )}

                <div className="p-4 rounded-xl bg-muted/30 border border-dashed space-y-1">
                  <p className="text-xs font-semibold text-muted-foreground">
                    WAF / NGWAF log fields (group J) will only be available in
                    the next step if a workspace is selected here.
                  </p>
                </div>
              </div>
            </div>
          )}

          {step === "fields" && (
            <div className="flex-1 overflow-y-auto min-h-0">
              <div className="p-8 space-y-6 max-w-4xl mx-auto">
                <div className="flex items-center justify-between pb-2 border-b">
                  <div className="flex items-center gap-2">
                    <FileJson className="h-5 w-5 text-primary" />
                    <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
                      Log Fields
                    </h3>
                  </div>
                  {!isLoadingCatalog && (
                    <div className="text-xs font-mono text-muted-foreground bg-muted/50 px-3 py-1 rounded-md border">
                      Est. ~{formatBytes(estimatedBytes)} / line
                    </div>
                  )}
                </div>

                {isLoadingCatalog ? (
                  <div className="h-[200px] flex items-center justify-center bg-muted/10 rounded-lg border border-dashed">
                    <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                  </div>
                ) : (
                  <div className="space-y-6">
                    <div className="space-y-4">
                      <p className="text-sm text-muted-foreground">
                        Select the data fields to capture at the edge. More
                        fields provide richer insights but increase storage and
                        bandwidth costs.
                      </p>
                      <div className="p-3 bg-blue-500/10 border border-blue-500/20 text-blue-700 dark:text-blue-400 rounded-md text-xs">
                        <strong>Note:</strong> Custom log fields (e.g. tracking
                        specific HTTP headers or application IDs) can be
                        configured from the Admin dashboard after initial
                        provisioning is complete.
                      </div>
                      {catalog?.presets && (
                        <div className="flex flex-wrap gap-2 pt-2 items-center">
                          {Object.entries(catalog.presets).map(
                            ([key, preset]: [string, any]) => {
                              const isMinimal = key === "minimal";
                              const active =
                                isMinimal ||
                                isPresetActive(preset.groups || []);
                              return (
                                <Button
                                  key={key}
                                  variant={active ? "default" : "outline"}
                                  size="sm"
                                  className={cn(
                                    "h-8 text-xs font-semibold transition-all",
                                    active && "ring-2 ring-primary/20",
                                    isMinimal && "opacity-80",
                                  )}
                                  title={preset.description}
                                  onClick={() =>
                                    !isMinimal &&
                                    togglePreset(preset.groups || [])
                                  }
                                  disabled={isMinimal}
                                >
                                  {preset.label || key}
                                </Button>
                              );
                            },
                          )}
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-8 text-xs font-semibold text-muted-foreground hover:text-foreground ml-auto"
                            onClick={() =>
                              setConfig((prev) => ({
                                ...prev,
                                log_fields: { groups: [], field_overrides: {} },
                              }))
                            }
                          >
                            Clear All
                          </Button>
                        </div>
                      )}
                    </div>
                    {!config.ngwaf_workspace_id && (
                      <div className="flex items-center gap-2 text-xs text-muted-foreground bg-muted/30 border border-dashed rounded-lg px-3 py-2">
                        <Shield className="h-3.5 w-3.5 shrink-0" />
                        WAF / NGWAF fields (group J) are hidden — no NGWAF
                        workspace selected.
                      </div>
                    )}
                    <div className="grid grid-cols-1 gap-3 pb-8">
                      {(catalog?.groups ?? [])
                        .filter(
                          (g: any) => config.ngwaf_workspace_id || g.id !== "J",
                        )
                        .map((g: any) => (
                          <CollapsibleGroup
                            key={g.id}
                            group={g}
                            catalog={catalog}
                            config={config.log_fields}
                            toggleGroup={toggleGroup}
                            toggleField={toggleField}
                            updateFieldLimit={updateFieldLimit}
                          />
                        ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
          {step === "execute" && (
            <div className="flex-1 overflow-y-auto min-h-0 flex flex-col p-8 items-center text-left">
              <div className="w-full max-w-2xl space-y-8">
                {isDeploying ? (
                  <div className="space-y-6 w-full animate-in fade-in slide-in-from-bottom-4 duration-500">
                    <div className="text-center space-y-2">
                      <h3 className="text-2xl font-semibold tracking-tight">
                        Provisioning: {selectedService?.name}
                      </h3>
                      <p className="text-sm text-muted-foreground leading-relaxed">
                        Setting up Fastly Object Storage, logging endpoints, and
                        CDN proxy...
                      </p>
                    </div>

                    <SSEProgressView
                      lines={lines}
                      status={status}
                      error={sseError}
                      className="h-[400px]"
                      progressLabel="Progress"
                      doneMessage="Provisioning completed successfully! You may now close this window."
                    />
                  </div>
                ) : (
                  <>
                    <div className="text-center space-y-2">
                      <h3 className="text-2xl font-semibold tracking-tight">
                        Review & Deploy
                      </h3>
                      <p className="text-sm text-muted-foreground leading-relaxed">
                        You are about to provision the following resources.
                      </p>
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <ReviewCard>
                        <ReviewHeader icon={Cloud}>Target Service</ReviewHeader>
                        <ReviewContent>
                          <ReviewItem
                            label="Service Name"
                            value={selectedService?.name}
                          />
                          <ReviewItem
                            label="Log Endpoint"
                            value={config.endpoint_name}
                          />
                          <ReviewItem
                            label="Sampling Rate / Period"
                            value={`${config.sample_rate}% / ${config.log_period}s`}
                          />
                          {config.custom_condition && (
                            <ReviewItem
                              label="Custom Condition"
                              value={config.custom_condition}
                              className="truncate font-mono text-[10px]"
                            />
                          )}
                        </ReviewContent>{" "}
                      </ReviewCard>

                      <ReviewCard>
                        <ReviewHeader icon={Globe}>CDN Edge Proxy</ReviewHeader>
                        <ReviewContent>
                          <ReviewItem
                            label="Domain"
                            value={`${config.cdn_prefix}.global.ssl.fastly.net`}
                          />
                          <ReviewItem
                            label="Shield POP"
                            value={SHIELD_LABELS[config.cdn_shield] || "None"}
                          />
                        </ReviewContent>
                      </ReviewCard>

                      <ReviewCard>
                        <ReviewHeader icon={Database}>
                          Object Storage
                        </ReviewHeader>
                        <ReviewContent>
                          <ReviewItem
                            label="Bucket"
                            value={config.fos_bucket_name}
                          />
                          <ReviewItem
                            label="Region"
                            value={REGION_LABELS[config.fos_region]}
                          />
                          <ReviewItem
                            label="Edge Only"
                            value={config.edge_only ? "Yes" : "No"}
                          />
                        </ReviewContent>
                      </ReviewCard>

                      <ReviewCard>
                        <ReviewHeader icon={Settings}>Automation</ReviewHeader>
                        <ReviewContent className="gap-2.5">
                          <ReviewItem
                            variant="between"
                            label={`Sync every ${config.log_period >= 120 ? Math.floor(config.log_period / 120) + "m" : Math.floor(config.log_period / 2) + "s"}`}
                            className={cn(
                              !config.enable_cron_sync &&
                                "text-muted-foreground",
                            )}
                            value={
                              config.enable_cron_sync ? (
                                <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                              ) : (
                                <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                              )
                            }
                          />
                          <ReviewItem
                            variant="between"
                            label={`Commit to Iceberg every ${config.commit_interval_mins}m`}
                            className={cn(
                              !config.enable_cron_sync &&
                                "text-muted-foreground",
                            )}
                            value={
                              config.enable_cron_sync ? (
                                <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                              ) : (
                                <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                              )
                            }
                          />
                          <ReviewItem
                            variant="between"
                            label="Auto-delete Raw Logs"
                            className={cn(
                              (!config.delete_after ||
                                !config.enable_cron_sync) &&
                                "text-muted-foreground",
                            )}
                            value={
                              config.delete_after && config.enable_cron_sync ? (
                                <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                              ) : (
                                <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                              )
                            }
                          />
                          <ReviewItem
                            variant="between"
                            label="Daily Iceberg Optimization"
                            className={cn(
                              (!config.enable_cron_compact ||
                                !config.enable_cron_sync) &&
                                "text-muted-foreground",
                            )}
                            value={
                              config.enable_cron_compact &&
                              config.enable_cron_sync ? (
                                <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />
                              ) : (
                                <XCircle className="h-3.5 w-3.5 text-muted-foreground/30" />
                              )
                            }
                          />
                        </ReviewContent>
                      </ReviewCard>
                      {/* Full Width Log Fields */}
                      <ReviewCard className="col-span-2 space-y-3">
                        <div className="flex justify-between items-center">
                          <ReviewHeader icon={FileJson}>
                            Log Configuration
                          </ReviewHeader>
                          <span className="font-mono text-[10px] bg-muted px-2 py-0.5 rounded text-muted-foreground border">
                            ~{formatBytes(estimatedBytes)} / line
                          </span>
                        </div>
                        <div className="flex flex-wrap gap-1.5 pt-1">
                          {(() => {
                            const enabledGroupsSet = new Set(
                              config.log_fields?.groups || [],
                            );
                            const overrides =
                              config.log_fields?.field_overrides || {};
                            const hasOverrides =
                              Object.keys(overrides).length > 0;

                            let bestPresetName = null;
                            if (catalog?.presets && !hasOverrides) {
                              for (const [key, preset] of Object.entries(
                                catalog.presets,
                              )) {
                                const presetGroups =
                                  (preset as any).groups || [];
                                if (
                                  presetGroups.length ===
                                    enabledGroupsSet.size &&
                                  presetGroups.every((g: string) =>
                                    enabledGroupsSet.has(g),
                                  )
                                ) {
                                  bestPresetName = (preset as any).label || key;
                                  break;
                                }
                              }
                            }

                            const disabledCount =
                              catalog?.groups.filter(
                                (g: any) =>
                                  !(g.locked || enabledGroupsSet.has(g.id)),
                              ).length || 0;

                            if (bestPresetName) {
                              return (
                                <>
                                  <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                                    {bestPresetName} Preset
                                  </div>
                                  {disabledCount > 0 && (
                                    <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                                      +{disabledCount} disabled
                                    </div>
                                  )}
                                </>
                              );
                            }

                            return (
                              <>
                                <div className="px-2.5 py-0.5 rounded-full text-[10px] font-semibold bg-primary text-primary-foreground">
                                  Custom Configuration
                                </div>
                                {catalog?.groups.map((g: any) => {
                                  const isEnabled =
                                    g.locked || enabledGroupsSet.has(g.id);
                                  if (!isEnabled) return null;
                                  return (
                                    <div
                                      key={g.id || "core"}
                                      className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-primary/10 text-primary border border-primary/20"
                                    >
                                      {g.label}
                                    </div>
                                  );
                                })}
                                {disabledCount > 0 && (
                                  <div className="px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-muted text-muted-foreground border border-transparent">
                                    +{disabledCount} disabled
                                  </div>
                                )}
                              </>
                            );
                          })()}
                        </div>
                      </ReviewCard>

                      {/* Insights Section */}
                      <ReviewCard className="col-span-2 space-y-3">
                        <div className="flex justify-between items-center">
                          <ReviewHeader icon={Sparkles}>
                            Automated Insights
                          </ReviewHeader>
                          <span className="text-[10px] text-muted-foreground">
                            Derived from logs
                          </span>
                        </div>
                        <div className="grid grid-cols-2 gap-3 pt-1">
                          {(catalog as any)?.insights?.map((insight: any) => {
                            const enabledGroups = new Set([
                              null,
                              ...(config.log_fields?.groups || []),
                            ]);
                            // Also include dependencies
                            const catalogGroups =
                              (catalog as any)?.groups || [];
                            let changed = true;
                            while (changed) {
                              changed = false;
                              catalogGroups.forEach((g: any) => {
                                if (
                                  enabledGroups.has(g.id) &&
                                  g.requires &&
                                  !enabledGroups.has(g.requires)
                                ) {
                                  enabledGroups.add(g.requires);
                                  changed = true;
                                }
                              });
                            }

                            const isEnabled = insight.required_groups?.every(
                              (rg: any) => enabledGroups.has(rg),
                            );
                            return (
                              <div
                                key={insight.id}
                                className={cn(
                                  "flex items-start gap-3 border rounded-lg p-2.5 bg-background shadow-sm transition-all",
                                  !isEnabled && "opacity-50 grayscale",
                                )}
                              >
                                <div className="mt-0.5 shrink-0">
                                  {isEnabled ? (
                                    <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                                  ) : (
                                    <XCircle className="h-4 w-4 text-muted-foreground" />
                                  )}
                                </div>
                                <div className="flex flex-col min-w-0">
                                  <span
                                    className={cn(
                                      "text-xs font-semibold truncate",
                                      !isEnabled &&
                                        "line-through text-muted-foreground",
                                    )}
                                  >
                                    {insight.title}
                                  </span>
                                  <span
                                    className="text-[10px] text-muted-foreground line-clamp-2 leading-tight mt-0.5"
                                    title={insight.description}
                                  >
                                    {insight.description}
                                  </span>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </ReviewCard>
                    </div>
                  </>
                )}
              </div>
            </div>
          )}

          {step === "terraform" && (
            <div className="flex-1 overflow-hidden p-8 flex flex-col">
              <div className="w-full max-w-6xl mx-auto flex flex-col h-full space-y-6">
                <div className="flex items-center justify-between pb-4 border-b shrink-0">
                  <div className="space-y-1">
                    <h3 className="text-lg font-bold tracking-tight flex items-center gap-2">
                      <FileJson className="h-5 w-5 text-primary" />
                      Terraform & VCL Preview
                    </h3>
                    <p className="text-sm text-muted-foreground">
                      Review and export the generated configuration files.
                    </p>
                  </div>
                  <Button
                    onClick={handleExportTerraform}
                    className="h-9 font-bold"
                  >
                    Export as ZIP
                  </Button>
                </div>

                {isFetchingTerraform ? (
                  <div className="flex-1 flex items-center justify-center bg-muted/10 rounded-lg border border-dashed">
                    <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                  </div>
                ) : (
                  <Tabs 
                    defaultValue="logging" 
                    className="flex-1 flex flex-col min-h-0"
                    onValueChange={(tab) => {
                      if (tab === "logging") setSelectedTfFile("logging_service.tf");
                      else if (tab === "cdn") setSelectedTfFile("fos.tf");
                      else if (tab === "instructions") setSelectedTfFile("instructions");
                    }}
                  >
                    <TabsList className="grid w-full grid-cols-4 shrink-0">
                      <TabsTrigger value="logging" className="flex items-center gap-2">
                        <Zap className="w-3.5 h-3.5" />
                        Logging Service
                      </TabsTrigger>
                      <TabsTrigger value="cdn" className="flex items-center gap-2">
                        <Globe className="w-3.5 h-3.5" />
                        CDN & Storage
                      </TabsTrigger>
                      <TabsTrigger value="instructions" className="flex items-center gap-2">
                        <FileText className="w-3.5 h-3.5" />
                        Instructions
                      </TabsTrigger>
                      <TabsTrigger value="all" className="flex items-center gap-2">
                        <FileJson className="w-3.5 h-3.5" />
                        All Files
                      </TabsTrigger>
                    </TabsList>

                    {["logging", "cdn", "instructions", "all"].map((tab) => (
                      <TabsContent key={tab} value={tab} className="flex-1 flex gap-4 min-h-0 pt-4 mt-0">
                        <div className="w-64 shrink-0 flex flex-col gap-1 overflow-y-auto pr-2 custom-scrollbar border-r">
                          {Object.keys(terraformFiles)
                            .filter((f) => {
                              if (tab === "logging") return f === "logging_service.tf" || f === "log_format.vcl" || f.startsWith("capture_snippets/");
                              if (tab === "cdn") return f === "fos.tf" || f === "cdn_proxy.tf" || f === "cdn_proxy.vcl" || f.startsWith("cdn_snippets/");
                              if (tab === "instructions") return f === "instructions";
                              return true;
                            })
                            .sort((a, b) => {
                              // Prioritize .tf files
                              if (a.endsWith(".tf") && !b.endsWith(".tf")) return -1;
                              if (!a.endsWith(".tf") && b.endsWith(".tf")) return 1;
                              return a.localeCompare(b);
                            })
                            .map((fileName) => (
                              <button
                                key={fileName}
                                onClick={() => setSelectedTfFile(fileName)}
                                className={cn(
                                  "text-left px-3 py-2 rounded-md text-[11px] font-mono transition-colors truncate",
                                  selectedTfFile === fileName
                                    ? "bg-primary text-primary-foreground font-bold shadow-sm"
                                    : "hover:bg-muted text-muted-foreground"
                                )}
                              >
                                {fileName}
                              </button>
                            ))}
                        </div>
                        <div className="flex-1 bg-muted rounded-lg border overflow-hidden flex flex-col">
                          <div className="px-4 py-2 border-b bg-muted/50 flex items-center justify-between shrink-0">
                            <span className="text-[10px] font-mono text-muted-foreground">
                              {selectedTfFile}
                            </span>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-6 w-6 hover:bg-muted-foreground/10"
                              onClick={() => {
                                navigator.clipboard.writeText(
                                  terraformFiles[selectedTfFile]
                                );
                              }}
                            >
                              <Copy className="h-3 w-3" />
                            </Button>
                          </div>
                          <div className="flex-1 overflow-auto p-4 custom-scrollbar">
                            <pre className="text-xs font-mono text-muted-foreground whitespace-pre leading-relaxed">
                              {terraformFiles[selectedTfFile] ||
                                "Select a file on the left to preview its content."}
                            </pre>
                          </div>
                        </div>
                      </TabsContent>
                    ))}
                  </Tabs>
                )}
              </div>
            </div>
          )}
        </div>

        <DialogFooter className={panelDialogFooter}>
          {!isDeploying && step !== "mode" && (
            <Button
              variant="ghost"
              className="mr-auto h-9 text-xs"
              disabled={isAnalyzing}
              onClick={() => {
                const order: Step[] =
                  mode === "join"
                    ? ["mode", "join", "analyze", "settings", "confirm"]
                    : [
                        "mode",
                        "token",
                        "service",
                        "storage",
                        "ngwaf",
                        "fields",
                        "execute",
                      ];
                const idx = order.indexOf(step);
                if (idx > 0) setStep(order[idx - 1] as Step);
              }}
            >
              <ChevronLeft className="h-4 w-4 mr-1" /> Back
            </Button>
          )}

          {status !== "streaming" && (
            <Button
              variant="outline"
              className="h-9 text-xs"
              onClick={() => handleModalClose(false)}
            >
              {status === "done" || isDone ? "Close & Reload" : "Cancel"}
            </Button>
          )}

          {!isDeploying && (
            <>
              {step === "mode" && (
                <Button disabled className="h-9 text-xs">
                  Select a Role
                </Button>
              )}
              {step === "token" && (
                <Button
                  disabled={!token || isLoadingServices}
                  onClick={handleTokenSubmit}
                  className="h-9 text-xs"
                >
                  {isLoadingServices && (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  )}
                  Fetch Services
                </Button>
              )}
              {step === "service" && (
                <Button
                  disabled={!selectedService || validateMutation.isPending}
                  onClick={() => {}}
                  className="h-9 text-xs"
                >
                  {validateMutation.isPending && (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  )}
                  Continue
                </Button>
              )}
              {step === "storage" && (
                <Button
                  onClick={() => setStep("ngwaf")}
                  disabled={
                    domainStatus === "taken" || domainStatus === "checking"
                  }
                  className="h-9 text-xs"
                >
                  Continue
                </Button>
              )}
              {step === "ngwaf" && (
                <Button
                  onClick={() => setStep("fields")}
                  className="h-9 text-xs"
                >
                  {config.ngwaf_workspace_id ? "Continue" : "Skip"}
                </Button>
              )}
              {step === "fields" && (
                <Button
                  onClick={() => setStep("execute")}
                  className="h-9 text-xs"
                >
                  Review Settings
                </Button>
              )}

              {step === "execute" && (
                <div className="flex gap-2">
                  <Button
                    variant="secondary"
                    className="h-9 font-bold"
                    onClick={() => {
                      fetchTerraformPreview();
                      setStep("terraform");
                    }}
                  >
                    View & Export Terraform
                  </Button>
                  {mode !== "ingest" ? (
                    <Button
                      size="lg"
                      disabled={
                        domainStatus === "taken" || !config.fos_bucket_name
                      }
                      className="h-9 px-6 font-bold"
                      onClick={handleDeploy}
                    >
                      Deploy to Fastly
                    </Button>
                  ) : (
                    <Button
                      size="lg"
                      disabled={!config.fos_bucket_name}
                      className="h-9 px-6 font-bold"
                      onClick={handleAdminIngest}
                    >
                      Complete Setup
                    </Button>
                  )}
                </div>
              )}

              {step === "terraform" && (
                <Button
                  size="lg"
                  className="h-9 px-6 font-bold"
                  onClick={handleAdminIngest}
                >
                  Complete Setup
                </Button>
              )}

              {step === "join" && joinPhase === "form" && (
                <Button
                  size="lg"
                  disabled={
                    !config.endpoint_name ||
                    !config.cdn_service_name ||
                    fosStatus !== "success" ||
                    isAnalyzing
                  }
                  className="h-9 px-6 font-bold"
                  onClick={handleAnalyzeLake}
                >
                  {isAnalyzing && (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  )}
                  Analyze Data Lake
                </Button>
              )}

              {step === "analyze" && (
                <Button
                  className="h-9 text-xs"
                  onClick={() => setStep("settings")}
                >
                  Continue
                </Button>
              )}

              {step === "settings" && (
                <Button
                  className="h-9 text-xs"
                  onClick={() => setStep("confirm")}
                >
                  Review Summary
                </Button>
              )}

              {step === "confirm" && (
                <Button
                  size="lg"
                  className="h-9 px-6 font-bold"
                  onClick={handleJoin}
                  disabled={
                    importMode === "range" &&
                    (!importRange.start || !importRange.end)
                  }
                >
                  Confirm & Connect
                </Button>
              )}
            </>
          )}

          {status === "streaming" && (
            <Button variant="outline" onClick={stop} className="h-9 text-xs">
              Stop
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
