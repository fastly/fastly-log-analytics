"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { SSEProgressView } from "@/components/SSEModal";
import { SectionHeader } from "@/components/ui/section-header";
import { LabelWithInfo } from "@/components/ui/label-with-info";
import {
  CheckCircle2,
  Database,
  Loader2,
  XCircle,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { WizardState } from "../useWizardState";
import { JsonImportSection } from "../JsonImportSection";
import { REGION_LABELS } from "../types";

export function JoinStep({ s }: { s: WizardState }) {
  // Sub-phase: connecting / done show SSE progress
  if (s.joinPhase === "connecting" || s.joinPhase === "done") {
    return (
      <div className="flex-1 overflow-y-auto min-h-0 p-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
        <div className="text-center space-y-1">
          <h3 className="text-lg font-semibold tracking-tight">
            {s.joinPhase === "connecting"
              ? `Connecting to ${s.config.endpoint_name}`
              : "Setup Complete"}
          </h3>
          <p className="text-sm text-muted-foreground">
            {s.joinPhase === "connecting"
              ? "Please wait while we secure your connection and import initial data."
              : "Your service is connected and the initial data import is complete."}
          </p>
        </div>
        <SSEProgressView
          lines={s.lines}
          status={s.status}
          error={s.sseError}
          className="h-[320px]"
          progressLabel="Progress"
          doneMessage=""
        />
      </div>
    );
  }

  // form phase
  const { config, setConfig, mode } = s;

  return (
    <div className="flex-1 overflow-y-auto min-h-0">
      <div
        className={cn(
          "p-8 space-y-10 pb-12 max-w-3xl mx-auto transition-opacity duration-300",
          s.isAnalyzing && "pointer-events-none opacity-50",
        )}
      >
        <div className="space-y-5">
          <SectionHeader title="Connect to Existing Storage" icon={Database} />
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
                fos_bucket_name: parsed.fos_bucket ?? prev.fos_bucket_name,
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
                s.setIcebergMetadataLocation(parsed.iceberg_metadata_location);
              }
              s.handleCheckFos({
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
                info={
                  mode === "ingest"
                    ? "The Fastly service that is streaming logs to Object Storage."
                    : "A friendly name for this service in your local dashboard."
                }
              />
              {mode === "ingest" ? (
                <Select
                  value={s.selectedService?.id || ""}
                  onValueChange={(id) => {
                    const svc = (s.servicesData as any[]).find(
                      (svc) => svc.id === id,
                    );
                    if (svc) s.setSelectedService(svc);
                  }}
                >
                  <SelectTrigger className="h-9 font-mono text-sm">
                    <SelectValue placeholder="Select logging service..." />
                  </SelectTrigger>
                  <SelectContent>
                    {(s.servicesData as any[])?.map((svc) => (
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
                    setConfig({ ...config, endpoint_name: e.target.value })
                  }
                  className="h-9 font-mono text-sm"
                  placeholder="e.g. Production Logs"
                />
              )}
            </div>
            <div className="space-y-1.5">
              <LabelWithInfo
                label={
                  mode === "ingest" ? "CDN Proxy Service" : "Fastly Service ID"
                }
                info={
                  mode === "ingest"
                    ? "The Fastly service used to front the Object Storage bucket."
                    : "The Fastly Service ID you are pulling logs for."
                }
              />
              {mode === "ingest" ? (
                <Select
                  value={s.selectedCdnService?.id || ""}
                  onValueChange={(id) => {
                    const svc = (s.servicesData as any[]).find(
                      (svc) => svc.id === id,
                    );
                    if (svc) s.setSelectedCdnService(svc);
                  }}
                >
                  <SelectTrigger className="h-9 font-mono text-sm">
                    <SelectValue placeholder="Select CDN service..." />
                  </SelectTrigger>
                  <SelectContent>
                    {(s.servicesData as any[])?.map((svc) => (
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
                    setConfig({ ...config, cdn_service_name: e.target.value })
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
                  We will verify that both services have the correct resources
                  and VCL snippets.
                </div>
                <Button
                  variant="secondary"
                  size="sm"
                  disabled={
                    s.isCheckingConfig ||
                    !s.selectedService ||
                    !s.selectedCdnService ||
                    !config.fos_bucket_name
                  }
                  onClick={s.handleCheckConfig}
                >
                  {s.isCheckingConfig && (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  )}
                  Verify Configuration
                </Button>
              </div>

              {s.configStatus && (
                <div className="grid grid-cols-2 gap-4">
                  <div
                    className={cn(
                      "p-3 rounded-lg border text-xs space-y-1",
                      s.configStatus.logging_service.ok
                        ? "bg-emerald-500/5 border-emerald-500/20"
                        : "bg-destructive/5 border-destructive/20",
                    )}
                  >
                    <div className="flex items-center gap-2 font-bold">
                      {s.configStatus.logging_service.ok ? (
                        <CheckCircle2 className="w-3 h-3 text-emerald-500" />
                      ) : (
                        <XCircle className="w-3 h-3 text-destructive" />
                      )}
                      Logging Service
                    </div>
                    <p className="text-muted-foreground leading-relaxed">
                      {s.configStatus.logging_service.details}
                    </p>
                  </div>
                  <div
                    className={cn(
                      "p-3 rounded-lg border text-xs space-y-1",
                      s.configStatus.cdn_service.ok
                        ? "bg-emerald-500/5 border-emerald-500/20"
                        : "bg-destructive/5 border-destructive/20",
                    )}
                  >
                    <div className="flex items-center gap-2 font-bold">
                      {s.configStatus.cdn_service.ok ? (
                        <CheckCircle2 className="w-3 h-3 text-emerald-500" />
                      ) : (
                        <XCircle className="w-3 h-3 text-destructive" />
                      )}
                      CDN Proxy Service
                    </div>
                    <p className="text-muted-foreground leading-relaxed">
                      {s.configStatus.cdn_service.details}
                    </p>
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
                  <SelectItem value="us-east-1">US East (Ashburn)</SelectItem>
                  <SelectItem value="us-west">US West (Seattle)</SelectItem>
                  <SelectItem value="us-central-1">
                    US Central (Chicago)
                  </SelectItem>
                  <SelectItem value="eu-central">
                    EU Central (Frankfurt)
                  </SelectItem>
                  <SelectItem value="eu-south-1">EU South (Milan)</SelectItem>
                  <SelectItem value="uk-east-1">UK East (London)</SelectItem>
                  <SelectItem value="jp-central-1">
                    JP Central (Tokyo)
                  </SelectItem>
                  <SelectItem value="au-east-1">AU East (Sydney)</SelectItem>
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
              value={s.icebergMetadataLocation}
              onChange={(e) => s.setIcebergMetadataLocation(e.target.value)}
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
                  setConfig({ ...config, fos_access_key: e.target.value })
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
                  setConfig({ ...config, fos_secret_key: e.target.value })
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
            {s.fosStatus === "idle" || s.fosStatus === "checking" ? (
              <div className="text-sm text-muted-foreground">
                Please verify your credentials before connecting.
              </div>
            ) : s.fosStatus === "success" ? (
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
              variant={s.fosStatus === "success" ? "outline" : "secondary"}
              size="sm"
              onClick={() => s.handleCheckFos()}
              disabled={
                s.fosStatus === "checking" ||
                !config.fos_bucket_name ||
                !config.fos_region ||
                !config.fos_access_key ||
                !config.fos_secret_key
              }
            >
              {s.fosStatus === "checking" && (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              )}
              Verify Access
            </Button>
          </div>

          {s.fosStatus === "error" && (
            <div className="text-sm text-destructive bg-destructive/10 p-3 rounded-md">
              {s.fosError}
            </div>
          )}

          {s.fosStatus === "success" && (
            <p className="text-xs text-muted-foreground leading-relaxed animate-in fade-in slide-in-from-top-1">
              {mode === "ingest" ? (
                <>
                  We will connect to this service in <strong>Admin</strong>{" "}
                  mode. We will set up background ingestion and metadata
                  management.
                </>
              ) : (
                <>
                  We will connect to this service in <strong>Read-Only</strong>{" "}
                  mode. We will not create any resources or modify your logging
                  configuration.
                </>
              )}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
