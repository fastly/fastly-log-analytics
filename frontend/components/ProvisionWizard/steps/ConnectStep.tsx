"use client"

import React from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { SectionHeader, LabelWithInfo } from '@/components/ui/confirm-dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { CheckCircle2, Database, Loader2, XCircle } from 'lucide-react'
import { cn, REGION_LABELS } from '@/lib/utils'
import type { ProvisionService, WizardState } from '../types'

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
