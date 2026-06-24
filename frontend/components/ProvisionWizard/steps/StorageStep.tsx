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
import { Switch } from "@/components/ui/switch";
import { SectionHeader } from "@/components/ui/section-header";
import { LabelWithInfo } from "@/components/ui/label-with-info";
import {
  AlertCircle,
  CheckCircle2,
  Globe,
  Search,
  Settings,
  Zap,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { WizardState } from "../useWizardState";
import { PERIOD_LABELS, REGION_LABELS, SHIELD_LABELS } from "../types";

export function StorageStep({ s }: { s: WizardState }) {
  const { config, setConfig, domainStatus, domainMessage, checkDomain } = s;
  return (
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
                  setConfig({ ...config, endpoint_name: e.target.value })
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
                  <SelectItem value="us-east-1">US East (Ashburn)</SelectItem>{" "}
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
                onCheckedChange={(v) => setConfig({ ...config, edge_only: v })}
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
                  setConfig({ ...config, sample_rate: Number(e.target.value) })
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
                setConfig({ ...config, custom_condition: e.target.value })
              }
              className="h-9 font-mono text-xs"
            />
          </div>
        </div>

        {/* Section: CDN Access */}
        <div className="space-y-5">
          <SectionHeader title="CDN Performance Front" icon={Globe} />
          <p className="text-xs text-muted-foreground leading-relaxed">
            Highly recommended. Provision a secondary Fastly service to front
            the Object Storage bucket for faster dashboard queries and secure
            access.
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
                  <SelectItem value="iad-va-us">IAD (Ashburn)</SelectItem>
                  <SelectItem value="sea-wa-us">SEA (Seattle)</SelectItem>
                  <SelectItem value="mdw-il-us">MDW (Chicago)</SelectItem>
                  <SelectItem value="fra-de-eu">FRA (Frankfurt)</SelectItem>
                  <SelectItem value="mxp-it-eu">MXP (Milan)</SelectItem>
                  <SelectItem value="lcy-gb-eu">LCY (London)</SelectItem>
                  <SelectItem value="tyo-jp-asia">TYO (Tokyo)</SelectItem>
                  <SelectItem value="syd-au-aus">SYD (Sydney)</SelectItem>
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
                !config.enable_cron_sync && "opacity-30 pointer-events-none",
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
              !config.enable_cron_sync && "opacity-30 pointer-events-none",
            )}
          >
            <div className="flex items-start justify-between gap-4">
              <div className="space-y-1">
                <LabelWithInfo
                  label="Cloud Commit Interval"
                  info="How often the local buffer is flushed to the shared Iceberg table in Fastly Object Storage. This determines how quickly data becomes visible to other users or tools querying the Iceberg table directly. More frequent commits mean fresher data but create more small files — the daily Iceberg optimization consolidates them."
                />
                <p className="text-[10px] text-muted-foreground leading-relaxed">
                  Controls data freshness for shared access. Every commit
                  creates one Iceberg snapshot in FOS.
                </p>
              </div>
              <Select
                value={String(config.commit_interval_mins)}
                onValueChange={(v) =>
                  v &&
                  setConfig({ ...config, commit_interval_mins: Number(v) })
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
              log period and a {config.commit_interval_mins}-minute commit
              interval, the system will create ~
              {Math.round(1440 / config.commit_interval_mins)} Iceberg snapshots
              per day before the daily optimization consolidates them.
            </div>
          </div>

          <div
            className={cn(
              "flex items-center justify-between p-3 border rounded-md bg-muted/5 transition-opacity",
              !config.enable_cron_sync && "opacity-30 pointer-events-none",
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
  );
}
