"use client";

import React from "react";
import { Button } from "@/components/ui/button";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import {
  Copy,
  FileJson,
  FileText,
  Globe,
  Loader2,
  Zap,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { WizardState } from "../useWizardState";

export function TerraformStep({ s }: { s: WizardState }) {
  const { terraformFiles, selectedTfFile, setSelectedTfFile } = s;
  return (
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
          <Button onClick={s.handleExportTerraform} className="h-9 font-bold">
            Export as ZIP
          </Button>
        </div>

        {s.isFetchingTerraform ? (
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
              <TabsTrigger
                value="instructions"
                className="flex items-center gap-2"
              >
                <FileText className="w-3.5 h-3.5" />
                Instructions
              </TabsTrigger>
              <TabsTrigger value="all" className="flex items-center gap-2">
                <FileJson className="w-3.5 h-3.5" />
                All Files
              </TabsTrigger>
            </TabsList>

            {["logging", "cdn", "instructions", "all"].map((tab) => (
              <TabsContent
                key={tab}
                value={tab}
                className="flex-1 flex gap-4 min-h-0 pt-4 mt-0"
              >
                <div className="w-64 shrink-0 flex flex-col gap-1 overflow-y-auto pr-2 custom-scrollbar border-r">
                  {Object.keys(terraformFiles)
                    .filter((f) => {
                      if (tab === "logging")
                        return (
                          f === "logging_service.tf" ||
                          f === "log_format.vcl" ||
                          f.startsWith("capture_snippets/")
                        );
                      if (tab === "cdn")
                        return (
                          f === "fos.tf" ||
                          f === "cdn_proxy.tf" ||
                          f === "cdn_proxy.vcl" ||
                          f.startsWith("cdn_snippets/")
                        );
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
                            : "hover:bg-muted text-muted-foreground",
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
                          terraformFiles[selectedTfFile],
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
  );
}
