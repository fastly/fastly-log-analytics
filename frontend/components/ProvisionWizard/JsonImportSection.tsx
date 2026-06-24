"use client";

import React, { useState } from "react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { FileJson } from "lucide-react";

export interface JsonImportSectionProps {
  onImport: (parsed: Record<string, string>) => void;
}

export function JsonImportSection({ onImport }: JsonImportSectionProps) {
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
      {/* a11y: was a <div onClick> — invisible to the keyboard. Native
          button with aria-expanded/aria-controls so SR users hear the
          collapse state and can Tab to it like any disclosure. */}
      <button
        type="button"
        aria-expanded={open}
        aria-controls="json-import-body"
        className="w-full flex items-center justify-between select-none focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring rounded-sm"
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
      </button>
      {open && (
        <div id="json-import-body" className="space-y-3 animate-in fade-in slide-in-from-top-2 duration-200">
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
