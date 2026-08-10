"use client";

import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { RefreshCw, TriangleAlert } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { LabelWithInfo } from "@/components/ui/label-with-info";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { client } from "@/lib/api";
import type { components } from "@/types/api.generated";

type RumVersionsResponse = components["schemas"]["RumVersionsResponse"];

interface RumVersionPickerProps {
  serviceId: string;
  value: string | null;
  onChange: (version: string | null) => void;
}

const INFO_TEXT =
  "The self-hosted Grafana Faro Web SDK build served from your Object Storage bucket. Pin a version to control exactly which RUM client ships to real users; leave it unpinned and the service self-hosts the default vetted version instead.";

/**
 * RUM version picker shown in StorageStep when rum_enabled is true.
 *
 * The npm registry backing this list is a third party and WILL be down
 * sometimes (backend surfaces that as a 503) — this component degrades to
 * an inline message + manual retry rather than blocking the wizard. Leaving
 * faro_version unpinned still self-hosts: the backend resolves it to
 * DEFAULT_FARO_VERSION, so this degraded path never leaves a service
 * without a bundle behind /js/faro-sdk.js (there is no CDN fallback).
 */
export function RumVersionPicker({ serviceId, value, onChange }: RumVersionPickerProps) {
  const { data, isLoading, isError, refetch, isFetching } = useQuery<RumVersionsResponse>({
    queryKey: ["rum-versions", serviceId],
    queryFn: async ({ signal }) => {
      const { data, response } = await client.GET(
        "/api/services/{service_id}/rum/versions",
        { params: { path: { service_id: serviceId } }, signal },
      );
      if (!response.ok) throw new Error(`status ${response.status}`);
      return data as RumVersionsResponse;
    },
    enabled: !!serviceId,
    retry: false,
    staleTime: Infinity,
  });

  const available = data?.available ?? [];
  const latest = data?.latest ?? null;
  const current = data?.current ?? null;
  const degraded = isError || (!isLoading && available.length === 0);

  // Seed the wizard's default once the catalog resolves: a fresh service
  // should come up pinned to the newest stable release rather than forcing
  // the operator to pick before they even know what's available. Only
  // fires while the field is still at its untouched default (null) so it
  // never clobbers an explicit choice.
  React.useEffect(() => {
    if (latest && value === null) {
      onChange(latest);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- seed-once on latest arrival, not on every value/onChange identity change
  }, [latest]);

  if (isLoading) {
    return (
      <div className="space-y-1.5">
        <LabelWithInfo label="Faro Web SDK Version" info={INFO_TEXT} />
        <Skeleton className="h-8 w-full" />
      </div>
    );
  }

  if (degraded) {
    return (
      <div className="space-y-1.5">
        <LabelWithInfo label="Faro Web SDK Version" info={INFO_TEXT} />
        <Alert variant="destructive">
          <TriangleAlert />
          <AlertTitle>
            {isError ? "Couldn't reach the npm registry" : "No versions available"}
          </AlertTitle>
          <AlertDescription className="flex items-center justify-between gap-2">
            <span>
              RUM will provision using the default self-hosted version — you can pin a
              specific one later from the service&apos;s RUM settings.
            </span>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-7 text-xs shrink-0"
              onClick={() => refetch()}
              disabled={isFetching}
            >
              <RefreshCw className={cn("h-3 w-3 mr-1", isFetching && "animate-spin")} />
              {isFetching ? "Retrying…" : "Retry"}
            </Button>
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  return (
    <div className="space-y-1.5">
      <LabelWithInfo label="Faro Web SDK Version" info={INFO_TEXT} />
      <Select value={value ?? undefined} onValueChange={(v) => onChange(v)}>
        <SelectTrigger className="h-8 text-xs w-full" aria-label="Faro Web SDK Version">
          <SelectValue placeholder="Select a version" />
        </SelectTrigger>
        <SelectContent>
          {available.map((v) => {
            const tags = [v === latest && "latest", v === current && "current"].filter(Boolean);
            return (
              <SelectItem key={v} value={v} className="text-xs">
                {v}
                {tags.length > 0 ? ` (${tags.join(", ")})` : ""}
              </SelectItem>
            );
          })}
        </SelectContent>
      </Select>
    </div>
  );
}
