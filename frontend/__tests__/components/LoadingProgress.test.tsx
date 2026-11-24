import { act, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { CloudLoadingProgress, LoadingProgress } from "@/components/LoadingProgress";
import { beginCloudLoading, cloudLoadingStore } from "@/lib/cloud-engine/loading-progress";

afterEach(() => cloudLoadingStore.reset());

describe("LoadingProgress", () => {
  it("shows discovery stages and reactive file counts, bytes and aggregation", () => {
    render(<CloudLoadingProgress />);
    expect(screen.queryByRole("region")).not.toBeInTheDocument();
    let job!: ReturnType<typeof beginCloudLoading>;
    act(() => { job = beginCloudLoading("session"); });
    expect(screen.getByText("Checking your session")).toBeInTheDocument();
    act(() => job.update({ stage: "range" }));
    expect(screen.getByText("Log range").closest("li")).toHaveAttribute("aria-current", "step");
    act(() => job.update({ stage: "listing" }));
    expect(screen.getByText("Checking log coverage")).toBeInTheDocument();
    act(() => job.update({
      stage: "files",
      progress: { phase: "files", cached: 1, toFetch: 3, toDecode: 1, done: 2, downloadedBytes: 2048, cachedBytes: 1024 },
    }));
    expect(screen.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "50");
    expect(screen.getByText("2 of 4 files")).toBeInTheDocument();
    expect(screen.getByText("2 KB downloaded · 1 KB from cache")).toBeInTheDocument();
    act(() => job.update({
      progress: { phase: "aggregate", cached: 1, toFetch: 3, toDecode: 1, done: 4, downloadedBytes: 2048, cachedBytes: 1024 },
      refreshing: true,
    }));
    expect(screen.getByText("Preparing dashboard results")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
    expect(screen.getByText("Your previous results stay visible while this finishes.")).toBeInTheDocument();
    act(() => job.finish());
    expect(screen.queryByRole("region")).not.toBeInTheDocument();
  });

  it("removes failed work without hiding a concurrent run", () => {
    render(<CloudLoadingProgress />);
    let first!: ReturnType<typeof beginCloudLoading>;
    let second!: ReturnType<typeof beginCloudLoading>;
    act(() => {
      first = beginCloudLoading("listing");
      second = beginCloudLoading("files");
      first.finish(false);
    });
    expect(screen.getByText("Preparing local analysis")).toBeInTheDocument();
    act(() => second.finish(false));
    expect(screen.queryByRole("region")).not.toBeInTheDocument();
  });

  it("says when range reads are not included in the byte totals", () => {
    render(<LoadingProgress stage="files" progress={{ phase: "files", cached: 0, toFetch: 502, toDecode: 0, done: 109, downloadedBytes: 0, cachedBytes: 0, rangedFiles: 109 }} />);
    expect(screen.getByText("109 of 502 files")).toBeInTheDocument();
    expect(screen.getByText("0 B downloaded · 0 B from cache · 109 files read in ranges (bytes not counted)")).toBeInTheDocument();
  });

  it("keeps zero-file cache checks indeterminate", () => {
    render(<LoadingProgress stage="files" progress={{ phase: "cache", cached: 0, toFetch: 0, toDecode: 0, done: 0, downloadedBytes: 0, cachedBytes: 0 }} />);
    expect(screen.getByText("Checking cached data")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();
  });
});
