import { beforeEach, describe, expect, it, vi } from "vitest";
import { beginCloudLoading, cloudLoadingStore } from "./loading-progress";
import { setCloudModeRuntime } from "./mode";

const progress = {
  phase: "files" as const, cached: 1, toFetch: 3, toDecode: 1, done: 2,
  downloadedBytes: 1024, cachedBytes: 512,
};

beforeEach(() => cloudLoadingStore.reset());

describe("cloud loading progress", () => {
  it("publishes discovery transitions and keeps the server snapshot empty", () => {
    const changed = vi.fn();
    const unsubscribe = cloudLoadingStore.subscribe(changed);
    const job = beginCloudLoading("session");
    job.update({ stage: "range" });
    expect(cloudLoadingStore.getSnapshot()?.stage).toBe("range");
    job.update({ stage: "listing" });
    expect(cloudLoadingStore.getSnapshot()?.stage).toBe("listing");
    expect(cloudLoadingStore.getServerSnapshot()).toBeNull();
    job.finish();
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
    expect(changed).toHaveBeenCalledTimes(4);
    unsubscribe();
  });

  it("aggregates concurrent jobs without clearing or resurrecting another run", () => {
    const first = beginCloudLoading("files");
    const second = beginCloudLoading("files");
    first.update({ progress });
    second.update({ progress });
    expect(cloudLoadingStore.getSnapshot()?.progress).toMatchObject({
      toFetch: 6, toDecode: 2, done: 4, downloadedBytes: 2048, cachedBytes: 1024,
    });
    first.finish(false);
    first.update({ stage: "session" });
    first.finish();
    expect(cloudLoadingStore.getSnapshot()?.progress).toEqual(progress);
    second.finish();
    expect(cloudLoadingStore.getSnapshot()).toBeNull();
  });

  it("sums unmeasured range reads across concurrent jobs", () => {
    beginCloudLoading("files").update({ progress: { ...progress, rangedFiles: 2 } });
    beginCloudLoading("files").update({ progress });
    beginCloudLoading("files").update({ progress: { ...progress, rangedFiles: 3 } });
    expect(cloudLoadingStore.getSnapshot()?.progress?.rangedFiles).toBe(5);
  });

  it("marks subsequent analysis as refreshing only after success", () => {
    beginCloudLoading("files").finish(false);
    const initial = beginCloudLoading("files");
    expect(cloudLoadingStore.getSnapshot()?.refreshing).toBe(false);
    initial.finish();
    const refresh = beginCloudLoading("files");
    expect(cloudLoadingStore.getSnapshot()?.refreshing).toBe(true);
    cloudLoadingStore.reset();
    const nextSession = beginCloudLoading("session");
    refresh.finish();
    expect(cloudLoadingStore.getSnapshot()?.stage).toBe("session");
    nextSession.finish(false);
  });

  it("publishes each loading change for readiness diagnostics in Cloud mode", () => {
    setCloudModeRuntime(true);
    const events: unknown[] = [];
    const listener = (event: Event) => events.push((event as CustomEvent).detail);
    window.addEventListener("fla:cloud-loading", listener);
    try {
      const job = beginCloudLoading("files");
      job.update({ progress });
      job.finish();
      expect(events).toEqual([
        { stage: "files", jobs: 1, progress: null },
        { stage: "files", jobs: 1, progress: expect.objectContaining({ phase: "files", done: 2, toFetch: 3 }) },
        { stage: null, jobs: 0, progress: null },
      ]);
    } finally {
      window.removeEventListener("fla:cloud-loading", listener);
      setCloudModeRuntime(false);
    }
  });
});
