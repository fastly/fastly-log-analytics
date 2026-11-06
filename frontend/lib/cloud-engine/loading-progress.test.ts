import { beforeEach, describe, expect, it, vi } from "vitest";
import { beginCloudLoading, cloudLoadingStore } from "./loading-progress";

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
});
