import { describe, expect, it } from "vitest";
import { createFetchLanes, LARGE_WHOLE_MIN_BYTES, rawLaneForBytes } from "./fetch-lanes";

function recorder() {
  const started: string[] = [];
  const open = new Map<string, () => void>();
  const task = (name: string) => () => {
    started.push(name);
    return new Promise<string>((resolve) => open.set(name, () => resolve(name)));
  };
  const finish = (name: string) => {
    open.get(name)!();
    open.delete(name);
  };
  return { started, open, task, finish };
}

const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

describe("fetch lanes", () => {
  it("starts every summary range before the first raw range queued with it", async () => {
    const lanes = createFetchLanes({ summaryLimit: 16, rawWhileSummary: 10 });
    const { started, task } = recorder();
    for (let i = 0; i < 20; i++) void lanes.run("raw", task(`raw${i}`));
    for (let i = 0; i < 5; i++) void lanes.run("summary", task(`summary${i}`));
    await settle();
    const firstRaw = started.findIndex((name) => name.startsWith("raw"));
    expect(started.slice(0, 5)).toEqual(["summary0", "summary1", "summary2", "summary3", "summary4"]);
    expect(firstRaw).toBe(5);
  });

  it("holds raw ranges to their reserved share while a summary read is pending, then frees them", async () => {
    const lanes = createFetchLanes({ summaryLimit: 16, rawWhileSummary: 10 });
    const { started, open, task, finish } = recorder();
    const summary = lanes.run("summary", task("summary"));
    for (let i = 0; i < 20; i++) void lanes.run("raw", task(`raw${i}`));
    await settle();
    expect(started.filter((name) => name.startsWith("raw"))).toHaveLength(10);
    finish("summary");
    await expect(summary).resolves.toBe("summary");
    await settle();
    expect(started.filter((name) => name.startsWith("raw"))).toHaveLength(20);
    for (const name of [...open.keys()]) finish(name);
  });

  it("caps summary reads and passes a failure through without leaking its lane", async () => {
    const lanes = createFetchLanes({ summaryLimit: 2, rawWhileSummary: 1 });
    const { started, task, finish } = recorder();
    const failing = lanes.run("summary", () => Promise.reject(new Error("boom")));
    const a = lanes.run("summary", task("a"));
    const b = lanes.run("summary", task("b"));
    await expect(failing).rejects.toThrow("boom");
    await settle();
    expect(started).toEqual(["a", "b"]);
    finish("a");
    finish("b");
    await expect(Promise.all([a, b])).resolves.toEqual(["a", "b"]);
  });

  it("holds large whole-file reads behind active summaries and caps large concurrency at 2 by default", async () => {
    const lanes = createFetchLanes();
    const { started, open, task, finish } = recorder();
    const summary = lanes.run("summary", task("summary0"));
    for (let i = 0; i < 6; i++) void lanes.run("raw", task(`raw${i}`));
    for (let i = 0; i < 5; i++) void lanes.run("large", task(`large${i}`));
    await settle();
    expect(started.filter((n) => n.startsWith("summary"))).toHaveLength(1);
    expect(started.filter((n) => n.startsWith("raw"))).toHaveLength(4);
    expect(started.filter((n) => n.startsWith("large"))).toHaveLength(0);

    finish("summary0");
    await expect(summary).resolves.toBe("summary0");
    await settle();
    expect(started.filter((n) => n.startsWith("raw"))).toHaveLength(6);
    expect(started.filter((n) => n.startsWith("large"))).toHaveLength(2);

    finish("large0");
    await settle();
    expect(started.filter((n) => n.startsWith("large"))).toHaveLength(3);

    for (const name of [...open.keys()]) finish(name);
    await settle();
    for (const name of [...open.keys()]) finish(name);
    await settle();
    expect(started.filter((n) => n.startsWith("large"))).toHaveLength(5);
    for (const name of [...open.keys()]) finish(name);
  });

  it("aborts a queued waiter without leaking its lane and wakes dependent lanes", async () => {
    const lanes = createFetchLanes({ summaryLimit: 1, rawWhileSummary: 0, largeLimit: 2, largeWhileSummary: 0 });
    const { started, open, task, finish } = recorder();
    const s0 = lanes.run("summary", task("s0"));
    const ac = new AbortController();
    const s1 = lanes.run("summary", task("s1"), ac.signal);
    const large = lanes.run("large", task("large0"));
    await settle();
    expect(started).toEqual(["s0"]);

    ac.abort(new Error("cancelled"));
    await expect(s1).rejects.toThrow("cancelled");
    finish("s0");
    await expect(s0).resolves.toBe("s0");
    await settle();
    expect(started).toEqual(["s0", "large0"]);
    finish("large0");
    await expect(large).resolves.toBe("large0");
    for (const name of [...open.keys()]) finish(name);
  });

  it("classifies files strictly above LARGE_WHOLE_MIN_BYTES into the large lane", () => {
    expect(rawLaneForBytes(LARGE_WHOLE_MIN_BYTES)).toBe("raw");
    expect(rawLaneForBytes(LARGE_WHOLE_MIN_BYTES + 1)).toBe("large");
  });
});
