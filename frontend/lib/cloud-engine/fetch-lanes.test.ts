import { describe, expect, it } from "vitest";
import { createFetchLanes } from "./fetch-lanes";

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
});
