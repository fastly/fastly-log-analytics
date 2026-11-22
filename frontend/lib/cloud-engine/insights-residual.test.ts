// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { describe, expect, it } from "vitest";
import { decodeRollup, mergeRollups, type Rollup, type RollupAgg } from "./dashboard/rollup-query";
import { loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { INSIGHTS_RESIDUAL, INSIGHTS_RESIDUAL_FIELDS, rustF64 } from "./insights-residual";
import { INSIGHTS_SUMMARY_SPEC } from "./insights-rollup";

const GROUPS = [["ins_url", "url"], ["ins_ip", "ip"], ["ins_lat", "city"], ["ins_lat", "pop"], ["ins_asn", "asn"], ["urls", "url"]];

const sums = (values: Map<string, RollupAgg> | undefined) =>
  Object.fromEntries(
    [...(values ?? [])].map(([value, a]) => [
      value,
      [a.requests, a.errors, a.count, a.status5xx, a.hitRequests, a.missRequests, a.passRequests, a.elapsedCount,
        a.hists.elapsed?.toBytes()?.join(",") ?? null],
    ]),
  );

/** What the /insights rollup reads, with the columns the residual fills. */
function insightsView(rollup: Rollup) {
  const pick = (values: Map<string, RollupAgg> | undefined, keys: (keyof RollupAgg)[]) =>
    Object.fromEntries([...(values ?? [])].map(([v, a]) => [v, keys.map((k) => a[k])]));
  return {
    city: pick(rollup.dims.get("city"), ["requests", "errors"]),
    overall: Object.fromEntries(
      [...(rollup.series.get("overall")?.get("") ?? [])].map(([t, a]) => [t, a.requests]),
    ),
    fields: Object.fromEntries(
      INSIGHTS_RESIDUAL_FIELDS.map((field) => [field, Object.fromEntries(rollup.fields.get(field) ?? [])]),
    ),
    groups: Object.fromEntries(
      GROUPS.map(([group, field]) => [`${group}/${field}`, sums(rollup.groups?.get(group)?.get(field))]),
    ),
    present: GROUPS.map(([group, field]) => rollup.present?.has(`${group}/${field}`)),
  };
}

async function residualOf(buffers: ArrayBuffer[], fromMs = -Infinity, toMs = Infinity) {
  const into = INSIGHTS_RESIDUAL.accumulate(fromMs, toMs);
  for (const buffer of buffers) await into.add(buffer);
  return into.finish();
}

describe("INSIGHTS_RESIDUAL", () => {
  for (const dir of ["network-summaries", "origin-summaries", "security-value-summaries"]) {
    it(`rolls up raw windows as summary.rs does (${dir})`, async () => {
      const { buffers, files } = loadSummaryFixtures(dir);
      const raw = files("raw");
      const summaries = files("window");
      expect(raw.length).toBeGreaterThan(0);
      for (const file of raw) {
        const summary = summaries.find((s) => s.start === file.start && s.end === file.end)!;
        const decoded = (await decodeRollup(buffers[summary.key], 300_000, INSIGHTS_SUMMARY_SPEC))!;
        for (const [group, field] of GROUPS) expect(decoded.truncated.has(`${group}/${field}`)).toBe(false);
        expect(insightsView(await residualOf([buffers[file.key]]))).toEqual(insightsView(decoded));
      }
    });
  }

  it("counts 404s and auth-path denials per IP as ins_ip does", async () => {
    const rows: [string | null, number | null, string | null][] = [
      ["1.1.1.1", 404, "/x"],
      ["1.1.1.1", 403, "/API/OAuth/cb"],
      ["1.1.1.1", 401, "/public"],
      ["1.1.1.1", 403, ""],
      ["2.2.2.2", 401, "/wp/XMLRPC.php"],
      ["", 404, "/x"],
      [null, 404, "/x"],
      ["3.3.3.3", null, "/login"],
      ["3.3.3.3", 200, "/login"],
    ];
    const file = parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: rows.map(() => new Date(1_000)) },
        { name: "ip", type: "STRING", data: rows.map((r) => r[0]) },
        { name: "status", type: "INT64", data: rows.map((r) => (r[1] === null ? null : BigInt(r[1]))) },
        { name: "url", type: "STRING", data: rows.map((r) => r[2]) },
      ],
    });
    const ips = (await residualOf([file])).groups!.get("ins_ip")!.get("ip")!;
    expect(Object.fromEntries([...ips].map(([ip, a]) => [ip, [a.requests, a.errors, a.count]]))).toEqual({
      "1.1.1.1": [2, 1, 1],
      "2.2.2.2": [1, 1, 0],
    });
  });

  it("counts only rows in the range, adding up across a split", async () => {
    const { buffers, files } = loadSummaryFixtures("network-summaries");
    const raw = files("raw").map((file) => buffers[file.key]);
    const cut = Date.parse(files("raw")[2].start) + 7 * 60_000;
    const whole = await residualOf(raw);
    const [head, tail] = await Promise.all([residualOf(raw, -Infinity, cut), residualOf(raw, cut, Infinity)]);
    const total = (r: Rollup) => [...r.series.get("overall")!.get("")!.values()].reduce((n, a) => n + a.requests, 0);
    expect(total(head)).toBeGreaterThan(0);
    expect(total(tail)).toBeGreaterThan(0);
    expect(total(head) + total(tail)).toBe(total(whole));
    expect(insightsView(mergeRollups([head, tail]))).toEqual(insightsView(whole));
  });

  it("renders floats as Rust's f64 Display", () => {
    expect([3, 0.1, 1e21, 1.5e-7, -2.5e22, 123.456].map(rustF64)).toEqual([
      "3", "0.1", "1000000000000000000000", "0.00000015", "-25000000000000000000000", "123.456",
    ]);
  });
});
