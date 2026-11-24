import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const frontendRoot = join(__dirname, "../../..");
const computeCloudSrc = join(frontendRoot, "../compute/cloud/src");
const read = (rel: string) => readFileSync(join(frontendRoot, rel), "utf8");

describe("analyst and admin page access", () => {
  it("keeps Data Management visible to analysts while operational pages stay admin-only", () => {
    const chrome = read("components/AppLayoutChrome.tsx");
    expect(chrome).toMatch(
      /name:\s*['"]Data Management['"],\s*href:\s*['"]\/logs['"][^\n]*analystVisible:\s*true/,
    );
    expect(chrome).toMatch(
      /name:\s*['"]Usage & Cost['"],\s*href:\s*['"]\/usage['"][^\n]*analystVisible:\s*false/,
    );
    expect(chrome).toContain("SYSTEM_NAVIGATION");
  });

  it("redacts user and invite ids in Compute request.end logs and maps analyst-no-pii to analyst_masked", () => {
    const main = readFileSync(join(computeCloudSrc, "main.rs"), "utf8");
    expect(main).toContain("fn safe_path");
    expect(main).toContain('"/api/admin/users/:id"');
    expect(main).toContain('"/api/admin/invites/:id"');

    const auth = readFileSync(join(computeCloudSrc, "auth/mod.rs"), "utf8");
    expect(auth).toContain('"analyst-no-pii"');
    expect(auth).toContain('Self::AnalystNoPii => "analyst_masked"');
  });
});
