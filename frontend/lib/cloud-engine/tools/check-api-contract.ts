import ts from "typescript";

export type Contract = { paths: Record<string, Record<string, unknown>> };
export type OpenApiContract = Contract;
export const INGEST_SHAPE_MODULE = "usage/ingestShape";

export function undeclaredApiCalls(
  source: string,
  contract: Contract,
): string[] {
  const file = ts.createSourceFile(
    "frontend.tsx",
    source,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TSX,
  );
  const errors = new Set<string>();
  const visit = (node: ts.Node) => {
    if (
      ts.isStringLiteralLike(node) &&
      node.text.startsWith("/api/") &&
      !contract.paths[node.text]
    ) {
      errors.add(node.text);
    }
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression)
    ) {
      const method = node.expression.name.text.toLowerCase();
      const first = node.arguments[0];
      if (
        ["get", "post", "put", "patch", "delete"].includes(method) &&
        first &&
        ts.isStringLiteralLike(first) &&
        first.text.startsWith("/api/") &&
        !contract.paths[first.text]?.[method]
      ) {
        errors.add(`${method.toUpperCase()} ${first.text}`);
      }
    }
    if (
      ts.isNewExpression(node) &&
      node.expression.getText(file) === "EventSource"
    )
      errors.add("EventSource");
    ts.forEachChild(node, visit);
  };
  visit(file);
  return [...errors];
}
