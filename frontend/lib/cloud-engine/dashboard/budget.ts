/** What a refused range needed against its limit, in bytes or files. */
export type CloudBudgetDetail = { unit: "bytes" | "files"; needed: number; limit: number };

/** A range the browser won't scan: retrying the same request fails the same way. */
export class CloudBudgetError extends Error {
  readonly detail?: CloudBudgetDetail;

  constructor(message: string, detail?: CloudBudgetDetail) {
    super(message);
    this.detail = detail;
  }
}

/** Compressed Parquet bytes DuckDB may hold registered at once. */
export const MAX_REGISTERED_BYTES = 64 * 1024 * 1024;
